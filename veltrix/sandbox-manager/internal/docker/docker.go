// internal/docker/docker.go — Docker client wrapper using fsouza/go-dockerclient.
// Uses the properly-modular docker client library that builds cleanly on Linux.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	docker "github.com/fsouza/go-dockerclient"
)

const (
	// BuildTimeout caps how long a contestant's Docker image build may take.
	// Prevents a slow/infinite apt-get or cargo build from holding a worker forever.
	BuildTimeout = 10 * time.Minute

	// StartupTimeout is how long we poll for port 9999 to open.
	StartupTimeout = 15 * time.Second

	// SandboxPort is the port every contestant server must bind to.
	SandboxPort = 9999
)

// Client wraps the Docker client.
type Client struct {
	cli    *docker.Client
	logger *log.Logger
}

// New creates a Docker client connected to the local Docker daemon socket.
func New(logger *log.Logger) (*Client, error) {
	cli, err := docker.NewVersionedClientFromEnv("1.41")
	if err != nil {
		return nil, fmt.Errorf("docker client init: %w", err)
	}
	if err := cli.Ping(); err != nil {
		return nil, fmt.Errorf("docker ping: %w", err)
	}
	return &Client{cli: cli, logger: logger}, nil
}

// SandboxResult holds the outcome of starting a sandbox container.
type SandboxResult struct {
	ContainerID string
	EndpointURL string
	TargetHost  string
}

// BuildImage builds a Docker image from buildContextDir using a rendered Dockerfile
// for the given contestant language. A per-call context enforces BuildTimeout.
func (c *Client) BuildImage(ctx context.Context, buildContextDir, imageTag, language string) error {
	buildCtx, cancel := context.WithTimeout(ctx, BuildTimeout)
	defer cancel()

	// Write the generated Dockerfile into the build context directory.
	dockerfile := renderDockerfile(language)
	if err := os.WriteFile(buildContextDir+"/Dockerfile", []byte(dockerfile), 0o644); err != nil {
		return fmt.Errorf("write dockerfile: %w", err)
	}

	var buildOutput bytes.Buffer
	err := c.cli.BuildImage(docker.BuildImageOptions{
		Name:           imageTag,
		ContextDir:     buildContextDir,
		Dockerfile:     "Dockerfile",
		RmTmpContainer: true,
		OutputStream:   &buildOutput,
		Context:        buildCtx,
	})
	if err != nil {
		// Include the last 512 chars of build output for diagnostics.
		out := buildOutput.String()
		if len(out) > 512 {
			out = "…" + out[len(out)-512:]
		}
		return fmt.Errorf("docker build: %w\noutput: %s", err, out)
	}

	// Scan the output stream for explicit Docker build error lines.
	return scanBuildOutput(bytes.NewReader(buildOutput.Bytes()))
}

// RunSandbox creates and starts a contestant container with strict resource limits,
// then polls until the sandbox's port 9999 is accepting connections.
func (c *Client) RunSandbox(ctx context.Context, imageTag, submissionID, sandboxNetwork string) (*SandboxResult, error) {
	containerName := "sandbox-" + submissionID

	// Remove any stale container with the same name (idempotent).
	_ = c.cli.RemoveContainer(docker.RemoveContainerOptions{
		ID: containerName, Force: true, RemoveVolumes: false,
	})

	// 512MB RAM, 1 CPU core (1e9 nanocpus), 1000 PID limit.
	mem := int64(512 * 1024 * 1024)
	nano := int64(1_000_000_000)
	pidsLimit := int64(1000)

	container, err := c.cli.CreateContainer(docker.CreateContainerOptions{
		Name:    containerName,
		Context: ctx,
		Config:  &docker.Config{Image: imageTag},
		HostConfig: &docker.HostConfig{
			NetworkMode: sandboxNetwork,
			Memory:      mem,
			NanoCPUs:    nano,
			PidsLimit:   &pidsLimit,
			CapDrop:     []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges:true"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("container create: %w", err)
	}

	if err := c.cli.StartContainer(container.ID, nil); err != nil {
		return nil, fmt.Errorf("container start: %w", err)
	}

	// Brief pause before the first inspect so the runtime can initialise.
	time.Sleep(1 * time.Second)

	inspect, err := c.cli.InspectContainerWithContext(container.ID, ctx)
	if err != nil {
		return nil, fmt.Errorf("container inspect: %w", err)
	}
	if !inspect.State.Running {
		return nil, fmt.Errorf("container exited immediately with code %d", inspect.State.ExitCode)
	}

	// Resolve the container's IP on the sandbox network (fall back to DNS name).
	targetHost := containerName
	if nets := inspect.NetworkSettings.Networks; nets != nil {
		if n, ok := nets[sandboxNetwork]; ok && n.IPAddress != "" {
			targetHost = n.IPAddress
		}
	}

	// Poll until port 9999 opens.
	if !waitForPort(targetHost, SandboxPort, StartupTimeout) {
		_ = c.cli.KillContainer(docker.KillContainerOptions{ID: container.ID, Signal: docker.SIGKILL})
		_ = c.cli.RemoveContainer(docker.RemoveContainerOptions{ID: container.ID, Force: true})
		return nil, fmt.Errorf("sandbox did not bind to port %d within %s", SandboxPort, StartupTimeout)
	}

	return &SandboxResult{
		ContainerID: container.ID,
		EndpointURL: fmt.Sprintf("http://%s:%d", containerName, SandboxPort),
		TargetHost:  targetHost,
	}, nil
}

// RemoveContainer stops and removes a container and its associated image.
func (c *Client) RemoveContainer(ctx context.Context, containerID, imageTag string) {
	timeout := uint(5)
	if err := c.cli.StopContainer(containerID, timeout); err != nil {
		c.logger.Printf("[docker] stop %s: %v", containerID[:min(12, len(containerID))], err)
	}
	if err := c.cli.RemoveContainer(docker.RemoveContainerOptions{
		ID: containerID, Force: true,
	}); err != nil {
		c.logger.Printf("[docker] remove container %s: %v", containerID[:min(12, len(containerID))], err)
	}
	if imageTag != "" {
		if err := c.cli.RemoveImage(imageTag); err != nil {
			c.logger.Printf("[docker] remove image %s: %v", imageTag, err)
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func waitForPort(host string, port int, timeout time.Duration) bool {
	addr := fmt.Sprintf("%s:%d", host, port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func scanBuildOutput(r io.Reader) error {
	dec := json.NewDecoder(r)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			// Non-JSON output from the daemon — ignore decode errors.
			break
		}
		if msg.Error != "" {
			return fmt.Errorf("docker build error: %s", msg.Error)
		}
	}
	return nil
}

func renderDockerfile(language string) string {
	switch language {
	case "cpp":
		return `FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ cmake make libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY src/ /app/
RUN cmake -S . -B build \
    && cmake --build build --parallel $(nproc) \
    && test -f build/server \
    || (echo "ERROR: CMake build must produce a binary named 'server'" >&2 && exit 1)
EXPOSE 9999
CMD ["./build/server"]
`
	case "rust":
		return `FROM rust:1.78-slim
WORKDIR /app
COPY src/ /app/
RUN cargo build --release \
    && test -f target/release/server \
    || (echo "ERROR: Cargo build must produce a binary named 'server'" >&2 && exit 1)
EXPOSE 9999
CMD ["./target/release/server"]
`
	case "go":
		return `FROM golang:1.22-bookworm
WORKDIR /app
COPY src/ /app/
RUN go build -o server . \
    && test -f server \
    || (echo "ERROR: Go build must produce a binary named 'server'" >&2 && exit 1)
EXPOSE 9999
CMD ["./server"]
`
	default:
		return ""
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
