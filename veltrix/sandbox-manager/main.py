# sandbox-manager/main.py
import os, time, uuid, tarfile, tempfile, docker, redis, asyncpg, asyncio, boto3, socket, zipfile, shutil, stat
import httpx
import threading
import http.server
import json
from pathlib import PurePosixPath
from docker.errors import NotFound, APIError
from pydantic_settings import BaseSettings, SettingsConfigDict

print("Sandbox Manager booting up...", flush=True)

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int = 5432
    redis_host: str
    redis_port: int = 6379
    minio_host: str
    minio_port: int = 9000
    minio_root_user: str
    minio_root_password: str
    minio_bucket: str
    sandbox_network: str = "sandbox-net"
    sandbox_host: str = "localhost"
    max_extract_size_mb: int = 200
    max_file_size_mb: int = 50
    max_file_count: int = 500
    fleet_commander_url: str = "http://bot-fleet:7070"
    # Benchmark defaults (can be overridden per-job)
    default_num_bots: int = 100
    default_duration_secs: int = 60
    sandbox_manager_health_port: int = 8081

cfg = Settings()
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        payload = {"status": "ok"}
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return

def start_health_server(port: int) -> None:
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[INFO] Health server listening on :{port}")


docker_client = docker.from_env()
redis_client  = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True)
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{cfg.minio_host}:{cfg.minio_port}",
    aws_access_key_id=cfg.minio_root_user,
    aws_secret_access_key=cfg.minio_root_password,
)

async def get_db():
    return await asyncpg.connect(
        user=cfg.postgres_user, password=cfg.postgres_password,
        database=cfg.postgres_db, host=cfg.postgres_host, port=cfg.postgres_port
    )

async def update_submission(db, submission_id, **fields):
    """Update the submissions table with the given fields."""
    set_clause = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
    values = list(fields.values())
    await db.execute(
        f"UPDATE submissions SET {set_clause}, updated_at = NOW() WHERE id = $1",
        submission_id, *values
    )

async def cleanup_sandbox_after_run(submission_id: str,
                                   container_id: str,
                                   image_tag: str | None,
                                   delay_secs: int) -> None:
    """Wait for the benchmark to finish, then stop and remove the container and image."""
    await asyncio.sleep(delay_secs)

    try:
        container = docker_client.containers.get(container_id)
    except NotFound:
        return

    try:
        exit_code = get_container_exit_code(container)
        if exit_code is None:
            container.stop(timeout=5)
    except APIError as e:
        print(f"[WARN] Failed to stop container {container_id}: {e}")

    try:
        container.remove(force=True)
        print(f"[INFO] Cleaned up sandbox container {container_id}")
    except APIError as e:
        print(f"[WARN] Failed to remove container {container_id}: {e}")

    if image_tag:
        try:
            docker_client.images.remove(image=image_tag, force=True)
        except APIError as e:
            print(f"[WARN] Failed to remove image {image_tag}: {e}")

    try:
        db = await get_db()
        row = await db.fetchrow(
            "SELECT status FROM submissions WHERE id = $1",
            submission_id
        )
        if row and row["status"] == "RUNNING":
            await update_submission(db, submission_id, status="SUCCESS")
    finally:
        try:
            await db.close()
        except Exception:
            pass

def _is_unsafe_path(path: str) -> bool:
    if os.path.isabs(path):
        return True
    parts = PurePosixPath(path).parts
    return ".." in parts

def _safe_join(base_dir: str, path: str) -> str:
    dest = os.path.abspath(os.path.join(base_dir, path))
    base = os.path.abspath(base_dir)
    if os.path.commonpath([base, dest]) != base:
        raise ValueError(f"Unsafe path detected: {path}")
    return dest

def _extract_zip(archive_path: str, dest_dir: str, max_total: int, max_file: int, max_files: int) -> None:
    with zipfile.ZipFile(archive_path) as zf:
        total = 0
        file_count = 0
        for info in zf.infolist():
            if info.is_dir():
                continue
            if _is_unsafe_path(info.filename):
                raise ValueError(f"Unsafe path in zip: {info.filename}")
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError(f"Symlinks not allowed in zip: {info.filename}")
            if info.file_size > max_file:
                raise ValueError(f"File too large in zip: {info.filename}")
            total += info.file_size
            file_count += 1
            if file_count > max_files:
                raise ValueError("Too many files in zip")
            if total > max_total:
                raise ValueError("Uncompressed size exceeds limit")

        for info in zf.infolist():
            if info.is_dir():
                os.makedirs(_safe_join(dest_dir, info.filename), exist_ok=True)
                continue
            dest_path = _safe_join(dest_dir, info.filename)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with zf.open(info) as src, open(dest_path, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)

def _extract_tar(archive_path: str, dest_dir: str, max_total: int, max_file: int, max_files: int) -> None:
    with tarfile.open(archive_path, "r:*") as tf:
        members = tf.getmembers()
        total = 0
        file_count = 0
        for member in members:
            if _is_unsafe_path(member.name):
                raise ValueError(f"Unsafe path in tar: {member.name}")
            if member.islnk() or member.issym():
                raise ValueError(f"Symlinks not allowed in tar: {member.name}")
            if member.isreg():
                if member.size > max_file:
                    raise ValueError(f"File too large in tar: {member.name}")
                total += member.size
                file_count += 1
                if file_count > max_files:
                    raise ValueError("Too many files in tar")
                if total > max_total:
                    raise ValueError("Uncompressed size exceeds limit")
            elif member.isdir():
                continue
            else:
                raise ValueError(f"Unsupported tar entry: {member.name}")

        for member in members:
            if member.isdir():
                os.makedirs(_safe_join(dest_dir, member.name), exist_ok=True)
            elif member.isreg():
                dest_path = _safe_join(dest_dir, member.name)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                src = tf.extractfile(member)
                if src is None:
                    raise ValueError(f"Failed to read tar entry: {member.name}")
                with src, open(dest_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)

def extract_archive(archive_path: str, dest_dir: str, max_total: int, max_file: int, max_files: int) -> None:
    if zipfile.is_zipfile(archive_path):
        _extract_zip(archive_path, dest_dir, max_total, max_file, max_files)
        return
    if tarfile.is_tarfile(archive_path):
        _extract_tar(archive_path, dest_dir, max_total, max_file, max_files)
        return
    raise ValueError("Unsupported archive format (expected .zip or .tar.gz)")

def render_dockerfile(language: str) -> str:
    if language == "cpp":
        return """FROM ubuntu:22.04
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
            """

    elif language == "rust":
        return """FROM rust:1.78-slim
            WORKDIR /app
            COPY src/ /app/
            RUN cargo build --release \
                && test -f target/release/server \
                || (echo "ERROR: Cargo build must produce a binary named 'server'" >&2 && exit 1)
            EXPOSE 9999
            CMD ["./target/release/server"]
            """

    elif language == "go":
        return """FROM golang:1.22-bookworm
            WORKDIR /app
            COPY src/ /app/
            RUN go build -o server . \
                && test -f server \
                || (echo "ERROR: Go build must produce a binary named 'server'" >&2 && exit 1)
            EXPOSE 9999
            CMD ["./server"]
            """

    else:
        raise ValueError(f"Unsupported language: {language}. Must be cpp, rust, or go.")

def build_sandbox_image(submission_id: str, storage_key: str, language: str) -> str:
    """Download code from MinIO, build a Docker image, return image tag."""
    image_tag = f"sandbox-{submission_id}"
    max_total = cfg.max_extract_size_mb * 1024 * 1024
    max_file = cfg.max_file_size_mb * 1024 * 1024
    max_files = cfg.max_file_count

    with tempfile.TemporaryDirectory() as tmpdir:
        code_path = os.path.join(tmpdir, "submission.archive")
        src_dir = os.path.join(tmpdir, "src")
        os.makedirs(src_dir, exist_ok=True)

        # Download from MinIO
        s3.download_file(cfg.minio_bucket, storage_key, code_path)

        # Extract the submission safely into src/
        extract_archive(code_path, src_dir, max_total, max_file, max_files)

        # Write a Dockerfile that compiles and serves the submission
        dockerfile = render_dockerfile(language)
        with open(os.path.join(tmpdir, "Dockerfile"), "w") as f:
            f.write(dockerfile)

        # Build the Docker image
        docker_client.images.build(path=tmpdir, tag=image_tag, rm=True)

    return image_tag

def run_sandbox(image_tag: str, submission_id: str) -> tuple[str, str, str, int]:
    """
    Run the container with strict resource limits.
    Returns (container_id, endpoint_url, target_host, target_port)
    """
    container_name = f"sandbox-{submission_id}"
    try:
        existing = docker_client.containers.get(container_name)
        existing.remove(force=True)
    except NotFound:
        pass

    container = docker_client.containers.run(
        image=image_tag,
        name=container_name,
        detach=True,
        network=cfg.sandbox_network,

        # ── Security: these are NON-NEGOTIABLE ──────────────────
        mem_limit="512m",           # max 512MB RAM
        nano_cpus=1_000_000_000,    # max 1 CPU core
        read_only=False,
        cap_drop=["ALL"],           # drop all Linux capabilities
        security_opt=["no-new-privileges:true"],
        network_disabled=False,     # needs network for bots to reach it

        # ── Resource limits ──────────────────────────────────────
        pids_limit=1000,            # max 1000 processes/threads
        # ports={"9999/tcp": None},   # random host port assigned by Docker
    )

    # Wait for container to start and capture its sandbox-net IP
    time.sleep(2)
    container.reload()

    target_port = 9999
    endpoint_url = f"http://{container_name}:{target_port}"

    sandbox_ip = ""
    networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
    if cfg.sandbox_network in networks:
        sandbox_ip = networks[cfg.sandbox_network].get("IPAddress", "")

    target_host = sandbox_ip if sandbox_ip else container_name

    return container.id, endpoint_url, target_host, target_port
    # container.reload()
    # port_info = container.ports.get("9999/tcp")
    # if not port_info:
    #     raise RuntimeError("Port mapping not assigned")
    # host_port = int(port_info[0]["HostPort"])
    # endpoint_url = f"http://{cfg.sandbox_host}:{host_port}"

    # return container.id, endpoint_url, host_port

# ── Constants ─────────────────────────────────────────────────────
STARTUP_TIMEOUT_SECS = 15       # how long to wait for port to open
STARTUP_POLL_INTERVAL = 0.5     # check every 500ms

# ── Helpers ───────────────────────────────────────────────────────

def is_port_open(host: str, port: int) -> bool:
    """Check if the container's port is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False

def wait_for_startup(host: str, port: int, timeout: int) -> bool:
    """
    Poll the container port until it opens or timeout is reached.
    Returns True if port opened, False if timed out.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_port_open(host, port):
            return True
        time.sleep(STARTUP_POLL_INTERVAL)
    return False

def get_container_exit_code(container) -> int | None:
    """Reload container state and return exit code if stopped."""
    try:
        container.reload()
        if container.status in ("exited", "dead"):
            return container.attrs["State"]["ExitCode"]
    except Exception:
        pass
    return None

def map_exit_code_to_status(exit_code: int) -> str:
    """
    Map Docker exit codes to our leaderboard failure states.

    Exit Code 0   → clean exit (shouldn't happen mid-benchmark)
    Exit Code 1   → runtime/logic error (FAILED_LOGIC)
    Exit Code 137 → OOM kill, SIGKILL (FAILED_RESOURCE)
    Exit Code 139 → segfault (FAILED_LOGIC)
    Anything else → treat as FAILED_LOGIC
    """
    mapping = {
        1:   "FAILED_LOGIC",
        139: "FAILED_LOGIC",     # segfault — code bug
        137: "FAILED_RESOURCE",  # OOM killed by kernel
        143: "FAILED_RESOURCE",  # SIGTERM → container resource limit
    }
    return mapping.get(exit_code, "FAILED_LOGIC")

async def trigger_fleet_commander(submission_id: str, host: str, port: str,
                                   num_bots: int, duration_secs: int):
    """
    POST to the C++ Fleet Commander with benchmark parameters.
    The Fleet Commander then:
      - Detects CPU cores
      - Divides bots across cores
      - Starts io_uring workers
      - Streams metrics to Redpanda
    """
    payload = {
        "submission_id":  submission_id,
        "target_host":    host,
        "target_port":    port,
        "num_bots":       num_bots,
        "duration_secs":  duration_secs,
        "protocol":       "rest"
    }
 
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{cfg.fleet_commander_url}/benchmark",
                json=payload
            )
            if resp.status_code == 202:
                print(f"[INFO] Fleet Commander accepted benchmark for {submission_id}")
            else:
                print(f"[WARN] Fleet Commander returned {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[ERROR] Failed to trigger Fleet Commander: {e}")

async def process_submission(submission_id: str):
    db = await get_db()
    container = None
    image_tag = None

    try:
        row = await db.fetchrow(
            "SELECT * FROM submissions WHERE id = $1", submission_id
        )
        if not row:
            print(f"[ERROR] Submission {submission_id} not found")
            return

        print(f"[INFO] Processing {submission_id} ({row['language']})")
        await update_submission(db, submission_id, status="BUILDING")

        # ── 1. Build Docker image ─────────────────────────────────
        try:
            image_tag = build_sandbox_image(
                submission_id, row["storage_key"], row["language"]
            )
        except Exception as e:
            # Build failed — platform issue or bad Dockerfile
            print(f"[ERROR] Image build failed: {e}")
            await update_submission(
                db, submission_id,
                status="FAILED_SYSTEM",
                error_message=f"Build error: {str(e)}"
            )
            return

        # ── 2. Start container ────────────────────────────────────
        try:
            container_id, endpoint_url, sandbox_host, sandbox_port = run_sandbox(image_tag, submission_id)
            container = docker_client.containers.get(container_id)
        except Exception as e:
            print(f"[ERROR] Docker run failed: {e}")
            await update_submission(
                db, submission_id,
                status="FAILED_SYSTEM",
                error_message=f"Docker error: {str(e)}"
            )
            if image_tag:
                try:
                    docker_client.images.remove(image=image_tag, force=True)
                except APIError as cleanup_err:
                    print(f"[WARN] Failed to remove image {image_tag}: {cleanup_err}")
            return

        # ── 3. Check if container crashed immediately ─────────────
        # Give it 1 second then check exit code before waiting for port
        time.sleep(1)
        early_exit_code = get_container_exit_code(container)
        if early_exit_code is not None:
            status = map_exit_code_to_status(early_exit_code)
            print(f"[WARN] Container exited early (code {early_exit_code}) → {status}")
            await update_submission(
                db, submission_id,
                status=status,
                exit_code=early_exit_code,
                error_message=f"Container exited immediately with code {early_exit_code}"
            )
            try:
                container.remove(force=True)
            except APIError as cleanup_err:
                print(f"[WARN] Failed to remove container {container.id}: {cleanup_err}")
            return

        # ── 4. Wait for port to open (FAILED_STARTUP if timeout) ──
        host = sandbox_host
        port = sandbox_port
        
        print(f"[INFO] Waiting for sandbox to bind on port {port}...")
        started = wait_for_startup(host, port, STARTUP_TIMEOUT_SECS)

        if not started:
            # Check if container died while we were waiting
            exit_code = get_container_exit_code(container)

            if exit_code is not None:
                # It crashed during startup
                status = map_exit_code_to_status(exit_code)
                print(f"[WARN] Container crashed during startup (code {exit_code}) → {status}")
                await update_submission(
                    db, submission_id,
                    status=status,
                    exit_code=exit_code,
                    error_message=f"Crashed during startup with exit code {exit_code}"
                )
                try:
                    container.remove(force=True)
                except APIError as cleanup_err:
                    print(f"[WARN] Failed to remove container {container.id}: {cleanup_err}")
            else:
                # Still running but never opened the port
                print(f"[WARN] Port {port} never opened within {STARTUP_TIMEOUT_SECS}s → FAILED_STARTUP")
                try:
                    container.kill()
                except APIError as cleanup_err:
                    print(f"[WARN] Failed to kill container {container.id}: {cleanup_err}")
                try:
                    container.remove(force=True)
                except APIError as cleanup_err:
                    print(f"[WARN] Failed to remove container {container.id}: {cleanup_err}")
                await update_submission(
                    db, submission_id,
                    status="FAILED_STARTUP",
                    error_message=f"Did not bind to port within {STARTUP_TIMEOUT_SECS}s"
                )
            return

        # ── 5. All good — mark READY ──────────────────────────────
        await update_submission(
            db, submission_id,
            status="READY",
            container_id=container_id,
            endpoint_url=endpoint_url
        )
        print(f"[INFO] Sandbox READY → {endpoint_url}")

        await trigger_fleet_commander(
            submission_id = submission_id,
            host          = sandbox_host,
            port          = str(sandbox_port),
            num_bots      = cfg.default_num_bots,
            duration_secs = cfg.default_duration_secs
        )

        await update_submission(db, submission_id, status="RUNNING")

        cleanup_delay = cfg.default_duration_secs + 300
        asyncio.create_task(
            cleanup_sandbox_after_run(
                submission_id,
                container_id,
                image_tag,
                cleanup_delay
            )
        )

    except Exception as e:
        # Catch-all for anything unexpected — platform fault, not contestant fault
        print(f"[ERROR] Unexpected platform error: {e}")
        await update_submission(
            db, submission_id,
            status="FAILED_SYSTEM",
            error_message=f"Platform error: {str(e)}"
        )

    finally:
        await db.close()

async def main():
    print("[INFO] Sandbox Manager started. Waiting for jobs...")
    while True:
        # Blocking pop from Redis queue (waits up to 5s then loops)
        job = await asyncio.to_thread(redis_client.blpop, "submission_queue", timeout=5)
        if job:
            _, submission_id = job
            print(f"[INFO] Got job: {submission_id}")
            await process_submission(submission_id)

if __name__ == "__main__":
    start_health_server(cfg.sandbox_manager_health_port)
    asyncio.run(main())