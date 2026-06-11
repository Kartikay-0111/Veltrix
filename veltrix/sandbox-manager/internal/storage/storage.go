// internal/storage/storage.go — MinIO download helper for the sandbox manager.
package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps the MinIO SDK for object downloads.
type Client struct {
	mc     *minio.Client
	bucket string
}

// New initialises the MinIO client.
func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio init: %w", err)
	}
	return &Client{mc: mc, bucket: bucket}, nil
}

// DownloadObject fetches an object and returns a ReadCloser.
// Caller is responsible for closing the returned reader.
func (c *Client) DownloadObject(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := c.mc.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("minio get object %q: %w", key, err)
	}
	return obj, nil
}
