// internal/storage/storage.go — MinIO object storage wrapper.
package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps the MinIO SDK client.
type Client struct {
	mc     *minio.Client
	bucket string
}

// New creates a MinIO client and ensures the target bucket exists.
func New(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Client, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio init: %w", err)
	}

	// Idempotent bucket creation.
	exists, err := mc.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("minio bucket check: %w", err)
	}
	if !exists {
		if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("minio make bucket: %w", err)
		}
	}

	return &Client{mc: mc, bucket: bucket}, nil
}

// UploadStream stores a reader as an object at the given key.
// objectSize can be -1 if the size is not known in advance.
func (c *Client) UploadStream(ctx context.Context, key string, r io.Reader, objectSize int64) error {
	_, err := c.mc.PutObject(ctx, c.bucket, key, r, objectSize, minio.PutObjectOptions{
		ContentType: "application/zip",
	})
	if err != nil {
		return fmt.Errorf("minio put object %q: %w", key, err)
	}
	return nil
}

// DownloadObject retrieves an object and returns a ReadCloser.
// Caller must close the returned reader.
func (c *Client) DownloadObject(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := c.mc.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("minio get object %q: %w", key, err)
	}
	return obj, nil
}
