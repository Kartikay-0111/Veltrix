// internal/queue/queue.go — Redis queue wrapper.
package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const SubmissionQueue = "submission_queue"

// Client wraps the Redis client for queue operations.
type Client struct {
	rdb *redis.Client
}

// New creates a Redis client and pings to verify connectivity.
func New(ctx context.Context, host string, port int) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", host, port),
		DB:   0,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Enqueue pushes a value onto the right end of the list (RPUSH).
// The sandbox manager BLPOPs from the left, giving FIFO ordering.
func (c *Client) Enqueue(ctx context.Context, queue, value string) error {
	if err := c.rdb.RPush(ctx, queue, value).Err(); err != nil {
		return fmt.Errorf("rpush %q: %w", queue, err)
	}
	return nil
}

// BLPop blocks until an item is available in one of the named lists,
// or until the context is cancelled. Returns the list name and value.
func (c *Client) BLPop(ctx context.Context, queues ...string) (string, string, error) {
	result, err := c.rdb.BLPop(ctx, 0, queues...).Result()
	if err != nil {
		return "", "", fmt.Errorf("blpop: %w", err)
	}
	// result[0] = key, result[1] = value
	return result[0], result[1], nil
}

// Publish sends a message to a Redis Pub/Sub channel.
func (c *Client) Publish(ctx context.Context, channel, payload string) error {
	if err := c.rdb.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("publish %q: %w", channel, err)
	}
	return nil
}

// Close shuts down the Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
