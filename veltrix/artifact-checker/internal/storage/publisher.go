package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"veltrix/artifact-checker/internal/models"
)

const (
	defaultDBTimeout    = 2 * time.Second
	defaultRedisTimeout = 2 * time.Second
)

type Config struct {
	PostgresURL string
	RedisAddr   string
	RedisDB     int

	DBTimeout    time.Duration
	RedisTimeout time.Duration

	Logger *log.Logger
}

type Publisher struct {
	db     *pgxpool.Pool
	redis  *redis.Client
	config Config
	logger *log.Logger
	wg     sync.WaitGroup
}

type leaderboardPayload struct {
	SubmissionID string  `json:"submission_id"`
	TeamName     string  `json:"team_name"`
	TPS          int     `json:"tps"`
	P50Ms        float64 `json:"p50_ms"`
	P90Ms        float64 `json:"p90_ms"`
	P99Ms        float64 `json:"p99_ms"`
	P99Bucket    int     `json:"p99_bucket"`
	Correct      bool    `json:"correct"`
	IsCorrect    bool    `json:"is_correct"`
}

func NewPublisher(ctx context.Context, config Config) (*Publisher, error) {
	if config.PostgresURL == "" {
		return nil, fmt.Errorf("postgres URL is required")
	}
	if config.RedisAddr == "" {
		return nil, fmt.Errorf("redis address is required")
	}
	if config.DBTimeout <= 0 {
		config.DBTimeout = defaultDBTimeout
	}
	if config.RedisTimeout <= 0 {
		config.RedisTimeout = defaultRedisTimeout
	}
	if config.Logger == nil {
		config.Logger = log.Default()
	}

	db, err := pgxpool.New(ctx, config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("connect TimescaleDB: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: config.RedisAddr,
		DB:   config.RedisDB,
	})

	redisCtx, cancel := context.WithTimeout(ctx, config.RedisTimeout)
	defer cancel()
	if err := redisClient.Ping(redisCtx).Err(); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect Redis: %w", err)
	}

	return &Publisher{
		db:     db,
		redis:  redisClient,
		config: config,
		logger: config.Logger,
	}, nil
}

func (publisher *Publisher) Run(ctx context.Context, scores <-chan models.Score) error {
	defer publisher.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case score, ok := <-scores:
			if !ok {
				return nil
			}
			if err := publisher.Publish(ctx, score); err != nil {
				return err
			}
		}
	}
}

func (publisher *Publisher) Publish(ctx context.Context, score models.Score) error {
	publisher.insertTimescaleAsync(score)

	payload, err := json.Marshal(toPayload(score))
	if err != nil {
		return fmt.Errorf("marshal leaderboard payload: %w", err)
	}

	redisCtx, cancel := context.WithTimeout(ctx, publisher.config.RedisTimeout)
	defer cancel()

	pipe := publisher.redis.Pipeline()
	pipe.HSet(redisCtx, "leaderboard_state", score.SubmissionID, payload)
	pipe.Publish(redisCtx, "leaderboard_updates", payload)
	if _, err := pipe.Exec(redisCtx); err != nil {
		return fmt.Errorf("redis pipeline publish: %w", err)
	}

	return nil
}

func (publisher *Publisher) insertTimescaleAsync(score models.Score) {
	publisher.wg.Add(1)
	go func() {
		defer publisher.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), publisher.config.DBTimeout)
		defer cancel()

		_, err := publisher.db.Exec(ctx, `
			INSERT INTO leaderboard_metrics
				(time, team_id, tps, p50_latency_ms, p90_latency_ms, p99_latency_ms, is_correct)
			VALUES
				(NOW(), $1, $2, $3, $4, $5, $6)
		`,
			score.SubmissionID,
			score.TPS,
			score.P50Ms,
			score.P90Ms,
			score.P99Ms,
			score.Correct,
		)
		if err != nil {
			publisher.logger.Printf("[storage] TimescaleDB insert failed submission=%s: %v", score.SubmissionID, err)
		}
	}()
}

func (publisher *Publisher) Wait() {
	publisher.wg.Wait()
}

func (publisher *Publisher) Close() {
	publisher.Wait()
	if publisher.redis != nil {
		if err := publisher.redis.Close(); err != nil {
			publisher.logger.Printf("[storage] Redis close failed: %v", err)
		}
	}
	if publisher.db != nil {
		publisher.db.Close()
	}
}

func toPayload(score models.Score) leaderboardPayload {
	teamName := score.TeamName
	if teamName == "" {
		teamName = score.SubmissionID
	}

	return leaderboardPayload{
		SubmissionID: score.SubmissionID,
		TeamName:     teamName,
		TPS:          score.TPS,
		P50Ms:        score.P50Ms,
		P90Ms:        score.P90Ms,
		P99Ms:        score.P99Ms,
		P99Bucket:    score.P99Bucket,
		Correct:      score.Correct,
		IsCorrect:    score.Correct,
	}
}
