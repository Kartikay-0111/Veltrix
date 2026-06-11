// internal/db/db.go — PostgreSQL connection pool and submission state machine.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool for clean dependency injection.
type Pool struct {
	*pgxpool.Pool
}

// Submission holds the fields needed by the sandbox manager.
type Submission struct {
	ID         string
	TeamID     string
	Language   string
	StorageKey string
	Status     string
}

// Connect creates a connection pool and verifies it with a ping.
func Connect(ctx context.Context, dsn string) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pgx config: %w", err)
	}

	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Pool{pool}, nil
}

// GetSubmission fetches a submission row by ID.
func (p *Pool) GetSubmission(ctx context.Context, id string) (*Submission, error) {
	rows, err := p.Pool.Query(ctx,
		"SELECT id, team_id, language, storage_key, status FROM submissions WHERE id = $1", id)
	if err != nil {
		return nil, fmt.Errorf("query submission: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var s Submission
	if err := rows.Scan(&s.ID, &s.TeamID, &s.Language, &s.StorageKey, &s.Status); err != nil {
		return nil, fmt.Errorf("scan submission: %w", err)
	}
	return &s, nil
}

// UpdateStatus transitions the submission to a new state with an optional
// set of extra fields (container_id, endpoint_url, error_message, exit_code).
func (p *Pool) UpdateStatus(ctx context.Context, id, status string, extra map[string]any) error {
	// Always set updated_at. Extra fields are handled case-by-case to avoid
	// a dynamic SQL builder (which would require string concatenation or reflection).
	switch {
	case len(extra) == 0:
		_, err := p.Pool.Exec(ctx,
			"UPDATE submissions SET status = $1, updated_at = NOW() WHERE id = $2",
			status, id)
		return err

	case hasKeys(extra, "container_id", "endpoint_url"):
		_, err := p.Pool.Exec(ctx, `
			UPDATE submissions
			SET status = $1, container_id = $2, endpoint_url = $3, updated_at = NOW()
			WHERE id = $4`,
			status, extra["container_id"], extra["endpoint_url"], id)
		return err

	case hasKeys(extra, "error_message", "exit_code"):
		_, err := p.Pool.Exec(ctx, `
			UPDATE submissions
			SET status = $1, error_message = $2, exit_code = $3, updated_at = NOW()
			WHERE id = $4`,
			status, extra["error_message"], extra["exit_code"], id)
		return err

	case hasKeys(extra, "error_message"):
		_, err := p.Pool.Exec(ctx, `
			UPDATE submissions
			SET status = $1, error_message = $2, updated_at = NOW()
			WHERE id = $3`,
			status, extra["error_message"], id)
		return err

	default:
		_, err := p.Pool.Exec(ctx,
			"UPDATE submissions SET status = $1, updated_at = NOW() WHERE id = $2",
			status, id)
		return err
	}
}

func hasKeys(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}
