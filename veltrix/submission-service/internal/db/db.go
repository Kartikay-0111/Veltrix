// internal/db/db.go — PostgreSQL connection pool and query helpers.
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

// Connect creates a pgx connection pool with a 10s dial timeout.
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

	// Validate the connection is live.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Pool{pool}, nil
}

// GetTeamByAPIKey resolves an API key to its team row.
// Returns nil, nil if no matching row exists.
func (p *Pool) GetTeamByAPIKey(ctx context.Context, apiKey string) (map[string]any, error) {
	row, err := p.Pool.Query(ctx,
		"SELECT id, name FROM teams WHERE api_key = $1", apiKey)
	if err != nil {
		return nil, fmt.Errorf("query team: %w", err)
	}
	defer row.Close()

	if !row.Next() {
		return nil, nil
	}

	var id, name string
	if err := row.Scan(&id, &name); err != nil {
		return nil, fmt.Errorf("scan team: %w", err)
	}
	return map[string]any{"id": id, "name": name}, nil
}

// InsertSubmission writes a new PENDING row into the submissions table.
func (p *Pool) InsertSubmission(ctx context.Context, id, teamID, language, storageKey string) error {
	_, err := p.Pool.Exec(ctx, `
		INSERT INTO submissions (id, team_id, language, status, storage_key)
		VALUES ($1, $2, $3, 'PENDING', $4)
	`, id, teamID, language, storageKey)
	if err != nil {
		return fmt.Errorf("insert submission: %w", err)
	}
	return nil
}

// GetSubmission fetches a single submission row owned by the given team.
// Returns nil, nil if not found.
func (p *Pool) GetSubmission(ctx context.Context, submissionID, teamID string) (map[string]any, error) {
	rows, err := p.Pool.Query(ctx, `
		SELECT id, status, endpoint_url, error_message
		FROM submissions
		WHERE id = $1 AND team_id = $2
	`, submissionID, teamID)
	if err != nil {
		return nil, fmt.Errorf("query submission: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var id, status string
	var endpointURL, errMsg *string
	if err := rows.Scan(&id, &status, &endpointURL, &errMsg); err != nil {
		return nil, fmt.Errorf("scan submission: %w", err)
	}

	result := map[string]any{
		"id":     id,
		"status": status,
	}
	if endpointURL != nil {
		result["endpoint_url"] = *endpointURL
	}
	if errMsg != nil {
		result["error"] = *errMsg
	}
	return result, nil
}
