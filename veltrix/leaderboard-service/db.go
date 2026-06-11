package main

import (
	"context"
	"log"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// fetchCurrentLeaderboard — called when a new WebSocket client connects
//
// Returns the latest metric row per team, sorted by TPS descending.
// This gives the browser the full current state immediately on connect
// without waiting for the next Redis publish.
// ─────────────────────────────────────────────────────────────────────────────
func fetchCurrentLeaderboard(ctx context.Context, pool *pgxpool.Pool) ([]MetricMessage, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ON (lm.team_id)
		    lm.team_id,
		    COALESCE(t.name, lm.team_id) AS team_name,
		    lm.tps,
		    lm.p50_latency_ms,
		    lm.p90_latency_ms,
		    lm.p99_latency_ms,
		    lm.is_correct
		FROM leaderboard_metrics lm
		LEFT JOIN submissions s ON s.id::text = lm.team_id
		LEFT JOIN teams t       ON t.id = s.team_id
		ORDER BY lm.team_id, lm.time DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []MetricMessage
	for rows.Next() {
		var m MetricMessage
		err := rows.Scan(
			&m.SubmissionID,
			&m.TeamName,
			&m.TPS,
			&m.P50Ms,
			&m.P90Ms,
			&m.P99Ms,
			&m.IsCorrect,
		)
		if err != nil {
			log.Printf("[DB] Row scan error: %v", err)
			continue
		}
		metrics = append(metrics, m)
	}

	// Sort by TPS descending, then by P99 ascending
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].TPS == metrics[j].TPS {
			return metrics[i].P99Ms < metrics[j].P99Ms
		}
		return metrics[i].TPS > metrics[j].TPS
	})

	return metrics, nil
}
