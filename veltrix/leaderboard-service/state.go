package main

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/redis/go-redis/v9"
)

// fetchLeaderboardState returns the current per-submission state from the Redis
// "leaderboard_state" hash. Each value is the full payload the checker published,
// so it carries the tri-state `status` (correct/incorrect/unverified) — unlike the
// Postgres `is_correct` boolean, which cannot distinguish unverified from
// incorrect. Sorted by TPS descending, then P99 ascending.
func fetchLeaderboardState(ctx context.Context, rdb *redis.Client) ([]MetricMessage, error) {
	vals, err := rdb.HGetAll(ctx, "leaderboard_state").Result()
	if err != nil {
		return nil, err
	}

	metrics := make([]MetricMessage, 0, len(vals))
	for _, v := range vals {
		var m MetricMessage
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			continue
		}
		metrics = append(metrics, m)
	}

	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].TPS == metrics[j].TPS {
			return metrics[i].P99Ms < metrics[j].P99Ms
		}
		return metrics[i].TPS > metrics[j].TPS
	})

	return metrics, nil
}
