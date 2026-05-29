package main

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"log"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// MetricMessage — shape of the JSON published by the C++ artifact checker
// ─────────────────────────────────────────────────────────────────────────────
type MetricMessage struct {
	SubmissionID string  `json:"submission_id"`
	TeamName     string  `json:"team_name"`
	TPS          int     `json:"tps"`
	P50Ms        float64 `json:"p50_ms"`
	P90Ms        float64 `json:"p90_ms"`
	P99Ms        float64 `json:"p99_ms"`
	IsCorrect    bool    `json:"is_correct"`
}

// ─────────────────────────────────────────────────────────────────────────────
// RedisSubscriber — listens on "leaderboard_updates" and pushes HTML to Hub
// ─────────────────────────────────────────────────────────────────────────────
type RedisSubscriber struct {
	client   *redis.Client
	hub      *Hub
	rowTmpl  *template.Template
}

func newRedisSubscriber(rdb *redis.Client, hub *Hub, rowTmpl *template.Template) *RedisSubscriber {
	return &RedisSubscriber{
		client:  rdb,
		hub:     hub,
		rowTmpl: rowTmpl,
	}
}

// Listen — blocking. Call in a goroutine.
func (s *RedisSubscriber) Listen(ctx context.Context) {
	pubsub := s.client.Subscribe(ctx, "leaderboard_updates")
	defer pubsub.Close()

	log.Println("[Redis] Subscribed to leaderboard_updates")

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-ch:
			if !ok {
				log.Println("[Redis] Channel closed — reconnecting")
				return
			}

			var m MetricMessage
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				log.Printf("[Redis] Bad payload: %v", err)
				continue
			}

			html, err := s.renderRow(m)
			if err != nil {
				log.Printf("[Redis] Template error: %v", err)
				continue
			}

			// Push rendered HTML to all connected browsers
			s.hub.Broadcast(html)
			log.Printf("[Redis] Broadcasted update for %s TPS=%d p99=%.3fms",
				m.TeamName, m.TPS, m.P99Ms)
		}
	}
}

func (s *RedisSubscriber) renderRow(m MetricMessage) ([]byte, error) {
	var buf bytes.Buffer
	err := s.rowTmpl.Execute(&buf, m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
