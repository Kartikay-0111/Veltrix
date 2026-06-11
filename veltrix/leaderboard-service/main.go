package main

import (
	"bytes"
	"context"
	"html/template"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// Config from environment
// ─────────────────────────────────────────────────────────────────────────────
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─────────────────────────────────────────────────────────────────────────────
// Globals (set in main, used in handlers)
// ─────────────────────────────────────────────────────────────────────────────
var (
	hub      *Hub
	dbPool   *pgxpool.Pool
	rdb      *redis.Client
	tmpls    *template.Template
	upgrader = websocket.Upgrader{
		CheckOrigin:     func(r *http.Request) bool { return true },
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
	}
)

func main() {
	ctx := context.Background()

	// ── Templates ─────────────────────────────────────────────────────────────
	var err error
	funcs := template.FuncMap{
		"add": func(a, b int) int { return a + b },
	}
	tmpls, err = template.New("").Funcs(funcs).ParseGlob("templates/*.html")
	if err != nil {
		log.Fatalf("[Main] Template parse error: %v", err)
	}

	// ── PostgreSQL pool ───────────────────────────────────────────────────────
	pgConn := "postgres://" +
		getenv("POSTGRES_USER", "iicpc") + ":" +
		getenv("POSTGRES_PASSWORD", "iicpc_secret") + "@" +
		getenv("POSTGRES_HOST", "postgres") + ":" +
		getenv("POSTGRES_PORT", "5432") + "/" +
		getenv("POSTGRES_DB", "iicpc_db")

	dbPool, err = pgxpool.New(ctx, pgConn)
	if err != nil {
		log.Fatalf("[Main] DB connect error: %v", err)
	}
	defer dbPool.Close()

	// ── Redis client ──────────────────────────────────────────────────────────
	rdb = redis.NewClient(&redis.Options{
		Addr: getenv("REDIS_HOST", "redis") + ":" + getenv("REDIS_PORT", "6379"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[Main] Redis connect error: %v", err)
	}

	// ── WebSocket hub ─────────────────────────────────────────────────────────
	hub = newHub()
	go hub.Run()

	// ── Redis subscriber → broadcasts HTML to hub ─────────────────────────────
	rowTmpl := tmpls.Lookup("row.html")
	if rowTmpl == nil {
		log.Fatal("[Main] row.html template not found")
	}
	sub := newRedisSubscriber(rdb, hub, rowTmpl)
	go sub.Listen(ctx)

	// ── HTTP routes ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", handleIndex)
	mux.HandleFunc("GET /leaderboard", handleLeaderboard)
	mux.HandleFunc("GET /ws/leaderboard", handleWebSocket)
	mux.HandleFunc("GET /health", handleHealth)

	// Static files (CSS loaded from CDN in templates, no static dir needed)

	port := getenv("PORT", "8085")
	log.Printf("╔══════════════════════════════════════════╗")
	log.Printf("║   VELTRIX Leaderboard  (Part 4)         ║")
	log.Printf("╚══════════════════════════════════════════╝")
	log.Printf("  Listening on :%s", port)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // 0 = no timeout for WebSocket connections
		IdleTimeout:  120 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	if err := tmpls.ExecuteTemplate(w, "base.html", nil); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	// Return just the leaderboard table fragment (used by HTMX hx-get)
	metrics, err := fetchCurrentLeaderboard(r.Context(), dbPool)
	if err != nil {
		log.Printf("[Handler] DB error: %v", err)
		http.Error(w, "db error", 500)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := tmpls.ExecuteTemplate(w, "table.html", metrics); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v", err)
		return
	}

	client := &Client{
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
	}

	hub.register <- client

	// Send current leaderboard state immediately to this new client
	go func() {
		metrics, err := fetchCurrentLeaderboard(context.Background(), dbPool)
		if err != nil {
			log.Printf("[WS] Initial fetch error: %v", err)
			return
		}

		rowTmpl := tmpls.Lookup("row.html")
		for _, m := range metrics {
			var buf bytes.Buffer
			if err := rowTmpl.Execute(&buf, m); err == nil {
				client.send <- buf.Bytes()
			}
		}
	}()

	go client.writePump(hub)
	go client.readPump(hub)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}
