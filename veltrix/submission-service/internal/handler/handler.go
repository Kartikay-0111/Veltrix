// internal/handler/handler.go — HTTP handlers for the submission service.
package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"veltrix/submission-service/internal/db"
	"veltrix/submission-service/internal/queue"
	"veltrix/submission-service/internal/storage"

	"github.com/google/uuid"
)

// Handler groups all HTTP handler state.
type Handler struct {
	db      *db.Pool
	storage *storage.Client
	queue   *queue.Client
	logger  *log.Logger
}

// New returns a Handler with all dependencies injected.
func New(db *db.Pool, store *storage.Client, q *queue.Client, logger *log.Logger) *Handler {
	return &Handler{db: db, storage: store, queue: q, logger: logger}
}

// RegisterRoutes wires all routes onto the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /submit", h.submit)
	mux.HandleFunc("GET /submission/{id}", h.getSubmission)
}

// ── GET /health ───────────────────────────────────────────────────────────────

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ── POST /submit ──────────────────────────────────────────────────────────────

// validLanguages is the set of supported contestant runtimes.
var validLanguages = map[string]bool{"cpp": true, "rust": true, "go": true}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// ── 1. Authenticate via API key header ───────────────────────────────────
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "missing X-API-Key header")
		return
	}

	team, err := h.db.GetTeamByAPIKey(ctx, apiKey)
	if err != nil {
		h.logger.Printf("[submit] db error on team lookup: %v", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if team == nil {
		writeError(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	// ── 2. Parse the multipart form (32MB in-memory buffer) ──────────────────
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse multipart form")
		return
	}

	language := r.FormValue("language")
	if language == "" {
		language = "cpp" // sensible default
	}
	if !validLanguages[language] {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("language must be one of: cpp, rust, go — got %q", language))
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing 'file' field in form")
		return
	}
	defer file.Close()

	// ── 3. Generate IDs and upload to MinIO ──────────────────────────────────
	submissionID := uuid.New().String()
	teamID := team["id"].(string)
	storageKey := fmt.Sprintf("%s/%s/%s", teamID, submissionID, header.Filename)

	// Stream directly to MinIO — no intermediate disk write.
	// Pass -1 as size since Content-Length may be unknown with chunked encoding.
	if err := h.storage.UploadStream(ctx, storageKey, file, -1); err != nil {
		h.logger.Printf("[submit] minio upload error: %v", err)
		writeError(w, http.StatusInternalServerError, "storage error")
		return
	}

	// ── 4. Insert PENDING row into PostgreSQL ─────────────────────────────────
	if err := h.db.InsertSubmission(ctx, submissionID, teamID, language, storageKey); err != nil {
		h.logger.Printf("[submit] db insert error: %v", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	// ── 5. Enqueue to Redis (Sandbox Manager picks this up) ───────────────────
	if err := h.queue.Enqueue(ctx, queue.SubmissionQueue, submissionID); err != nil {
		h.logger.Printf("[submit] redis enqueue error: %v", err)
		// Non-fatal: submission is in DB, operator can re-queue manually.
		// Return 202 anyway — the job is recorded and recoverable.
	}

	h.logger.Printf("[submit] accepted %s (team=%s language=%s)", submissionID, teamID, language)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"submission_id": submissionID,
		"status":        "PENDING",
		"message":       "Submission received. Container will be ready shortly.",
	})
}

// ── GET /submission/{id} ──────────────────────────────────────────────────────

func (h *Handler) getSubmission(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		writeError(w, http.StatusUnauthorized, "missing X-API-Key header")
		return
	}

	team, err := h.db.GetTeamByAPIKey(ctx, apiKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if team == nil {
		writeError(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	// Go 1.22+ pattern variables in ServeMux
	submissionID := r.PathValue("id")
	if submissionID == "" {
		writeError(w, http.StatusBadRequest, "missing submission id")
		return
	}

	row, err := h.db.GetSubmission(ctx, submissionID, team["id"].(string))
	if err != nil {
		h.logger.Printf("[getSubmission] db error: %v", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "submission not found")
		return
	}

	writeJSON(w, http.StatusOK, row)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
