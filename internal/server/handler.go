package server

import (
	"encoding/json"
	"log"
	"maps"
	"net/http"
	"strings"

	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"
)

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	Manager  *download.Manager
	Config   *config.Store
	Template []byte // embedded HTML template
}

// Index serves the main web page.
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(h.Template)
}

// GetConfig returns the current configuration as JSON.
func (h *Handler) GetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.Config.Get())
}

// UpdateConfig replaces the entire configuration.
func (h *Handler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var cfg config.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 3
	}
	if cfg.ThreadCount <= 0 {
		cfg.ThreadCount = 16
	}
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}
	if cfg.Nm3u8dlPath == "" {
		cfg.Nm3u8dlPath = "N_m3u8DL-RE"
	}
	if err := h.Config.Update(&cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	h.Manager.RefreshSem()
	writeJSON(w, http.StatusOK, cfg)
}

// Download triggers a new download task.
// Supports both GET (query params) and POST (JSON body or form).
func (h *Handler) Download(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL     string            `json:"url"`
		Title   string            `json:"title"`
		Referer string            `json:"referer"`
		Cookie  string            `json:"cookie"`
		UA      string            `json:"ua"`
		BaseURL string            `json:"base_url"`
		SaveDir string            `json:"save_dir"`
		Headers map[string]string `json:"headers"`
	}

	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		req.URL = q.Get("url")
		req.Title = q.Get("title")
		req.Referer = q.Get("referer")
		req.Cookie = q.Get("cookie")
		req.UA = q.Get("ua")
		req.BaseURL = q.Get("base_url")
		req.SaveDir = q.Get("save_dir")

	case http.MethodPost:
		ct := r.Header.Get("Content-Type")
		if strings.Contains(ct, "application/json") {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		} else {
			// form-encoded
			if err := r.ParseForm(); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			req.URL = r.FormValue("url")
			req.Title = r.FormValue("title")
			req.Referer = r.FormValue("referer")
			req.Cookie = r.FormValue("cookie")
			req.UA = r.FormValue("ua")
			req.BaseURL = r.FormValue("base_url")
			req.SaveDir = r.FormValue("save_dir")
		}
	}

	if req.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url is required"})
		return
	}

	// Build headers map.
	headers := make(map[string]string)
	if req.Referer != "" {
		headers["Referer"] = req.Referer
	}
	if req.Cookie != "" {
		headers["Cookie"] = req.Cookie
	}
	if req.UA != "" {
		headers["User-Agent"] = req.UA
	}
	// Merge extra headers from JSON post.
	maps.Copy(headers, req.Headers)

	task := h.Manager.Submit(req.URL, req.Title, headers, req.BaseURL, req.SaveDir)
	log.Printf("New download task: id=%s url=%s title=%s", task.ID, task.URL, task.Title)
	writeJSON(w, http.StatusAccepted, task)
}

// ListTasks returns all download tasks.
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.Manager.List())
}

// DeleteTask cancels or deletes a task.
func (h *Handler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	// Extract ID from /api/tasks/<id>
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	id := strings.TrimSuffix(path, "/")
	// Strip /retry suffix if present
	id = strings.TrimSuffix(id, "/retry")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task id required"})
		return
	}

	action := r.URL.Query().Get("action")
	if action == "delete" {
		if err := h.Manager.Delete(id); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
		return
	}

	// Default: cancel.
	if err := h.Manager.Cancel(id); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// RetryTask re-submits a failed/cancelled task.
func (h *Handler) RetryTask(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	id := strings.TrimSuffix(path, "/retry")
	id = strings.TrimSuffix(id, "/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task id required"})
		return
	}
	task, err := h.Manager.Retry(id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
