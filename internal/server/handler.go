package server

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yuzl1/go-m3u8/internal/agent"
	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"
	"github.com/yuzl1/go-m3u8/internal/translate"
)

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	Manager  *download.Manager
	Config   *config.Store
	AgentHub *agent.Hub
	Template []byte // embedded HTML template
}

// ListAgents returns connected agents.
func (h *Handler) ListAgents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.AgentHub.Agents())
}

// ListTransfers returns all sync transfers.
func (h *Handler) ListTransfers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.AgentHub.Transfers())
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
	if cfg.DownloadRetryCount <= 0 {
		cfg.DownloadRetryCount = 5
	}
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}
	if cfg.Nm3u8dlPath == "" {
		cfg.Nm3u8dlPath = "N_m3u8DL-RE"
	}
	// Empty token in the form means "keep the current one".
	if cfg.AgentToken == "" {
		cfg.AgentToken = h.Config.Get().AgentToken
	}
	if err := h.Config.Update(&cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	h.Manager.RefreshSem()
	writeJSON(w, http.StatusOK, cfg)
}

// downloadReq is the /api/download request payload. JSON field names match
// Cat-Catch replacement tags (userAgent, fullFileName, ...) so a POST body
// template can use ${...} directly.
type downloadReq struct {
	URL          string            `json:"url"`
	Title        string            `json:"title"`
	Referer      string            `json:"referer"`
	Cookie       string            `json:"cookie"`
	UA           string            `json:"ua"`           // alias of userAgent
	UserAgent    string            `json:"userAgent"`    // cat-catch tag ${userAgent}
	FileName     string            `json:"fileName"`     // cat-catch tag ${fileName}, no extension
	FullFileName string            `json:"fullFileName"` // cat-catch tag ${fullFileName}
	BaseURL      string            `json:"base_url"`
	SaveDir      string            `json:"save_dir"`
	Headers      map[string]string `json:"headers"`
}

// Download triggers a new download task.
// Supports both GET (query params) and POST (JSON body or form).
func (h *Handler) Download(w http.ResponseWriter, r *http.Request) {
	var req downloadReq

	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		req.URL = q.Get("url")
		req.Title = q.Get("title")
		req.Referer = q.Get("referer")
		req.Cookie = q.Get("cookie")
		req.UA = q.Get("ua")
		req.UserAgent = q.Get("userAgent")
		req.FileName = q.Get("fileName")
		req.FullFileName = q.Get("fullFileName")
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
			req.UserAgent = r.FormValue("userAgent")
			req.FileName = r.FormValue("fileName")
			req.FullFileName = r.FormValue("fullFileName")
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
	userAgent := req.UserAgent
	if userAgent == "" {
		userAgent = req.UA
	}
	if userAgent != "" {
		headers["User-Agent"] = userAgent
	}
	// Merge extra headers from JSON post.
	maps.Copy(headers, req.Headers)

	saveName := h.resolveSaveName(r, &req)

	task := h.Manager.Submit(req.URL, saveName, headers, req.BaseURL, req.SaveDir)
	log.Printf("New download task: id=%s url=%s title=%q", task.ID, task.URL, task.Title)
	writeJSON(w, http.StatusAccepted, task)
}

// resolveSaveName picks the filename source per config, strips site
// suffixes ("Title | SiteName"), and optionally translates the result.
func (h *Handler) resolveSaveName(r *http.Request, req *downloadReq) string {
	cfg := h.Config.Get()

	saveName := ""
	switch cfg.FilenameSource {
	case "title":
		saveName = req.Title
	case "fullFileName":
		if req.FullFileName != "" {
			saveName = strings.TrimSuffix(req.FullFileName, filepath.Ext(req.FullFileName))
		} else if req.FileName != "" {
			saveName = req.FileName
		} else {
			saveName = req.Title
		}
	default: // auto: prefer file name tags, fall back to title.
		// BUT when translation is enabled, the title is the human-readable
		// text worth translating — technical file names are not.
		if cfg.TranslateEnabled && req.Title != "" {
			saveName = req.Title
		} else {
			saveName = req.Title
			if req.FileName != "" {
				saveName = req.FileName
			} else if req.FullFileName != "" {
				saveName = strings.TrimSuffix(req.FullFileName, filepath.Ext(req.FullFileName))
			}
		}
	}

	// "Title | SiteName" -> "Title" (cleaner filenames, better translation)
	if i := strings.Index(saveName, "|"); i >= 0 {
		saveName = strings.TrimSpace(saveName[:i])
	}

	// Optional filename translation (English -> Chinese etc.).
	if cfg.TranslateEnabled && saveName != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if zh, err := translate.Translate(ctx, saveName, cfg.TranslateTarget, cfg.TranslateAPIURL); err == nil && zh != "" {
			log.Printf("Translated filename %q -> %q", saveName, zh)
			saveName = zh
		} else {
			log.Printf("Filename translation failed for %q: %v (using original)", saveName, err)
		}
	}

	return saveName
}

// ListTasks returns all download tasks.
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.Manager.List())
}

// TranslateText tests the translation configuration:
// GET /api/translate?text=...&target=...&api_url=...
func (h *Handler) TranslateText(w http.ResponseWriter, r *http.Request) {
	text := r.URL.Query().Get("text")
	if text == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "text is required"})
		return
	}
	cfg := h.Config.Get()
	target := r.URL.Query().Get("target")
	if target == "" {
		target = cfg.TranslateTarget
	}
	apiURL := r.URL.Query().Get("api_url")
	if apiURL == "" {
		apiURL = cfg.TranslateAPIURL
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	zh, err := translate.Translate(ctx, text, target, apiURL)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"original":   text,
		"translated": zh,
		"target":     target,
	})
}

// extractTaskID pulls the task ID from /api/tasks/<id>[/retry|/log].
func extractTaskID(r *http.Request) string {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	id := strings.TrimSuffix(path, "/")
	id = strings.TrimSuffix(id, "/retry")
	id = strings.TrimSuffix(id, "/log")
	return strings.TrimSuffix(id, "/")
}

// DeleteTask cancels or deletes a task.
func (h *Handler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	id := extractTaskID(r)
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
	id := extractTaskID(r)
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

// GetTaskLog returns the full N_m3u8DL-RE output for a task.
func (h *Handler) GetTaskLog(w http.ResponseWriter, r *http.Request) {
	id := extractTaskID(r)
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task id required"})
		return
	}
	task := h.Manager.Get(id)
	if task == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":  id,
		"log": task.Log,
	})
}

// ListFiles lists files in the configured save directory.
func (h *Handler) ListFiles(w http.ResponseWriter, r *http.Request) {
	cfg := h.Config.Get()
	dir := cfg.SaveDir
	if dir == "" {
		dir = "/downloads"
	}
	if !filepath.IsAbs(dir) {
		if abs, err := filepath.Abs(dir); err == nil {
			dir = abs
		}
	}

	type fileInfo struct {
		Name    string    `json:"name"`
		Size    int64     `json:"size"`
		ModTime time.Time `json:"mod_time"`
		IsDir   bool      `json:"is_dir"`
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"dir":    dir,
			"exists": false,
			"error":  err.Error(),
			"files":  []fileInfo{},
		})
		return
	}

	files := make([]fileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileInfo{
			Name:    e.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		})
	}
	// Newest first
	for i, j := 0, len(files)-1; i < j; i, j = i+1, j-1 {
		files[i], files[j] = files[j], files[i]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"dir":    dir,
		"exists": true,
		"files":  files,
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
