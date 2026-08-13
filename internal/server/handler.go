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
	"github.com/yuzl1/go-m3u8/internal/clash"
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

// TransferAction handles POST /api/transfers/sync-file (manual re-sync of
// a save-dir file) and POST /api/transfers/{id}/retry (re-queue failed).
func (h *Handler) TransferAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/transfers/"), "/")

	if path == "sync-file" {
		var body struct {
			File string `json:"file"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.File == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "file name required"})
			return
		}
		t, err := h.AgentHub.SyncFile(body.File)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, t)
		return
	}

	if id, ok := strings.CutSuffix(path, "/retry"); ok {
		if err := h.AgentHub.RetryTransfer(id); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "retrying"})
		return
	}

	writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown transfer action"})
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

// UpdateConfig merges the request into the existing configuration:
// fields ABSENT from the request keep their current values. This matters
// because a stale cached web page may submit a form without newer fields
// — a full replace would silently wipe them (e.g. translate_enabled).
func (h *Handler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	old := h.Config.Get()
	cfg := *old // start from current values, overlay only present fields
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

	// Clash: extract the API secret from the imported yaml and push the
	// config to the mihomo sidecar (async — don't block the save).
	if cfg.ClashEnabled && cfg.ClashYAML != "" {
		if secret := clash.ExtractSecret(cfg.ClashYAML); secret != "" {
			cfg.ClashSecret = secret
		}
		go func(cfgCopy config.Config) {
			c := clash.New(cfgCopy.ClashAPI, cfgCopy.ClashSecret)
			if err := c.UploadConfig(clash.SanitizePayload(cfgCopy.ClashYAML)); err != nil {
				log.Printf("Failed to push clash config: %v", err)
			} else {
				log.Printf("Clash config pushed to %s", cfgCopy.ClashAPI)
			}
		}(cfg)
	}

	// Clash subscription: fetch on save so the user doesn't need to
	// paste YAML manually.
	if cfg.ClashEnabled && cfg.ClashSubscribeURL != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		yaml, err := clash.FetchSubscription(ctx, cfg.ClashSubscribeURL)
		cancel()
		if err != nil {
			log.Printf("Subscription fetch failed: %v", err)
		} else {
			cfg.ClashYAML = yaml
			if secret := clash.ExtractSecret(yaml); secret != "" {
				cfg.ClashSecret = secret
			}
			log.Printf("Subscription fetched: %d bytes, %d proxies", len(yaml), strings.Count(yaml, "\n  - name:"))
		}
	}

	if err := h.Config.Update(&cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	h.Manager.RefreshSem()
	writeJSON(w, http.StatusOK, cfg)
}

// ClashSubscribe refreshes the subscription now (POST /api/clash/subscribe).
func (h *Handler) ClashSubscribe(w http.ResponseWriter, r *http.Request) {
	cfg := h.Config.Get()
	if cfg.ClashSubscribeURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "未配置订阅地址 (clash_subscribe_url)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	yaml, err := clash.FetchSubscription(ctx, cfg.ClashSubscribeURL)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	if secret := clash.ExtractSecret(yaml); secret != "" {
		cfg.ClashSecret = secret
	}
	cfg.ClashYAML = yaml
	if err := h.Config.Update(cfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Push to the sidecar + drop the stale healthy-node cache.
	go func() {
		c := clash.New(cfg.ClashAPI, cfg.ClashSecret)
		if err := c.UploadConfig(clash.SanitizePayload(yaml)); err != nil {
			log.Printf("Failed to push refreshed subscription: %v", err)
		}
	}()
	h.Manager.InvalidateClashCache()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"size":    len(yaml),
		"proxies": strings.Count(yaml, "\n  - name:"),
	})
}

// ClashStatus reports clash connectivity, the rotation group, the
// currently selected node, and the health of every node (delay test).
func (h *Handler) ClashStatus(w http.ResponseWriter, r *http.Request) {
	cfg := h.Config.Get()
	if !cfg.ClashEnabled {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "clash 未启用"})
		return
	}

	// force = re-run the node health check now
	if r.URL.Query().Get("refresh") == "1" {
		h.Manager.InvalidateClashCache()
	}

	c := clash.New(cfg.ClashAPI, cfg.ClashSecret)
	group, nodes, delays, err := h.Manager.ClashHealthy(cfg)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	current, _ := c.CurrentNode(group)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"group":   group,
		"current": current,
		"nodes":   nodes,
		"delays":  delays,
	})
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

	// NOTE: no translation here — the task is created immediately so a slow
	// or failing translation service never delays the download start (the
	// site's m3u8 URLs carry an expiry timestamp). Translation happens in
	// the download pipeline, before N_m3u8DL-RE actually starts.
	saveName := h.resolveSaveName(&req)

	task := h.Manager.Submit(req.URL, saveName, headers, req.BaseURL, req.SaveDir)
	log.Printf("New download task: id=%s url=%s title=%q", task.ID, task.URL, task.Title)
	writeJSON(w, http.StatusAccepted, task)
}

// resolveSaveName picks the filename source per config and strips site
// suffixes ("Title | SiteName").
func (h *Handler) resolveSaveName(req *downloadReq) string {
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

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	provider := r.URL.Query().Get("provider")
	if provider == "" {
		provider = cfg.TranslateProvider
	}
	appID := r.URL.Query().Get("appid")
	if appID == "" {
		appID = cfg.BaiduAppID
	}
	appKey := r.URL.Query().Get("appkey")
	if appKey == "" {
		appKey = cfg.BaiduAppKey
	}
	zh, err := translate.Translate(ctx, text, target, translate.Config{
		Provider: provider,
		APIURL:   apiURL,
		AppID:    appID,
		AppKey:   appKey,
	})
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
