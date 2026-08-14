package config

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

// Config holds all persistent configuration for the app.
type Config struct {
	SaveDir            string            `json:"save_dir"`
	TempDir            string            `json:"tmp_dir"`
	ThreadCount        int               `json:"thread_count"`
	AutoSelect         bool              `json:"auto_select"`
	DelAfterDone       bool              `json:"del_after_done"`
	Concurrent         bool              `json:"concurrent"`
	MaxConcurrent      int               `json:"max_concurrent"`
	DownloadRetryCount int               `json:"download_retry_count"` // per-segment retry, passed to N_m3u8DL-RE
	CheckSegments      bool              `json:"check_segments"`       // verify segment count before muxing
	DefaultHeaders     map[string]string `json:"default_headers"`
	BaseURL            string            `json:"base_url"`
	Nm3u8dlPath        string            `json:"nm3u8dl_path"`
	Port               int               `json:"port"`

	// Agent sync: after download completes, the file is transferred to a
	// connected agent node and (optionally) deleted locally to save disk.
	AgentEnabled      bool   `json:"agent_enabled"`       // accept agent connections
	AgentToken        string `json:"agent_token"`         // shared secret, auto-generated if empty
	SyncAfterDownload bool   `json:"sync_after_download"` // create transfer after download done
	DeleteAfterSync   bool   `json:"delete_after_sync"`   // delete local file after sync
	SyncConnections   int    `json:"sync_connections"`    // parallel chunk connections for sync (1-16)

	// MinSegments: fail the task when the parsed playlist has fewer
	// segments than this (0 = off). Sites often serve a short teaser
	// playlist to suspicious IPs (e.g. datacenter proxies) — a 2-hour
	// video must never be a 2-segment playlist.
	MinSegments int `json:"min_segments"`

	// AppendURLParams: pass the input URL's query params (u/s/e tokens)
	// to every segment request — required by sites that validate the
	// token per segment (bondagetea-style).
	AppendURLParams bool `json:"append_url_params"`

	// Clash integration: import a clash config (paste YAML in the UI),
	// pushed to a mihomo sidecar container; each download task rotates
	// to the next node of the selector group.
	ClashEnabled bool   `json:"clash_enabled"`
	ClashAPI     string `json:"clash_api"`    // mihomo external-controller, default http://127.0.0.1:9090
	ClashSecret  string `json:"clash_secret"` // auto-extracted from the yaml when present
	ClashProxy   string `json:"clash_proxy"`  // clash mixed port used as --custom-proxy, default http://127.0.0.1:7890
	ClashGroup   string `json:"clash_group"`  // selector group name, empty = auto-detect
	ClashYAML    string `json:"clash_yaml"`   // user's clash config content

	// Clash subscription: fetch config from a subscription URL (base64 /
	// v2ray links auto-converted). Refreshed on save and hourly.
	ClashSubscribeURL string `json:"clash_subscribe_url"`

	// ClashNodeFilter: regex of node names to exclude from rotation
	// (airport info entries like 套餐到期/剩余流量). Empty = default.
	ClashNodeFilter string `json:"clash_node_filter"`

	// Filename handling: which cat-catch field becomes the saved name,
	// and optional translation of the filename (e.g. English -> Chinese).
	FilenameSource    string `json:"filename_source"`    // auto | title | fullFileName
	TranslateEnabled  bool   `json:"translate_enabled"`  // translate filename before saving
	TranslateTarget   string `json:"translate_target"`   // target language, default zh-CN
	TranslateProvider string `json:"translate_provider"` // google | baidu | custom
	TranslateAPIURL   string `json:"translate_api_url"`  // custom API template with {text}
	BaiduAppID        string `json:"baidu_appid"`        // Baidu translate appid
	BaiduAppKey       string `json:"baidu_appkey"`       // Baidu translate secret
}

// envOr returns the env var value or a fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		SaveDir:            "/downloads",
		TempDir:            "",
		ThreadCount:        16,
		AutoSelect:         true,
		DelAfterDone:       true,
		Concurrent:         true,
		MaxConcurrent:      2,
		DownloadRetryCount: 5,
		CheckSegments:      true,
		DefaultHeaders:     map[string]string{},
		BaseURL:            "",
		Nm3u8dlPath:        "N_m3u8DL-RE",
		Port:               8080,
		AgentEnabled:       true,
		SyncAfterDownload:  true,
		DeleteAfterSync:    true,
		SyncConnections:    8,
		FilenameSource:     "auto",
		TranslateEnabled:   false,
		TranslateTarget:    "zh-CN",
		TranslateProvider:  "google",
		ClashAPI:           envOr("CLASH_API", "http://127.0.0.1:9090"),
		ClashProxy:         envOr("CLASH_PROXY", "http://127.0.0.1:7890"),
	}
}

// Store persists config to a JSON file.
type Store struct {
	mu   sync.RWMutex
	path string
	cfg  *Config
}

// NewStore creates a Store, loading config from configDir. If configDir
// is empty, it defaults to the current directory.
func NewStore(configDir string) (*Store, error) {
	if configDir == "" {
		configDir = "."
	}
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(configDir, "config.json")
	s := &Store{path: path}
	cfg, err := s.load()
	if err != nil {
		cfg = DefaultConfig()
	}
	s.cfg = cfg
	return s, nil
}

func (s *Store) load() (*Config, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Get returns a copy of the current config.
func (s *Store) Get() *Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := *s.cfg
	cp.DefaultHeaders = make(map[string]string, len(s.cfg.DefaultHeaders))
	maps.Copy(cp.DefaultHeaders, s.cfg.DefaultHeaders)
	return &cp
}

// Update replaces the config and persists to disk.
func (s *Store) Update(cfg *Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}
