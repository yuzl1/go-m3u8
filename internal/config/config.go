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
	SaveDir        string            `json:"save_dir"`
	TempDir        string            `json:"tmp_dir"`
	ThreadCount    int               `json:"thread_count"`
	AutoSelect     bool              `json:"auto_select"`
	DelAfterDone   bool              `json:"del_after_done"`
	Concurrent     bool              `json:"concurrent"`
	MaxConcurrent  int               `json:"max_concurrent"`
	DefaultHeaders map[string]string `json:"default_headers"`
	BaseURL        string            `json:"base_url"`
	Nm3u8dlPath    string            `json:"nm3u8dl_path"`
	Port           int               `json:"port"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		SaveDir:        "./downloads",
		TempDir:        "",
		ThreadCount:    16,
		AutoSelect:     true,
		DelAfterDone:   true,
		Concurrent:     true,
		MaxConcurrent:  3,
		DefaultHeaders: map[string]string{},
		BaseURL:        "",
		Nm3u8dlPath:    "N_m3u8DL-RE",
		Port:           8080,
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
