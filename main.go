package main

import (
	"context"
	"embed"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yuzl1/go-m3u8/internal/agent"
	"github.com/yuzl1/go-m3u8/internal/clash"
	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"
	"github.com/yuzl1/go-m3u8/internal/server"
	"github.com/yuzl1/go-m3u8/internal/ws"
)

// envInt reads an int env var, returning 0 when unset/invalid.
func envInt(key string) int {
	n, _ := strconv.Atoi(os.Getenv(key))
	return n
}

//go:embed web/templates/index.html
var templateFS embed.FS

func main() {
	// Agent mode: run only the sync client, no web UI / download manager.
	// Triggered with -agent flag or AGENT_MODE=1. Default is main server mode.
	agentMode := flag.Bool("agent", os.Getenv("AGENT_MODE") == "1", "run in agent mode")
	flag.Parse()

	if *agentMode {
		agent.RunClient(agent.ClientConfig{
			Server:            os.Getenv("AGENT_SERVER"),
			Token:             os.Getenv("AGENT_TOKEN"),
			Name:              os.Getenv("AGENT_NAME"),
			Dir:               os.Getenv("AGENT_DIR"),
			Connections:       envInt("AGENT_CONNECTIONS"),
			ClashSubscribeURL: os.Getenv("AGENT_CLASH_SUBSCRIBE"),
			ClashFilter:       os.Getenv("AGENT_CLASH_FILTER"),
		})
		return
	}

	configDir := os.Getenv("CONFIG_DIR")
	if configDir == "" {
		configDir = "."
	}

	// Load configuration.
	store, err := config.NewStore(configDir)
	if err != nil {
		log.Fatalf("Failed to init config store: %v", err)
	}

	// Read embedded template.
	tmpl, err := templateFS.ReadFile("web/templates/index.html")
	if err != nil {
		log.Fatalf("Failed to read embedded template: %v", err)
	}

	// Create download manager with persisted task history.
	tasksFile := ""
	if configDir != "" {
		tasksFile = filepath.Join(configDir, "tasks.json")
	}
	mgr := download.NewManager(store, tasksFile)

	// Agent hub: agents dial in, transfers created after downloads finish.
	// Transfer history is persisted so sync jobs survive restarts.
	transfersFile := ""
	if configDir != "" {
		transfersFile = filepath.Join(configDir, "transfers.json")
	}
	hub := agent.NewHub(store, mgr, transfersFile)
	mgr.SetOnTaskDone(hub.OnDownloadDone)

	// Create WebSocket handler.
	wsHandler := ws.NewHandler(mgr)

	// Create and start server.
	srv := server.New(store, mgr, wsHandler, hub, tmpl)

	// Clash subscription: refresh hourly so node lists stay current.
	go func() {
		for {
			time.Sleep(1 * time.Hour)
			cfg := store.Get()
			if !cfg.ClashEnabled || cfg.ClashSubscribeURL == "" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			yaml, err := clash.FetchSubscription(ctx, cfg.ClashSubscribeURL)
			cancel()
			if err != nil {
				log.Printf("Hourly subscription refresh failed: %v", err)
				continue
			}
			if yaml == cfg.ClashYAML {
				continue // unchanged
			}
			if secret := clash.ExtractSecret(yaml); secret != "" {
				cfg.ClashSecret = secret
			}
			cfg.ClashYAML = yaml
			if err := store.Update(cfg); err != nil {
				log.Printf("Failed to persist refreshed subscription: %v", err)
				continue
			}
			log.Printf("Subscription auto-refreshed: %d bytes", len(yaml))
			mgr.InvalidateClashCache()
			go func() {
				c := clash.New(cfg.ClashAPI, cfg.ClashSecret)
				if err := c.UploadConfig(clash.SanitizePayload(yaml)); err != nil {
					log.Printf("Failed to push refreshed subscription: %v", err)
				}
			}()
		}
	}()

	// Push the stored clash config on startup (the mihomo sidecar starts
	// empty and needs the user's config after every restart).
	go func() {
		cfg := store.Get()
		if !cfg.ClashEnabled || cfg.ClashYAML == "" {
			return
		}
		c := clash.New(cfg.ClashAPI, cfg.ClashSecret)
		for range 10 {
			if err := c.UploadConfig(clash.SanitizePayload(cfg.ClashYAML)); err == nil {
				log.Printf("Clash config pushed on startup")
				return
			} else {
				log.Printf("Clash config push retry: %v", err)
			}
			time.Sleep(5 * time.Second)
		}
		log.Printf("Giving up pushing clash config — push it again from the config page")
	}()

	log.Printf("go-m3u8 web service starting...")
	log.Printf("Open http://localhost:%d in your browser", store.Get().Port)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
