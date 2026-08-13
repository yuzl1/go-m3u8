package main

import (
	"embed"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/yuzl1/go-m3u8/internal/agent"
	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"
	"github.com/yuzl1/go-m3u8/internal/server"
	"github.com/yuzl1/go-m3u8/internal/ws"
)

//go:embed web/templates/index.html
var templateFS embed.FS

func main() {
	// Agent mode: run only the sync client, no web UI / download manager.
	// Triggered with -agent flag or AGENT_MODE=1. Default is main server mode.
	agentMode := flag.Bool("agent", os.Getenv("AGENT_MODE") == "1", "run in agent mode")
	flag.Parse()

	if *agentMode {
		agent.RunClient(agent.ClientConfig{
			Server: os.Getenv("AGENT_SERVER"),
			Token:  os.Getenv("AGENT_TOKEN"),
			Name:   os.Getenv("AGENT_NAME"),
			Dir:    os.Getenv("AGENT_DIR"),
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
	hub := agent.NewHub(store, mgr)
	mgr.SetOnTaskDone(hub.OnDownloadDone)

	// Create WebSocket handler.
	wsHandler := ws.NewHandler(mgr)

	// Create and start server.
	srv := server.New(store, mgr, wsHandler, hub, tmpl)

	log.Printf("go-m3u8 web service starting...")
	log.Printf("Open http://localhost:%d in your browser", store.Get().Port)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
