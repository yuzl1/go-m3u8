package main

import (
	"embed"
	"log"
	"os"

	"go-m3u8/internal/config"
	"go-m3u8/internal/download"
	"go-m3u8/internal/server"
	"go-m3u8/internal/ws"
)

//go:embed web/templates/index.html
var templateFS embed.FS

func main() {
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

	// Create download manager.
	mgr := download.NewManager(store)

	// Create WebSocket handler.
	wsHandler := ws.NewHandler(mgr)

	// Create and start server.
	srv := server.New(store, mgr, wsHandler, tmpl)

	log.Printf("go-m3u8 web service starting...")
	log.Printf("Open http://localhost:%d in your browser", store.Get().Port)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
