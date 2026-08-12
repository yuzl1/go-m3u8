package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"
	"github.com/yuzl1/go-m3u8/internal/ws"
)

// Server wraps the HTTP server and its dependencies.
type Server struct {
	http    *http.Server
	handler *Handler
	ws      *ws.Handler
	manager *download.Manager
	config  *config.Store
}

// New creates a new Server.
func New(cfgStore *config.Store, manager *download.Manager, wsHandler *ws.Handler, template []byte) *Server {
	h := &Handler{
		Manager:  manager,
		Config:   cfgStore,
		Template: template,
	}

	mux := http.NewServeMux()

	// Page
	mux.HandleFunc("/", h.Index)

	// API
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.GetConfig(w, r)
		case http.MethodPut:
			h.UpdateConfig(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodPost:
			h.Download(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.ListTasks(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			h.DeleteTask(w, r)
		case http.MethodPost:
			h.RetryTask(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// WebSocket
	mux.Handle("/ws", wsHandler)

	// CORS middleware for Cat-Catch
	cors := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	cfg := cfgStore.Get()
	addr := fmt.Sprintf(":%d", cfg.Port)

	return &Server{
		http:    &http.Server{Addr: addr, Handler: cors(mux)},
		handler: h,
		ws:      wsHandler,
		manager: manager,
		config:  cfgStore,
	}
}

// Start begins listening and blocks.
func (s *Server) Start() error {
	log.Printf("Server starting on %s", s.http.Addr)
	return s.http.ListenAndServe()
}
