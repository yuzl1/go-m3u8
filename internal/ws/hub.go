package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"go-m3u8/internal/download"

	"golang.org/x/net/websocket"
)

// Handler bridges the download manager to WebSocket clients.
type Handler struct {
	manager *download.Manager
	mu      sync.Mutex
	conns   map[*websocket.Conn]struct{}
}

// NewHandler creates a new WebSocket handler.
func NewHandler(mgr *download.Manager) *Handler {
	return &Handler{
		manager: mgr,
		conns:   make(map[*websocket.Conn]struct{}),
	}
}

// ServeHTTP handles WebSocket connections at /ws.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	websocket.Handler(func(conn *websocket.Conn) {
		h.mu.Lock()
		h.conns[conn] = struct{}{}
		h.mu.Unlock()

		defer func() {
			h.mu.Lock()
			delete(h.conns, conn)
			h.mu.Unlock()
			conn.Close()
		}()

		// Subscribe to task updates.
		ch := h.manager.Subscribe()
		defer h.manager.Unsubscribe(ch)

		// Send current task list on connect.
		tasks := h.manager.List()
		msg, _ := json.Marshal(map[string]any{
			"type":  "snapshot",
			"tasks": tasks,
		})
		if err := websocket.Message.Send(conn, string(msg)); err != nil {
			return
		}

		// Stream updates.
		for task := range ch {
			msg, err := json.Marshal(map[string]any{
				"type": "update",
				"task": task,
			})
			if err != nil {
				continue
			}
			if err := websocket.Message.Send(conn, string(msg)); err != nil {
				return
			}
		}
	}).ServeHTTP(w, r)
}

// Broadcast sends a message to all connected clients.
func (h *Handler) Broadcast(data any) {
	msg, err := json.Marshal(data)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.conns {
		if err := websocket.Message.Send(conn, string(msg)); err != nil {
			log.Printf("ws send error: %v", err)
		}
	}
}
