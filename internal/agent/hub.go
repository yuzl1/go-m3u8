package agent

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/download"

	"golang.org/x/net/websocket"
)

// maxAttempts is how many times a transfer is retried before giving up.
const maxAttempts = 3

// Hub tracks connected agents and orchestrates file transfers on the
// MAIN server. Agents dial in (they have no public IP), receive transfer
// assignments over the WebSocket control channel, and pull file bytes
// via HTTP GET /agent/files/{id}.
type Hub struct {
	mu         sync.RWMutex
	agents     map[string]*Agent
	agentConns map[string]*websocket.Conn
	transfers  map[string]*Transfer
	order      []string // transfer insertion order

	cfgStore *config.Store
	manager  *download.Manager
}

// NewHub creates the agent hub and ensures a shared token exists.
func NewHub(cfgStore *config.Store, mgr *download.Manager) *Hub {
	h := &Hub{
		agents:     make(map[string]*Agent),
		agentConns: make(map[string]*websocket.Conn),
		transfers:  make(map[string]*Transfer),
		cfgStore:   cfgStore,
		manager:    mgr,
	}
	h.ensureToken()
	return h
}

// ensureToken generates and persists a random agent token on first use.
func (h *Hub) ensureToken() {
	cfg := h.cfgStore.Get()
	if cfg.AgentToken == "" {
		cfg.AgentToken = newID()
		if err := h.cfgStore.Update(cfg); err != nil {
			log.Printf("Failed to persist agent token: %v", err)
		}
		log.Printf("Generated agent token: %s", cfg.AgentToken)
	}
}

// OnDownloadDone is called by the download manager after a task
// completes successfully. It creates a transfer for the output file.
func (h *Hub) OnDownloadDone(task *download.Task) {
	cfg := h.cfgStore.Get()
	if !cfg.AgentEnabled || !cfg.SyncAfterDownload {
		return
	}
	if task.OutputFile == "" {
		return
	}
	st, err := os.Stat(task.OutputFile)
	if err != nil {
		log.Printf("Transfer skipped, output missing: %v", err)
		return
	}
	t := &Transfer{
		ID:        newID(),
		TaskID:    task.ID,
		FileName:  filepath.Base(task.OutputFile),
		FilePath:  task.OutputFile,
		Size:      st.Size(),
		Status:    "waiting",
		CreatedAt: time.Now(),
	}

	h.mu.Lock()
	h.transfers[t.ID] = t
	h.order = append(h.order, t.ID)
	h.mu.Unlock()
	log.Printf("Transfer created: %s (%s, %d bytes) from task %s", t.ID, t.FileName, t.Size, t.TaskID)

	h.assign(t)
}

// Agents returns all registered agents.
func (h *Hub) Agents() []*Agent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Agent, 0, len(h.agents))
	for _, a := range h.agents {
		cp := *a
		out = append(out, &cp)
	}
	return out
}

// Transfers returns all transfers in insertion order.
func (h *Hub) Transfers() []*Transfer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Transfer, 0, len(h.order))
	for _, id := range h.order {
		if t, ok := h.transfers[id]; ok {
			cp := *t
			out = append(out, &cp)
		}
	}
	return out
}

// HandleWS handles the agent control channel at /agent/ws.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	cfg := h.cfgStore.Get()
	if !cfg.AgentEnabled {
		http.Error(w, "agent hub disabled", http.StatusForbidden)
		return
	}
	if cfg.AgentToken == "" || r.URL.Query().Get("token") != cfg.AgentToken {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "agent-" + newID()[:6]
	}

	websocket.Handler(func(conn *websocket.Conn) {
		a := &Agent{
			ID:          newID(),
			Name:        name,
			Remote:      conn.Request().RemoteAddr,
			Online:      true,
			ConnectedAt: time.Now(),
			LastSeen:    time.Now(),
		}

		h.mu.Lock()
		h.agents[a.ID] = a
		h.agentConns[a.ID] = conn
		h.mu.Unlock()
		log.Printf("Agent connected: %s (%s) from %s", a.Name, a.ID, a.Remote)

		defer func() {
			h.mu.Lock()
			a.Online = false
			delete(h.agentConns, a.ID)
			h.mu.Unlock()
			h.onAgentDisconnect(a)
			log.Printf("Agent disconnected: %s (%s)", a.Name, a.ID)
		}()

		// Keepalive: ping every 30s; agent must pong.
		stop := make(chan struct{})
		defer close(stop)
		go h.pingLoop(a, conn, stop)

		// Assign any queued transfers now that an agent is free.
		go h.drainQueue()

		for {
			var msg Message
			if err := websocket.JSON.Receive(conn, &msg); err != nil {
				return
			}
			a.LastSeen = time.Now()
			h.handleMessage(a, &msg)
		}
	}).ServeHTTP(w, r)
}

// ServeFile streams a transfer's file to the pulling agent.
func (h *Hub) ServeFile(w http.ResponseWriter, r *http.Request) {
	cfg := h.cfgStore.Get()
	if r.URL.Query().Get("token") != cfg.AgentToken {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/agent/files/")
	id = strings.TrimSuffix(id, "/")

	h.mu.RLock()
	t := h.transfers[id]
	h.mu.RUnlock()
	if t == nil {
		http.Error(w, "transfer not found", http.StatusNotFound)
		return
	}
	if t.Status != "transferring" {
		http.Error(w, fmt.Sprintf("transfer not active (status: %s)", t.Status), http.StatusConflict)
		return
	}

	f, err := os.Open(filepath.Clean(t.FilePath))
	if err != nil {
		http.Error(w, "file not found on server", http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Disposition", `attachment; filename="`+t.FileName+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(t.Size, 10))

	// Stream to the client while counting bytes for progress display.
	counter := &progressWriter{hub: h, transferID: id}
	if _, err := io.Copy(io.MultiWriter(w, counter), f); err != nil {
		log.Printf("Transfer %s stream error: %v", id, err)
	}
}

// progressWriter updates the transfer's byte count as data flows.
type progressWriter struct {
	hub        *Hub
	transferID string
}

func (p *progressWriter) Write(b []byte) (int, error) {
	p.hub.mu.Lock()
	if t := p.hub.transfers[p.transferID]; t != nil {
		t.Transferred += int64(len(b))
	}
	p.hub.mu.Unlock()
	return len(b), nil
}

func (h *Hub) pingLoop(a *Agent, conn *websocket.Conn, stop chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if time.Since(a.LastSeen) > 90*time.Second {
				conn.Close() // unresponsive
				return
			}
			if err := websocket.JSON.Send(conn, &Message{Type: "ping"}); err != nil {
				return
			}
		}
	}
}

func (h *Hub) handleMessage(a *Agent, msg *Message) {
	switch msg.Type {
	case "hello":
		if msg.AgentName != "" {
			h.mu.Lock()
			a.Name = msg.AgentName
			h.mu.Unlock()
		}
	case "pong":
		// keepalive only
	case "done":
		h.onTransferDone(a, msg.TransferID)
	case "failed":
		h.onTransferFailed(a, msg.TransferID, msg.Error)
	}
}

// onTransferDone marks the transfer complete and deletes the local file
// (per config) to reclaim disk on the main server.
func (h *Hub) onTransferDone(a *Agent, transferID string) {
	h.mu.Lock()
	t := h.transfers[transferID]
	h.mu.Unlock()
	if t == nil || t.Status == "done" {
		return
	}
	t.Status = "done"
	t.Transferred = t.Size
	t.DoneAt = time.Now()

	cfg := h.cfgStore.Get()
	if cfg.DeleteAfterSync {
		if err := os.Remove(t.FilePath); err != nil && !os.IsNotExist(err) {
			log.Printf("Failed to delete %s after sync: %v", t.FilePath, err)
		} else {
			log.Printf("Deleted %s after sync to %s", t.FilePath, a.Name)
		}
	}
	if t.TaskID != "" {
		h.manager.MarkSynced(t.TaskID, a.Name)
	}

	h.clearActive(a)
	h.drainQueue()
}

func (h *Hub) onTransferFailed(a *Agent, transferID, errMsg string) {
	h.mu.Lock()
	t := h.transfers[transferID]
	h.mu.Unlock()
	if t == nil || t.Status == "done" {
		return
	}
	t.Error = errMsg
	h.clearActive(a)

	if t.Attempts >= maxAttempts {
		t.Status = "failed"
		log.Printf("Transfer %s failed permanently after %d attempts: %s", t.ID, t.Attempts, errMsg)
		return
	}
	t.Status = "waiting"
	log.Printf("Transfer %s failed (attempt %d): %s — will retry", t.ID, t.Attempts, errMsg)
	h.drainQueue()
}

// onAgentDisconnect fails the agent's active transfer and re-queues it.
func (h *Hub) onAgentDisconnect(a *Agent) {
	if a.ActiveTransfer != "" {
		h.onTransferFailed(a, a.ActiveTransfer, "agent disconnected")
		a.ActiveTransfer = ""
	}
}

func (h *Hub) clearActive(a *Agent) {
	h.mu.Lock()
	if a.ActiveTransfer != "" {
		a.ActiveTransfer = ""
	}
	h.mu.Unlock()
}

// assign tries to give a transfer to an idle online agent.
func (h *Hub) assign(t *Transfer) bool {
	// Sanity: file must still exist.
	if _, err := os.Stat(t.FilePath); err != nil {
		t.Status = "failed"
		t.Error = "file missing on server: " + err.Error()
		return false
	}

	h.mu.Lock()
	for _, a := range h.agents {
		if !a.Online || a.ActiveTransfer != "" {
			continue
		}
		a.ActiveTransfer = t.ID
		t.AgentID = a.ID
		t.AgentName = a.Name
		t.Status = "transferring"
		t.Attempts++
		msg := &Message{Type: "assign", Transfer: &TransferInfo{ID: t.ID, FileName: t.FileName, Size: t.Size}}
		conn := h.agentConns[a.ID]
		h.mu.Unlock()

		if conn == nil {
			h.onTransferFailed(a, t.ID, "agent connection not ready")
			return false
		}
		if err := websocket.JSON.Send(conn, msg); err != nil {
			log.Printf("Failed to send assign to %s: %v", a.Name, err)
			h.onTransferFailed(a, t.ID, "send failed: "+err.Error())
			return false
		}
		log.Printf("Transfer %s (%s) assigned to agent %s (attempt %d)", t.ID, t.FileName, a.Name, t.Attempts)
		return true
	}
	h.mu.Unlock()
	return false
}

// drainQueue assigns all waiting transfers while agents are free.
func (h *Hub) drainQueue() {
	for {
		h.mu.RLock()
		var next *Transfer
		for _, id := range h.order {
			if t := h.transfers[id]; t.Status == "waiting" {
				next = t
				break
			}
		}
		h.mu.RUnlock()
		if next == nil {
			return
		}
		if !h.assign(next) {
			return // no free agent; stop
		}
	}
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
