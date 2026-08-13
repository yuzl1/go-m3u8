package agent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	mu            sync.RWMutex
	agents        map[string]*Agent
	agentConns    map[string]*websocket.Conn
	transfers     map[string]*Transfer
	order         []string // transfer insertion order
	transfersFile string   // persisted transfer history, "" disables persistence

	cfgStore *config.Store
	manager  *download.Manager
}

// NewHub creates the agent hub and ensures a shared token exists.
// Transfer history is persisted to transfersFile so sync jobs survive
// restarts and are re-queued automatically.
func NewHub(cfgStore *config.Store, mgr *download.Manager, transfersFile string) *Hub {
	h := &Hub{
		agents:        make(map[string]*Agent),
		agentConns:    make(map[string]*websocket.Conn),
		transfers:     make(map[string]*Transfer),
		transfersFile: transfersFile,
		cfgStore:      cfgStore,
		manager:       mgr,
	}
	h.ensureToken()
	h.loadTransfers()
	// Periodic snapshot so in-progress byte counts survive restarts.
	go func() {
		for range time.NewTicker(30 * time.Second).C {
			h.saveTransfers()
		}
	}()
	// Re-queue restored transfers once agents connect (or immediately if
	// one is already online).
	go func() {
		time.Sleep(2 * time.Second)
		h.drainQueue()
	}()
	return h
}

// loadTransfers restores transfer history from disk. Interrupted
// transfers go back to "waiting" so they auto-retry on the next agent
// connection; FilePath is reconstructed from the save dir.
func (h *Hub) loadTransfers() {
	if h.transfersFile == "" {
		return
	}
	data, err := os.ReadFile(h.transfersFile)
	if err != nil {
		return
	}
	var ts []*Transfer
	if err := json.Unmarshal(data, &ts); err != nil {
		log.Printf("Failed to load transfer history: %v", err)
		return
	}
	saveDir := h.cfgStore.Get().SaveDir
	for _, t := range ts {
		if t == nil || t.ID == "" {
			continue
		}
		if t.FilePath == "" {
			t.FilePath = filepath.Join(saveDir, t.FileName)
		}
		if t.Status == "transferring" || t.Status == "waiting" {
			t.Status = "waiting"
			t.Error = "服务重启，等待重新传输"
		}
		h.transfers[t.ID] = t
		h.order = append(h.order, t.ID)
	}
	log.Printf("Restored %d transfers from history", len(ts))
}

// saveTransfers persists the current transfer list to disk.
func (h *Hub) saveTransfers() {
	if h.transfersFile == "" {
		return
	}
	h.mu.RLock()
	data, err := json.Marshal(h.Transfers())
	h.mu.RUnlock()
	if err != nil {
		return
	}
	if err := os.WriteFile(h.transfersFile, data, 0644); err != nil {
		log.Printf("Failed to persist transfers: %v", err)
	}
}

// SyncFile creates a manual transfer for a file in the save directory —
// used by the "传输" button in the UI to re-sync failed or missing files.
func (h *Hub) SyncFile(fileName string) (*Transfer, error) {
	name := filepath.Base(fileName) // no path traversal
	if name == "." || name == "/" {
		return nil, fmt.Errorf("invalid file name")
	}
	cfg := h.cfgStore.Get()
	path := filepath.Join(cfg.SaveDir, name)
	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("file not found: %s", name)
	}
	t := &Transfer{
		ID:        newID(),
		FileName:  name,
		FilePath:  path,
		Size:      st.Size(),
		Status:    "waiting",
		CreatedAt: time.Now(),
	}
	h.mu.Lock()
	h.transfers[t.ID] = t
	h.order = append(h.order, t.ID)
	h.mu.Unlock()
	h.saveTransfers()
	log.Printf("Manual transfer created for %s (%d bytes)", name, st.Size())
	h.assign(t)
	return t, nil
}

// RetryTransfer re-queues a failed transfer.
func (h *Hub) RetryTransfer(id string) error {
	h.mu.Lock()
	t := h.transfers[id]
	h.mu.Unlock()
	if t == nil {
		return fmt.Errorf("transfer not found")
	}
	if t.Status == "transferring" {
		return fmt.Errorf("transfer already in progress")
	}
	if _, err := os.Stat(t.FilePath); err != nil {
		t.Status = "failed"
		t.Error = "file missing on server: " + err.Error()
		h.saveTransfers()
		return fmt.Errorf("file missing on server: %v", err)
	}
	t.Status = "waiting"
	t.Error = ""
	h.saveTransfers()
	go h.assign(t)
	return nil
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
	h.saveTransfers()
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

	// ServeContent supports HTTP Range requests — the agent pulls the file
	// in parallel chunks over multiple connections for speed. Bytes read
	// are counted so the UI can show progress.
	counter := &countingReadSeeker{rs: f, hub: h, transferID: id}
	http.ServeContent(w, r, t.FileName, t.CreatedAt, counter)
}

// countingReadSeeker wraps a ReadSeeker and updates the transfer's byte
// count as data is read (works with ServeContent incl. Range requests).
type countingReadSeeker struct {
	rs         io.ReadSeeker
	hub        *Hub
	transferID string
}

func (c *countingReadSeeker) Read(p []byte) (int, error) {
	n, err := c.rs.Read(p)
	if n > 0 {
		c.hub.mu.Lock()
		if t := c.hub.transfers[c.transferID]; t != nil {
			t.Transferred += int64(n)
		}
		c.hub.mu.Unlock()
	}
	return n, err
}

func (c *countingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return c.rs.Seek(offset, whence)
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
	h.saveTransfers()
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
		h.saveTransfers()
		log.Printf("Transfer %s failed permanently after %d attempts: %s", t.ID, t.Attempts, errMsg)
		return
	}
	t.Status = "waiting"
	h.saveTransfers()
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

// syncConnections returns the configured number of parallel chunk
// connections for sync transfers.
func (h *Hub) syncConnections() int {
	n := h.cfgStore.Get().SyncConnections
	if n < 1 || n > 16 {
		return 4
	}
	return n
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
		msg := &Message{Type: "assign", Transfer: &TransferInfo{
			ID:          t.ID,
			FileName:    t.FileName,
			Size:        t.Size,
			Connections: h.syncConnections(),
		}}
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
