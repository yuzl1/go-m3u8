package download

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuzl1/go-m3u8/internal/clash"
	"github.com/yuzl1/go-m3u8/internal/config"
	"github.com/yuzl1/go-m3u8/internal/translate"
)

// StatusChange is broadcast when a task changes state.
type StatusChange struct {
	Task *Task `json:"task"`
}

// Manager orchestrates download tasks.
type Manager struct {
	mu        sync.RWMutex
	tasks     map[string]*Task
	order     []string // keep insertion order
	cfgStore  *config.Store
	tasksFile string // persisted task history, "" disables persistence

	// onTaskDone is invoked after a download completes successfully.
	onTaskDone func(*Task)

	// proxyCounter round-robins the clash node list across tasks.
	proxyCounter atomic.Uint32

	// clash node rotation cache (healthy nodes only, via delay test).
	clashMu     sync.Mutex
	clashGroup  string
	clashNodes  []string         // healthy node names, rotation order
	clashDelays map[string]int64 // delay test results (0 = dead)
	clashExpiry time.Time

	// Per-task clash instances: every download gets its own mihomo
	// process + node + port, so concurrent downloads are fully isolated.
	clashRunMu   sync.Mutex
	clashNodeIdx int // round-robin node picker
	clashPorts   *portAllocator

	// Concurrency control: buffered channel as semaphore.
	sem chan struct{}

	// Broadcast channel for task status changes.
	broadcast chan *Task
	subs      map[chan *Task]struct{}
	subsMu    sync.RWMutex
}

// NewManager creates a new Manager. Task history is persisted to
// tasksFile (JSON) so downloads survive server/container restarts.
func NewManager(cfgStore *config.Store, tasksFile string) *Manager {
	m := &Manager{
		tasks:      make(map[string]*Task),
		broadcast:  make(chan *Task, 64),
		subs:       make(map[chan *Task]struct{}),
		cfgStore:   cfgStore,
		tasksFile:  tasksFile,
		clashPorts: newPortAllocator(7910, 20),
	}
	// Default max concurrent = 3; tightened by config.
	m.sem = make(chan struct{}, 3)
	m.applySem()
	m.loadTasks()
	go m.dispatch()
	return m
}

// loadTasks restores task history from disk. Tasks that were still
// running when the server stopped are marked as interrupted.
func (m *Manager) loadTasks() {
	if m.tasksFile == "" {
		return
	}
	data, err := os.ReadFile(m.tasksFile)
	if err != nil {
		return
	}
	var tasks []*Task
	if err := json.Unmarshal(data, &tasks); err != nil {
		log.Printf("Failed to load task history: %v", err)
		return
	}
	for _, t := range tasks {
		if t == nil || t.ID == "" {
			continue
		}
		if t.Status == "pending" || t.Status == "downloading" || t.Status == "merging" {
			t.Status = "cancelled"
			t.Progress = "服务重启，任务中断"
		}
		m.tasks[t.ID] = t
		m.order = append(m.order, t.ID)
	}
	log.Printf("Restored %d tasks from history", len(tasks))
}

// saveTasks persists the current task list to disk.
func (m *Manager) saveTasks() {
	if m.tasksFile == "" {
		return
	}
	m.mu.RLock()
	data, err := json.Marshal(m.List())
	m.mu.RUnlock()
	if err != nil {
		return
	}
	if err := os.WriteFile(m.tasksFile, data, 0644); err != nil {
		log.Printf("Failed to persist tasks: %v", err)
	}
}

func (m *Manager) applySem() {
	cfg := m.cfgStore.Get()
	n := cfg.MaxConcurrent
	if n <= 0 {
		n = 3
	}
	// Drain and recreate semaphore with new capacity.
	// This is a best-effort approach; it won't kill running downloads.
	m.sem = make(chan struct{}, n)
}

// RefreshSem updates the semaphore capacity from config.
func (m *Manager) RefreshSem() {
	m.applySem()
}

// SetOnTaskDone registers a callback invoked after a download succeeds.
func (m *Manager) SetOnTaskDone(fn func(*Task)) {
	m.onTaskDone = fn
}

// MarkSynced flags a task as transferred to an agent node.
func (m *Manager) MarkSynced(id, agentName string) {
	m.mu.Lock()
	t := m.tasks[id]
	m.mu.Unlock()
	if t == nil {
		return
	}
	t.Synced = true
	t.SyncedTo = agentName
	t.Progress = "已同步到 " + agentName
	m.broadcast <- t
}

// Submit creates a new download task and enqueues it.
func (m *Manager) Submit(url, title string, headers map[string]string, baseURL, saveDir string) *Task {
	id := newID()
	task := &Task{
		ID:          id,
		URL:         url,
		Title:       title,
		Status:      "pending",
		CreatedAt:   time.Now(),
		Headers:     headers,
		BaseURL:     baseURL,
		SaveDir:     saveDir,
		ProxyOffset: int(m.proxyCounter.Add(1)) - 1,
	}

	m.mu.Lock()
	m.tasks[id] = task
	m.order = append(m.order, id)
	m.mu.Unlock()

	// Send to broadcast for WebSocket.
	m.broadcast <- task

	// Enqueue the download.
	go m.enqueue(task)
	return task
}

// List returns all tasks in insertion order.
func (m *Manager) List() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Task, 0, len(m.order))
	for _, id := range m.order {
		if t, ok := m.tasks[id]; ok {
			out = append(out, t)
		}
	}
	return out
}

// Get returns a single task by ID.
func (m *Manager) Get(id string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[id]
}

// Cancel cancels a pending or running task.
func (m *Manager) Cancel(id string) error {
	m.mu.Lock()
	task, ok := m.tasks[id]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}
	if task.Status != "pending" && task.Status != "downloading" {
		return fmt.Errorf("cannot cancel task in status %q", task.Status)
	}
	// Mark cancelled; the executor will pick it up if already running.
	task.Status = "cancelled"
	m.broadcast <- task
	return nil
}

// Retry re-submits a failed or cancelled task.
func (m *Manager) Retry(id string) (*Task, error) {
	m.mu.Lock()
	task, ok := m.tasks[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("task not found: %s", id)
	}
	if task.Status != "failed" && task.Status != "cancelled" && task.Status != "done" {
		return nil, fmt.Errorf("cannot retry task in status %q", task.Status)
	}
	// Reset and re-enqueue.
	task.Status = "pending"
	task.Error = ""
	task.Progress = ""
	m.broadcast <- task
	go m.enqueue(task)
	return task, nil
}

// Delete removes a task from history.
// NOTE: saveTasks takes the read lock, so it must be called AFTER
// releasing the write lock — otherwise this deadlocks.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	task, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("task not found: %s", id)
	}
	if task.Status == "downloading" {
		m.mu.Unlock()
		return fmt.Errorf("cannot delete a running task")
	}
	delete(m.tasks, id)
	for i, oid := range m.order {
		if oid == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	m.saveTasks()
	return nil
}

// Subscribe returns a channel that receives task status updates.
func (m *Manager) Subscribe() chan *Task {
	ch := make(chan *Task, 32)
	m.subsMu.Lock()
	m.subs[ch] = struct{}{}
	m.subsMu.Unlock()
	return ch
}

// Unsubscribe removes a subscription.
func (m *Manager) Unsubscribe(ch chan *Task) {
	m.subsMu.Lock()
	delete(m.subs, ch)
	m.subsMu.Unlock()
}

// dispatch reads from the broadcast channel, fans out to subscribers,
// and persists the task list on every change.
func (m *Manager) dispatch() {
	for task := range m.broadcast {
		m.subsMu.RLock()
		for ch := range m.subs {
			select {
			case ch <- task:
			default:
				// drop if subscriber is too slow
			}
		}
		m.subsMu.RUnlock()
		m.saveTasks()
	}
}

// clashHealthyNodes returns the group's node list filtered by mihomo's
// built-in delay test (dead nodes excluded). Results are cached briefly.
func (m *Manager) clashHealthyNodes(cfg *config.Config) ([]string, string, error) {
	c := clash.New(cfg.ClashAPI, cfg.ClashSecret)

	m.clashMu.Lock()
	defer m.clashMu.Unlock()

	if time.Now().Before(m.clashExpiry) {
		return m.clashNodes, m.clashGroup, nil
	}

	group, nodes, err := c.SelectorNodes(cfg.ClashGroup)
	if err != nil {
		return nil, "", err
	}
	// Built-in delay test: dead nodes report 0 / are absent.
	delays, derr := c.TestGroupDelay(group, "", 5000)
	if derr != nil {
		// Delay test unavailable — fall back to all nodes.
		delays = map[string]int64{}
	}
	healthy := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if d, ok := delays[n]; ok && d > 0 {
			healthy = append(healthy, n)
		}
	}

	// The delay test also reports proxy-GROUP members (e.g. "自动选择"
	// url-test groups) — those cannot be used as a node. Keep only REAL
	// proxy names from the config.
	if proxyNames, perr := clash.ParseProxyNames(cfg.ClashYAML); perr == nil && len(proxyNames) > 0 {
		valid := make(map[string]bool, len(proxyNames))
		for _, n := range proxyNames {
			valid[n] = true
		}
		filtered := make([]string, 0, len(healthy))
		for _, n := range healthy {
			if valid[n] {
				filtered = append(filtered, n)
			}
		}
		if len(filtered) > 0 {
			healthy = filtered
		} else {
			// Nothing real survived the delay filter — fall back to all
			// real nodes (health unknown) rather than failing outright.
			healthy = proxyNames
		}
	}

	if len(healthy) == 0 {
		// Nothing passed the test; keep all nodes rather than failing
		// (the test itself may have been blocked).
		healthy = nodes
	}
	m.clashGroup = group
	m.clashNodes = healthy
	m.clashDelays = delays
	m.clashExpiry = time.Now().Add(5 * time.Minute)
	return m.clashNodes, m.clashGroup, nil
}

// startTaskClash spawns a DEDICATED mihomo instance for a download:
// one node, one port, one process. Concurrent downloads get their own
// node and never interfere (no shared selector switching).
func (m *Manager) startTaskClash(cfg *config.Config, nodes []string) (*ClashSession, error) {
	if cfg.ClashYAML == "" {
		return nil, fmt.Errorf("clash config yaml is empty — paste it in the config page")
	}
	m.clashRunMu.Lock()
	if len(nodes) == 0 {
		m.clashRunMu.Unlock()
		return nil, fmt.Errorf("no healthy nodes")
	}
	node := nodes[m.clashNodeIdx%len(nodes)]
	m.clashNodeIdx++
	m.clashRunMu.Unlock()

	block, err := clash.ExtractNode(cfg.ClashYAML, node)
	if err != nil {
		return nil, err
	}
	port, err := m.clashPorts.Alloc()
	if err != nil {
		return nil, err
	}
	instance, err := clash.StartInstance(node, block, port)
	if err != nil {
		m.clashPorts.Free(port)
		return nil, err
	}
	s := &ClashSession{
		Node:  node,
		Proxy: fmt.Sprintf("http://127.0.0.1:%d", port),
		port:  port,
	}
	s.release = func() {
		instance.Stop() // kills the process + removes its temp dir
		m.clashPorts.Free(port)
	}
	return s, nil
}

// ClashSession is a per-task clash instance (own node + process + port).
type ClashSession struct {
	Node    string
	Proxy   string
	port    int
	release func()
}

// Release stops the instance and frees its port.
func (s *ClashSession) Release() {
	if s != nil && s.release != nil {
		s.release()
	}
}

// portAllocator hands out proxy ports from a fixed range.
type portAllocator struct {
	mu   sync.Mutex
	free []int
}

func newPortAllocator(start, n int) *portAllocator {
	free := make([]int, n)
	for i := range n {
		free[i] = start + i
	}
	return &portAllocator{free: free}
}

func (p *portAllocator) Alloc() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.free) == 0 {
		return 0, fmt.Errorf("no free clash proxy ports")
	}
	port := p.free[0]
	p.free = p.free[1:]
	return port, nil
}

func (p *portAllocator) Free(port int) {
	p.mu.Lock()
	p.free = append(p.free, port)
	p.mu.Unlock()
}

// ClashInfo returns cached clash state for the UI status display.
func (m *Manager) ClashInfo() (group string, nodes []string, delays map[string]int64) {
	m.clashMu.Lock()
	defer m.clashMu.Unlock()
	return m.clashGroup, append([]string{}, m.clashNodes...), m.clashDelays
}

// ClashHealthy returns the group name, healthy node list and delay test
// results, refreshing the cache when needed.
func (m *Manager) ClashHealthy(cfg *config.Config) (string, []string, map[string]int64, error) {
	nodes, group, err := m.clashHealthyNodes(cfg)
	if err != nil {
		return "", nil, nil, err
	}
	m.clashMu.Lock()
	delays := m.clashDelays
	m.clashMu.Unlock()
	return group, nodes, delays, nil
}

// InvalidateClashCache forces a fresh node health check on next use.
func (m *Manager) InvalidateClashCache() {
	m.clashMu.Lock()
	m.clashExpiry = time.Time{}
	m.clashMu.Unlock()
}

// translateFilename translates a task filename per config, with a
// generous timeout and retries.
func translateFilename(text string, cfg *config.Config) (string, error) {
	var lastErr error
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		zh, err := translate.Translate(ctx, text, cfg.TranslateTarget, translate.Config{
			Provider: cfg.TranslateProvider,
			APIURL:   cfg.TranslateAPIURL,
			AppID:    cfg.BaiduAppID,
			AppKey:   cfg.BaiduAppKey,
		})
		cancel()
		if err == nil && zh != "" {
			return zh, nil
		}
		lastErr = err
		log.Printf("Translation attempt %d failed for %q: %v", attempt+1, text, err)
	}
	return "", fmt.Errorf("all 3 attempts failed for %q: %w", text, lastErr)
}

// enqueue blocks on the semaphore, then runs the download.
func (m *Manager) enqueue(task *Task) {
	cfg := m.cfgStore.Get()

	// Clash: refresh the healthy node list (delay test) and hand it to
	// the task; Run spawns a dedicated per-task mihomo instance.
	if cfg.ClashEnabled && cfg.ClashYAML != "" {
		nodes, group, err := m.clashHealthyNodes(cfg)
		if err != nil {
			task.Log += fmt.Sprintf("== Clash Proxy ==\nFAILED: %v (continuing without proxy)\n\n", err)
		} else {
			task.ClashNodes = nodes
			task.ClashGroup = group
			task.ClashStart = func() (*ClashSession, error) { return m.startTaskClash(cfg, nodes) }
			task.Log += fmt.Sprintf("== Clash Proxy ==\ngroup: %s\nhealthy nodes: %d\n", group, len(nodes))
		}
	}

	// Filename translation happens HERE, in the pipeline — not in the HTTP
	// handler — so a slow or failing translation service never delays task
	// creation. Sites often expire their m3u8 URLs quickly; the download
	// must be able to start immediately.
	// Every outcome is recorded in the task log so it can be diagnosed
	// from the web UI (任务卡片 → 日志).
	if cfg.TranslateEnabled && task.Title != "" && !task.Translated {
		task.Status = "pending"
		task.Progress = "翻译文件名中..."
		task.OriginalName = task.Title
		task.Log += fmt.Sprintf("== Translation ==\nprovider=%s target=%s source=%q\n",
			cfg.TranslateProvider, cfg.TranslateTarget, task.Title)
		m.broadcast <- task

		zh, err := translateFilename(task.Title, cfg)
		if err == nil {
			task.Title = zh
			task.Translated = true
			task.Log += fmt.Sprintf("translated: %q -> %q\n\n", task.OriginalName, zh)
		} else {
			task.Log += fmt.Sprintf("FAILED: %v (using original name)\n\n", err)
		}
		task.Progress = ""
		m.broadcast <- task
	} else if !task.Translated {
		task.Log += fmt.Sprintf("== Translation ==\nskipped (translate_enabled=%v)\n\n", cfg.TranslateEnabled)
	}

	m.sem <- struct{}{}
	defer func() { <-m.sem }()

	// Check if already cancelled before starting.
	if task.Status == "cancelled" {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor for cancellation.
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			t := m.Get(task.ID)
			if t == nil || t.Status == "cancelled" {
				cancel()
				return
			}
		}
	}()

	statusCh := make(chan *Task, 16)
	done := make(chan error, 1)

	go func() {
		done <- Run(ctx, cfg, task, statusCh)
		close(statusCh)
	}()

	// Forward status updates.
	for t := range statusCh {
		m.mu.Lock()
		m.tasks[t.ID] = t
		m.mu.Unlock()
		m.broadcast <- t
	}

	if err := <-done; err != nil {
		log.Printf("Task %s failed: %v", task.ID, err)
	} else if m.onTaskDone != nil {
		go m.onTaskDone(task)
	}
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
