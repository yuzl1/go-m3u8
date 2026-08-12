package download

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"go-m3u8/internal/config"
)

// StatusChange is broadcast when a task changes state.
type StatusChange struct {
	Task *Task `json:"task"`
}

// Manager orchestrates download tasks.
type Manager struct {
	mu       sync.RWMutex
	tasks    map[string]*Task
	order    []string // keep insertion order
	cfgStore *config.Store

	// Concurrency control: buffered channel as semaphore.
	sem chan struct{}

	// Broadcast channel for task status changes.
	broadcast chan *Task
	subs      map[chan *Task]struct{}
	subsMu    sync.RWMutex
}

// NewManager creates a new Manager.
func NewManager(cfgStore *config.Store) *Manager {
	m := &Manager{
		tasks:     make(map[string]*Task),
		broadcast: make(chan *Task, 64),
		subs:      make(map[chan *Task]struct{}),
		cfgStore:  cfgStore,
	}
	// Default max concurrent = 3; tightened by config.
	m.sem = make(chan struct{}, 3)
	m.applySem()
	go m.dispatch()
	return m
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

// Submit creates a new download task and enqueues it.
func (m *Manager) Submit(url, title string, headers map[string]string, baseURL, saveDir string) *Task {
	id := newID()
	task := &Task{
		ID:        id,
		URL:       url,
		Title:     title,
		Status:    "pending",
		CreatedAt: time.Now(),
		Headers:   headers,
		BaseURL:   baseURL,
		SaveDir:   saveDir,
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

// Delete removes a task from history.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}
	if task.Status == "downloading" {
		return fmt.Errorf("cannot delete a running task")
	}
	delete(m.tasks, id)
	for i, oid := range m.order {
		if oid == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
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

// dispatch reads from the broadcast channel and fans out to subscribers.
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
	}
}

// enqueue blocks on the semaphore, then runs the download.
func (m *Manager) enqueue(task *Task) {
	m.sem <- struct{}{}
	defer func() { <-m.sem }()

	// Check if already cancelled before starting.
	if task.Status == "cancelled" {
		return
	}

	cfg := m.cfgStore.Get()
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
	}
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
