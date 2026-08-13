package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// ClientConfig configures agent mode.
type ClientConfig struct {
	Server      string // ws://main-host:port/agent/ws
	Token       string
	Name        string
	Dir         string // where received files are stored
	Connections int    // override chunk connections (0 = use server value)
}

// RunClient runs in agent mode forever: connects to the main server,
// handles transfer assignments, and reconnects on failure.
// The agent has no public IP — every connection is dialed OUT.
func RunClient(cfg ClientConfig) {
	if cfg.Server == "" {
		log.Fatal("AGENT_SERVER is required in agent mode (e.g. ws://main:5000/agent/ws)")
	}
	if cfg.Token == "" {
		log.Fatal("AGENT_TOKEN is required in agent mode (see the agent tab in the main server UI)")
	}
	if cfg.Dir == "" {
		cfg.Dir = "/downloads"
	}
	if cfg.Name == "" {
		cfg.Name, _ = os.Hostname()
	}
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		log.Fatalf("Failed to create agent dir %s: %v", cfg.Dir, err)
	}

	// File pull goes over plain HTTP on the same host as the WS URL.
	// Derive scheme+host only — the /agent/ws path must NOT carry over,
	// otherwise requests hit the wrong handler.
	u, err := url.Parse(cfg.Server)
	if err != nil {
		log.Fatalf("Invalid AGENT_SERVER %q: %v", cfg.Server, err)
	}
	scheme := "http"
	if u.Scheme == "wss" {
		scheme = "https"
	}
	httpBase := scheme + "://" + u.Host

	for {
		if err := runOnce(cfg, httpBase); err != nil {
			log.Printf("Agent connection error: %v — reconnecting in 5s", err)
		}
		time.Sleep(5 * time.Second)
	}
}

// runOnce maintains a single control-channel session until it drops.
// In-flight pulls are awaited before the session returns so a reconnect
// session never races with leftovers from the previous one.
func runOnce(cfg ClientConfig, httpBase string) error {
	wsURL := cfg.Server + "?token=" + url.QueryEscape(cfg.Token) + "&name=" + url.QueryEscape(cfg.Name)
	conn, err := websocket.Dial(wsURL, "", "http://localhost/")
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("Agent %q connected to %s, saving to %s", cfg.Name, cfg.Server, cfg.Dir)

	var wg sync.WaitGroup
	defer wg.Wait() // wait for in-flight pulls before reconnecting

	for {
		var msg Message
		if err := websocket.JSON.Receive(conn, &msg); err != nil {
			return err
		}
		switch msg.Type {
		case "assign":
			info := msg.Transfer
			if info == nil {
				continue
			}
			log.Printf("Received transfer %s: %s (%d bytes)", info.ID, info.FileName, info.Size)
			wg.Go(func() {
				err := pullFile(httpBase, cfg, info)
				reply := &Message{Type: "done", TransferID: info.ID}
				if err != nil {
					log.Printf("Transfer %s failed: %v", info.ID, err)
					reply = &Message{Type: "failed", TransferID: info.ID, Error: err.Error()}
				} else {
					log.Printf("Transfer %s completed: %s", info.ID, info.FileName)
				}
				if serr := websocket.JSON.Send(conn, reply); serr != nil {
					log.Printf("Failed to report transfer %s: %v", info.ID, serr)
				}
			})
		case "ping":
			if err := websocket.JSON.Send(conn, &Message{Type: "pong"}); err != nil {
				return err
			}
		}
	}
}

// copyBufSize is the buffer used when streaming file data.
const copyBufSize = 256 * 1024

// chunkManifest records which parts of a .part file are complete, enabling
// resume across retries and reconnections. Stored as <file>.part.json.
type chunkManifest struct {
	TransferID string        `json:"transfer_id"`
	Size       int64         `json:"size"`
	Single     bool          `json:"single"` // sequential single-connection download
	Chunks     []chunkStatus `json:"chunks"`
}

type chunkStatus struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  bool  `json:"done"`
}

// pullFile downloads a transfer from the main server to the local dir.
// Uses parallel chunked (HTTP Range) downloads when possible — much
// faster than a single connection over high-latency links. Failed
// attempts keep partial data (chunk manifest) and resume on retry.
func pullFile(httpBase string, cfg ClientConfig, info *TransferInfo) error {
	conns := cfg.Connections
	if conns <= 0 {
		conns = info.Connections
	}
	if conns <= 1 || info.Size <= 0 {
		return pullSingle(httpBase, cfg, info)
	}
	if err := pullMulti(httpBase, cfg, info, conns); err != nil {
		if strings.Contains(err.Error(), "server ignored range") {
			// Server doesn't support ranged pulls; start single from zero.
			os.Remove(filepath.Join(cfg.Dir, info.FileName+".part"))
			os.Remove(filepath.Join(cfg.Dir, info.FileName+".part.json"))
			return pullSingle(httpBase, cfg, info)
		}
		return err
	}
	return nil
}

// loadManifest reads a chunk manifest; nil when absent or corrupt.
func loadManifest(path string) *chunkManifest {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m chunkManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

// saveManifest writes the manifest atomically (tmp + rename).
func saveManifest(path string, m *chunkManifest) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	os.Rename(tmp, path)
}

// pullSingle downloads the whole file over one connection, resuming from
// the valid prefix of a previous attempt when the manifest allows.
func pullSingle(httpBase string, cfg ClientConfig, info *TransferInfo) error {
	part := filepath.Join(cfg.Dir, info.FileName+".part")
	final := filepath.Join(cfg.Dir, info.FileName)
	manifestPath := part + ".json"

	// Resume: only trust a manifest recorded by a previous single-connection
	// attempt for the same transfer — the file is sequential then.
	offset := int64(0)
	if m := loadManifest(manifestPath); m != nil && m.TransferID == info.ID && m.Size == info.Size && m.Single {
		if st, err := os.Stat(part); err == nil {
			if st.Size() == info.Size {
				// Already complete from a previous attempt.
				os.Remove(manifestPath)
				if err := os.Rename(part, final); err != nil {
					return err
				}
				return nil
			}
			if st.Size() > 0 && st.Size() < info.Size {
				offset = st.Size()
				log.Printf("Transfer %s: resuming from byte %d/%d", info.ID, offset, info.Size)
			}
		}
	} else {
		os.Remove(part)
	}

	fileURL := httpBase + "/agent/files/" + info.ID + "?token=" + url.QueryEscape(cfg.Token)
	var resp *http.Response
	var err error
	if offset > 0 {
		req, reqErr := http.NewRequest(http.MethodGet, fileURL, nil)
		if reqErr != nil {
			return reqErr
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		resp, err = http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return fmt.Errorf("HTTP %d (range not supported)", resp.StatusCode)
		}
	} else {
		resp, err = http.Get(fileURL)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	out, err := os.OpenFile(part, flags, 0644)
	if err != nil {
		return err
	}
	_, cerr := io.CopyBuffer(out, resp.Body, make([]byte, copyBufSize))
	closeErr := out.Close()
	if cerr != nil || closeErr != nil {
		// Keep the valid prefix + manifest so the next attempt resumes.
		saveManifest(manifestPath, &chunkManifest{TransferID: info.ID, Size: info.Size, Single: true})
		if cerr != nil {
			return cerr
		}
		return closeErr
	}

	st, statErr := os.Stat(part)
	if statErr != nil || st.Size() != info.Size {
		saveManifest(manifestPath, &chunkManifest{TransferID: info.ID, Size: info.Size, Single: true})
		return fmt.Errorf("size mismatch: want %d", info.Size)
	}
	os.Remove(manifestPath)
	if err := os.Rename(part, final); err != nil {
		return err
	}
	return nil
}

// pullMulti downloads the file in parallel chunks over N connections,
// resuming chunks already completed in a previous attempt.
func pullMulti(httpBase string, cfg ClientConfig, info *TransferInfo, conns int) error {
	part := filepath.Join(cfg.Dir, info.FileName+".part")
	final := filepath.Join(cfg.Dir, info.FileName)
	manifestPath := part + ".json"

	chunk := (info.Size + int64(conns) - 1) / int64(conns)
	chunks := make([]chunkStatus, 0, conns)
	for i := range conns {
		start := int64(i) * chunk
		if start >= info.Size {
			break
		}
		end := min(start+chunk-1, info.Size-1)
		chunks = append(chunks, chunkStatus{Start: start, End: end})
	}

	// Resume: mark chunks already complete in a previous attempt.
	if m := loadManifest(manifestPath); m != nil && m.TransferID == info.ID && m.Size == info.Size && !m.Single {
		done := 0
		for i := range chunks {
			for _, c := range m.Chunks {
				if c.Done && c.Start == chunks[i].Start && c.End == chunks[i].End {
					chunks[i].Done = true
					done++
					break
				}
			}
		}
		if done > 0 {
			log.Printf("Transfer %s: resuming, %d/%d chunks already complete", info.ID, done, len(chunks))
		}
	}

	out, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	// Pre-allocate so chunks can write at their offsets concurrently.
	// (Truncating to the same size preserves already-written data.)
	if err := out.Truncate(info.Size); err != nil {
		out.Close()
		os.Remove(part)
		os.Remove(manifestPath)
		return err
	}

	var mu sync.Mutex
	errs := make(chan error, conns)
	var wg sync.WaitGroup
	for i := range chunks {
		if chunks[i].Done {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := &chunks[idx]
			if err := pullChunk(httpBase, cfg, info, out, c.Start, c.End); err != nil {
				errs <- fmt.Errorf("chunk %d-%d: %w", c.Start, c.End, err)
				return
			}
			c.Done = true
			mu.Lock()
			saveManifest(manifestPath, &chunkManifest{TransferID: info.ID, Size: info.Size, Chunks: chunks})
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errs)

	var firstErr error
	for err := range errs {
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		// Keep .part + manifest so the next attempt resumes.
		out.Close()
		return firstErr
	}
	if err := out.Close(); err != nil {
		return err
	}

	// Full size sanity check before publishing the file.
	if st, err := os.Stat(part); err != nil || st.Size() != info.Size {
		return fmt.Errorf("size mismatch: want %d", info.Size)
	}
	os.Remove(manifestPath)
	if err := os.Rename(part, final); err != nil {
		return err
	}
	return nil
}

// offsetWriter writes into a file at a fixed offset, capping total bytes.
// os.File.WriteAt is safe for concurrent use at different offsets, which
// lets chunks be downloaded in parallel.
type offsetWriter struct {
	out     *os.File
	offset  int64
	limit   int64
	written int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	if w.written >= w.limit {
		return 0, io.EOF
	}
	if int64(len(p)) > w.limit-w.written {
		p = p[:w.limit-w.written]
	}
	n, err := w.out.WriteAt(p, w.offset+w.written)
	w.written += int64(n)
	return n, err
}

// pullChunk downloads one byte range with retries.
func pullChunk(httpBase string, cfg ClientConfig, info *TransferInfo, out *os.File, start, end int64) error {
	fileURL := httpBase + "/agent/files/" + info.ID + "?token=" + url.QueryEscape(cfg.Token)
	var lastErr error
	for range 3 {
		req, err := http.NewRequest(http.MethodGet, fileURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusPartialContent {
			sw := &offsetWriter{out: out, offset: start, limit: end - start + 1}
			_, err := io.CopyBuffer(sw, resp.Body, make([]byte, copyBufSize))
			resp.Body.Close()
			if err != nil {
				lastErr = err
				continue
			}
			return nil
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			// Server ignored the Range header — caller falls back to single.
			return fmt.Errorf("server ignored range")
		}
		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return lastErr
}
