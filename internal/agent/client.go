package agent

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/websocket"
)

// ClientConfig configures agent mode.
type ClientConfig struct {
	Server string // ws://main-host:port/agent/ws
	Token  string
	Name   string
	Dir    string // where received files are stored
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
func runOnce(cfg ClientConfig, httpBase string) error {
	wsURL := cfg.Server + "?token=" + url.QueryEscape(cfg.Token) + "&name=" + url.QueryEscape(cfg.Name)
	conn, err := websocket.Dial(wsURL, "", "http://localhost/")
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("Agent %q connected to %s, saving to %s", cfg.Name, cfg.Server, cfg.Dir)

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
			go func() {
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
			}()
		case "ping":
			if err := websocket.JSON.Send(conn, &Message{Type: "pong"}); err != nil {
				return err
			}
		}
	}
}

// pullFile downloads a transfer from the main server to the local dir.
func pullFile(httpBase string, cfg ClientConfig, info *TransferInfo) error {
	fileURL := httpBase + "/agent/files/" + info.ID + "?token=" + url.QueryEscape(cfg.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Write to .part then rename, so a partial download never looks done.
	part := filepath.Join(cfg.Dir, info.FileName+".part")
	final := filepath.Join(cfg.Dir, info.FileName)
	out, err := os.Create(part)
	if err != nil {
		return err
	}
	n, err := io.Copy(out, resp.Body)
	cerr := out.Close()
	if err != nil {
		os.Remove(part)
		return err
	}
	if cerr != nil {
		os.Remove(part)
		return cerr
	}
	// Size sanity check before publishing the file.
	if info.Size > 0 && n != info.Size {
		os.Remove(part)
		return fmt.Errorf("size mismatch: got %d bytes, want %d", n, info.Size)
	}
	if err := os.Rename(part, final); err != nil {
		return err
	}
	return nil
}
