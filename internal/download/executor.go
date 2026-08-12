package download

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yuzl1/go-m3u8/internal/config"
)

// Task represents a single download job.
type Task struct {
	ID        string            `json:"id"`
	URL       string            `json:"url"`
	Title     string            `json:"title"`
	Status    string            `json:"status"` // pending, downloading, merging, done, failed, cancelled
	Progress  string            `json:"progress"`
	Percent   float64           `json:"percent"`
	Error     string            `json:"error,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	Headers   map[string]string `json:"headers,omitempty"`
	BaseURL   string            `json:"base_url,omitempty"`
	SaveDir   string            `json:"save_dir,omitempty"`
}

// BuildCommand builds the N_m3u8DL-RE command-line for a task.
func BuildCommand(cfg *config.Config, task *Task) (string, []string) {
	args := []string{}

	// Input URL
	args = append(args, task.URL)

	// Save directory
	saveDir := cfg.SaveDir
	if task.SaveDir != "" {
		saveDir = task.SaveDir
	}
	if saveDir != "" {
		args = append(args, "--save-dir", saveDir)
	}

	// Save name
	if task.Title != "" {
		args = append(args, "--save-name", sanitizeFilename(task.Title))
	}

	// Temp directory
	if cfg.TempDir != "" {
		args = append(args, "--tmp-dir", cfg.TempDir)
	}

	// Thread count
	if cfg.ThreadCount > 0 {
		args = append(args, "--thread-count", fmt.Sprintf("%d", cfg.ThreadCount))
	}

	// Auto select
	if cfg.AutoSelect {
		args = append(args, "--auto-select")
	}

	// Delete after done
	if cfg.DelAfterDone {
		args = append(args, "--del-after-done")
	}

	// Concurrent download
	if cfg.Concurrent {
		args = append(args, "-mt")
	}

	// Base URL
	baseURL := cfg.BaseURL
	if task.BaseURL != "" {
		baseURL = task.BaseURL
	}
	if baseURL != "" {
		args = append(args, "--base-url", baseURL)
	}

	// Headers: merge task headers with default headers
	headers := make(map[string]string)
	maps.Copy(headers, cfg.DefaultHeaders)
	maps.Copy(headers, task.Headers)
	for k, v := range headers {
		args = append(args, "--header", fmt.Sprintf("%s: %s", k, v))
	}

	return cfg.Nm3u8dlPath, args
}

// maxRetries is the number of download retries on failure.
const maxRetries = 3

// Run starts N_m3u8DL-RE and blocks until completion.
// It streams stdout/stderr, parses progress and sends status updates.
// Retries up to maxRetries times on failure.
func Run(ctx context.Context, cfg *config.Config, task *Task, statusCh chan<- *Task) error {
	exe, args := BuildCommand(cfg, task)

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			task.Progress = fmt.Sprintf("Retrying (attempt %d/%d)...", attempt, maxRetries)
			task.Percent = 0
			notify(statusCh, task)
		} else {
			task.Status = "downloading"
			task.Progress = "Starting N_m3u8DL-RE..."
			task.Percent = 0
			notify(statusCh, task)
		}

		cmd := exec.CommandContext(ctx, exe, args...)
		err := runStreaming(cmd, task, statusCh)

		if err == nil {
			task.Status = "done"
			task.Progress = "Download complete"
			task.Percent = 100
			task.Error = ""
			notify(statusCh, task)
			return nil
		}

		// Check if cancelled
		if ctx.Err() != nil {
			task.Status = "cancelled"
			task.Progress = ""
			notify(statusCh, task)
			return ctx.Err()
		}

		lastErr = err
		if attempt < maxRetries {
			continue
		}
	}

	task.Status = "failed"
	task.Error = lastErr.Error()
	task.Progress = ""
	task.Percent = 0
	notify(statusCh, task)
	return fmt.Errorf("N_m3u8DL-RE failed after %d attempts: %w", maxRetries, lastErr)
}

var (
	pctRe   = regexp.MustCompile(`(\d+(?:\.\d+)?)\s*%`)
	segRe   = regexp.MustCompile(`(\d+)\s*/\s*(\d+)`)
	ansiRe  = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]`)
)

// runStreaming starts cmd, streams its output line by line, and parses
// progress info (percent or segment counts) into task. Returns the
// command's error, including captured output.
func runStreaming(cmd *exec.Cmd, task *Task, statusCh chan<- *Task) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start %s: %w", cmd.Path, err)
	}

	var mu sync.Mutex
	var output strings.Builder

	lines := make(chan string, 256)
	var wg sync.WaitGroup

	readPipe := func(r io.Reader) {
		defer wg.Done()
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-time.After(time.Second):
				// avoid blocking forever if reader is stuck
			}
		}
		if err := scanner.Err(); err != nil {
			mu.Lock()
			output.WriteString("read error: ")
			output.WriteString(err.Error())
			output.WriteString("\n")
			mu.Unlock()
		}
	}

	wg.Add(2)
	go readPipe(stdout)
	go readPipe(stderr)
	go func() {
		wg.Wait()
		close(lines)
	}()

	lastNotify := time.Time{}
	for line := range lines {
		line = ansiRe.ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		mu.Lock()
		output.WriteString(line)
		output.WriteString("\n")
		mu.Unlock()

		updateTaskProgress(task, line)

		// Throttle notifications to ~4/sec to avoid flooding websocket
		now := time.Now()
		if now.Sub(lastNotify) >= 250*time.Millisecond {
			notify(statusCh, task)
			lastNotify = now
		}
	}

	err = cmd.Wait()
	notify(statusCh, task) // final update with last known state

	if err != nil {
		mu.Lock()
		full := output.String()
		mu.Unlock()
		return fmt.Errorf("%s\n%s", err.Error(), full)
	}
	return nil
}

// updateTaskProgress parses a single output line and updates task
// status/progress fields.
func updateTaskProgress(task *Task, line string) {
	lower := strings.ToLower(line)

	// Detect merge phase
	if strings.Contains(lower, "muxing") || strings.Contains(lower, "merge") ||
		strings.Contains(line, "合并") || strings.Contains(lower, "remux") {
		task.Status = "merging"
		task.Progress = "Merging segments..."
		return
	}

	// Percentage
	if m := pctRe.FindStringSubmatch(line); m != nil {
		if p, err := strconv.ParseFloat(m[1], 64); err == nil {
			if p > task.Percent { // keep the max seen
				task.Percent = p
				task.Progress = fmt.Sprintf("%.1f%%", p)
			}
			return
		}
	}

	// Segment counts: "123/456"
	if m := segRe.FindStringSubmatch(line); m != nil {
		done, err1 := strconv.Atoi(m[1])
		total, err2 := strconv.Atoi(m[2])
		if err1 == nil && err2 == nil && total > 0 {
			p := float64(done) / float64(total) * 100
			if p > task.Percent {
				task.Percent = p
				task.Progress = fmt.Sprintf("%d/%d (%.1f%%)", done, total, p)
			}
			return
		}
	}

	// Speed info lines are useful as progress text even without percent
	if strings.Contains(lower, "speed") || strings.Contains(lower, "download") {
		if task.Percent > 0 {
			task.Progress = fmt.Sprintf("%.1f%% - %s", task.Percent, shortLine(line))
		}
	}
}

// shortLine truncates a line to a reasonable display length.
func shortLine(s string) string {
	s = strings.TrimSpace(s)
	// Strip leading timestamp like "16:21:59.139 INFO : "
	if idx := strings.Index(s, "INFO"); idx >= 0 && idx < 20 {
		s = strings.TrimSpace(s[idx+4:])
		s = strings.TrimPrefix(s, ":")
		s = strings.TrimSpace(s)
	}
	if len(s) > 60 {
		s = s[:60] + "..."
	}
	return s
}

func notify(ch chan<- *Task, task *Task) {
	select {
	case ch <- task:
	default:
	}
}

// sanitizeFilename removes characters that are illegal in filenames.
func sanitizeFilename(name string) string {
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", "\"", "_", "<", "_", ">", "_", "|", "_",
	)
	return replacer.Replace(name)
}
