package download

import (
	"context"
	"fmt"
	"maps"
	"os/exec"
	"strings"
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
// It sends status updates to the provided channel.
// Retries up to maxRetries times on failure.
func Run(ctx context.Context, cfg *config.Config, task *Task, statusCh chan<- *Task) error {
	exe, args := BuildCommand(cfg, task)

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			task.Progress = fmt.Sprintf("Retrying (attempt %d/%d)...", attempt, maxRetries)
			notify(statusCh, task)
		} else {
			task.Status = "downloading"
			task.Progress = "Starting N_m3u8DL-RE..."
			notify(statusCh, task)
		}

		cmd := exec.CommandContext(ctx, exe, args...)
		output, err := cmd.CombinedOutput()

		if err == nil {
			task.Status = "done"
			task.Progress = "Download complete"
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

		lastErr = fmt.Errorf("%s\n%s", err.Error(), string(output))
		if attempt < maxRetries {
			continue
		}
	}

	task.Status = "failed"
	task.Error = lastErr.Error()
	task.Progress = ""
	notify(statusCh, task)
	return fmt.Errorf("N_m3u8DL-RE failed after %d attempts: %w", maxRetries, lastErr)
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
