package download

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yuzl1/go-m3u8/internal/config"
)

// Task represents a single download job.
type Task struct {
	ID            string                        `json:"id"`
	URL           string                        `json:"url"`
	Title         string                        `json:"title"`
	Status        string                        `json:"status"` // pending, downloading, merging, done, failed, cancelled
	Progress      string                        `json:"progress"`
	Percent       float64                       `json:"percent"`
	TotalSegments int                           `json:"total_segments,omitempty"`
	DoneSegments  int                           `json:"done_segments,omitempty"`
	OutputFile    string                        `json:"output_file,omitempty"`
	Synced        bool                          `json:"synced,omitempty"`
	SyncedTo      string                        `json:"synced_to,omitempty"`
	OriginalName  string                        `json:"original_name,omitempty"` // pre-translation name
	Translated    bool                          `json:"translated,omitempty"`    // filename was translated
	ProxyOffset   int                           `json:"-"`                       // rotation start index into the clash node list
	ClashNodes    []string                      `json:"-"`                       // healthy clash nodes for this task
	ClashGroup    string                        `json:"-"`                       // clash selector group
	ClashNode     string                        `json:"clash_node,omitempty"`    // node actually used (shown in UI)
	ClashStart    func() (*ClashSession, error) `json:"-"`                       // manager callback: spawn per-task clash instance
	Error         string                        `json:"error,omitempty"`
	CreatedAt     time.Time                     `json:"created_at"`
	Headers       map[string]string             `json:"headers,omitempty"`
	BaseURL       string                        `json:"base_url,omitempty"`
	SaveDir       string                        `json:"save_dir,omitempty"`
	Log           string                        `json:"-"` // full process output, not sent over websocket
}

// BuildCommand builds the N_m3u8DL-RE command-line for a task.
// proxy, when non-empty, is passed as --proxy (one proxy per process —
// the manager rotates proxies across tasks and retry attempts).
func BuildCommand(cfg *config.Config, task *Task, proxy string) (string, []string) {
	args := []string{}

	// Input URL
	args = append(args, task.URL)

	// Save directory — always pass an absolute path so N_m3u8DL-RE
	// cannot misinterpret a relative path (e.g. against its own exe dir).
	if saveDir := resolveSaveDir(cfg, task); saveDir != "" {
		args = append(args, "--save-dir", saveDir)
	}

	// Save name
	if task.Title != "" {
		args = append(args, "--save-name", sanitizeFilename(task.Title))
	}

	// Temp directory
	if cfg.TempDir != "" {
		tmpDir := cfg.TempDir
		if !filepath.IsAbs(tmpDir) {
			if abs, err := filepath.Abs(tmpDir); err == nil {
				tmpDir = abs
			}
		}
		args = append(args, "--tmp-dir", tmpDir)
	}

	// Thread count
	if cfg.ThreadCount > 0 {
		args = append(args, "--thread-count", fmt.Sprintf("%d", cfg.ThreadCount))
	}

	// Per-segment download retry count
	if cfg.DownloadRetryCount > 0 {
		args = append(args, "--download-retry-count", fmt.Sprintf("%d", cfg.DownloadRetryCount))
	}

	// Segment count check before muxing
	if !cfg.CheckSegments {
		args = append(args, "--check-segments-count", "false")
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

	// HTTP proxy for this attempt (rotated by the caller).
	// N_m3u8DL-RE's flag is --custom-proxy (not --proxy).
	if proxy != "" {
		args = append(args, "--custom-proxy", proxy)
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

// resolveSaveDir returns the absolute save directory for a task.
func resolveSaveDir(cfg *config.Config, task *Task) string {
	dir := cfg.SaveDir
	if task.SaveDir != "" {
		dir = task.SaveDir
	}
	if dir != "" && !filepath.IsAbs(dir) {
		if abs, err := filepath.Abs(dir); err == nil {
			dir = abs
		}
	}
	return dir
}

// maxRetries is the number of download retries on failure.
const maxRetries = 3

// maxLogBytes caps how much process output we keep per task.
const maxLogBytes = 64 * 1024

// Run starts N_m3u8DL-RE and blocks until completion.
// It streams stdout/stderr, parses progress and sends status updates.
// Retries up to maxRetries times on failure. After a successful exit it
// verifies an output file actually appeared in the save directory.
func Run(ctx context.Context, cfg *config.Config, task *Task, statusCh chan<- *Task) error {
	exe := cfg.Nm3u8dlPath
	saveDir := resolveSaveDir(cfg, task)

	// Dedicated clash instance for this task: own node, own port, own
	// process — fully isolated from concurrent downloads.
	proxy := ""
	var session *ClashSession
	if task.ClashStart != nil {
		s, err := task.ClashStart()
		if err == nil {
			session = s
			proxy = s.Proxy
			task.ClashNode = s.Node
		} else {
			task.Log += fmt.Sprintf("clash instance failed: %v — falling back to shared clash proxy\n", err)
			proxy = cfg.ClashProxy
			task.ClashNode = "共享代理(回退)"
		}
	}
	defer func() {
		if session != nil {
			session.Release()
		}
	}()

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, args := BuildCommand(cfg, task, proxy)

		// NOTE: += preserves any log content written before Run (e.g. the
		// translation diagnostics from the download pipeline).
		if attempt == 1 {
			task.Log += "== Command ==\n" + exe + " " + strings.Join(args, " ") + "\n\n"
		} else {
			task.Log += fmt.Sprintf("\n== Retry attempt %d/%d ==\n%s %s\n", attempt, maxRetries, exe, strings.Join(args, " "))
		}

		if attempt > 1 {
			task.Progress = fmt.Sprintf("Retrying (attempt %d/%d)...", attempt, maxRetries)
			task.Percent = 0
			task.DoneSegments = 0
			notify(statusCh, task)
		} else {
			task.Status = "downloading"
			task.Progress = "Starting N_m3u8DL-RE..."
			task.Percent = 0
			task.DoneSegments = 0
			notify(statusCh, task)
		}

		// Only inspect output produced by THIS attempt — the log
		// accumulates across retries, and matching against historical
		// failures would falsely fail a successful retry.
		attemptLogStart := len(task.Log)

		cmd := buildExec(ctx, exe, args)
		err := runStreaming(cmd, task, statusCh)

		// Check if cancelled
		if ctx.Err() != nil {
			task.Status = "cancelled"
			task.Progress = ""
			notify(statusCh, task)
			return ctx.Err()
		}

		// N_m3u8DL-RE exits 0 on some fatal errors (e.g. segment count
		// check failure) — detect them from the log.
		if err == nil && checkLogForFailure(task.Log[attemptLogStart:]) {
			err = fmt.Errorf("N_m3u8DL-RE logged fatal errors but exited 0:\n%s", tailLog(task.Log[attemptLogStart:], 4000))
		}

		// Teaser-playlist guard: sites serve suspicious IPs (datacenter
		// proxies) a short preview playlist. A real episode is never
		// just a handful of segments.
		if err == nil && cfg.MinSegments > 0 && task.TotalSegments > 0 && task.TotalSegments < cfg.MinSegments {
			err = fmt.Errorf(
				"播放列表仅 %d 个分片（配置要求至少 %d）——疑似预览/无效播放列表（IP 被网站识别或代理内容被替换）\n\nFull log:\n%s",
				task.TotalSegments, cfg.MinSegments, tailLog(task.Log, 4000))
		}

		if err == nil {
			// Verify the file actually exists.
			if task.OutputFile == "" {
				task.OutputFile = findNewestFile(saveDir, task.CreatedAt, sanitizeFilename(task.Title))
			}
			if task.OutputFile == "" && saveDir != "" {
				err = fmt.Errorf(
					"no output file was found in %s (exit code 0)\n\nFull log:\n%s",
					saveDir, tailLog(task.Log, 4000))
			}
		}

		if err == nil {
			task.Status = "done"
			task.Progress = "Download complete"
			task.Percent = 100
			if task.TotalSegments > 0 {
				task.DoneSegments = task.TotalSegments
			}
			task.Error = ""
			notify(statusCh, task)
			return nil
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
	pctRe       = regexp.MustCompile(`(\d+(?:\.\d+)?)\s*%`)
	segRe       = regexp.MustCompile(`(\d+)\s*/\s*(\d+)`)
	sizeSpeedRe = regexp.MustCompile(`(\d+(?:\.\d+)?(?:B|KB|MB|GB))\s*/\s*(\d+(?:\.\d+)?(?:B|KB|MB|GB))\s*(\d+(?:\.\d+)?(?:B|KB|MB)ps)`)
	totalSegRe  = regexp.MustCompile(`(?i)(?:total\s+segments?|分片总数|total)[^\d]{0,20}(\d+)`)
	ansiRe      = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]`)
	outFileRe   = regexp.MustCompile(`(?i)(?:muxing\s+to|output(?: file)?[^:]*:|muxed\s+to|saving\s+to)\s+(.+)`)
	// segDeclRe matches playlist declarations like "Vid Kbps | 734 Segments | ~02h02m20s".
	segDeclRe = regexp.MustCompile(`(?i)(\d+)\s*segments`)
	// failureRe matches fatal errors in N_m3u8DL-RE output. It logs
	// "ERROR: Failed" on segment-count-check failure but still exits 0,
	// so we must detect failure from the log, not just the exit code.
	failureRe = regexp.MustCompile(`(?i)(segment count check not pass|check segments count not pass|^.*ERROR:\s*failed|fatal error|invalid data found|error opening input)`)
)

// checkLogForFailure reports whether the captured output contains a fatal
// error even though the process exited 0.
func checkLogForFailure(log string) bool {
	return failureRe.MatchString(log)
}

// buildExec constructs the command to run N_m3u8DL-RE. On Linux it wraps
// the process in a pseudo-TTY via `script` so the progress display flushes
// in real time — N_m3u8DL-RE buffers stdout heavily when it is a plain
// pipe, which made progress appear only at the end of the run.
func buildExec(ctx context.Context, exe string, args []string) *exec.Cmd {
	if runtime.GOOS == "linux" {
		if scriptPath, err := exec.LookPath("script"); err == nil {
			cmdStr := quoteArgs(exe, args)
			return exec.CommandContext(ctx, scriptPath, "-qec", cmdStr, "/dev/null")
		}
	}
	return exec.CommandContext(ctx, exe, args...)
}

// quoteArgs joins a command and its args into a single-quoted shell string.
func quoteArgs(exe string, args []string) string {
	parts := append([]string{exe}, args...)
	for i, p := range parts {
		parts[i] = "'" + strings.ReplaceAll(p, "'", `'\''`) + "'"
	}
	return strings.Join(parts, " ")
}

// splitAnyLine splits on \n or \r — N_m3u8DL-RE redraws its progress bar
// with \r, so plain \n scanning would swallow all progress updates into
// one giant "line".
func splitAnyLine(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i := range data {
		if data[i] == '\n' || data[i] == '\r' {
			return i + 1, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// runStreaming starts cmd, streams its output and parses progress info
// into task. Returns the command's error, including captured output.
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

	lines := make(chan string, 256)
	var wg sync.WaitGroup

	readPipe := func(r io.Reader) {
		defer wg.Done()
		scanner := bufio.NewScanner(r)
		scanner.Split(splitAnyLine)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-time.After(time.Second):
				// avoid blocking forever if consumer is stuck
			}
		}
		if err := scanner.Err(); err != nil {
			mu.Lock()
			appendLog(task, "read error: "+err.Error())
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
		appendLog(task, line)
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
		return fmt.Errorf("%s\n%s", err.Error(), tailLog(task.Log, 4000))
	}
	return nil
}

// updateTaskProgress parses a single output line and updates task
// status/progress fields.
func updateTaskProgress(task *Task, line string) {
	lower := strings.ToLower(line)

	// Capture final output file path, e.g. "Muxing to /downloads/xxx.mp4"
	if m := outFileRe.FindStringSubmatch(line); m != nil {
		p := strings.TrimSpace(m[1])
		if p != "" {
			task.OutputFile = p
		}
	}

	// Detect merge phase
	if strings.Contains(lower, "muxing") || strings.Contains(lower, "merge") ||
		strings.Contains(line, "合并") || strings.Contains(lower, "remux") {
		task.Status = "merging"
		task.Progress = "Merging segments..."
		return
	}

	// Segment counts FIRST: progress bar lines contain BOTH "148/195" and
	// "75.90%", and we need the counts before the percent branch returns.
	// Total must be plausible to avoid false positives on dates (2026/08/12).
	if m := segRe.FindStringSubmatch(line); m != nil {
		done, err1 := strconv.Atoi(m[1])
		total, err2 := strconv.Atoi(m[2])
		if err1 == nil && err2 == nil && total >= 10 && total < 100000 && done <= total {
			task.DoneSegments = done
			task.TotalSegments = total
			p := float64(done) / float64(total) * 100
			if p > task.Percent {
				task.Percent = p
			}
			task.Progress = fmt.Sprintf("%d/%d 分片 (%.1f%%)", done, total, p)
			// Append size/speed when present: "216.77MB/285.61MB 2.62MBps"
			if sm := sizeSpeedRe.FindStringSubmatch(line); sm != nil {
				task.Progress = fmt.Sprintf("%d/%d 分片 (%.1f%%)  %s/%s @%s", done, total, p, sm[1], sm[2], sm[3])
			}
			return
		}
	}

	// Percentage only (e.g. merge phase progress without segment counts)
	if m := pctRe.FindStringSubmatch(line); m != nil {
		if p, err := strconv.ParseFloat(m[1], 64); err == nil && p >= 0 && p <= 100 {
			if p > task.Percent {
				task.Percent = p
			}
			if task.TotalSegments > 0 {
				task.Progress = fmt.Sprintf("%d/%d 分片 (%.1f%%)", task.DoneSegments, task.TotalSegments, p)
			} else {
				task.Progress = fmt.Sprintf("%.1f%%", p)
			}
			return
		}
	}

	// Total segments declaration, e.g. "Total segments: 123" or
	// "Vid Kbps | 734 Segments | ~02h02m20s"
	if m := totalSegRe.FindStringSubmatch(line); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > task.TotalSegments {
			task.TotalSegments = n
		}
	}
	if m := segDeclRe.FindStringSubmatch(line); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > task.TotalSegments {
			task.TotalSegments = n
		}
	}
}

// appendLog appends to task.Log, keeping only the tail under maxLogBytes.
func appendLog(task *Task, line string) {
	if len(task.Log)+len(line)+1 > maxLogBytes {
		overflow := len(task.Log) + len(line) + 1 - maxLogBytes
		if overflow < len(task.Log) {
			task.Log = task.Log[overflow:]
		} else {
			task.Log = ""
		}
	}
	task.Log += line + "\n"
}

func tailLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...(truncated)...\n" + s[len(s)-n:]
}

// findNewestFile returns the newest non-directory file in dir modified
// around the task's lifetime. When namePrefix is set, only files whose
// name starts with it are considered — concurrent downloads share the
// save dir and must not pick each other's output. Returns "" if none.
func findNewestFile(dir string, since time.Time, namePrefix string) string {
	if dir == "" {
		return ""
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	threshold := since.Add(-10 * time.Minute)
	var best string
	var bestTime time.Time
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if namePrefix != "" && !strings.HasPrefix(e.Name(), namePrefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(threshold) && info.ModTime().After(bestTime) {
			best = filepath.Join(dir, e.Name())
			bestTime = info.ModTime()
		}
	}
	return best
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
