package download

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yuzl1/go-m3u8/internal/config"
)

// TestParseProgressBar verifies parsing of real N_m3u8DL-RE progress bar
// lines captured from actual logs.
func TestParseProgressBar(t *testing.T) {
	task := &Task{}

	lines := []string{
		"Vid Kbps ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 148/195 75.90% 216.77MB/285.61MB2.62MBps00:00:10",
		"Vid Kbps ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 149/195 76.41% 216.77MB/285.61MB2.62MBps00:00:10",
	}
	for _, l := range lines {
		updateTaskProgress(task, l)
	}

	if task.DoneSegments != 149 {
		t.Errorf("DoneSegments = %d, want 149", task.DoneSegments)
	}
	if task.TotalSegments != 195 {
		t.Errorf("TotalSegments = %d, want 195", task.TotalSegments)
	}
	if task.Percent < 76.4 || task.Percent > 76.5 {
		t.Errorf("Percent = %f, want ~76.41", task.Percent)
	}
	if task.Progress == "" {
		t.Error("Progress text should be set")
	}
	t.Logf("Progress: %s", task.Progress)
}

// TestParseFailureLog verifies log-level failure detection.
func TestParseFailureLog(t *testing.T) {
	log := "15:31:47.543 ERROR: Segment count check not pass, total: 195, downloaded: 149.\n" +
		"15:31:47.548 ERROR: Failed"
	if !checkLogForFailure(log) {
		t.Error("should detect segment count check failure")
	}
}

// TestNoDateFalsePositive verifies dates don't parse as segment counts.
func TestNoDateFalsePositive(t *testing.T) {
	task := &Task{}
	updateTaskProgress(task, "2026/08/12 15:31:47 INFO : some log")
	if task.TotalSegments != 0 || task.DoneSegments != 0 {
		t.Errorf("date parsed as segments: %d/%d", task.DoneSegments, task.TotalSegments)
	}
}

// TestRealTimeProgressUpdates runs a fake downloader that emits progress
// lines over ~1.5s and verifies status updates arrive WHILE it runs,
// not only at completion.
func TestRealTimeProgressUpdates(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
i=0
while [ $i -lt 10 ]; do
  i=$((i+1))
  printf '\rVid Kbps %d/10 %d.00%% 1.00MB/10.00MB 1.00MBps 00:00:0%d\n' $i $((i*10)) $((10-i))
  sleep 0.15
done
echo "Muxing to ` + filepath.Join(dir, "out.mp4") + `"
`
	scriptPath := filepath.Join(dir, "fake-downloader.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		SaveDir:     dir,
		Nm3u8dlPath: scriptPath,
		ThreadCount: 1,
	}
	task := &Task{ID: "t1", URL: "https://example.com/x.m3u8", CreatedAt: time.Now()}
	statusCh := make(chan *Task, 32)

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), cfg, task, statusCh)
		close(statusCh)
	}()

	var firstUpdate time.Time
	var lastPercent float64
	for tsk := range statusCh {
		if firstUpdate.IsZero() && tsk.Percent > 0 {
			firstUpdate = time.Now()
		}
		lastPercent = tsk.Percent
	}
	err := <-done
	finished := time.Now()

	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if lastPercent < 100 {
		t.Errorf("final percent = %f, want 100", lastPercent)
	}
	if task.TotalSegments != 10 || task.DoneSegments != 10 {
		t.Errorf("segments = %d/%d, want 10/10", task.DoneSegments, task.TotalSegments)
	}
	if firstUpdate.IsZero() {
		t.Fatal("no progress update received before completion")
	}
	// First progress update should arrive well before the run finishes
	// (~1.5s script runtime); if updates only came at the end, this fails.
	if finished.Sub(firstUpdate) < 500*time.Millisecond {
		t.Errorf("progress updates only arrived at the end (first update %v before finish)", finished.Sub(firstUpdate))
	}
	t.Logf("first progress at %v after start, finish %v after start", firstUpdate.Sub(start), finished.Sub(start))
}
