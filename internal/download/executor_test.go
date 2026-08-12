package download

import (
	"testing"
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
