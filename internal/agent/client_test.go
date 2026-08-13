package agent

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testFileServer serves a source file with Range support (like the main
// server's /agent/files endpoint) and records which ranges were requested.
type testFileServer struct {
	t       *testing.T
	path    string
	mu      sync.Mutex
	ranges  map[string]int  // Range header -> request count
	fail    map[string]bool // Range header -> force 500
	handler http.Handler
}

func newTestFileServer(t *testing.T, path string) *testFileServer {
	s := &testFileServer{t: t, path: path, ranges: map[string]int{}, fail: map[string]bool{}}
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rg := r.Header.Get("Range")
		s.mu.Lock()
		s.ranges[rg]++
		shouldFail := s.fail[rg]
		s.mu.Unlock()
		if shouldFail {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		}
		f, err := os.Open(s.path)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer f.Close()
		http.ServeContent(w, r, "file.bin", time.Now(), f)
	})
	return s
}

// makeSource writes a deterministic pseudo-random file and returns its path.
func makeSource(t *testing.T, size int64) (string, []byte) {
	t.Helper()
	r := rand.New(rand.NewSource(42))
	data := make([]byte, size)
	r.Read(data)
	path := filepath.Join(t.TempDir(), "src.bin")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func buildChunks(size int64, conns int) []chunkStatus {
	chunk := (size + int64(conns) - 1) / int64(conns)
	var out []chunkStatus
	for i := range conns {
		start := int64(i) * chunk
		if start >= size {
			break
		}
		end := min(start+chunk-1, size-1)
		out = append(out, chunkStatus{Start: start, End: end})
	}
	return out
}

// TestPullMultiSkipsDoneChunks verifies that chunks marked done in the
// manifest are not requested again.
func TestPullMultiSkipsDoneChunks(t *testing.T) {
	size := int64(4*1024*1024 + 123) // deliberately not chunk-aligned
	srcPath, src := makeSource(t, size)

	srv := newTestFileServer(t, srcPath)
	ts := httptest.NewServer(srv.handler)
	defer ts.Close()

	dir := t.TempDir()
	cfg := ClientConfig{Dir: dir, Token: "test-token"}
	info := &TransferInfo{ID: "t1", FileName: "f.mp4", Size: size, Connections: 4}
	conns := 4

	// Pre-seed: chunk 0 and chunk 2 already done (with correct bytes).
	chunks := buildChunks(size, conns)
	chunks[0].Done = true
	chunks[2].Done = true
	saveManifest(filepath.Join(dir, "f.mp4.part.json"),
		&chunkManifest{TransferID: "t1", Size: size, Chunks: chunks})
	// Write full correct content so the final verify passes.
	if err := os.WriteFile(filepath.Join(dir, "f.mp4.part"), src, 0644); err != nil {
		t.Fatal(err)
	}

	if err := pullMulti(ts.URL, cfg, info, conns, http.DefaultClient); err != nil {
		t.Fatalf("pullMulti: %v", err)
	}

	// Final file must exist with exact content.
	got, err := os.ReadFile(filepath.Join(dir, "f.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("downloaded file content mismatch")
	}

	// Done chunks must NOT have been requested.
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for _, c := range chunks {
		rg := fmt.Sprintf("bytes=%d-%d", c.Start, c.End)
		if c.Done && srv.ranges[rg] > 0 {
			t.Errorf("chunk %s was downloaded despite being done", rg)
		}
		if !c.Done && srv.ranges[rg] == 0 {
			t.Errorf("chunk %s was never downloaded", rg)
		}
	}
}

// TestPullMultiResumeAfterFailure verifies that after a failed attempt
// (one chunk errors), the next attempt downloads only the missing chunk.
func TestPullMultiResumeAfterFailure(t *testing.T) {
	size := int64(2 * 1024 * 1024)
	srcPath, src := makeSource(t, size)

	srv := newTestFileServer(t, srcPath)
	ts := httptest.NewServer(srv.handler)
	defer ts.Close()

	dir := t.TempDir()
	cfg := ClientConfig{Dir: dir, Token: "test-token"}
	info := &TransferInfo{ID: "t2", FileName: "g.mp4", Size: size, Connections: 4}
	conns := 4
	chunks := buildChunks(size, conns)

	// Inject failure for chunk 2 only.
	srv.mu.Lock()
	srv.fail[fmt.Sprintf("bytes=%d-%d", chunks[2].Start, chunks[2].End)] = true
	srv.mu.Unlock()

	// Attempt 1: chunk 2 fails after 3 retries -> error, partial kept.
	if err := pullMulti(ts.URL, cfg, info, conns, http.DefaultClient); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	part := filepath.Join(dir, "g.mp4.part")
	if _, err := os.Stat(part); err != nil {
		t.Fatal("partial file should be kept for resume")
	}
	m := loadManifest(part + ".json")
	if m == nil {
		t.Fatal("manifest should be saved")
	}
	doneCount := 0
	for _, c := range m.Chunks {
		if c.Done {
			doneCount++
		}
	}
	if doneCount != 3 {
		t.Fatalf("expected 3/4 chunks done in manifest, got %d", doneCount)
	}

	// Attempt 2: failure removed -> only chunk 2 downloaded, transfer done.
	srv.mu.Lock()
	delete(srv.fail, fmt.Sprintf("bytes=%d-%d", chunks[2].Start, chunks[2].End))
	srv.mu.Unlock()

	if err := pullMulti(ts.URL, cfg, info, conns, http.DefaultClient); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "g.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("resumed file content mismatch")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	// Chunk 2 must be requested again; others must not have extra requests
	// beyond attempt 1 (each successful chunk requested exactly once).
	for _, c := range chunks {
		rg := fmt.Sprintf("bytes=%d-%d", c.Start, c.End)
		want := 1
		if c.Start == chunks[2].Start {
			want = 3 + 1 // 3 failed retries in attempt 1 + 1 in attempt 2
		}
		if srv.ranges[rg] != want {
			t.Errorf("chunk %s requested %d times, want %d", rg, srv.ranges[rg], want)
		}
	}
}
