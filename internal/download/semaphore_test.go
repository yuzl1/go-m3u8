package download

import (
	"testing"
	"time"
)

// TestSemaphoreReleaseWakesWaiter verifies a blocked acquire proceeds
// when a slot is released.
func TestSemaphoreReleaseWakesWaiter(t *testing.T) {
	s := newSemaphore(1)
	s.acquire()

	done := make(chan struct{})
	go func() {
		s.acquire() // blocks until release
		close(done)
		s.release()
	}()

	select {
	case <-done:
		t.Fatal("acquired while semaphore full")
	case <-time.After(50 * time.Millisecond):
	}

	s.release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken by release")
	}
	s.release()
}

// TestSemaphoreDynamicResize verifies raising the limit wakes waiters
// and lowering it doesn't lose them — the old channel-based semaphore
// orphaned queued tasks whenever the config was saved.
func TestSemaphoreDynamicResize(t *testing.T) {
	s := newSemaphore(1)
	s.acquire() // full

	done := make(chan struct{})
	go func() {
		s.acquire()
		close(done)
		s.release() // give the slot back
	}()

	select {
	case <-done:
		t.Fatal("acquired while full")
	case <-time.After(50 * time.Millisecond):
	}

	// Raise the limit — the waiter must be woken.
	s.setMax(2)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken after setMax")
	}
	s.release() // main's original slot

	// Lower the limit — no panic, existing holds stay valid.
	s.setMax(1)
	s.acquire()
	s.release()
}

// TestSemaphoreConcurrency verifies the counting behavior.
func TestSemaphoreConcurrency(t *testing.T) {
	s := newSemaphore(2)
	s.acquire()
	s.acquire()

	entered := make(chan struct{})
	go func() {
		s.acquire() // third — must block
		close(entered)
		s.release()
	}()

	select {
	case <-entered:
		t.Fatal("third acquire should block at max=2")
	case <-time.After(50 * time.Millisecond):
	}

	s.release() // free one slot
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("third acquire should proceed after release")
	}
	s.release()
}
