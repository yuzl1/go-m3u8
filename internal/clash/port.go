package clash

import (
	"fmt"
	"sync"
)

// PortAllocator hands out proxy ports from a fixed range.
type PortAllocator struct {
	mu   sync.Mutex
	free []int
}

// NewPortAllocator creates an allocator for ports start..start+n-1.
func NewPortAllocator(start, n int) *PortAllocator {
	free := make([]int, n)
	for i := range n {
		free[i] = start + i
	}
	return &PortAllocator{free: free}
}

// Alloc takes a free port.
func (p *PortAllocator) Alloc() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.free) == 0 {
		return 0, fmt.Errorf("no free clash proxy ports")
	}
	port := p.free[0]
	p.free = p.free[1:]
	return port, nil
}

// Free returns a port to the pool.
func (p *PortAllocator) Free(port int) {
	p.mu.Lock()
	p.free = append(p.free, port)
	p.mu.Unlock()
}
