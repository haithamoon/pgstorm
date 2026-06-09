package workload

import (
	"sync"

	"github.com/google/uuid"
)

type SessionRing struct {
	mu   sync.Mutex
	buf  []uuid.UUID
	size int
	head int
	fill int
}

func NewSessionRing(size int) *SessionRing {
	return &SessionRing{
		buf:  make([]uuid.UUID, size),
		size: size,
	}
}

func (r *SessionRing) Push(id uuid.UUID) {
	r.mu.Lock()
	r.buf[r.head] = id
	r.head = (r.head + 1) % r.size
	if r.fill < r.size {
		r.fill++
	}
	r.mu.Unlock()
}

// Sample returns a random populated slot. Returns uuid.Nil, false if the buffer is empty.
func (r *SessionRing) Sample(rng interface{ Intn(int) int }) (uuid.UUID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fill == 0 {
		return uuid.Nil, false
	}
	idx := rng.Intn(r.fill)
	return r.buf[idx], true
}
