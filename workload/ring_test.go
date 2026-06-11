package workload

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestSessionRing_empty(t *testing.T) {
	r := NewSessionRing(5)
	rng := rand.New(rand.NewSource(1))
	_, ok := r.Sample(rng)
	if ok {
		t.Fatal("expected ok=false from empty ring")
	}
}

func TestSessionRing_pushThenSample(t *testing.T) {
	r := NewSessionRing(5)
	id := uuid.New()
	r.Push(id)
	rng := rand.New(rand.NewSource(1))
	got, ok := r.Sample(rng)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != id {
		t.Errorf("want %v, got %v", id, got)
	}
}

func TestSessionRing_fillNeverExceedsSize(t *testing.T) {
	size := 5
	r := NewSessionRing(size)
	for i := 0; i < size*3; i++ {
		r.Push(uuid.New())
		r.mu.Lock()
		if r.fill > size {
			r.mu.Unlock()
			t.Fatalf("fill %d exceeds size %d after %d pushes", r.fill, size, i+1)
		}
		r.mu.Unlock()
	}
}

func TestSessionRing_wraparound(t *testing.T) {
	// After size+1 pushes the first slot is overwritten.
	size := 3
	r := NewSessionRing(size)
	ids := make([]uuid.UUID, size+1)
	for i := range ids {
		ids[i] = uuid.New()
		r.Push(ids[i])
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		got, ok := r.Sample(rng)
		if !ok {
			t.Fatal("expected ok=true, ring is full")
		}
		if got == ids[0] {
			t.Errorf("sample returned overwritten slot ids[0]")
		}
	}
}

func TestSessionRing_partiallyFilled_noZeroUUID(t *testing.T) {
	// Ring capacity 10, only 3 items pushed. Sample must never return uuid.Nil
	// (a zero-value slot that was never written).
	size := 10
	r := NewSessionRing(size)
	for i := 0; i < 3; i++ {
		r.Push(uuid.New())
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 200; i++ {
		got, ok := r.Sample(rng)
		if !ok {
			t.Fatal("expected ok=true, ring is not empty")
		}
		if got == uuid.Nil {
			t.Fatalf("iteration %d: Sample returned uuid.Nil — zero-value slot leaked", i)
		}
	}
}

func TestSessionRing_concurrentPushSample(t *testing.T) {
	r := NewSessionRing(500)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.Push(uuid.New())
			}
		}()
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for j := 0; j < 200; j++ {
				r.Sample(rng)
			}
		}(int64(i))
	}
	wg.Wait()
}
