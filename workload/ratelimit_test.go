package workload

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter_nilIsUnlimited(t *testing.T) {
	var rl *RateLimiter // nil == disabled
	if !rl.Wait(context.Background()) {
		t.Fatal("nil limiter should return true immediately")
	}
	if NewRateLimiter(context.Background(), 0) != nil {
		t.Fatal("rate 0 should return a nil limiter (unlimited)")
	}
	if NewRateLimiter(context.Background(), -5) != nil {
		t.Fatal("negative rate should return a nil limiter (unlimited)")
	}
}

func TestRateLimiter_ctxCancelUnblocks(t *testing.T) {
	feedCtx, feedCancel := context.WithCancel(context.Background())
	rl := NewRateLimiter(feedCtx, 100)
	// Stop the feeder before its first tick (10ms away) so the bucket stays empty.
	feedCancel()

	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitCancel() // already cancelled

	if rl.Wait(waitCtx) {
		t.Fatal("Wait should return false when ctx is cancelled and no token is available")
	}
}

func TestRateLimiter_capsRate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const rate = 500
	rl := NewRateLimiter(ctx, rate)

	deadline := time.Now().Add(200 * time.Millisecond)
	count := 0
	for time.Now().Before(deadline) {
		if rl.Wait(ctx) {
			count++
		}
	}

	// In 200ms at 500/s we expect ~100 tokens plus up to the burst (~50). Use a
	// generous upper bound so the test isn't flaky, but tight enough that an
	// unthrottled loop (which would do millions) fails clearly.
	maxExpected := rate*200/1000 + rate/10 + rate/5 // ~250
	if count > maxExpected {
		t.Errorf("acquired %d tokens in 200ms; target %d/s implies <= ~%d — not throttling", count, rate, maxExpected)
	}
	if count == 0 {
		t.Error("acquired 0 tokens — limiter never granted")
	}
}
