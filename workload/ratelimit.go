package workload

import (
	"context"
	"time"
)

// RateLimiter paces the aggregate operation rate across all workers to a target
// ops/sec (closed-loop load). It is a token bucket fed by a fixed-interval ticker:
// tokens accrue at the target rate (fractionally, so low rates work), capped at a
// small burst so a quiet period can't unleash a large catch-up spike. A nil
// *RateLimiter means "unlimited" — Wait is a no-op — so the zero-config path keeps
// the original open-throttle behavior.
type RateLimiter struct {
	tokens chan struct{}
}

// rateLimiterTick is how often the feeder tops up the bucket. 10ms (100 Hz) is
// fine-grained enough for smooth pacing without a high-frequency ticker.
const rateLimiterTick = 10 * time.Millisecond

// NewRateLimiter starts a token feeder bound to ctx and returns the limiter.
// ratePerSec <= 0 disables limiting and returns nil (Wait becomes a no-op).
func NewRateLimiter(ctx context.Context, ratePerSec int) *RateLimiter {
	if ratePerSec <= 0 {
		return nil
	}
	// Burst = 100ms worth of ops (at least 1), so pacing stays tight rather than
	// allowing a full second of ops to fire at once after an idle stretch.
	burst := ratePerSec / 10
	if burst < 1 {
		burst = 1
	}
	rl := &RateLimiter{tokens: make(chan struct{}, burst)}

	go func() {
		ticker := time.NewTicker(rateLimiterTick)
		defer ticker.Stop()
		last := time.Now()
		var acc float64
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				// Accrue tokens by the *actual* elapsed time (from the tick
				// timestamps), not a fixed per-tick amount. A Go ticker fires late
				// and coalesces missed ticks under load, so elapsed-based accrual
				// keeps the delivered rate on target instead of silently
				// undershooting when the host is busy.
				acc += float64(ratePerSec) * now.Sub(last).Seconds()
				last = now
				if acc > float64(burst) {
					acc = float64(burst)
				}
				for acc >= 1 {
					select {
					case rl.tokens <- struct{}{}:
						acc--
					default:
						// Bucket full (workers can't keep up): drop the backlog so a
						// stall can't unleash a catch-up spike above target once they
						// recover. Like any token bucket, this caps bursts and does
						// not "make up" a long stall's deficit (matches x/time/rate).
						acc = 0
					}
				}
			}
		}
	}()
	return rl
}

// Wait blocks until a token is available or ctx is done. It returns true if a
// token was acquired (proceed) and false if ctx was cancelled (stop). It paces
// op *attempts*, not successes — an op that later errors still consumed its
// token, which is the intended load-generation semantics. A nil receiver returns
// true immediately (unlimited).
func (rl *RateLimiter) Wait(ctx context.Context) bool {
	if rl == nil {
		return true
	}
	// Prioritise cancellation: if ctx is already done, don't grab a token and run
	// another op. A plain two-case select would pick randomly between a ready token
	// and a done ctx, letting a worker leak one extra op past shutdown.
	select {
	case <-ctx.Done():
		return false
	default:
	}
	select {
	case <-ctx.Done():
		return false
	case <-rl.tokens:
		return true
	}
}
