package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "pgloadgen"

var (
	OpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ops_total",
		Help:      "Total number of DB operations executed.",
	}, []string{"op", "status"})

	OpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "op_duration_seconds",
		Help:      "Duration of each DB operation.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5, 10, 30},
	}, []string{"op"})

	WorkersActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "workers_active",
		Help:      "Number of worker goroutines currently executing a DB op.",
	})

	OpsSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ops_skipped_total",
		Help:      "Ops that intentionally did no DB work (e.g. empty ring at cold start). Not counted in ops_total or op_duration_seconds, so they don't skew rate/latency.",
	}, []string{"op"})
)

func Register() {
	prometheus.MustRegister(OpsTotal, OpDuration, WorkersActive, OpsSkipped)
}

func RecordOp(op string, durationSec float64, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	OpsTotal.WithLabelValues(op, status).Inc()
	OpDuration.WithLabelValues(op).Observe(durationSec)
}

// RecordSkip counts an op that intentionally did no DB work (returned errSkipped),
// keeping it out of the op-rate and latency metrics.
func RecordSkip(op string) {
	OpsSkipped.WithLabelValues(op).Inc()
}
