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
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"op"})

	WorkersActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "workers_active",
		Help:      "Number of worker goroutines currently executing a DB op.",
	})
)

func Register() {
	prometheus.MustRegister(OpsTotal, OpDuration, WorkersActive)
}

func RecordOp(op string, durationSec float64, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	OpsTotal.WithLabelValues(op, status).Inc()
	OpDuration.WithLabelValues(op).Observe(durationSec)
}
