// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Haitham Gadelrab
// Licensed under the Apache License, Version 2.0; see the LICENSE file.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "pgstorm"

var (
	OpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "ops_total",
		Help:      "Total number of DB operations executed.",
	}, []string{"op", "status"})

	OpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "op_duration_seconds",
		Help:      "Duration of each SUCCESSFUL DB operation. Failed ops are excluded — they return in microseconds without touching data and would drag every quantile down as the server degrades. Count them via ops_total{status=\"error\"}; op_duration_seconds_count therefore does not equal sum(ops_total).",
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

// RecordOp counts an executed op and, when it succeeded, observes its latency.
//
// Errors are counted but deliberately not observed, keeping this histogram the
// same population as the exact latency map in workload/stats.go. Mixing them
// would let a flood of microsecond failures pull the Grafana quantiles down
// exactly when the database is at its worst. During a total outage the latency
// panels go blank rather than to zero — that is the honest rendering, since no
// successful op was measured, and the error-rate panels explain the gap.
func RecordOp(op string, durationSec float64, err error) {
	if err != nil {
		OpsTotal.WithLabelValues(op, "error").Inc()
		return
	}
	OpsTotal.WithLabelValues(op, "ok").Inc()
	OpDuration.WithLabelValues(op).Observe(durationSec)
}

// RecordSkip counts an op that intentionally did no DB work (returned errSkipped),
// keeping it out of the op-rate and latency metrics.
func RecordSkip(op string) {
	OpsSkipped.WithLabelValues(op).Inc()
}
