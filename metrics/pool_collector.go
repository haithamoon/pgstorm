// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// poolStats is a scrape-time snapshot of the pool's stats. The gauge fields are
// point-in-time; the acquire fields are cumulative counters. AcquireDuration is
// pre-converted to seconds.
type poolStats struct {
	acquired int32
	idle     int32
	total    int32
	max      int32

	acquireCount           int64
	emptyAcquireCount      int64
	canceledAcquireCount   int64
	acquireDurationSeconds float64
}

type PoolCollector struct {
	statFn func() poolStats

	acquired *prometheus.Desc
	idle     *prometheus.Desc
	total    *prometheus.Desc
	maxConns *prometheus.Desc

	acquireCount         *prometheus.Desc
	emptyAcquireCount    *prometheus.Desc
	canceledAcquireCount *prometheus.Desc
	acquireDuration      *prometheus.Desc
}

func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	return newPoolCollectorWith(func() poolStats {
		s := pool.Stat()
		return poolStats{
			acquired:               s.AcquiredConns(),
			idle:                   s.IdleConns(),
			total:                  s.TotalConns(),
			max:                    s.MaxConns(),
			acquireCount:           s.AcquireCount(),
			emptyAcquireCount:      s.EmptyAcquireCount(),
			canceledAcquireCount:   s.CanceledAcquireCount(),
			acquireDurationSeconds: s.AcquireDuration().Seconds(),
		}
	})
}

func newPoolCollectorWith(fn func() poolStats) *PoolCollector {
	return &PoolCollector{
		statFn: fn,
		acquired: prometheus.NewDesc(
			namespace+"_pool_acquired_conns",
			"Number of currently acquired connections.",
			nil, nil,
		),
		idle: prometheus.NewDesc(
			namespace+"_pool_idle_conns",
			"Number of idle connections in the pool.",
			nil, nil,
		),
		total: prometheus.NewDesc(
			namespace+"_pool_total_conns",
			"Total number of connections in the pool.",
			nil, nil,
		),
		maxConns: prometheus.NewDesc(
			namespace+"_pool_max_conns",
			"Maximum number of connections allowed by the pool.",
			nil, nil,
		),
		acquireCount: prometheus.NewDesc(
			namespace+"_pool_acquire_count_total",
			"Cumulative count of successful connection acquisitions from the pool.",
			nil, nil,
		),
		emptyAcquireCount: prometheus.NewDesc(
			namespace+"_pool_empty_acquire_count_total",
			"Cumulative count of acquisitions that had to wait for a connection (pool was empty). Rising values mean workers are queueing on the pool, and that wait is charged to op latency.",
			nil, nil,
		),
		canceledAcquireCount: prometheus.NewDesc(
			namespace+"_pool_canceled_acquire_count_total",
			"Cumulative count of acquisitions cancelled by context before a connection was obtained.",
			nil, nil,
		),
		acquireDuration: prometheus.NewDesc(
			namespace+"_pool_acquire_duration_seconds_total",
			"Cumulative time spent waiting to acquire a connection from the pool, in seconds.",
			nil, nil,
		),
	}
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.maxConns
	ch <- c.acquireCount
	ch <- c.emptyAcquireCount
	ch <- c.canceledAcquireCount
	ch <- c.acquireDuration
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.statFn()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(s.acquired))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.idle))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(s.total))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(s.max))
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(s.acquireCount))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquireCount, prometheus.CounterValue, float64(s.emptyAcquireCount))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquireCount, prometheus.CounterValue, float64(s.canceledAcquireCount))
	ch <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, s.acquireDurationSeconds)
}
