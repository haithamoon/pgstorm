package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type PoolCollector struct {
	statFn   func() (acquired, idle, total, max int32)
	acquired *prometheus.Desc
	idle     *prometheus.Desc
	total    *prometheus.Desc
	maxConns *prometheus.Desc
}

func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	return newPoolCollectorWith(func() (int32, int32, int32, int32) {
		s := pool.Stat()
		return s.AcquiredConns(), s.IdleConns(), s.TotalConns(), s.MaxConns()
	})
}

func newPoolCollectorWith(fn func() (int32, int32, int32, int32)) *PoolCollector {
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
	}
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.maxConns
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	acquired, idle, total, max := c.statFn()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(acquired))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(idle))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(total))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(max))
}
