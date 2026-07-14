package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPoolCollector_Describe_sends8Descriptors(t *testing.T) {
	c := newPoolCollectorWith(func() poolStats { return poolStats{} })
	ch := make(chan *prometheus.Desc, 16)
	c.Describe(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 8 {
		t.Errorf("expected 8 descriptors, got %d", count)
	}
}

func TestPoolCollector_Collect_sends8Metrics(t *testing.T) {
	c := newPoolCollectorWith(func() poolStats {
		return poolStats{acquired: 5, idle: 3, total: 8, max: 20}
	})
	ch := make(chan prometheus.Metric, 16)
	c.Collect(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 8 {
		t.Errorf("expected 8 metrics, got %d", count)
	}
}

func TestPoolCollector_Collect_acquireMetricValues(t *testing.T) {
	c := newPoolCollectorWith(func() poolStats {
		return poolStats{
			acquired:               5,
			idle:                   3,
			total:                  8,
			max:                    20,
			acquireCount:           100,
			emptyAcquireCount:      7,
			canceledAcquireCount:   2,
			acquireDurationSeconds: 1.5,
		}
	})
	ch := make(chan prometheus.Metric, 16)
	c.Collect(ch)
	close(ch)

	// Map each metric's fq name to its value.
	got := make(map[string]float64)
	for m := range ch {
		var dm dto.Metric
		if err := m.Write(&dm); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		desc := m.Desc().String()
		var v float64
		switch {
		case dm.Gauge != nil:
			v = dm.GetGauge().GetValue()
		case dm.Counter != nil:
			v = dm.GetCounter().GetValue()
		}
		got[desc] = v
	}

	want := map[string]float64{
		namespace + "_pool_acquire_count_total":            100,
		namespace + "_pool_empty_acquire_count_total":      7,
		namespace + "_pool_canceled_acquire_count_total":   2,
		namespace + "_pool_acquire_duration_seconds_total": 1.5,
	}
	for name, wantVal := range want {
		found := false
		for desc, v := range got {
			if contains(desc, name) {
				found = true
				if v != wantVal {
					t.Errorf("%s: want %v, got %v", name, wantVal, v)
				}
			}
		}
		if !found {
			t.Errorf("metric %s not emitted", name)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func TestPoolCollector_Collect_statFnCalledOnEachCollect(t *testing.T) {
	calls := 0
	c := newPoolCollectorWith(func() poolStats {
		calls++
		return poolStats{acquired: 1, idle: 1, total: 2, max: 10}
	})
	ch := make(chan prometheus.Metric, 16)
	c.Collect(ch)
	if calls != 1 {
		t.Errorf("expected statFn called once per Collect, got %d", calls)
	}
	// drain
	close(ch)
	for range ch {
	}

	ch2 := make(chan prometheus.Metric, 16)
	c.Collect(ch2)
	close(ch2)
	for range ch2 {
	}
	if calls != 2 {
		t.Errorf("expected statFn called again on second Collect, got %d total", calls)
	}
}
