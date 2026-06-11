package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPoolCollector_Describe_sends4Descriptors(t *testing.T) {
	c := newPoolCollectorWith(func() (int32, int32, int32, int32) {
		return 0, 0, 0, 0
	})
	ch := make(chan *prometheus.Desc, 10)
	c.Describe(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 4 {
		t.Errorf("expected 4 descriptors, got %d", count)
	}
}

func TestPoolCollector_Collect_sends4Metrics(t *testing.T) {
	c := newPoolCollectorWith(func() (int32, int32, int32, int32) {
		return 5, 3, 8, 20
	})
	ch := make(chan prometheus.Metric, 10)
	c.Collect(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 4 {
		t.Errorf("expected 4 metrics, got %d", count)
	}
}

func TestPoolCollector_Collect_statFnCalledOnEachCollect(t *testing.T) {
	calls := 0
	c := newPoolCollectorWith(func() (int32, int32, int32, int32) {
		calls++
		return 1, 1, 2, 10
	})
	ch := make(chan prometheus.Metric, 10)
	c.Collect(ch)
	if calls != 1 {
		t.Errorf("expected statFn called once per Collect, got %d", calls)
	}
	// drain
	close(ch)
	for range ch {
	}

	ch2 := make(chan prometheus.Metric, 10)
	c.Collect(ch2)
	close(ch2)
	for range ch2 {
	}
	if calls != 2 {
		t.Errorf("expected statFn called again on second Collect, got %d total", calls)
	}
}
