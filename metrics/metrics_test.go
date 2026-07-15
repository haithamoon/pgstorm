// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter Write: %v", err)
	}
	return m.GetCounter().GetValue()
}

func TestRecordOp_countsOkAndError(t *testing.T) {
	const op = "unit_test_op" // a label unused elsewhere, so counts start at 0
	RecordOp(op, 0.010, nil)
	RecordOp(op, 0.020, nil)
	RecordOp(op, 0.030, fmt.Errorf("boom"))

	if v := counterValue(t, OpsTotal.WithLabelValues(op, "ok")); v != 2 {
		t.Errorf("ok count: want 2, got %v", v)
	}
	if v := counterValue(t, OpsTotal.WithLabelValues(op, "error")); v != 1 {
		t.Errorf("error count: want 1, got %v", v)
	}
}
