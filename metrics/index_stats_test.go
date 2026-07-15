// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package metrics

import "testing"

func TestIndexScanTracker_firstObservationIsZero(t *testing.T) {
	tr := newIndexScanTracker()
	if d := tr.delta("idx", 100); d != 0 {
		t.Errorf("first observation: want 0, got %d", d)
	}
}

func TestIndexScanTracker_normalIncrement(t *testing.T) {
	tr := newIndexScanTracker()
	tr.delta("idx", 100) // seed
	if d := tr.delta("idx", 150); d != 50 {
		t.Errorf("normal increment: want 50, got %d", d)
	}
}

func TestIndexScanTracker_pgStatReset(t *testing.T) {
	tr := newIndexScanTracker()
	tr.delta("idx", 100) // seed
	tr.delta("idx", 200) // +100
	// Simulate pg_stat_reset(): current drops below previous.
	if d := tr.delta("idx", 10); d != 0 {
		t.Errorf("pg_stat_reset: want 0, got %d", d)
	}
}

func TestIndexScanTracker_postResetDelta(t *testing.T) {
	// After a reset-triggered zero the tracker stores the reset point.
	// The next observation must compute delta from that reset point, not the
	// pre-reset peak.
	tr := newIndexScanTracker()
	tr.delta("idx", 100) // seed
	tr.delta("idx", 10)  // reset → stored as 10
	if d := tr.delta("idx", 60); d != 50 {
		t.Errorf("post-reset delta: want 50 (60-10), got %d", d)
	}
}

func TestIndexScanTracker_independentKeys(t *testing.T) {
	tr := newIndexScanTracker()
	tr.delta("a", 100)
	tr.delta("b", 200)
	// Keys must not interfere with each other.
	if d := tr.delta("a", 110); d != 10 {
		t.Errorf("key a: want 10, got %d", d)
	}
	if d := tr.delta("b", 250); d != 50 {
		t.Errorf("key b: want 50, got %d", d)
	}
}
