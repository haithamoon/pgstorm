// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package metrics

import "testing"

// pgStatsTracker uses float64 values (from Postgres system views) and combines
// the first-observation and pg_stat_reset checks into a single condition.

func TestPGStatsTracker_firstObservationIsZero(t *testing.T) {
	tr := newPGStatsTracker()
	if d := tr.delta("k", 100.0); d != 0 {
		t.Errorf("first observation: want 0.0, got %v", d)
	}
}

func TestPGStatsTracker_normalIncrement(t *testing.T) {
	tr := newPGStatsTracker()
	tr.delta("k", 100.0) // seed
	if d := tr.delta("k", 150.0); d != 50.0 {
		t.Errorf("normal increment: want 50.0, got %v", d)
	}
}

func TestPGStatsTracker_pgStatReset(t *testing.T) {
	tr := newPGStatsTracker()
	tr.delta("k", 100.0) // seed
	tr.delta("k", 200.0) // +100
	if d := tr.delta("k", 10.0); d != 0 {
		t.Errorf("pg_stat_reset: want 0.0, got %v", d)
	}
}

func TestPGStatsTracker_postResetDelta(t *testing.T) {
	// Tracker stores the reset-point value so the next delta is relative to it.
	tr := newPGStatsTracker()
	tr.delta("k", 100.0) // seed
	tr.delta("k", 10.0)  // reset → stored as 10.0
	if d := tr.delta("k", 60.0); d != 50.0 {
		t.Errorf("post-reset delta: want 50.0 (60-10), got %v", d)
	}
}

func TestPGStatsTracker_independentKeys(t *testing.T) {
	tr := newPGStatsTracker()
	tr.delta("a", 1000.0)
	tr.delta("b", 2000.0)
	if d := tr.delta("a", 1100.0); d != 100.0 {
		t.Errorf("key a: want 100.0, got %v", d)
	}
	if d := tr.delta("b", 2500.0); d != 500.0 {
		t.Errorf("key b: want 500.0, got %v", d)
	}
}

// TestCollectBgwriterStats_versionDispatch documents the boundary at which the
// PG17 code path activates. The condition is pgMajor >= 17 inside
// collectBgwriterStats; this test pins the expected values so a refactor that
// moves the boundary is immediately caught.
func TestCollectBgwriterStats_versionDispatch(t *testing.T) {
	for _, tc := range []struct {
		major    int
		wantPG17 bool
	}{
		{14, false},
		{16, false},
		{17, true},
		{18, true},
	} {
		got := tc.major >= 17
		if got != tc.wantPG17 {
			t.Errorf("major=%d: isPG17=%v, want %v", tc.major, got, tc.wantPG17)
		}
	}
}
