// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"math/rand"
	"testing"

	"pg-loadgen/config"
)

func TestGetProfile_knownAndUnknown(t *testing.T) {
	p, err := GetProfile(ProfileOLTP)
	if err != nil {
		t.Fatalf("GetProfile(%q): %v", ProfileOLTP, err)
	}
	if p.Name() != ProfileOLTP {
		t.Errorf("Name: want %q, got %q", ProfileOLTP, p.Name())
	}
	if _, err := GetProfile("does-not-exist"); err == nil {
		t.Error("expected an error for an unknown profile")
	}
}

func TestProfileNames_sortedAndIncludesOLTP(t *testing.T) {
	names := ProfileNames()
	found := false
	for i, n := range names {
		if n == ProfileOLTP {
			found = true
		}
		if i > 0 && names[i-1] > n {
			t.Errorf("ProfileNames not sorted: %v", names)
		}
	}
	if !found {
		t.Errorf("ProfileNames %v missing %q", names, ProfileOLTP)
	}
}

func TestOLTPProfile_OpsSumTo100(t *testing.T) {
	ops := (&OLTPProfile{}).Ops()
	if len(ops) != 6 {
		t.Fatalf("want 6 ops, got %d", len(ops))
	}
	total := 0
	for _, od := range ops {
		total += od.DefaultWeight
	}
	if total != 100 {
		t.Errorf("default op weights sum to %d, want 100", total)
	}
}

func TestOLTPProfile_Schema(t *testing.T) {
	s := (&OLTPProfile{}).Schema()
	if len(s.Tables) != 3 {
		t.Errorf("want 3 tables, got %d", len(s.Tables))
	}
	if len(s.Indexes) != 8 {
		t.Errorf("want 8 indexes, got %d", len(s.Indexes))
	}
	if len(s.TrackedTables) != 3 {
		t.Errorf("want 3 tracked tables, got %d", len(s.TrackedTables))
	}
	if s.SentinelIndex == "" {
		t.Error("SentinelIndex should be set (last index for follower wait)")
	}
}

func TestOLTPProfile_InitBuildsRingAndExecutor(t *testing.T) {
	p := &OLTPProfile{}
	cfg := &config.Config{RingSize: 16, MinPayloadKB: 8, MaxPayloadKB: 16}
	// Init does not touch the pool, so a nil pool is fine for this unit test.
	if err := p.Init(cfg, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if p.ring == nil {
		t.Fatal("Init should build the session ring")
	}
	exec := p.NewExecutor(rand.New(rand.NewSource(1)))
	if exec == nil {
		t.Error("NewExecutor returned nil")
	}
}
