package workload

import (
	"strings"
	"testing"
)

func TestSelectOp_allSixOps(t *testing.T) {
	// oltp-jsonb default mix, resolved to weighted ops in declaration order.
	ops := []WeightedOp{
		{OpInsert, 35}, {OpReadSimple, 15}, {OpReadJoin, 20},
		{OpUpdate, 15}, {OpDelete, 10}, {OpReadByIP, 5},
	}
	// Cumulative boundaries: insert [0,35), read_simple [35,50), read_join [50,70),
	// update [70,85), delete [85,95), read_by_ip [95,100).
	tests := []struct {
		roll int
		want string
	}{
		{0, OpInsert}, {34, OpInsert},
		{35, OpReadSimple}, {49, OpReadSimple},
		{50, OpReadJoin}, {69, OpReadJoin},
		{70, OpUpdate}, {84, OpUpdate},
		{85, OpDelete}, {94, OpDelete},
		{95, OpReadByIP}, {99, OpReadByIP},
	}
	for _, tc := range tests {
		if got := SelectOp(tc.roll, ops); got != tc.want {
			t.Errorf("roll=%d: want %s, got %s", tc.roll, tc.want, got)
		}
	}
}

func TestSelectOp_singleOp100(t *testing.T) {
	ops := []WeightedOp{{OpInsert, 100}}
	for roll := 0; roll < 100; roll++ {
		if SelectOp(roll, ops) != OpInsert {
			t.Fatalf("roll=%d: expected OpInsert when insert=100", roll)
		}
	}
}

func TestSelectOp_lastBucketFallback(t *testing.T) {
	// roll lands exactly on the sum boundary → last op absorbs it.
	ops := []WeightedOp{{OpInsert, 50}, {OpReadSimple, 50}}
	if got := SelectOp(99, ops); got != OpReadSimple {
		t.Errorf("want OpReadSimple, got %s", got)
	}
}

func TestSelectOp_empty(t *testing.T) {
	if got := SelectOp(0, nil); got != "" {
		t.Errorf(`empty ops: want "", got %q`, got)
	}
}

func TestSelectOp_rollAboveSumReturnsLast(t *testing.T) {
	// Defensive fallthrough: if roll lands past the cumulative weight (e.g. weights
	// don't reach 100), the last op absorbs it rather than returning "".
	ops := []WeightedOp{{OpInsert, 10}, {OpDelete, 20}}
	if got := SelectOp(50, ops); got != OpDelete {
		t.Errorf("roll past sum: want %s, got %s", OpDelete, got)
	}
}

func TestOpNames_preservesOrder(t *testing.T) {
	names := OpNames([]WeightedOp{{OpInsert, 35}, {OpDelete, 10}, {OpReadByIP, 5}})
	want := []string{OpInsert, OpDelete, OpReadByIP}
	if len(names) != len(want) {
		t.Fatalf("want %d names, got %d", len(want), len(names))
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("names[%d]: want %s, got %s", i, want[i], names[i])
		}
	}
}

// ── ResolveWeights ───────────────────────────────────────────────────────────

// oltpOps is the shipped oltp-jsonb OpDef list, taken from the profile itself so
// these tests validate the real defaults rather than a copy that could silently
// drift out of sync.
var oltpOps = (&OLTPProfile{}).Ops()

func clearOpEnv(t *testing.T) {
	t.Helper()
	for _, od := range oltpOps {
		t.Setenv(od.EnvVar, "")
	}
}

func TestResolveWeights_defaultsSumTo100(t *testing.T) {
	clearOpEnv(t)
	got, err := ResolveWeights(oltpOps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(oltpOps) {
		t.Fatalf("want %d ops, got %d", len(oltpOps), len(got))
	}
	if got[0].Name != OpInsert || got[0].Weight != 35 {
		t.Errorf("first op: want {insert 35}, got %+v", got[0])
	}
}

func TestResolveWeights_envOverride(t *testing.T) {
	clearOpEnv(t)
	t.Setenv("WRITE_PCT", "100")
	t.Setenv("READ_JOIN_PCT", "0")
	t.Setenv("READ_SIMPLE_PCT", "0")
	t.Setenv("UPDATE_PCT", "0")
	t.Setenv("DELETE_PCT", "0")
	t.Setenv("READ_IP_PCT", "0")
	got, err := ResolveWeights(oltpOps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0].Weight != 100 {
		t.Errorf("insert weight: want 100, got %d", got[0].Weight)
	}
}

func TestResolveWeights_mustSum100(t *testing.T) {
	for _, tc := range []struct{ name, write string }{
		{"total 99", "34"},
		{"total 101", "36"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearOpEnv(t)
			t.Setenv("WRITE_PCT", tc.write)
			_, err := ResolveWeights(oltpOps)
			if err == nil || !strings.Contains(err.Error(), "sum to 100") {
				t.Fatalf("want sum error, got %v", err)
			}
		})
	}
}

func TestResolveWeights_negativeRejected(t *testing.T) {
	clearOpEnv(t)
	t.Setenv("WRITE_PCT", "-10")
	t.Setenv("READ_SIMPLE_PCT", "110")
	t.Setenv("READ_JOIN_PCT", "0")
	t.Setenv("UPDATE_PCT", "0")
	t.Setenv("DELETE_PCT", "0")
	t.Setenv("READ_IP_PCT", "0")
	_, err := ResolveWeights(oltpOps)
	if err == nil || !strings.Contains(err.Error(), ">= 0") {
		t.Fatalf("want >= 0 error, got %v", err)
	}
}

func TestResolveWeights_invalidIntFallsToDefault(t *testing.T) {
	// Matches config.getEnvInt: a malformed value falls back to the default rather
	// than aborting startup, so a typo'd *_PCT parses exactly as it did pre-refactor.
	clearOpEnv(t)
	t.Setenv("WRITE_PCT", "abc")
	got, err := ResolveWeights(oltpOps)
	if err != nil {
		t.Fatalf("malformed value should fall back to default, got error: %v", err)
	}
	if got[0].Name != OpInsert || got[0].Weight != 35 {
		t.Errorf("WRITE_PCT=abc should fall back to default 35, got %+v", got[0])
	}
}
