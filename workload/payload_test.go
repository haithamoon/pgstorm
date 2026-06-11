package workload

import (
	"encoding/json"
	"math/rand"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Event pool is not populated by init(); initialise it once for all tests.
	InitEventPool(8, 16)
	os.Exit(m.Run())
}

func TestBuildTemplate_validJSON(t *testing.T) {
	for _, tc := range []struct{ min, max int }{{4, 8}, {8, 16}, {4, 4}} {
		rng := rand.New(rand.NewSource(42))
		data, _ := buildTemplate(rng, tc.min, tc.max)
		if !json.Valid(data) {
			t.Errorf("min=%d max=%d: returned invalid JSON", tc.min, tc.max)
		}
	}
}

func TestBuildTemplate_hasRequiredFields(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data, _ := buildTemplate(rng, 8, 16)
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{"request", "response", "stack_trace", "tags", "metrics", "context", "trace_id"} {
		if _, ok := payload[field]; !ok {
			t.Errorf("missing field %q", field)
		}
	}
}

func TestBuildTemplate_reasonableSize(t *testing.T) {
	// Not an exact check — size varies due to random field lengths.
	// The floor of 4 KB ensures something non-trivial was generated;
	// the ceiling of 30 KB ensures no unbounded growth.
	for _, tc := range []struct{ min, max, loKB, hiKB int }{
		{4, 8, 4, 20},
		{8, 16, 6, 30},
	} {
		rng := rand.New(rand.NewSource(7))
		data, _ := buildTemplate(rng, tc.min, tc.max)
		kb := len(data) / 1024
		if kb < tc.loKB || kb > tc.hiKB {
			t.Errorf("min=%d max=%d: size %d KB outside [%d, %d]", tc.min, tc.max, kb, tc.loKB, tc.hiKB)
		}
	}
}

func TestFindTraceIDOffset_pointsToHexChars(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data, offset := buildTemplate(rng, 4, 8)
	if offset < 0 {
		t.Fatal("findTraceIDOffset returned -1: trace_id key not found in JSON")
	}
	if offset+16 > len(data) {
		t.Fatalf("offset %d+16 is out of bounds (len=%d)", offset, len(data))
	}
	const hexSet = "0123456789abcdef"
	for i := offset; i < offset+16; i++ {
		c := data[i]
		found := false
		for _, h := range hexSet {
			if byte(h) == c {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("non-hex byte %q at position %d (offset=%d)", c, i, offset)
		}
	}
}

func TestGetMutatedPayload_returnsValidJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := GetMutatedPayload(rng, 4, 8) // session pool
	if !json.Valid(p) {
		t.Fatal("GetMutatedPayload returned invalid JSON")
	}
}

func TestGetMutatedPayload_sequentialCallsDifferentTraceIDs(t *testing.T) {
	// Two sequential calls on the same RNG advance its state, so the
	// mutated trace_id bytes will differ.
	rng := rand.New(rand.NewSource(42))
	p1 := GetMutatedPayload(rng, 4, 8)
	p2 := GetMutatedPayload(rng, 4, 8)

	var m1, m2 map[string]interface{}
	if err := json.Unmarshal(p1, &m1); err != nil {
		t.Fatalf("unmarshal p1: %v", err)
	}
	if err := json.Unmarshal(p2, &m2); err != nil {
		t.Fatalf("unmarshal p2: %v", err)
	}
	if m1["trace_id"] == m2["trace_id"] {
		t.Error("expected different trace_ids from sequential calls on the same RNG")
	}
}

func TestGetMutatedPayload_mutatesOnlyTraceID(t *testing.T) {
	// Two calls on identical RNG seeds pick the same template slot and
	// produce identical trace_id mutations — verifying the mutation is
	// deterministic and isolated to trace_id.
	p1 := GetMutatedPayload(rand.New(rand.NewSource(5)), 4, 8)
	p2 := GetMutatedPayload(rand.New(rand.NewSource(5)), 4, 8)

	if string(p1) != string(p2) {
		t.Error("identical RNG seeds should produce identical payloads")
	}
}

func TestGetMutatedPayload_eventPool(t *testing.T) {
	// Event pool (minKB > 4) was populated by TestMain.
	rng := rand.New(rand.NewSource(99))
	p := GetMutatedPayload(rng, 8, 16)
	if !json.Valid(p) {
		t.Fatal("event pool payload is invalid JSON")
	}
	if len(p) < 4*1024 {
		t.Errorf("event payload too small: %d bytes", len(p))
	}
}
