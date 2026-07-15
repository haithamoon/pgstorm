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

func TestFindTraceIDOffset_notFound(t *testing.T) {
	if off := findTraceIDOffset([]byte(`{"no":"such marker here"}`)); off != -1 {
		t.Errorf("no trace_id marker: want -1, got %d", off)
	}
}

func TestRandomBase64Exact(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	if s := randomBase64Exact(rng, 0); s != "" {
		t.Errorf("n=0: want empty string, got %q", s)
	}
	if s := randomBase64Exact(rng, -4); s != "" {
		t.Errorf("n<0: want empty string, got %q", s)
	}
	for _, n := range []int{1, 2, 3, 7, 64, 1000} {
		if got := len(randomBase64Exact(rng, n)); got != n {
			t.Errorf("randomBase64Exact(rng, %d): length %d, want exactly %d", n, got, n)
		}
	}
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

func TestGetSessionPayload_returnsValidJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := GetSessionPayload(rng)
	if !json.Valid(p) {
		t.Fatal("GetSessionPayload returned invalid JSON")
	}
}

func TestGetSessionPayload_sequentialCallsDifferentTraceIDs(t *testing.T) {
	// Two sequential calls on the same RNG advance its state, so the
	// mutated trace_id bytes will differ.
	rng := rand.New(rand.NewSource(42))
	p1 := GetSessionPayload(rng)
	p2 := GetSessionPayload(rng)

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

func TestGetSessionPayload_mutatesOnlyTraceID(t *testing.T) {
	// Two calls on identical RNG seeds pick the same template slot and
	// produce identical trace_id mutations — verifying the mutation is
	// deterministic and isolated to trace_id.
	p1 := GetSessionPayload(rand.New(rand.NewSource(5)))
	p2 := GetSessionPayload(rand.New(rand.NewSource(5)))

	if string(p1) != string(p2) {
		t.Error("identical RNG seeds should produce identical payloads")
	}
}

func TestGetEventPayload_returnsValidJSON(t *testing.T) {
	// Event pool was populated by TestMain (InitEventPool(8, 16)).
	rng := rand.New(rand.NewSource(99))
	p := GetEventPayload(rng)
	if !json.Valid(p) {
		t.Fatal("event pool payload is invalid JSON")
	}
	if len(p) < 4*1024 {
		t.Errorf("event payload too small: %d bytes", len(p))
	}
}

// TestGetEventPayload_honorsConfiguredRange is the regression test for the
// pool-selection bug: previously GetMutatedPayload routed by minKB (<=4 → the
// fixed 4–8 KB session pool), so MAX_PAYLOAD_KB was silently ignored. Selection
// is now by field, so events always draw from the configured event pool. Build
// the event pool for a large range and confirm event payloads clearly exceed the
// session pool's 8 KB ceiling while session payloads do not.
func TestGetEventPayload_honorsConfiguredRange(t *testing.T) {
	InitEventPool(32, 64)      // large configured range
	defer InitEventPool(8, 16) // restore for other tests (run sequentially)

	rng := rand.New(rand.NewSource(7))

	var maxEvent int
	for i := 0; i < 100; i++ {
		if n := len(GetEventPayload(rng)); n > maxEvent {
			maxEvent = n
		}
	}
	if maxEvent <= 16*1024 {
		t.Errorf("event pool built for 32–64 KB but largest of 100 payloads was %d bytes; "+
			"selection appears to still ignore the configured range", maxEvent)
	}

	var maxSession int
	for i := 0; i < 100; i++ {
		if n := len(GetSessionPayload(rng)); n > maxSession {
			maxSession = n
		}
	}
	if maxSession > 16*1024 {
		t.Errorf("session payload should stay in the fixed 4–8 KB pool, got %d bytes", maxSession)
	}
}

// toastSplit is a size threshold that cleanly separates small inline payloads
// (session/event ≲1.4 KB, audit ≲1.2 KB) from large TOASTing ones (session ≥4 KB,
// event ≥8 KB, audit ≥~1.9 KB). Used only by the TOAST_PCT tests.
const toastSplit = 1600

func TestToastPct_allLargeAt100(t *testing.T) {
	SetToastPct(100)
	defer SetToastPct(100)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 300; i++ {
		if n := len(GetSessionPayload(rng)); n <= toastSplit {
			t.Fatalf("TOAST_PCT=100 session: want large (>%d), got %d", toastSplit, n)
		}
		if n := len(GetEventPayload(rng)); n <= toastSplit {
			t.Fatalf("TOAST_PCT=100 event: want large (>%d), got %d", toastSplit, n)
		}
		if n := len(GetAuditDiff(rng)); n <= toastSplit {
			t.Fatalf("TOAST_PCT=100 audit: want large (>%d), got %d", toastSplit, n)
		}
	}
}

func TestToastPct_allSmallAt0(t *testing.T) {
	SetToastPct(0)
	defer SetToastPct(100)
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 300; i++ {
		if n := len(GetSessionPayload(rng)); n >= toastSplit {
			t.Fatalf("TOAST_PCT=0 session: want small (<%d), got %d", toastSplit, n)
		}
		if n := len(GetEventPayload(rng)); n >= toastSplit {
			t.Fatalf("TOAST_PCT=0 event: want small (<%d), got %d", toastSplit, n)
		}
		if n := len(GetAuditDiff(rng)); n >= toastSplit {
			t.Fatalf("TOAST_PCT=0 audit: want small (<%d), got %d", toastSplit, n)
		}
	}
}

func TestToastPct_mixAt50(t *testing.T) {
	SetToastPct(50)
	defer SetToastPct(100)
	rng := rand.New(rand.NewSource(3))
	var big, small int
	for i := 0; i < 500; i++ {
		if len(GetEventPayload(rng)) >= toastSplit {
			big++
		} else {
			small++
		}
	}
	if big == 0 || small == 0 {
		t.Fatalf("TOAST_PCT=50: want both large and small over 500 samples, got big=%d small=%d", big, small)
	}
}

func TestToastPct_smallValuesValidAndUnique(t *testing.T) {
	SetToastPct(0)
	defer SetToastPct(100)
	rng := rand.New(rand.NewSource(4))
	for _, get := range []func(*rand.Rand) []byte{GetEventPayload, GetAuditDiff} {
		seen := make(map[string]bool, 200)
		for i := 0; i < 200; i++ {
			v := get(rng)
			if !json.Valid(v) {
				t.Fatal("small value is not valid JSON")
			}
			s := string(v)
			if seen[s] {
				t.Fatalf("iteration %d: small value byte-identical to an earlier one", i)
			}
			seen[s] = true
		}
	}
}
