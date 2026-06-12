package config

import (
	"strings"
	"testing"
)

func TestLoad_defaultsAreValid(t *testing.T) {
	// All env vars cleared; built-in defaults sum to 100.
	for _, k := range []string{
		"WRITE_PCT", "READ_SIMPLE_PCT", "READ_JOIN_PCT",
		"UPDATE_PCT", "DELETE_PCT", "READ_IP_PCT",
		"MIN_PAYLOAD_KB", "MAX_PAYLOAD_KB",
	} {
		t.Setenv(k, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error with default env, got: %v", err)
	}
	if cfg.Workers != 20 {
		t.Errorf("Workers: want 20, got %d", cfg.Workers)
	}
	if cfg.WritePct != 35 {
		t.Errorf("WritePct: want 35, got %d", cfg.WritePct)
	}
	if cfg.RingSize != 10000 {
		t.Errorf("RingSize: want 10000, got %d", cfg.RingSize)
	}
	if cfg.MinPayloadKB != 8 {
		t.Errorf("MinPayloadKB: want 8, got %d", cfg.MinPayloadKB)
	}
	if cfg.MaxPayloadKB != 16 {
		t.Errorf("MaxPayloadKB: want 16, got %d", cfg.MaxPayloadKB)
	}
}

func TestLoad_pctsMustSum100(t *testing.T) {
	tests := []struct {
		name string
		pcts map[string]string
	}{
		{
			name: "total 99",
			pcts: map[string]string{
				"WRITE_PCT": "34", "READ_SIMPLE_PCT": "15", "READ_JOIN_PCT": "20",
				"UPDATE_PCT": "15", "DELETE_PCT": "10", "READ_IP_PCT": "5",
			},
		},
		{
			name: "total 101",
			pcts: map[string]string{
				"WRITE_PCT": "36", "READ_SIMPLE_PCT": "15", "READ_JOIN_PCT": "20",
				"UPDATE_PCT": "15", "DELETE_PCT": "10", "READ_IP_PCT": "5",
			},
		},
		{
			name: "all zero",
			pcts: map[string]string{
				"WRITE_PCT": "0", "READ_SIMPLE_PCT": "0", "READ_JOIN_PCT": "0",
				"UPDATE_PCT": "0", "DELETE_PCT": "0", "READ_IP_PCT": "0",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.pcts {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "percentages must sum to 100") {
				t.Errorf("unexpected error message: %v", err)
			}
		})
	}
}

func TestLoad_singleOp100Pct(t *testing.T) {
	t.Setenv("WRITE_PCT", "100")
	t.Setenv("READ_SIMPLE_PCT", "0")
	t.Setenv("READ_JOIN_PCT", "0")
	t.Setenv("UPDATE_PCT", "0")
	t.Setenv("DELETE_PCT", "0")
	t.Setenv("READ_IP_PCT", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.WritePct != 100 {
		t.Errorf("WritePct: want 100, got %d", cfg.WritePct)
	}
}

func TestLoad_minPayloadExceedsMax(t *testing.T) {
	// Reset pcts to valid defaults.
	t.Setenv("WRITE_PCT", "35")
	t.Setenv("READ_SIMPLE_PCT", "15")
	t.Setenv("READ_JOIN_PCT", "20")
	t.Setenv("UPDATE_PCT", "15")
	t.Setenv("DELETE_PCT", "10")
	t.Setenv("READ_IP_PCT", "5")

	t.Setenv("MIN_PAYLOAD_KB", "16")
	t.Setenv("MAX_PAYLOAD_KB", "8")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when MIN_PAYLOAD_KB > MAX_PAYLOAD_KB, got nil")
	}
	if !strings.Contains(err.Error(), "MIN_PAYLOAD_KB") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoad_minPayloadEqualToMax(t *testing.T) {
	// MIN == MAX is valid: buildTemplate calls rng.Intn(1) which returns 0 safely.
	t.Setenv("WRITE_PCT", "35")
	t.Setenv("READ_SIMPLE_PCT", "15")
	t.Setenv("READ_JOIN_PCT", "20")
	t.Setenv("UPDATE_PCT", "15")
	t.Setenv("DELETE_PCT", "10")
	t.Setenv("READ_IP_PCT", "5")
	t.Setenv("MIN_PAYLOAD_KB", "8")
	t.Setenv("MAX_PAYLOAD_KB", "8")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error when MIN==MAX, got: %v", err)
	}
	if cfg.MinPayloadKB != 8 || cfg.MaxPayloadKB != 8 {
		t.Errorf("expected both payload KBs to be 8")
	}
}

func TestLoad_negativePct(t *testing.T) {
	// Negative individual percentage that still sums to 100 must be rejected.
	t.Setenv("WRITE_PCT", "-10")
	t.Setenv("READ_SIMPLE_PCT", "110")
	t.Setenv("READ_JOIN_PCT", "0")
	t.Setenv("UPDATE_PCT", "0")
	t.Setenv("DELETE_PCT", "0")
	t.Setenv("READ_IP_PCT", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for negative percentage, got nil")
	}
	if !strings.Contains(err.Error(), ">= 0") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoad_zeroWorkers(t *testing.T) {
	t.Setenv("WORKERS", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for WORKERS=0")
	}
}

func TestLoad_zeroRingSize(t *testing.T) {
	t.Setenv("RING_SIZE", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for RING_SIZE=0")
	}
}

func TestLoad_zeroSummaryInterval(t *testing.T) {
	t.Setenv("SUMMARY_INTERVAL_SECS", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for SUMMARY_INTERVAL_SECS=0")
	}
}

func TestLoad_zeroSchemaPollMs(t *testing.T) {
	t.Setenv("SCHEMA_POLL_MS", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for SCHEMA_POLL_MS=0")
	}
}

func TestGetEnvInt_invalidFallsToDefault(t *testing.T) {
	t.Setenv("TEST_INT_KEY", "not-a-number")
	got := getEnvInt("TEST_INT_KEY", 42)
	if got != 42 {
		t.Errorf("want 42, got %d", got)
	}
}

func TestGetEnvInt_emptyFallsToDefault(t *testing.T) {
	t.Setenv("TEST_INT_KEY", "")
	got := getEnvInt("TEST_INT_KEY", 99)
	if got != 99 {
		t.Errorf("want 99, got %d", got)
	}
}

func TestGetEnvInt_parsesValidValue(t *testing.T) {
	t.Setenv("TEST_INT_KEY", "7")
	got := getEnvInt("TEST_INT_KEY", 42)
	if got != 7 {
		t.Errorf("want 7, got %d", got)
	}
}

func TestGetEnvBool_invalidFallsToDefault(t *testing.T) {
	t.Setenv("TEST_BOOL_KEY", "maybe")
	got := getEnvBool("TEST_BOOL_KEY", true)
	if !got {
		t.Error("want true (default), got false")
	}
}

func TestGetEnvBool_parsesValidValues(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"true", true},
		{"false", false},
		{"1", true},
		{"0", false},
	} {
		t.Setenv("TEST_BOOL_KEY", tc.val)
		got := getEnvBool("TEST_BOOL_KEY", !tc.want)
		if got != tc.want {
			t.Errorf("val=%q: want %v, got %v", tc.val, tc.want, got)
		}
	}
}
