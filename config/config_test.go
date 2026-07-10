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
	if cfg.Profile != "oltp-jsonb" {
		t.Errorf("Profile: want oltp-jsonb, got %q", cfg.Profile)
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

func TestLoad_minPayloadExceedsMax(t *testing.T) {
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

func TestLoad_negativeTargetRate(t *testing.T) {
	t.Setenv("TARGET_RATE_PER_SEC", "-1")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "TARGET_RATE_PER_SEC") {
		t.Fatalf("want TARGET_RATE_PER_SEC error, got %v", err)
	}
}

func TestLoad_zeroTargetRateIsValid(t *testing.T) {
	t.Setenv("TARGET_RATE_PER_SEC", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("TARGET_RATE_PER_SEC=0 should be valid (unlimited): %v", err)
	}
	if cfg.TargetRatePerSec != 0 {
		t.Errorf("want 0, got %d", cfg.TargetRatePerSec)
	}
}

func TestGetEnv_setAndUnset(t *testing.T) {
	t.Setenv("TEST_STR_KEY", "custom")
	if got := getEnv("TEST_STR_KEY", "def"); got != "custom" {
		t.Errorf("set: want custom, got %q", got)
	}
	t.Setenv("TEST_STR_KEY", "")
	if got := getEnv("TEST_STR_KEY", "def"); got != "def" {
		t.Errorf("unset: want def, got %q", got)
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
