// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

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

func TestLoad_negativeMinPayload(t *testing.T) {
	t.Setenv("MIN_PAYLOAD_KB", "-1")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MIN_PAYLOAD_KB") {
		t.Fatalf("want MIN_PAYLOAD_KB error, got %v", err)
	}
}

func TestLoad_negativeMaxPayload(t *testing.T) {
	t.Setenv("MAX_PAYLOAD_KB", "-1")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MAX_PAYLOAD_KB") {
		t.Fatalf("want MAX_PAYLOAD_KB error, got %v", err)
	}
}

func TestLoad_zeroPayloadRejected(t *testing.T) {
	t.Setenv("MIN_PAYLOAD_KB", "0")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MIN_PAYLOAD_KB") {
		t.Fatalf("want MIN_PAYLOAD_KB error for zero, got %v", err)
	}
}

func TestLoad_toastPctDefault(t *testing.T) {
	t.Setenv("TOAST_PCT", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("default TOAST_PCT should be valid: %v", err)
	}
	if cfg.ToastPct != 20 {
		t.Errorf("ToastPct default: want 20, got %d", cfg.ToastPct)
	}
}

func TestLoad_toastPctBoundsValid(t *testing.T) {
	for _, v := range []string{"0", "100", "50"} {
		t.Setenv("TOAST_PCT", v)
		if _, err := Load(); err != nil {
			t.Errorf("TOAST_PCT=%s should be valid: %v", v, err)
		}
	}
}

func TestLoad_toastPctOutOfRangeRejected(t *testing.T) {
	for _, v := range []string{"-1", "101"} {
		t.Setenv("TOAST_PCT", v)
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "TOAST_PCT") {
			t.Fatalf("want TOAST_PCT error for %s, got %v", v, err)
		}
	}
}

func TestLoad_actorPoolDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
	if cfg.UserPoolSize != 10000 {
		t.Errorf("UserPoolSize default: want 10000, got %d", cfg.UserPoolSize)
	}
	if cfg.ActorPoolSize != 100 {
		t.Errorf("ActorPoolSize default: want 100, got %d", cfg.ActorPoolSize)
	}
}

func TestLoad_zeroUserPoolSize(t *testing.T) {
	t.Setenv("USER_POOL_SIZE", "0")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "USER_POOL_SIZE") {
		t.Fatalf("want USER_POOL_SIZE error for zero, got %v", err)
	}
}

func TestLoad_zeroActorPoolSize(t *testing.T) {
	t.Setenv("ACTOR_POOL_SIZE", "0")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "ACTOR_POOL_SIZE") {
		t.Fatalf("want ACTOR_POOL_SIZE error for zero, got %v", err)
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
