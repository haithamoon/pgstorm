package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	PGDSN   string
	Profile string
	Workers int

	CreateIndexes bool
	ReadPayload   bool
	RingSize      int

	MinPayloadKB int
	MaxPayloadKB int
	ToastPct     int

	DeleteBatchSize int

	UserPoolSize  int
	ActorPoolSize int

	ThinkTimeMs int

	MetricsPort int

	RunDurationSecs        int
	SummaryIntervalSecs    int
	IndexStatsIntervalSecs int
	ShutdownTimeoutSecs    int
	SchemaPollMs           int
}

func Load() (*Config, error) {
	cfg := &Config{
		PGDSN:                  getEnv("PG_DSN", "postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable"),
		Profile:                getEnv("PROFILE", "oltp-jsonb"),
		Workers:                getEnvInt("WORKERS", 20),
		CreateIndexes:          getEnvBool("CREATE_INDEXES", false),
		ReadPayload:            getEnvBool("READ_PAYLOAD", false),
		RingSize:               getEnvInt("RING_SIZE", 10000),
		MinPayloadKB:           getEnvInt("MIN_PAYLOAD_KB", 8),
		MaxPayloadKB:           getEnvInt("MAX_PAYLOAD_KB", 16),
		ToastPct:               getEnvInt("TOAST_PCT", 20),
		DeleteBatchSize:        getEnvInt("DELETE_BATCH_SIZE", 50),
		UserPoolSize:           getEnvInt("USER_POOL_SIZE", 10000),
		ActorPoolSize:          getEnvInt("ACTOR_POOL_SIZE", 100),
		ThinkTimeMs:            getEnvInt("THINK_TIME_MS", 0),
		MetricsPort:            getEnvInt("METRICS_PORT", 9090),
		RunDurationSecs:        getEnvInt("RUN_DURATION_SECS", 0),
		SummaryIntervalSecs:    getEnvInt("SUMMARY_INTERVAL_SECS", 30),
		IndexStatsIntervalSecs: getEnvInt("INDEX_STATS_INTERVAL_SECS", 30),
		ShutdownTimeoutSecs:    getEnvInt("SHUTDOWN_TIMEOUT_SECS", 5),
		SchemaPollMs:           getEnvInt("SCHEMA_POLL_MS", 500),
	}

	// Op-mix weights are validated per-profile by workload.ResolveWeights, since
	// each profile declares its own operations and env-var names.

	if cfg.MinPayloadKB < 1 {
		return nil, fmt.Errorf("MIN_PAYLOAD_KB must be >= 1, got %d", cfg.MinPayloadKB)
	}
	if cfg.MaxPayloadKB < 1 {
		return nil, fmt.Errorf("MAX_PAYLOAD_KB must be >= 1, got %d", cfg.MaxPayloadKB)
	}
	if cfg.MinPayloadKB > cfg.MaxPayloadKB {
		return nil, fmt.Errorf("MIN_PAYLOAD_KB (%d) must not exceed MAX_PAYLOAD_KB (%d)", cfg.MinPayloadKB, cfg.MaxPayloadKB)
	}
	if cfg.ToastPct < 0 || cfg.ToastPct > 100 {
		return nil, fmt.Errorf("TOAST_PCT must be between 0 and 100, got %d", cfg.ToastPct)
	}

	for _, v := range []struct {
		name string
		val  int
		min  int
	}{
		{"WORKERS", cfg.Workers, 1},
		{"RING_SIZE", cfg.RingSize, 1},
		{"DELETE_BATCH_SIZE", cfg.DeleteBatchSize, 1},
		{"USER_POOL_SIZE", cfg.UserPoolSize, 1},
		{"ACTOR_POOL_SIZE", cfg.ActorPoolSize, 1},
		{"METRICS_PORT", cfg.MetricsPort, 1},
		{"SUMMARY_INTERVAL_SECS", cfg.SummaryIntervalSecs, 1},
		{"INDEX_STATS_INTERVAL_SECS", cfg.IndexStatsIntervalSecs, 1},
		{"SCHEMA_POLL_MS", cfg.SchemaPollMs, 1},
		{"SHUTDOWN_TIMEOUT_SECS", cfg.ShutdownTimeoutSecs, 1},
	} {
		if v.val < v.min {
			return nil, fmt.Errorf("%s must be >= %d, got %d", v.name, v.min, v.val)
		}
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}
