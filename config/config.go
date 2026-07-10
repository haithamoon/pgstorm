package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	PGDSN   string
	Workers int

	CreateIndexes bool
	ReadPayload   bool
	RingSize      int

	WritePct      int
	ReadSimplePct int
	ReadJoinPct   int
	UpdatePct     int
	DeletePct     int
	ReadIPPct     int

	MinPayloadKB int
	MaxPayloadKB int

	DeleteBatchSize int

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
		PGDSN:              getEnv("PG_DSN", "postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable"),
		Workers:            getEnvInt("WORKERS", 20),
		CreateIndexes:      getEnvBool("CREATE_INDEXES", false),
		ReadPayload:        getEnvBool("READ_PAYLOAD", false),
		RingSize:           getEnvInt("RING_SIZE", 10000),
		WritePct:           getEnvInt("WRITE_PCT", 35),
		ReadSimplePct:      getEnvInt("READ_SIMPLE_PCT", 15),
		ReadJoinPct:        getEnvInt("READ_JOIN_PCT", 20),
		UpdatePct:          getEnvInt("UPDATE_PCT", 15),
		DeletePct:          getEnvInt("DELETE_PCT", 10),
		ReadIPPct:          getEnvInt("READ_IP_PCT", 5),
		MinPayloadKB:       getEnvInt("MIN_PAYLOAD_KB", 8),
		MaxPayloadKB:       getEnvInt("MAX_PAYLOAD_KB", 16),
		DeleteBatchSize: getEnvInt("DELETE_BATCH_SIZE", 50),
		ThinkTimeMs:        getEnvInt("THINK_TIME_MS", 0),
		MetricsPort:        getEnvInt("METRICS_PORT", 9090),
		RunDurationSecs:        getEnvInt("RUN_DURATION_SECS", 0),
		SummaryIntervalSecs:    getEnvInt("SUMMARY_INTERVAL_SECS", 30),
		IndexStatsIntervalSecs: getEnvInt("INDEX_STATS_INTERVAL_SECS", 30),
		ShutdownTimeoutSecs:    getEnvInt("SHUTDOWN_TIMEOUT_SECS", 5),
		SchemaPollMs:           getEnvInt("SCHEMA_POLL_MS", 500),
	}

	for _, p := range []struct {
		name string
		val  int
	}{
		{"WRITE_PCT", cfg.WritePct},
		{"READ_SIMPLE_PCT", cfg.ReadSimplePct},
		{"READ_JOIN_PCT", cfg.ReadJoinPct},
		{"UPDATE_PCT", cfg.UpdatePct},
		{"DELETE_PCT", cfg.DeletePct},
		{"READ_IP_PCT", cfg.ReadIPPct},
	} {
		if p.val < 0 {
			return nil, fmt.Errorf("%s must be >= 0, got %d", p.name, p.val)
		}
	}

	total := cfg.WritePct + cfg.ReadSimplePct + cfg.ReadJoinPct + cfg.UpdatePct + cfg.DeletePct + cfg.ReadIPPct
	if total != 100 {
		return nil, fmt.Errorf("operation percentages must sum to 100, got %d", total)
	}

	if cfg.MinPayloadKB > cfg.MaxPayloadKB {
		return nil, fmt.Errorf("MIN_PAYLOAD_KB (%d) must not exceed MAX_PAYLOAD_KB (%d)", cfg.MinPayloadKB, cfg.MaxPayloadKB)
	}

	for _, v := range []struct {
		name string
		val  int
		min  int
	}{
		{"WORKERS", cfg.Workers, 1},
		{"RING_SIZE", cfg.RingSize, 1},
		{"DELETE_BATCH_SIZE", cfg.DeleteBatchSize, 1},
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
