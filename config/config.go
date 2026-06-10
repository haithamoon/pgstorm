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
	RingSize      int

	WritePct      int
	ReadSimplePct int
	ReadJoinPct   int
	UpdatePct     int
	DeletePct     int

	MinPayloadKB int
	MaxPayloadKB int

	DeleteBatchSize    int
	DeleteOlderThanMin int

	ThinkTimeMs int

	MetricsPort int

	RunDurationSecs      int
	SummaryIntervalSecs  int
	IndexStatsIntervalSecs int
	LogLevel             string
}

func Load() (*Config, error) {
	cfg := &Config{
		PGDSN:              getEnv("PG_DSN", "postgres://loadgen:loadgen@localhost:5432/loadtest?sslmode=disable"),
		Workers:            getEnvInt("WORKERS", 20),
		CreateIndexes:      getEnvBool("CREATE_INDEXES", false),
		RingSize:           getEnvInt("RING_SIZE", 10000),
		WritePct:           getEnvInt("WRITE_PCT", 35),
		ReadSimplePct:      getEnvInt("READ_SIMPLE_PCT", 20),
		ReadJoinPct:        getEnvInt("READ_JOIN_PCT", 20),
		UpdatePct:          getEnvInt("UPDATE_PCT", 15),
		DeletePct:          getEnvInt("DELETE_PCT", 10),
		MinPayloadKB:       getEnvInt("MIN_PAYLOAD_KB", 8),
		MaxPayloadKB:       getEnvInt("MAX_PAYLOAD_KB", 16),
		DeleteBatchSize:    getEnvInt("DELETE_BATCH_SIZE", 50),
		DeleteOlderThanMin: getEnvInt("DELETE_OLDER_THAN_MINS", 10),
		ThinkTimeMs:        getEnvInt("THINK_TIME_MS", 0),
		MetricsPort:        getEnvInt("METRICS_PORT", 9090),
		RunDurationSecs:        getEnvInt("RUN_DURATION_SECS", 0),
		SummaryIntervalSecs:    getEnvInt("SUMMARY_INTERVAL_SECS", 30),
		IndexStatsIntervalSecs: getEnvInt("INDEX_STATS_INTERVAL_SECS", 30),
		LogLevel:               getEnv("LOG_LEVEL", "info"),
	}

	total := cfg.WritePct + cfg.ReadSimplePct + cfg.ReadJoinPct + cfg.UpdatePct + cfg.DeletePct
	if total != 100 {
		return nil, fmt.Errorf("operation percentages must sum to 100, got %d", total)
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
