package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
)

const advisoryLockID = 7654321

// Schema is the DDL a workload profile needs plus the relnames to track for
// stats. The migration runner executes Tables (always) and Indexes (only when
// CREATE_INDEXES=true) under an advisory lock so exactly one replica runs DDL;
// the others wait via WaitForSchema.
type Schema struct {
	Tables  []string // CREATE TABLE IF NOT EXISTS ... (one statement each)
	Indexes []string // CREATE INDEX CONCURRENTLY IF NOT EXISTS ...
	// TrackedTables lists the profile's table relnames. It serves two roles: the
	// tables reported in table/index stats, AND the follower schema-readiness set —
	// WaitForSchema polls until every TrackedTable exists before workers start. A
	// profile MUST therefore list every table its workers depend on; omitting one
	// (or leaving this empty) would let followers start before that table is created.
	TrackedTables []string
	SentinelIndex string // last index followers wait for; "" = no index wait
}

func MigrateWithLock(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, schema Schema) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migration: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryLockID).Scan(&locked); err != nil {
		return fmt.Errorf("try advisory lock: %w", err)
	}

	if locked {
		defer conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID) //nolint
		log.Println("acquired migration lock — running schema setup")

		if err := CreateTables(ctx, pool, schema); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
		if cfg.CreateIndexes {
			if err := CreateIndexes(ctx, pool, schema); err != nil {
				return fmt.Errorf("create indexes: %w", err)
			}
		}
		log.Println("schema setup complete — releasing lock")
	} else {
		log.Println("migration lock held by another pod — waiting for schema")
		pollInterval := time.Duration(cfg.SchemaPollMs) * time.Millisecond
		if err := WaitForSchema(ctx, pool, schema, cfg.CreateIndexes, pollInterval); err != nil {
			return fmt.Errorf("wait for schema: %w", err)
		}
		log.Println("schema ready — proceeding")
	}
	return nil
}

// CreateTables runs each of the schema's table DDL statements in sequence. Each
// is CREATE TABLE IF NOT EXISTS, so it is idempotent and safe to re-run.
func CreateTables(ctx context.Context, pool *pgxpool.Pool, schema Schema) error {
	for _, ddl := range schema.Tables {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

// CreateIndexes runs each index DDL in its own statement. Indexes are expected to
// use CREATE INDEX CONCURRENTLY, which avoids a ShareLock that would block DML on
// a table already under load — and which cannot run inside a transaction block,
// so the statements must not be batched.
func CreateIndexes(ctx context.Context, pool *pgxpool.Pool, schema Schema) error {
	for _, ddl := range schema.Indexes {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

func WaitForSchema(ctx context.Context, pool *pgxpool.Pool, schema Schema, waitIndexes bool, pollInterval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			missing := checkMissingTables(ctx, pool, schema.TrackedTables)
			if len(missing) > 0 {
				log.Printf("waiting for tables: %v", missing)
				continue
			}
			// Wait for the sentinel index too, so followers don't start workers
			// while CREATE INDEX still holds locks on the tables.
			if waitIndexes && schema.SentinelIndex != "" && !indexExists(ctx, pool, schema.SentinelIndex) {
				log.Printf("waiting for indexes (CREATE_INDEXES=true)")
				continue
			}
			return nil
		}
	}
}

func indexExists(ctx context.Context, pool *pgxpool.Pool, name string) bool {
	var exists bool
	pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1)",
		name,
	).Scan(&exists) //nolint
	return exists
}

func checkMissingTables(ctx context.Context, pool *pgxpool.Pool, required []string) []string {
	rows, err := pool.Query(ctx,
		"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY($1)",
		required,
	)
	if err != nil {
		return required
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			found[name] = true
		}
	}

	var missing []string
	for _, t := range required {
		if !found[t] {
			missing = append(missing, t)
		}
	}
	return missing
}
