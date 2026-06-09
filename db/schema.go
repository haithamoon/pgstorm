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

func MigrateWithLock(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) error {
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

		if err := CreateTables(ctx, pool); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
		if cfg.CreateIndexes {
			if err := CreateIndexes(ctx, pool); err != nil {
				return fmt.Errorf("create indexes: %w", err)
			}
		}
		log.Println("schema setup complete — releasing lock")
	} else {
		log.Println("migration lock held by another pod — waiting for schema")
		if err := WaitForSchema(ctx, pool); err != nil {
			return fmt.Errorf("wait for schema: %w", err)
		}
		log.Println("schema ready — proceeding")
	}
	return nil
}

func CreateTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS sessions (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id     UUID NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			ended_at    TIMESTAMPTZ,
			region      TEXT NOT NULL,
			metadata    JSONB NOT NULL,
			status      TEXT NOT NULL DEFAULT 'active',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE IF NOT EXISTS events (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			session_id    UUID NOT NULL REFERENCES sessions(id),
			event_type    TEXT NOT NULL,
			occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			payload       JSONB NOT NULL,
			severity      TEXT NOT NULL DEFAULT 'info',
			trace_id      TEXT NOT NULL,
			source_ip     INET,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE IF NOT EXISTS audit_log (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			entity_type   TEXT NOT NULL,
			entity_id     UUID NOT NULL,
			action        TEXT NOT NULL,
			changed_by    UUID NOT NULL,
			changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			diff          JSONB NOT NULL,
			checksum      TEXT NOT NULL
		);
	`)
	return err
}

func CreateIndexes(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_sessions_user_id        ON sessions (user_id);
		CREATE INDEX IF NOT EXISTS idx_sessions_status_created ON sessions (status, created_at DESC);

		CREATE INDEX IF NOT EXISTS idx_events_session_id          ON events (session_id);
		CREATE INDEX IF NOT EXISTS idx_events_occurred_at         ON events (occurred_at DESC);
		CREATE INDEX IF NOT EXISTS idx_events_severity_occurred   ON events (severity, occurred_at DESC);

		CREATE INDEX IF NOT EXISTS idx_audit_log_entity_id  ON audit_log (entity_id, changed_at DESC);
		CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log (changed_at DESC);
	`)
	return err
}

func WaitForSchema(ctx context.Context, pool *pgxpool.Pool) error {
	required := []string{"sessions", "events", "audit_log"}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			missing := checkMissingTables(ctx, pool, required)
			if len(missing) == 0 {
				return nil
			}
			log.Printf("waiting for tables: %v", missing)
		}
	}
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
