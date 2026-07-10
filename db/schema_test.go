//go:build integration

package db_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
	"pg-loadgen/db"
)

// testSchema is a minimal three-table schema (FK-ordered) that exercises the
// generic migration machinery without depending on any workload profile.
var testSchema = db.Schema{
	Tables: []string{
		`CREATE TABLE IF NOT EXISTS sessions (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`,
		`CREATE TABLE IF NOT EXISTS events (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_id UUID NOT NULL REFERENCES sessions(id))`,
		`CREATE TABLE IF NOT EXISTS audit_log (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`,
	},
	TrackedTables: []string{"sessions", "events", "audit_log"},
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("PG_DSN not set — skipping integration test")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

func dropTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS audit_log;
		DROP TABLE IF EXISTS events;
		DROP TABLE IF EXISTS sessions;
	`)
	if err != nil {
		t.Fatalf("dropTables: %v", err)
	}
}

func TestMigrateWithLock_createsTables(t *testing.T) {
	pool := testPool(t)
	dropTables(t, pool)
	t.Cleanup(func() { dropTables(t, pool) })

	cfg := &config.Config{CreateIndexes: false, SchemaPollMs: 500}
	if err := db.MigrateWithLock(context.Background(), pool, cfg, testSchema); err != nil {
		t.Fatalf("MigrateWithLock: %v", err)
	}

	// All three tables must now exist.
	for _, table := range []string{"sessions", "events", "audit_log"} {
		var exists bool
		err := pool.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=$1)",
			table,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s not found after migration", table)
		}
	}
}

func TestMigrateWithLock_idempotent(t *testing.T) {
	pool := testPool(t)
	dropTables(t, pool)
	t.Cleanup(func() { dropTables(t, pool) })

	cfg := &config.Config{CreateIndexes: false, SchemaPollMs: 500}
	if err := db.MigrateWithLock(context.Background(), pool, cfg, testSchema); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	// Running again must not error (IF NOT EXISTS guards).
	if err := db.MigrateWithLock(context.Background(), pool, cfg, testSchema); err != nil {
		t.Fatalf("second migration: %v", err)
	}
}

// TestWaitForSchema_blocks verifies that WaitForSchema actually polls until
// tables appear. The scaffold:
//   - goroutine A: calls WaitForSchema immediately (tables don't exist yet)
//   - goroutine B: sleeps 200 ms then creates the tables
//   - assertion: A unblocks within a 3 s deadline
func TestWaitForSchema_blocks(t *testing.T) {
	pool := testPool(t)
	dropTables(t, pool)
	t.Cleanup(func() { dropTables(t, pool) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ready := make(chan error, 1)
	go func() {
		ready <- db.WaitForSchema(ctx, pool, testSchema, false, 500*time.Millisecond)
	}()

	// Give goroutine A a head start before creating the tables.
	time.Sleep(50 * time.Millisecond)

	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := db.CreateTables(context.Background(), pool, testSchema); err != nil {
			t.Errorf("CreateTables: %v", err)
		}
	}()

	start := time.Now()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("WaitForSchema returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("WaitForSchema did not unblock within 3 s after tables were created")
	}

	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond {
		t.Errorf("WaitForSchema returned too quickly (%d ms) — it did not block", elapsed.Milliseconds())
	}
}

func TestMigrateWithLock_concurrent(t *testing.T) {
	// Two goroutines race for the advisory lock. Only one runs DDL; the other
	// calls WaitForSchema. Both must return nil.
	pool := testPool(t)
	dropTables(t, pool)
	t.Cleanup(func() { dropTables(t, pool) })

	cfg := &config.Config{CreateIndexes: false, SchemaPollMs: 500}
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = db.MigrateWithLock(ctx, pool, cfg, testSchema)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: MigrateWithLock returned error: %v", i, err)
		}
	}
}
