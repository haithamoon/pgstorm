package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"pg-loadgen/config"
)

// ── pure helper tests ────────────────────────────────────────────────────────

func TestRandomIP_format(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	re := regexp.MustCompile(`^192\.168\.\d{1,3}\.\d{1,3}$`)
	for i := 0; i < 200; i++ {
		ip := randomIP(rng)
		if !re.MatchString(ip) {
			t.Errorf("invalid IP format: %q", ip)
		}
	}
}

func TestGetAuditDiff_validJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	data := GetAuditDiff(rng)
	if !json.Valid(data) {
		t.Fatal("GetAuditDiff returned invalid JSON")
	}
	var m map[string]interface{}
	json.Unmarshal(data, &m) //nolint
	for _, key := range []string{"before", "after", "changed_fields", "context"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q in audit diff", key)
		}
	}
}

func TestGetAuditDiff_sizeRange(t *testing.T) {
	// Behavior preserved from the original buildAuditDiff: targetSize is 2–4 KB but
	// the padding logic subtracts ~12 bytes for key overhead, so a diff can land
	// fractionally below 2 KB (~2046 bytes) when the pre-pad JSON is already large.
	// Lower bound is 1.9 KB to account for this.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		data := GetAuditDiff(rng)
		kb := float64(len(data)) / 1024
		if kb < 1.9 || kb > 4.5 {
			t.Errorf("iteration %d: size %.2f KB outside [1.9, 4.5]", i, kb)
		}
	}
}

// TestGetAuditDiff_byteUnique proves the pooled diff is mutated per call (the
// 16-hex _nonce is rewritten), so no two writes emit byte-identical audit diffs.
func TestGetAuditDiff_byteUnique(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	seen := make(map[string]struct{}, 500)
	for i := 0; i < 500; i++ {
		s := string(GetAuditDiff(rng))
		if _, dup := seen[s]; dup {
			t.Fatalf("iteration %d: GetAuditDiff returned a byte-identical diff", i)
		}
		seen[s] = struct{}{}
	}
}

// ── mock DB types ────────────────────────────────────────────────────────────

type mockPool struct {
	beginTx      *mockTx
	beginErr     error
	beginCalled  bool
	execSQL      []string
	execErr      error
	queryErr     error  // error the next Query returns
	queryRows    int    // rows the next Query returns
	lastQuerySQL string // captured for assertions
}

func (m *mockPool) Begin(ctx context.Context) (pgx.Tx, error) {
	m.beginCalled = true
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.beginTx, nil
}

func (m *mockPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	m.lastQuerySQL = sql
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return &mockRows{remaining: m.queryRows}, nil
}

func (m *mockPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	m.execSQL = append(m.execSQL, sql)
	return pgconn.CommandTag{}, m.execErr
}

// mockTx implements pgx.Tx. Only Exec, QueryRow, Commit, and Rollback are
// actually exercised by ops.go; the rest panic to catch unexpected calls.
type mockTx struct {
	execSQL        []string
	execErr        error
	commitCalled   bool
	rollbackCalled bool
	queryRowResult uuid.UUID
	queryRowErr    error
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	panic("unexpected Begin on mockTx")
}
func (m *mockTx) Commit(ctx context.Context) error {
	m.commitCalled = true
	return nil
}
func (m *mockTx) Rollback(ctx context.Context) error {
	m.rollbackCalled = true
	return nil
}
func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	m.execSQL = append(m.execSQL, sql)
	return pgconn.CommandTag{}, m.execErr
}
func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &mockRow{id: m.queryRowResult, err: m.queryRowErr}
}
func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &mockRows{}, nil
}
func (m *mockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom")
}
func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	panic("unexpected SendBatch")
}
func (m *mockTx) LargeObjects() pgx.LargeObjects {
	panic("unexpected LargeObjects")
}
func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	panic("unexpected Prepare")
}
func (m *mockTx) Conn() *pgx.Conn { return nil }

type mockRow struct {
	id  uuid.UUID
	err error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if p, ok := dest[0].(*uuid.UUID); ok {
			*p = r.id
		}
	}
	return nil
}

type mockRows struct {
	closed    bool
	remaining int
}

func (r *mockRows) Close()     { r.closed = true }
func (r *mockRows) Err() error { return nil }
func (r *mockRows) Next() bool {
	if r.remaining > 0 {
		r.remaining--
		return true
	}
	return false
}
func (r *mockRows) Scan(dest ...any) error                       { return nil }
func (r *mockRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *mockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *mockRows) Values() ([]any, error)                       { return nil, nil }
func (r *mockRows) RawValues() [][]byte                          { return nil }
func (r *mockRows) Conn() *pgx.Conn                              { return nil }

// ── executor tests ───────────────────────────────────────────────────────────

func testConfig() *config.Config {
	return &config.Config{
		MinPayloadKB: 4, MaxPayloadKB: 8, DeleteBatchSize: 50,
	}
}

func TestExecute_emptyRing_skipsDB(t *testing.T) {
	// For all ops except insert, an empty ring must skip (return errSkipped)
	// without touching the pool at all.
	ring := NewSessionRing(10)
	cfg := testConfig()
	rng := rand.New(rand.NewSource(1))

	// nil pool — any call on it would panic.
	exec := newOLTPExecutor(nil, ring, cfg, rng)
	ctx := context.Background()

	for _, op := range []string{OpReadSimple, OpReadJoin, OpUpdate, OpDelete, OpReadByIP} {
		err := exec.Execute(ctx, op)
		if !errors.Is(err, errSkipped) {
			t.Errorf("op=%s with empty ring: want errSkipped, got %v", op, err)
		}
	}
}

func TestExecute_insert_commitsAndPushesRing(t *testing.T) {
	ring := NewSessionRing(10)
	tx := &mockTx{}
	pool := &mockPool{beginTx: tx}
	cfg := testConfig()
	rng := rand.New(rand.NewSource(1))

	exec := newOLTPExecutor(pool, ring, cfg, rng)
	if err := exec.Execute(context.Background(), OpInsert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !tx.commitCalled {
		t.Error("Commit was not called")
	}
	// Ring must have one entry after a successful insert.
	_, ok := ring.Sample(rng)
	if !ok {
		t.Error("ring is empty after insert — Push was not called")
	}
	// At minimum: INSERT sessions + at least 1 INSERT events + INSERT audit_log.
	if len(tx.execSQL) < 3 {
		t.Errorf("expected ≥3 Exec calls on tx, got %d", len(tx.execSQL))
	}
}

func TestExecute_insert_rollsBackOnError(t *testing.T) {
	ring := NewSessionRing(10)
	tx := &mockTx{execErr: fmt.Errorf("db error")}
	pool := &mockPool{beginTx: tx}
	cfg := testConfig()
	rng := rand.New(rand.NewSource(1))

	exec := newOLTPExecutor(pool, ring, cfg, rng)
	err := exec.Execute(context.Background(), OpInsert)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Ring must NOT have been pushed on failure.
	_, ok := ring.Sample(rng)
	if ok {
		t.Error("ring should be empty after failed insert")
	}
}

func TestExecute_delete_noTransaction(t *testing.T) {
	// doDelete uses pool.Exec directly — not pool.Begin.
	ring := NewSessionRing(10)
	ring.Push(uuid.New())

	pool := &mockPool{}
	cfg := testConfig()
	rng := rand.New(rand.NewSource(1))

	exec := newOLTPExecutor(pool, ring, cfg, rng)
	if err := exec.Execute(context.Background(), OpDelete); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool.beginCalled {
		t.Error("doDelete must not open a transaction")
	}
	if len(pool.execSQL) != 1 {
		t.Errorf("expected exactly 1 pool.Exec call, got %d", len(pool.execSQL))
	}
}

func TestExecute_unknownOp(t *testing.T) {
	ring := NewSessionRing(10)
	exec := newOLTPExecutor(nil, ring, testConfig(), rand.New(rand.NewSource(1)))
	err := exec.Execute(context.Background(), "no_such_op")
	if err == nil {
		t.Fatal("expected error for unknown op, got nil")
	}
}

func TestExecute_readSimple_scansRowsAndSelectsPayload(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	rng := rand.New(rand.NewSource(1))

	for _, readPayload := range []bool{false, true} {
		pool := &mockPool{queryRows: 2} // exercise the row-iteration + Scan branch
		cfg := testConfig()
		cfg.ReadPayload = readPayload
		exec := newOLTPExecutor(pool, ring, cfg, rng)
		if err := exec.Execute(context.Background(), OpReadSimple); err != nil {
			t.Fatalf("ReadPayload=%v: unexpected error %v", readPayload, err)
		}
		hasPayloadCol := strings.Contains(pool.lastQuerySQL, ", payload")
		if hasPayloadCol != readPayload {
			t.Errorf("ReadPayload=%v: query payload-column presence = %v", readPayload, hasPayloadCol)
		}
	}
}

func TestExecute_readByIP_scansRowsAndSelectsPayload(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	rng := rand.New(rand.NewSource(1))

	for _, readPayload := range []bool{false, true} {
		pool := &mockPool{queryRows: 3}
		cfg := testConfig()
		cfg.ReadPayload = readPayload
		exec := newOLTPExecutor(pool, ring, cfg, rng)
		if err := exec.Execute(context.Background(), OpReadByIP); err != nil {
			t.Fatalf("ReadPayload=%v: unexpected error %v", readPayload, err)
		}
		if !strings.Contains(pool.lastQuerySQL, "source_ip >= $1::inet") {
			t.Errorf("read_by_ip query missing inet range: %q", pool.lastQuerySQL)
		}
		if strings.Contains(pool.lastQuerySQL, ", payload") != readPayload {
			t.Errorf("ReadPayload=%v: payload-column mismatch", readPayload)
		}
	}
}

func TestExecute_readJoin_issuesJoinQuery(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	pool := &mockPool{queryRows: 2}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpReadJoin); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The read ops discard scanned data, so assert the actual observable: the
	// 3-table join query was issued (not a tautological err==nil check).
	if !strings.Contains(pool.lastQuerySQL, "JOIN events") || !strings.Contains(pool.lastQuerySQL, "audit_log") {
		t.Errorf("read_join did not issue the 3-table join query: %q", pool.lastQuerySQL)
	}
}

func TestExecute_update_lockAcquired_commits(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	tx := &mockTx{queryRowResult: uuid.New()} // FOR UPDATE returned a row (lock held)
	pool := &mockPool{beginTx: tx}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpUpdate); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tx.commitCalled {
		t.Error("update should commit when the row lock is acquired")
	}
	if len(tx.execSQL) != 2 { // UPDATE sessions + INSERT audit_log
		t.Errorf("expected 2 Exec calls (update + audit), got %d", len(tx.execSQL))
	}
}

func TestExecute_update_skipLocked_returnsNilNoCommit(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	tx := &mockTx{queryRowErr: pgx.ErrNoRows} // row locked by another worker → skip
	pool := &mockPool{beginTx: tx}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpUpdate); err != nil {
		t.Fatalf("skip-locked should return nil, got: %v", err)
	}
	if tx.commitCalled {
		t.Error("update must not commit when the lock was skipped")
	}
}

// ── error-path coverage ──────────────────────────────────────────────────────

func TestExecute_readSimple_queryError(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	pool := &mockPool{queryErr: fmt.Errorf("boom")}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpReadSimple); err == nil {
		t.Fatal("expected error when Query fails")
	}
}

func TestExecute_readByIP_queryError(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	pool := &mockPool{queryErr: fmt.Errorf("boom")}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpReadByIP); err == nil {
		t.Fatal("expected error when Query fails")
	}
}

func TestExecute_readJoin_queryError(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	pool := &mockPool{queryErr: fmt.Errorf("boom")}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpReadJoin); err == nil {
		t.Fatal("expected error when Query fails")
	}
}

func TestExecute_delete_execError(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	pool := &mockPool{execErr: fmt.Errorf("boom")}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpDelete); err == nil {
		t.Fatal("expected error when Exec fails")
	}
}

func TestExecute_update_lockError(t *testing.T) {
	ring := NewSessionRing(4)
	ring.Push(uuid.New())
	// A non-ErrNoRows error from the FOR UPDATE query must propagate.
	tx := &mockTx{queryRowErr: fmt.Errorf("connection lost")}
	pool := &mockPool{beginTx: tx}
	exec := newOLTPExecutor(pool, ring, testConfig(), rand.New(rand.NewSource(1)))
	if err := exec.Execute(context.Background(), OpUpdate); err == nil {
		t.Fatal("expected error when the lock query fails")
	}
	if tx.commitCalled {
		t.Error("must not commit after a lock-query error")
	}
}
