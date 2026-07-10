package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
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

func TestBuildAuditDiff_validJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	data := buildAuditDiff(rng)
	if !json.Valid(data) {
		t.Fatal("buildAuditDiff returned invalid JSON")
	}
	var m map[string]interface{}
	json.Unmarshal(data, &m) //nolint
	for _, key := range []string{"before", "after", "changed_fields", "context"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q in audit diff", key)
		}
	}
}

func TestBuildAuditDiff_sizeRange(t *testing.T) {
	// The padding logic subtracts 12 bytes for key overhead, so the result can
	// land fractionally below 2 KB when the pre-pad JSON is already large.
	// Lower bound is 1.9 KB to account for this.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		data := buildAuditDiff(rng)
		kb := float64(len(data)) / 1024
		if kb < 1.9 || kb > 4.5 {
			t.Errorf("iteration %d: size %.2f KB outside [1.9, 4.5]", i, kb)
		}
	}
}

// ── mock DB types ────────────────────────────────────────────────────────────

type mockPool struct {
	beginTx     *mockTx
	beginErr    error
	beginCalled bool
	execSQL     []string
	execErr     error
}

func (m *mockPool) Begin(ctx context.Context) (pgx.Tx, error) {
	m.beginCalled = true
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.beginTx, nil
}

func (m *mockPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &mockRows{}, nil
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

type mockRows struct{ closed bool }

func (r *mockRows) Close()                                       { r.closed = true }
func (r *mockRows) Err() error                                   { return nil }
func (r *mockRows) Next() bool                                   { return false }
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
	// For all ops except insert, an empty ring must return nil without
	// touching the pool at all.
	ring := NewSessionRing(10)
	cfg := testConfig()
	rng := rand.New(rand.NewSource(1))

	// nil pool — any call on it would panic.
	exec := newOLTPExecutor(nil, ring, cfg, rng)
	ctx := context.Background()

	for _, op := range []string{OpReadSimple, OpReadJoin, OpUpdate, OpDelete, OpReadByIP} {
		err := exec.Execute(ctx, op)
		if err != nil {
			t.Errorf("op=%s with empty ring: unexpected error %v", op, err)
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
