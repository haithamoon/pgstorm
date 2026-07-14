package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"pg-loadgen/config"
)

// errSkipped is returned by an op that intentionally did no database work — e.g.
// the session/message ring was empty at cold start, so there was nothing to read,
// update, delete, ack, or requeue. The worker treats it specially: it is NOT
// counted as an executed op or observed in the latency histogram (which would
// record a bogus ~0ms "success" and deflate percentiles / inflate throughput);
// instead it is surfaced via metrics.RecordSkip.
var errSkipped = errors.New("op skipped: empty ring")

// DBPool is the subset of pgxpool.Pool used by oltpExecutor.
// *pgxpool.Pool satisfies this interface.
type DBPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

const (
	OpInsert     = "insert"
	OpReadSimple = "read_simple"
	OpReadJoin   = "read_join"
	OpUpdate     = "update"
	OpDelete     = "delete"
	OpReadByIP   = "read_by_ip"
)

// Read queries are precomputed constants (two variants each) rather than rebuilt
// per call: e.cfg.ReadPayload is fixed for the process lifetime, so the hot path
// just selects the right constant with no allocation. The *WithPayload variants
// also fetch events.payload, so the read detoasts and transfers the out-of-line
// JSONB — exercising TOAST reads instead of only the scalar columns.
const (
	readSimpleSQL = `SELECT id, event_type, occurred_at, severity, trace_id
		 FROM events
		 WHERE session_id = $1
		 ORDER BY occurred_at DESC
		 LIMIT 20`
	readSimpleWithPayloadSQL = `SELECT id, event_type, occurred_at, severity, trace_id, payload
		 FROM events
		 WHERE session_id = $1
		 ORDER BY occurred_at DESC
		 LIMIT 20`
	readByIPSQL = `SELECT id, session_id, event_type, occurred_at, severity, trace_id
		 FROM events
		 WHERE source_ip >= $1::inet AND source_ip <= $2::inet
		 ORDER BY occurred_at DESC
		 LIMIT 50`
	readByIPWithPayloadSQL = `SELECT id, session_id, event_type, occurred_at, severity, trace_id, payload
		 FROM events
		 WHERE source_ip >= $1::inet AND source_ip <= $2::inet
		 ORDER BY occurred_at DESC
		 LIMIT 50`
)

var regions = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"}

func randomIP(rng *rand.Rand) string {
	return fmt.Sprintf("192.168.%d.%d", rng.Intn(256), rng.Intn(256))
}

var severities = []string{"info", "warn", "error", "debug"}
var eventTypes = []string{"request", "response", "error", "auth", "payment", "audit", "system"}

// oltpExecutor implements the Executor interface for the oltp-jsonb profile.
// One is created per worker, so its *rand.Rand needs no locking.
type oltpExecutor struct {
	pool DBPool
	ring *SessionRing
	cfg  *config.Config
	rng  *rand.Rand
}

func newOLTPExecutor(pool DBPool, ring *SessionRing, cfg *config.Config, rng *rand.Rand) *oltpExecutor {
	return &oltpExecutor{pool: pool, ring: ring, cfg: cfg, rng: rng}
}

func (e *oltpExecutor) Execute(ctx context.Context, op string) error {
	switch op {
	case OpInsert:
		return e.doInsert(ctx)
	case OpReadSimple:
		return e.doReadSimple(ctx)
	case OpReadJoin:
		return e.doReadJoin(ctx)
	case OpUpdate:
		return e.doUpdate(ctx)
	case OpDelete:
		return e.doDelete(ctx)
	case OpReadByIP:
		return e.doReadByIP(ctx)
	default:
		return fmt.Errorf("unknown op: %s", op)
	}
}

func (e *oltpExecutor) doInsert(ctx context.Context) error {
	sessionID := uuid.New()
	userID := uuid.New()
	region := regions[e.rng.Intn(len(regions))]
	metadata := GetMutatedPayload(e.rng, 4, 8)
	numEvents := 1 + e.rng.Intn(3)

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint

	_, err = tx.Exec(ctx,
		`INSERT INTO sessions (id, user_id, started_at, region, metadata, status)
		 VALUES ($1, $2, now(), $3, $4, 'active')`,
		sessionID, userID, region, metadata,
	)
	if err != nil {
		return fmt.Errorf("insert session: %w", err)
	}

	for i := 0; i < numEvents; i++ {
		eventID := uuid.New()
		payload := GetMutatedPayload(e.rng, e.cfg.MinPayloadKB, e.cfg.MaxPayloadKB)
		traceID := fmt.Sprintf("%016x", e.rng.Int63())
		severity := severities[e.rng.Intn(len(severities))]
		evType := eventTypes[e.rng.Intn(len(eventTypes))]
		sourceIP := randomIP(e.rng)

		_, err = tx.Exec(ctx,
			`INSERT INTO events (id, session_id, event_type, occurred_at, payload, severity, trace_id, source_ip)
			 VALUES ($1, $2, $3, now(), $4, $5, $6, $7)`,
			eventID, sessionID, evType, payload, severity, traceID, sourceIP,
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}

	auditID := uuid.New()
	changedBy := uuid.New()
	diff := buildAuditDiff(e.rng)
	checksum := fmt.Sprintf("%016x", e.rng.Int63())

	_, err = tx.Exec(ctx,
		`INSERT INTO audit_log (id, entity_type, entity_id, action, changed_by, diff, checksum)
		 VALUES ($1, 'session', $2, 'INSERT', $3, $4, $5)`,
		auditID, sessionID, changedBy, diff, checksum,
	)
	if err != nil {
		return fmt.Errorf("insert audit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit insert: %w", err)
	}

	e.ring.Push(sessionID)
	return nil
}

func (e *oltpExecutor) doReadSimple(ctx context.Context) error {
	sessionID, ok := e.ring.Sample(e.rng)
	if !ok {
		return errSkipped
	}

	query := readSimpleSQL
	if e.cfg.ReadPayload {
		query = readSimpleWithPayloadSQL
	}
	rows, err := e.pool.Query(ctx, query, sessionID)
	if err != nil {
		return fmt.Errorf("read simple: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		var eventType, severity, traceID string
		var occurredAt interface{}
		if e.cfg.ReadPayload {
			var payload []byte
			_ = rows.Scan(&id, &eventType, &occurredAt, &severity, &traceID, &payload)
		} else {
			_ = rows.Scan(&id, &eventType, &occurredAt, &severity, &traceID)
		}
	}
	return rows.Err()
}

func (e *oltpExecutor) doReadJoin(ctx context.Context) error {
	sessionID, ok := e.ring.Sample(e.rng)
	if !ok {
		return errSkipped
	}

	severity := severities[e.rng.Intn(len(severities))]

	rows, err := e.pool.Query(ctx,
		`SELECT
			s.id, s.user_id, s.region, s.status,
			e.id as event_id, e.event_type, e.severity, e.payload,
			al.action, al.changed_at
		 FROM sessions s
		 JOIN events e ON e.session_id = s.id
		 LEFT JOIN audit_log al ON al.entity_id = s.id
		 WHERE s.id = $1
		   AND e.severity = $2
		 ORDER BY e.occurred_at DESC
		 LIMIT 20`,
		sessionID, severity,
	)
	if err != nil {
		return fmt.Errorf("read join: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sID, userID uuid.UUID
		var region, status, eventID, eventType, sev string
		var payload []byte
		var action *string
		var changedAt interface{}
		_ = rows.Scan(&sID, &userID, &region, &status, &eventID, &eventType, &sev, &payload, &action, &changedAt)
	}
	return rows.Err()
}

func (e *oltpExecutor) doUpdate(ctx context.Context) error {
	sessionID, ok := e.ring.Sample(e.rng)
	if !ok {
		return errSkipped
	}

	metadata := GetMutatedPayload(e.rng, 4, 8)

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint

	var lockedID uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT id FROM sessions WHERE id = $1 FOR UPDATE SKIP LOCKED`,
		sessionID,
	).Scan(&lockedID)
	if err == pgx.ErrNoRows {
		// Session is locked by another worker; skip silently.
		// The deferred Rollback handles cleanup.
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock session: %w", err)
	}

	_, err = tx.Exec(ctx,
		`UPDATE sessions
		 SET status = 'closed', ended_at = now(), metadata = $2
		 WHERE id = $1`,
		sessionID, metadata,
	)
	if err != nil {
		return fmt.Errorf("update session: %w", err)
	}

	auditID := uuid.New()
	changedBy := uuid.New()
	diff := buildAuditDiff(e.rng)
	checksum := fmt.Sprintf("%016x", e.rng.Int63())

	_, err = tx.Exec(ctx,
		`INSERT INTO audit_log (id, entity_type, entity_id, action, changed_by, diff, checksum)
		 VALUES ($1, 'session', $2, 'UPDATE', $3, $4, $5)`,
		auditID, sessionID, changedBy, diff, checksum,
	)
	if err != nil {
		return fmt.Errorf("update audit: %w", err)
	}

	return tx.Commit(ctx)
}

func (e *oltpExecutor) doDelete(ctx context.Context) error {
	sessionID, ok := e.ring.Sample(e.rng)
	if !ok {
		return errSkipped
	}

	_, err := e.pool.Exec(ctx,
		`DELETE FROM events
		 WHERE id IN (
			 SELECT id FROM events
			 WHERE session_id = $1
			 ORDER BY occurred_at ASC
			 LIMIT $2
		 )`,
		sessionID, e.cfg.DeleteBatchSize,
	)
	if err != nil {
		return fmt.Errorf("delete events: %w", err)
	}
	return nil
}

func (e *oltpExecutor) doReadByIP(ctx context.Context) error {
	sessionID, ok := e.ring.Sample(e.rng)
	if !ok {
		return errSkipped
	}

	// Derive a stable /24 from the session's first UUID byte (0–255 → third octet).
	// Same session always maps to the same subnet, so results are consistent and
	// the query is a plain range scan that a B-tree index on source_ip can satisfy.
	octet := int(sessionID[0])
	lo := fmt.Sprintf("192.168.%d.0", octet)
	hi := fmt.Sprintf("192.168.%d.255", octet)

	// Explicit ::inet casts make these comparisons independent of driver parameter
	// typing. They aren't strictly required today — pgx sends the string params as
	// unspecified-type, so PostgreSQL infers inet from context and the query works
	// across every pgx exec mode (verified on PG16) — but the casts document intent
	// and guard against a driver/mode change silently turning $1 into text (which
	// has no `inet >= text` operator). This stays a B-tree range scan that
	// idx_events_source_ip can satisfy.
	query := readByIPSQL
	if e.cfg.ReadPayload {
		query = readByIPWithPayloadSQL
	}
	rows, err := e.pool.Query(ctx, query, lo, hi)
	if err != nil {
		return fmt.Errorf("read by ip: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, sid uuid.UUID
		var eventType, severity, traceID string
		var occurredAt interface{}
		if e.cfg.ReadPayload {
			var payload []byte
			_ = rows.Scan(&id, &sid, &eventType, &occurredAt, &severity, &traceID, &payload)
		} else {
			_ = rows.Scan(&id, &sid, &eventType, &occurredAt, &severity, &traceID)
		}
	}
	return rows.Err()
}

func buildAuditDiff(rng *rand.Rand) []byte {
	diff := map[string]interface{}{
		"before": map[string]interface{}{
			"status":   "active",
			"metadata": randomString(rng, 50, 100),
		},
		"after": map[string]interface{}{
			"status":   "closed",
			"metadata": randomString(rng, 50, 100),
		},
		"changed_fields": []string{"status", "metadata", "ended_at"},
		"context":        randomString(rng, 100, 200),
	}
	data, _ := json.Marshal(diff)

	// Pad to 2–4 KB with base64 of random bytes. Postgres's only TOAST compressors,
	// pglz and lz4, are LZ77 variants with no entropy coding, so they cannot shrink
	// high-entropy base64 — the diff survives compression above the ~2 KB TOAST
	// threshold and is genuinely stored out-of-line, matching sessions.metadata and
	// events.payload. (A repeated-byte pad would compress away and leave the row
	// inline, defeating the Toast stress this column exists to exercise.)
	// randomBase64Exact returns exactly padLen chars (its alphabet needs no JSON
	// escaping), so the document lands at targetSize even for tiny padLen — avoiding
	// the integer-truncation-to-empty-pad that padLen*3/4 would hit for padLen 1–3.
	targetSize := (2 + rng.Intn(3)) * 1024
	if padLen := targetSize - len(data) - 12; padLen > 0 {
		diff["_pad"] = randomBase64Exact(rng, padLen)
		data, _ = json.Marshal(diff)
	}
	return data
}
