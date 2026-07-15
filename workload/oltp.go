package workload

import (
	"math/rand"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
	"pg-loadgen/db"
)

// ProfileOLTP is the default profile: a mixed INSERT/READ/JOIN/UPDATE/DELETE
// workload over a sessions/events/audit_log schema with large JSONB payloads,
// stressing heap I/O, TOAST storage, and MVCC dead-tuple accumulation.
const ProfileOLTP = "oltp-jsonb"

func init() {
	RegisterProfile(ProfileOLTP, func() Profile { return &OLTPProfile{} })
}

// OLTPProfile holds the shared cross-worker state for the oltp-jsonb workload:
// the connection pool, the session-UUID ring, and the loaded config. The event
// payload template pool is process-global (see payload.go) and initialised in Init.
type OLTPProfile struct {
	pool *pgxpool.Pool
	ring *SessionRing
	cfg  *config.Config
}

func (p *OLTPProfile) Name() string { return ProfileOLTP }

func (p *OLTPProfile) Ops() []OpDef {
	return []OpDef{
		{Name: OpInsert, EnvVar: "WRITE_PCT", DefaultWeight: 35},
		{Name: OpReadSimple, EnvVar: "READ_SIMPLE_PCT", DefaultWeight: 15},
		{Name: OpReadJoin, EnvVar: "READ_JOIN_PCT", DefaultWeight: 20},
		{Name: OpUpdate, EnvVar: "UPDATE_PCT", DefaultWeight: 15},
		{Name: OpDelete, EnvVar: "DELETE_PCT", DefaultWeight: 10},
		{Name: OpReadByIP, EnvVar: "READ_IP_PCT", DefaultWeight: 5},
	}
}

func (p *OLTPProfile) Schema() db.Schema { return oltpSchema }

func (p *OLTPProfile) Init(cfg *config.Config, pool *pgxpool.Pool) error {
	p.cfg = cfg
	p.pool = pool
	p.ring = NewSessionRing(cfg.RingSize)
	InitEventPool(cfg.MinPayloadKB, cfg.MaxPayloadKB)
	InitActorPools(cfg.UserPoolSize, cfg.ActorPoolSize)
	SetToastPct(cfg.ToastPct)
	return nil
}

func (p *OLTPProfile) NewExecutor(rng *rand.Rand) Executor {
	return newOLTPExecutor(p.pool, p.ring, p.cfg, rng)
}

var oltpSchema = db.Schema{
	Tables: []string{
		`CREATE TABLE IF NOT EXISTS sessions (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id     UUID NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			ended_at    TIMESTAMPTZ,
			region      TEXT NOT NULL,
			metadata    JSONB NOT NULL,
			status      TEXT NOT NULL DEFAULT 'active',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			session_id    UUID NOT NULL REFERENCES sessions(id),
			event_type    TEXT NOT NULL,
			occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			payload       JSONB NOT NULL,
			severity      TEXT NOT NULL DEFAULT 'info',
			trace_id      TEXT NOT NULL,
			source_ip     INET,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS audit_log (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			entity_type   TEXT NOT NULL,
			entity_id     UUID NOT NULL,
			action        TEXT NOT NULL,
			changed_by    UUID NOT NULL,
			changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			diff          JSONB NOT NULL,
			checksum      TEXT NOT NULL
		)`,
	},
	Indexes: []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_user_id        ON sessions (user_id)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_status_created ON sessions (status, created_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_session_id          ON events (session_id)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_occurred_at         ON events (occurred_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_severity_occurred   ON events (severity, occurred_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_source_ip           ON events (source_ip)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_entity_id  ON audit_log (entity_id, changed_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_changed_at ON audit_log (changed_at DESC)`,
	},
	TrackedTables: []string{"sessions", "events", "audit_log"},
	SentinelIndex: "idx_audit_log_changed_at",
}
