package workload

import (
	"math/rand"

	"github.com/google/uuid"
)

// userPool and actorPool are bounded sets of UUIDs for the actor/owner columns
// (sessions.user_id, audit_log.changed_by). Drawing each write's owner from a
// fixed population — rather than a fresh uuid.New() per row — gives realistic
// index cardinality and cache behaviour: a bounded set of users/actors owns many
// rows each (1:N), instead of every row having a unique owner (1:1), which would
// inflate index size and flatten cache hotness. Built once by InitActorPools
// before any worker starts. The pick is uniform; Zipfian skew is a follow-up.
var (
	userPool  []uuid.UUID
	actorPool []uuid.UUID
)

// InitActorPools fills the user and actor UUID pools. Must be called after config
// is loaded and before any workers start (see OLTPProfile.Init). Sizes are
// validated >= 1 by config, so the pools are always non-empty.
func InitActorPools(userPoolSize, actorPoolSize int) {
	userPool = make([]uuid.UUID, userPoolSize)
	for i := range userPool {
		userPool[i] = uuid.New()
	}
	actorPool = make([]uuid.UUID, actorPoolSize)
	for i := range actorPool {
		actorPool[i] = uuid.New()
	}
}

// pickUser returns a uniformly-random user UUID from the bounded pool
// (sessions.user_id).
func pickUser(rng *rand.Rand) uuid.UUID {
	return userPool[rng.Intn(len(userPool))]
}

// pickActor returns a uniformly-random actor UUID from the bounded pool
// (audit_log.changed_by).
func pickActor(rng *rand.Rand) uuid.UUID {
	return actorPool[rng.Intn(len(actorPool))]
}
