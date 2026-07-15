// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

// After building small pools, many picks must yield at most pool-size distinct
// values (bounded cardinality) and every value must be a pool member — the
// property that fixes the 1:1 user↔row explosion.
func TestInitActorPools_boundedCardinality(t *testing.T) {
	InitActorPools(5, 3)
	rng := rand.New(rand.NewSource(1))

	userMembers := map[uuid.UUID]bool{}
	for _, u := range userPool {
		userMembers[u] = true
	}
	actorMembers := map[uuid.UUID]bool{}
	for _, a := range actorPool {
		actorMembers[a] = true
	}

	seenUsers := map[uuid.UUID]bool{}
	for i := 0; i < 1000; i++ {
		u := pickUser(rng)
		if !userMembers[u] {
			t.Fatalf("pickUser returned a UUID not in the pool: %s", u)
		}
		seenUsers[u] = true
	}
	if len(seenUsers) < 2 || len(seenUsers) > 5 {
		t.Errorf("user cardinality: want (1, 5], got %d distinct over 1000 draws", len(seenUsers))
	}

	seenActors := map[uuid.UUID]bool{}
	for i := 0; i < 1000; i++ {
		a := pickActor(rng)
		if !actorMembers[a] {
			t.Fatalf("pickActor returned a UUID not in the pool: %s", a)
		}
		seenActors[a] = true
	}
	if len(seenActors) < 2 || len(seenActors) > 3 {
		t.Errorf("actor cardinality: want (1, 3], got %d distinct over 1000 draws", len(seenActors))
	}
}

// Pool lengths must match the requested sizes.
func TestInitActorPools_sizes(t *testing.T) {
	InitActorPools(7, 4)
	if len(userPool) != 7 {
		t.Errorf("userPool len: want 7, got %d", len(userPool))
	}
	if len(actorPool) != 4 {
		t.Errorf("actorPool len: want 4, got %d", len(actorPool))
	}
}
