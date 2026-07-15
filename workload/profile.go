// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	"pg-loadgen/config"
	"pg-loadgen/db"
)

// Profile is a self-contained workload: its schema, its operation set, and a
// factory for per-worker executors. A profile is constructed once (it holds
// shared cross-worker state such as pools and rings), then produces one Executor
// per worker goroutine. New workload dimensions (e.g. pgvector, a queue broker)
// are added by implementing this interface and registering the profile.
type Profile interface {
	// Name is the profile's identifier, matched against the PROFILE env var.
	Name() string
	// Schema returns the DDL the profile needs and the tables to track for stats.
	Schema() db.Schema
	// Ops declares the operations with their env-var-driven default weights. The
	// runner resolves the weights (ResolveWeights) and validates they sum to 100.
	Ops() []OpDef
	// Init reads the profile's own configuration and builds shared state. Called
	// once, after the schema exists and before any worker starts.
	Init(cfg *config.Config, pool *pgxpool.Pool) error
	// NewExecutor returns a per-worker executor. Called once per worker goroutine.
	NewExecutor(rng *rand.Rand) Executor
}

// Executor runs a single named operation. One is created per worker goroutine, so
// implementations may hold per-worker state (e.g. an *rand.Rand) without locking.
type Executor interface {
	Execute(ctx context.Context, op string) error
}

// OpDef declares one operation: its name, the env var that sets its weight in the
// mix, and the default weight when that env var is unset.
type OpDef struct {
	Name          string
	EnvVar        string
	DefaultWeight int
}

// profileFactories holds the registered profiles, keyed by name. Profiles
// register themselves from an init() function.
var profileFactories = map[string]func() Profile{}

// RegisterProfile makes a profile available under the given name.
func RegisterProfile(name string, factory func() Profile) {
	profileFactories[name] = factory
}

// GetProfile constructs the profile registered under name, or an error listing
// the valid names.
func GetProfile(name string) (Profile, error) {
	factory, ok := profileFactories[name]
	if !ok {
		return nil, fmt.Errorf("unknown profile %q (valid: %v)", name, ProfileNames())
	}
	return factory(), nil
}

// ProfileNames returns the registered profile names, sorted.
func ProfileNames() []string {
	names := make([]string, 0, len(profileFactories))
	for n := range profileFactories {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
