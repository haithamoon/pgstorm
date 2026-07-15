// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// WeightedOp is an operation with its resolved weight (a percentage of the mix).
type WeightedOp struct {
	Name   string
	Weight int
}

// ResolveWeights reads each OpDef's env var (falling back to its default weight
// when unset), rejects negatives, and requires the weights to sum to exactly 100.
// This is the generic replacement for the old hardcoded six-percentage check —
// each profile declares its own ops and env-var names.
func ResolveWeights(ops []OpDef) ([]WeightedOp, error) {
	total := 0
	out := make([]WeightedOp, 0, len(ops))
	for _, od := range ops {
		w := od.DefaultWeight
		if v := os.Getenv(od.EnvVar); v != "" {
			// A malformed value falls back to the default, matching config.getEnvInt
			// so the op mix parses exactly as it did before profiles (fail-loud on
			// bad config would be a separate, codebase-wide decision). We do, however,
			// warn so a typo'd weight isn't silently ignored.
			if n, err := strconv.Atoi(v); err == nil {
				w = n
			} else {
				log.Printf("warning: %s=%q is not a valid integer; using default %d", od.EnvVar, v, od.DefaultWeight)
			}
		}
		if w < 0 {
			return nil, fmt.Errorf("%s must be >= 0, got %d", od.EnvVar, w)
		}
		total += w
		out = append(out, WeightedOp{Name: od.Name, Weight: w})
	}
	if total != 100 {
		return nil, fmt.Errorf("operation weights must sum to 100, got %d", total)
	}
	return out, nil
}

// OpNames returns just the op names, in order — used to seed the stats collector.
func OpNames(ops []WeightedOp) []string {
	names := make([]string, len(ops))
	for i, o := range ops {
		names[i] = o.Name
	}
	return names
}

// SelectOp picks an op by cumulative weight; roll must be in [0,100). Because the
// weights sum to 100, the final op absorbs any boundary roll via the fallthrough.
func SelectOp(roll int, ops []WeightedOp) string {
	cumulative := 0
	for _, o := range ops {
		cumulative += o.Weight
		if roll < cumulative {
			return o.Name
		}
	}
	if len(ops) > 0 {
		return ops[len(ops)-1].Name
	}
	return ""
}
