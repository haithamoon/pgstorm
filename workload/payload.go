// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Haitham Gadelrab
// This program is free software under the GNU AGPL v3.0; see the LICENSE file.

package workload

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
)

const templatePoolSize = 100

// Three template pools: sessions (4–8 KB), events (8–16 KB), and audit_log.diff
// (2–4 KB). Each entry is pre-marshaled JSON; a 16-hex mutable field (trace_id for
// sessions/events, _nonce for audit) is rewritten per use so every returned value
// is byte-unique without any marshaling on the write hot path.
var (
	sessionTemplatePool   [templatePoolSize][]byte
	sessionTraceIDOffsets [templatePoolSize]int
	eventTemplatePool     [templatePoolSize][]byte
	eventTraceIDOffsets   [templatePoolSize]int
	auditTemplatePool     [templatePoolSize][]byte
	auditNonceOffsets     [templatePoolSize]int

	// Small (<2 KB) inline variants. When a write is chosen to be "small" (see
	// toastPct), the value is drawn from these pools instead of the large ones, so it
	// stays inline in the heap rather than TOASTing. smallPayloadPool serves both
	// sessions.metadata and events.payload (both are just a small inline JSON body);
	// smallAuditPool serves audit_log.diff.
	smallPayloadPool    [templatePoolSize][]byte
	smallPayloadOffsets [templatePoolSize]int
	smallAuditPool      [templatePoolSize][]byte
	smallAuditOffsets   [templatePoolSize]int
)

// toastPct is the percentage of writes whose JSONB payload is large enough to store
// out-of-line (TOAST). The rest are small and stay inline. It defaults to 100 —
// legacy always-TOAST — so any code path (e.g. a unit test) that calls a Get*
// accessor without initializing the profile keeps the old behavior; production sets
// it from TOAST_PCT via SetToastPct in OLTPProfile.Init.
var toastPct = 100

// SetToastPct sets the large-payload (TOAST) percentage. Called once from
// OLTPProfile.Init after config is loaded, before workers start. Callers must pass a
// value in [0,100] (config validates this).
func SetToastPct(pct int) { toastPct = pct }

func init() {
	for i := 0; i < templatePoolSize; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		sessionTemplatePool[i], sessionTraceIDOffsets[i] = buildTemplate(rng, 4, 8)

		arng := rand.New(rand.NewSource(int64(i + 2*templatePoolSize)))
		auditTemplatePool[i], auditNonceOffsets[i] = buildAuditTemplate(arng)

		// Small inline variants (config-independent, so built here at init).
		srng := rand.New(rand.NewSource(int64(i + 3*templatePoolSize)))
		smallPayloadPool[i], smallPayloadOffsets[i] = buildSmallPayloadTemplate(srng)

		sarng := rand.New(rand.NewSource(int64(i + 4*templatePoolSize)))
		smallAuditPool[i], smallAuditOffsets[i] = buildSmallAuditTemplate(sarng)
	}
}

// InitEventPool builds the event template pool using the configured payload size range.
// Must be called from main after config is loaded, before any workers start.
func InitEventPool(minKB, maxKB int) {
	for i := 0; i < templatePoolSize; i++ {
		rng := rand.New(rand.NewSource(int64(i + templatePoolSize)))
		eventTemplatePool[i], eventTraceIDOffsets[i] = buildTemplate(rng, minKB, maxKB)
	}
}

// isLarge rolls whether this write should produce a large (TOASTing) payload.
// toastPct=100 → always true (legacy), toastPct=0 → always false.
func isLarge(rng *rand.Rand) bool { return rng.Intn(100) < toastPct }

// GetSessionPayload returns a mutated session-metadata value. With probability
// toastPct it's large (fixed 4–8 KB pool, TOASTs); otherwise a small inline value.
// Use for sessions.metadata.
func GetSessionPayload(rng *rand.Rand) []byte {
	if isLarge(rng) {
		return mutatedPayload(rng, &sessionTemplatePool, &sessionTraceIDOffsets)
	}
	return mutatedPayload(rng, &smallPayloadPool, &smallPayloadOffsets)
}

// GetEventPayload returns a mutated events.payload value. With probability toastPct
// it's large (the configured MIN/MAX_PAYLOAD_KB event pool, TOASTs); otherwise a
// small inline value. Large-pool selection is by field, not by size, so the
// configured range is always honored — even when MIN_PAYLOAD_KB <= 4.
func GetEventPayload(rng *rand.Rand) []byte {
	if isLarge(rng) {
		return mutatedPayload(rng, &eventTemplatePool, &eventTraceIDOffsets)
	}
	return mutatedPayload(rng, &smallPayloadPool, &smallPayloadOffsets)
}

// GetAuditDiff returns a mutated audit_log.diff value. With probability toastPct it's
// large (2–4 KB, incompressible base64 pad → stays above the ~2 KB TOAST threshold
// and stores out-of-line); otherwise a small inline diff. The 16-hex mutable field
// is rewritten per call so each written value is byte-unique — no json.Marshal or
// entropy generation on the hot path (that happens once per template at init).
func GetAuditDiff(rng *rand.Rand) []byte {
	if isLarge(rng) {
		return mutatedPayload(rng, &auditTemplatePool, &auditNonceOffsets)
	}
	return mutatedPayload(rng, &smallAuditPool, &smallAuditOffsets)
}

// mutatedPayload copies a random template from the given pool and rewrites its
// 16-hex mutable field (trace_id for payloads, _nonce for audit diffs) at the
// recorded offset so each returned value is byte-unique.
func mutatedPayload(rng *rand.Rand, pool *[templatePoolSize][]byte, offsets *[templatePoolSize]int) []byte {
	idx := rng.Intn(templatePoolSize)
	buf := make([]byte, len(pool[idx]))
	copy(buf, pool[idx])

	offset := offsets[idx]
	if offset >= 0 && offset+16 <= len(buf) {
		const hexChars = "0123456789abcdef"
		for i := offset; i < offset+16; i++ {
			buf[i] = hexChars[rng.Intn(16)]
		}
	}
	return buf
}

func buildTemplate(rng *rand.Rand, minKB, maxKB int) ([]byte, int) {
	targetKB := minKB + rng.Intn(maxKB-minKB+1)

	// Fixed fields (headers, stack_trace, tags, metrics, context) contribute ~4 KB.
	// Remaining budget goes to request/response bodies (base64 of random bytes —
	// high entropy, compresses poorly, looks like a real encoded API payload).
	bodyBudgetKB := targetKB - 4
	if bodyBudgetKB < 1 {
		bodyBudgetKB = 1
	}
	reqBodyKB := bodyBudgetKB * 3 / 5
	if reqBodyKB < 1 {
		reqBodyKB = 1
	}
	respBodyKB := bodyBudgetKB - reqBodyKB
	if respBodyKB < 1 {
		respBodyKB = 1
	}

	payload := map[string]interface{}{
		"request":     buildRequest(rng, reqBodyKB),
		"response":    buildResponse(rng, respBodyKB),
		"stack_trace": buildStackTrace(rng),
		"tags":        buildTags(rng),
		"metrics":     buildMetrics(rng),
		"context":     buildContext(rng),
		"trace_id":    fmt.Sprintf("%016x%016x", rng.Int63(), rng.Int63()),
	}

	data, _ := json.Marshal(payload)
	offset := findTraceIDOffset(data)
	return data, offset
}

func findTraceIDOffset(data []byte) int {
	marker := []byte(`"trace_id":"`)
	idx := bytes.Index(data, marker)
	if idx < 0 {
		return -1
	}
	return idx + len(marker)
}

// buildAuditTemplate builds one audit_log.diff template (2–4 KB) and returns it
// with the byte offset of its 16-hex _nonce field. All marshaling and entropy
// generation happen here — once per template at init — never on the write hot path;
// GetAuditDiff then copies a template and mutates the nonce.
func buildAuditTemplate(rng *rand.Rand) ([]byte, int) {
	diff := map[string]interface{}{
		// _nonce is a fixed-width 16-hex field mutated per use so each written diff
		// is byte-unique (analogous to trace_id in the payload templates). It sorts
		// first among the keys, so its offset is stable across all templates.
		"_nonce": fmt.Sprintf("%016x", rng.Int63()),
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
	return data, findAuditNonceOffset(data)
}

func findAuditNonceOffset(data []byte) int {
	marker := []byte(`"_nonce":"`)
	idx := bytes.Index(data, marker)
	if idx < 0 {
		return -1
	}
	return idx + len(marker)
}

// buildSmallPayloadTemplate builds one small (~0.3–1.4 KB) inline payload template
// for the not-TOAST case, returning it with the offset of its mutable trace_id.
// Deliberately compact — comfortably under the ~2 KB TOAST threshold — and no
// incompressible pad (small values stay inline regardless, so no need to defeat
// compression). Still byte-unique per write via the mutated trace_id.
func buildSmallPayloadTemplate(rng *rand.Rand) ([]byte, int) {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths := []string{"/api/v2/users", "/api/v2/sessions", "/api/v2/events"}
	// 128–896 random bytes → base64 ~170–1200 chars; with the small fixed fields the
	// document lands well under 2 KB.
	bodyBytes := 128 + rng.Intn(769)
	payload := map[string]interface{}{
		"method":   methods[rng.Intn(len(methods))],
		"path":     paths[rng.Intn(len(paths))],
		"status":   []int{200, 201, 400, 404, 500}[rng.Intn(5)],
		"tags":     []string{randomString(rng, 5, 20), randomString(rng, 5, 20), randomString(rng, 5, 20)},
		"body":     randomBase64(rng, bodyBytes),
		"trace_id": fmt.Sprintf("%016x%016x", rng.Int63(), rng.Int63()),
	}
	data, _ := json.Marshal(payload)
	return data, findTraceIDOffset(data)
}

// buildSmallAuditTemplate builds one small (~0.4–1.1 KB) inline audit_log.diff
// template for the not-TOAST case, returning it with the offset of its mutable
// _nonce. Like buildSmallPayloadTemplate: compact, no incompressible pad, byte-unique.
func buildSmallAuditTemplate(rng *rand.Rand) ([]byte, int) {
	diff := map[string]interface{}{
		"_nonce":         fmt.Sprintf("%016x", rng.Int63()),
		"before":         map[string]interface{}{"status": "active", "metadata": randomString(rng, 50, 100)},
		"after":          map[string]interface{}{"status": "closed", "metadata": randomString(rng, 50, 100)},
		"changed_fields": []string{"status", "metadata"},
		"context":        randomBase64(rng, 128+rng.Intn(513)),
	}
	data, _ := json.Marshal(diff)
	return data, findAuditNonceOffset(data)
}

func buildRequest(rng *rand.Rand, bodyKB int) map[string]interface{} {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths := []string{"/api/v2/users", "/api/v2/sessions", "/api/v2/events", "/api/v2/reports", "/api/v2/metrics"}
	headers := map[string]string{}
	for i := 0; i < 25; i++ {
		headers[fmt.Sprintf("X-Header-%d", i)] = randomString(rng, 20, 60)
	}
	return map[string]interface{}{
		"method":  methods[rng.Intn(len(methods))],
		"path":    paths[rng.Intn(len(paths))],
		"headers": headers,
		"body":    randomBase64(rng, bodyKB*1024*3/4),
	}
}

func buildResponse(rng *rand.Rand, bodyKB int) map[string]interface{} {
	statuses := []int{200, 201, 400, 404, 500}
	headers := map[string]string{}
	for i := 0; i < 15; i++ {
		headers[fmt.Sprintf("X-Resp-Header-%d", i)] = randomString(rng, 10, 40)
	}
	return map[string]interface{}{
		"status":  statuses[rng.Intn(len(statuses))],
		"headers": headers,
		"body":    randomBase64(rng, bodyKB*1024*3/4),
	}
}

// randomBase64 encodes n random bytes as a base64 string.
// Random bytes have near-maximum entropy so they compress poorly — intentional,
// to prevent Postgres from deflating the Toast value and reducing I/O stress.
func randomBase64(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// randomBase64Exact returns exactly n high-entropy characters (base64 of random
// bytes, sliced to length). Postgres's only TOAST compressors, pglz and lz4, are
// LZ77 variants with no entropy coding, so they cannot shrink this content — it is
// used to pad JSONB to a precise byte size that survives compression and forces
// out-of-line TOAST storage. Returns "" for n <= 0.
func randomBase64Exact(rng *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	// base64 turns 3 bytes into 4 chars; request enough bytes to cover n chars,
	// then slice to exactly n (still valid ASCII, still needs no JSON escaping).
	s := randomBase64(rng, n*3/4+3)
	return s[:n]
}

func buildStackTrace(rng *rand.Rand) []string {
	frames := make([]string, 20+rng.Intn(21))
	for i := range frames {
		frames[i] = fmt.Sprintf("at %s.%s (%s:%d)", randomString(rng, 5, 15), randomString(rng, 5, 15), randomString(rng, 5, 15)+".go", rng.Intn(500)+1)
	}
	return frames
}

func buildTags(rng *rand.Rand) []string {
	tags := make([]string, 50+rng.Intn(51))
	for i := range tags {
		tags[i] = randomString(rng, 5, 20)
	}
	return tags
}

func buildMetrics(rng *rand.Rand) map[string]float64 {
	m := make(map[string]float64, 40)
	for i := 0; i < 40; i++ {
		m[fmt.Sprintf("metric_%d", i)] = rng.Float64() * 1000
	}
	return m
}

func buildContext(rng *rand.Rand) map[string]interface{} {
	return map[string]interface{}{
		"level1": map[string]interface{}{
			"value": randomString(rng, 10, 30),
			"level2": map[string]interface{}{
				"value": randomString(rng, 10, 30),
				"level3": map[string]interface{}{
					"value": randomString(rng, 10, 30),
					"data":  randomString(rng, 50, 100),
				},
			},
		},
	}
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(rng *rand.Rand, minLen, maxLen int) string {
	n := minLen + rng.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}
