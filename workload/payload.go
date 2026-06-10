package workload

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
)

const templatePoolSize = 100

// Two template pools: one for sessions (4–8 KB) and one for events (8–16 KB).
var (
	sessionTemplatePool    [templatePoolSize][]byte
	sessionTraceIDOffsets  [templatePoolSize]int
	eventTemplatePool      [templatePoolSize][]byte
	eventTraceIDOffsets    [templatePoolSize]int
)

func init() {
	for i := 0; i < templatePoolSize; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		sessionTemplatePool[i], sessionTraceIDOffsets[i] = buildTemplate(rng, 4, 8)
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

// GetMutatedPayload returns a copy of a template with a mutated trace_id.
// minKB/maxKB select which pool to draw from: 4–8 → session pool, 8–16 → event pool.
func GetMutatedPayload(rng *rand.Rand, minKB, maxKB int) []byte {
	var pool *[templatePoolSize][]byte
	var offsets *[templatePoolSize]int
	if minKB <= 4 {
		pool = &sessionTemplatePool
		offsets = &sessionTraceIDOffsets
	} else {
		pool = &eventTemplatePool
		offsets = &eventTraceIDOffsets
	}

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
