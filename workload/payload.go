package workload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
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
		eventTemplatePool[i], eventTraceIDOffsets[i] = buildTemplate(rng, 8, 16)
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
	payload := map[string]interface{}{
		"request":     buildRequest(rng),
		"response":    buildResponse(rng),
		"stack_trace": buildStackTrace(rng),
		"tags":        buildTags(rng),
		"metrics":     buildMetrics(rng),
		"context":     buildContext(rng),
		"trace_id":    fmt.Sprintf("%016x%016x", rng.Int63(), rng.Int63()),
	}

	data, _ := json.Marshal(payload)

	targetSize := (minKB + rng.Intn(maxKB-minKB+1)) * 1024
	if padLen := targetSize - len(data) - 20; padLen > 0 {
		payload["_pad"] = strings.Repeat("x", padLen)
		data, _ = json.Marshal(payload)
	}

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

func buildRequest(rng *rand.Rand) map[string]interface{} {
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
		"body":    randomString(rng, 1000, 2000),
	}
}

func buildResponse(rng *rand.Rand) map[string]interface{} {
	statuses := []int{200, 201, 400, 404, 500}
	headers := map[string]string{}
	for i := 0; i < 15; i++ {
		headers[fmt.Sprintf("X-Resp-Header-%d", i)] = randomString(rng, 10, 40)
	}
	return map[string]interface{}{
		"status":  statuses[rng.Intn(len(statuses))],
		"headers": headers,
		"body":    randomString(rng, 1000, 2000),
	}
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
