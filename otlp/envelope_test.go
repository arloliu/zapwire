package otlp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEnvelopeGolden(t *testing.T) {
	o := applyOptions([]Option{WithServiceName("s"), WithScopeName("n"), WithScopeVersion("")})
	env := newEnvelope(o)

	// Resource = repeated KeyValue{service.name:"s"} tagged 0x0a:
	// kv body: 0x0a 0x0c "service.name" 0x12 0x03 0x0a 0x01 's' → len 19
	wantRes := append([]byte{0x0a, 0x13, 0x0a, 0x0c}, []byte("service.name")...)
	wantRes = append(wantRes, 0x12, 0x03, 0x0a, 0x01, 's')
	require.Equal(t, wantRes, env.resourceBlob)
	// Scope = name only: 0x0a 0x01 'n'
	require.Equal(t, []byte{0x0a, 0x01, 'n'}, env.scopeBlob)

	rec := []byte{0x10, 0x09} // minimal LogRecord{severity_number:9}
	got := env.assemble(nil, [][]byte{rec})

	// Inside-out: scopeLogs = scope(0x0a len blob) + records(0x12 len rec)
	scopeLogs := append([]byte{0x0a, 0x03, 0x0a, 0x01, 'n'}, 0x12, 0x02, 0x10, 0x09)
	// resourceLogs = resource(0x0a len res) + scope_logs(0x12 len scopeLogs)
	resourceLogs := append([]byte{0x0a, byte(len(wantRes))}, wantRes...)
	resourceLogs = append(resourceLogs, 0x12, byte(len(scopeLogs)))
	resourceLogs = append(resourceLogs, scopeLogs...)
	// request = resource_logs(0x0a len resourceLogs)
	want := append([]byte{0x0a, byte(len(resourceLogs))}, resourceLogs...)
	require.Equal(t, want, got)

	// Exact size arithmetic.
	require.Equal(t, len(got), env.sizeFor(env.recordCost(len(rec))))
}

func TestEnvelopeSizeForMultiByteVarints(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{WithServiceName("svc")}))
	// Cross the 127/128 and 16383/16384 varint length boundaries.
	for _, recLen := range []int{1, 100, 127, 128, 1000, 16383, 16384, 100000} {
		rec := bytes.Repeat([]byte{0x00}, recLen) // content irrelevant for sizing
		records := [][]byte{rec, rec[:recLen/2+1]}
		tagged := 0
		for _, r := range records {
			tagged += env.recordCost(len(r))
		}
		got := env.assemble(nil, records)
		require.Equal(t, len(got), env.sizeFor(tagged), "recLen=%d", recLen)
	}
}

func TestEnvelopeResourceFields(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{
		WithServiceName("s"),
		WithResource(zap.String("env", "prod"), zap.Int("shard", 3)),
	}))
	// service.name first, then WithResource fields in order: assert keys via
	// the kvlist structure (each entry 0x0a len KeyValue).
	keys := []string{}
	b := env.resourceBlob
	for len(b) > 0 {
		l := int(b[1]) // single-byte lens in this test
		kv := b[2 : 2+l]
		k, err := findField(kv, 1)
		require.NoError(t, err)
		keys = append(keys, string(k))
		b = b[2+l:]
	}
	require.Equal(t, []string{"service.name", "env", "shard"}, keys)
}

func TestEnvelopeLargeResourceSizeAgreement(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{
		WithServiceName(strings.Repeat("s", 200)),
		WithResource(zap.String("k", strings.Repeat("v", 150))),
		WithScopeName(strings.Repeat("n", 130)),
		WithScopeVersion("v1"),
	}))

	require.Greater(t, len(env.resourceBlob), 127)
	require.Greater(t, len(env.scopeBlob), 127)

	records := [][]byte{{0x10, 0x09}, bytes.Repeat([]byte{0x00}, 200)}
	tagged := 0
	for _, r := range records {
		tagged += env.recordCost(len(r))
	}
	require.Equal(t, len(env.assemble(nil, records)), env.sizeFor(tagged))
}

func TestEnvelopeVersionOnlyScope(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{
		WithServiceName("s"),
		WithScopeName(""),
		WithScopeVersion("v1"),
	}))
	want := append([]byte{0x12, 0x02}, []byte("v1")...)
	require.Equal(t, want, env.scopeBlob)
}

func TestEnvelopeEmptyScopeOmitted(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{WithServiceName("s"), WithScopeName(""), WithScopeVersion("")}))
	require.Empty(t, env.scopeBlob)
	got := env.assemble(nil, [][]byte{{0x10, 0x09}})
	// ScopeLogs must contain ONLY log_records (no 0x0a scope part).
	resourceLogs, err := findField(got, 1)
	require.NoError(t, err)
	scopeLogs, err := findField(resourceLogs, 2)
	require.NoError(t, err)
	require.Equal(t, byte(0x12), scopeLogs[0])
}
