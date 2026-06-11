package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// memSink captures every record written through the core.
type memSink struct {
	mu   sync.Mutex
	recs [][]byte
}

func (m *memSink) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, append([]byte(nil), p...))

	return len(p), nil
}
func (m *memSink) Sync() error { return nil }

func (m *memSink) last(t *testing.T) []byte {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.recs)

	return m.recs[len(m.recs)-1]
}

func newTestCore(t *testing.T, opts ...Option) (zapcore.Core, *memSink) {
	t.Helper()
	sink := &memSink{}
	enc := newEncoder(applyOptions(opts))

	return newOTLPCore(enc, sink, zapcore.DebugLevel), sink
}

func traceIDOf(t *testing.T, rec []byte) []byte {
	t.Helper()
	tid, err := findField(rec, 9)
	require.NoError(t, err)

	return tid
}

func TestStickyCtxThroughOTLPCore(t *testing.T) {
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core)

	// THE headline behavior: sticky zap.Any("context", ctx) works because the
	// custom core's With pre-scans raw fields (design §2.2).
	logger.With(zap.Any("context", ctx)).Info("sticky")
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
	require.Empty(t, attrKeys(t, sink.last(t))) // consumed, not an attribute

	// Eager helper, sticky.
	logger.With(SpanContext(ctx)).Info("eager-sticky")
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))

	// Per-call on the plain logger.
	logger.Info("per-call", zap.Any("context", ctx))
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
}

func TestStickyCtxDegradesOnStockCore(t *testing.T) {
	sc, ctx := testSpanContext(t)
	sink := &memSink{}
	stock := zapcore.NewCore(NewEncoder(), sink, zapcore.DebugLevel)
	logger := zap.New(stock)

	// Documented §2.2 degradation: ioCore.With stringifies the ctx
	// (StringerType) — junk string attribute, NO trace fields.
	logger.With(zap.Any("context", ctx)).Info("degraded")
	rec := sink.last(t)
	require.Nil(t, traceIDOf(t, rec))
	require.Equal(t, []string{"context"}, attrKeys(t, rec))

	// But the eager helper IS sticky on a stock core (ReflectType →
	// AddReflected hook), and per-call ctx works everywhere.
	logger.With(SpanContext(ctx)).Info("eager")
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
	logger.Info("per-call", zap.Any("context", ctx))
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
}

func TestWithRemainingFieldsStillApply(t *testing.T) {
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core).With(zap.Any("context", ctx), zap.String("req", "42"))
	logger.Info("m", zap.String("k", "v"))
	require.Equal(t, []string{"req", "k"}, attrKeys(t, sink.last(t)))
}

func TestInjectTraceFieldsThroughLogger(t *testing.T) {
	// Design §9: the structural injector through the plain Logger path.
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core)

	logger.Info("m", InjectTraceFields(ctx, zap.String("k", "v"))...)
	rec := sink.last(t)
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, rec))
	require.Equal(t, []string{"k"}, attrKeys(t, rec)) // helper field consumed

	// No-span variant stays branch-free: field appended, encoder omits.
	logger.Info("m", InjectTraceFields(context.Background())...)
	require.Nil(t, traceIDOf(t, sink.last(t)))
}

func TestSugaredInjectKVs(t *testing.T) {
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	sugar := zap.New(core).Sugar()

	sugar.Infow("m", InjectTraceKVs(ctx, "url", "https://x")...)
	rec := sink.last(t)
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, rec))
	require.Equal(t, []string{"url"}, attrKeys(t, rec))

	// Lone injected field, zero kvs: no dangling-key noise.
	sugar.Infow("m", InjectTraceKVs(ctx)...)
	require.Empty(t, attrKeys(t, sink.last(t)))
}

func TestTeeRendersEagerHelperLegibly(t *testing.T) {
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)

	var jsonOut bytes.Buffer
	jsonCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&jsonOut), zapcore.DebugLevel)

	logger := zap.New(zapcore.NewTee(core, jsonCore))
	logger.Info("teed", SpanContext(ctx))

	// OTLP side: structural, consumed.
	require.NotNil(t, traceIDOf(t, sink.last(t)))
	require.Empty(t, attrKeys(t, sink.last(t)))
	// JSON side: rendered under the documented key (trace.SpanContext
	// implements json.Marshaler).
	var m map[string]any
	require.NoError(t, json.Unmarshal(jsonOut.Bytes(), &m))
	require.Contains(t, m, "span_context")
}

func TestStockCoreWithInlineFailurePinsZapBehavior(t *testing.T) {
	// Documented caveat (doc.go / design §3.3 scope note): behind a STOCK
	// zapcore.NewCore, a failing zap.Inline applied via With follows zap's
	// standard partial-write behavior (ioCore.With dispatches inline
	// marshalers directly into the encoder — no interception point; zap's
	// own encoders behave identically). This test PINS that documented
	// degradation; on otlp.NewCore the same shape rolls back cleanly.
	stockSink := &memSink{}
	stock := zapcore.NewCore(NewEncoder(), stockSink, zapcore.DebugLevel)
	zap.New(stock).With(zap.Inline(failObj{partial: true})).Info("m")
	stockKeys := attrKeys(t, stockSink.last(t))
	require.Contains(t, stockKeys, "written", "zap-standard partial write persists on stock core")

	core, sink := newTestCore(t)
	zap.New(core).With(zap.Inline(failObj{partial: true})).Info("m")
	require.Equal(t, []string{"Error"}, attrKeys(t, sink.last(t)),
		"otlp core rolls back and keeps only the Error attribute")
}

func TestCorrelationFieldsAreAttributesOnOTLPCore(t *testing.T) {
	// Pins the documented §3.4 caveat.
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	zap.New(core).Info("flat", TraceCorrelationFields(ctx)...)
	rec := sink.last(t)
	require.Nil(t, traceIDOf(t, rec)) // NOT proto-field correlation
	require.Equal(t, []string{"trace_id", "span_id"}, attrKeys(t, rec))
}

func TestChainedWithStashOverwrite(t *testing.T) {
	// Chained With calls: the second With's span context wins (later stash
	// overwrites earlier), and persistent fields accumulate across both clones.
	sc1, ctx1 := testSpanContext(t)
	sc2 := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})
	require.True(t, sc2.IsValid())
	ctx2 := trace.ContextWithSpanContext(context.Background(), sc2)

	core, sink := newTestCore(t)
	logger := zap.New(core).
		With(zap.Any("context", ctx1), zap.String("a", "1")).
		With(zap.Any("context", ctx2), zap.String("b", "2"))
	logger.Info("m")

	rec := sink.last(t)
	// The second With's span context wins.
	want2 := sc2.TraceID()
	require.Equal(t, want2[:], traceIDOf(t, rec), "later With span context must win")
	// sc1 was not the winner but sc2 was — ensure we did not accidentally pin sc1.
	want1 := sc1.TraceID()
	require.NotEqual(t, want1[:], traceIDOf(t, rec))
	// Persistent fields accumulate across both clones.
	require.Equal(t, []string{"a", "b"}, attrKeys(t, rec))
}

func TestParentLoggerUnaffectedByChildWith(t *testing.T) {
	// Child With must not leak its stash or fields back to the parent.
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)

	parent := zap.New(core).With(zap.String("p", "1"))
	child := parent.With(SpanContext(ctx), zap.String("c", "2"))

	// Child: trace present, attrs ["p","c"].
	child.Info("child-msg")
	childRec := sink.last(t)
	require.NotNil(t, traceIDOf(t, childRec), "child must carry trace")
	require.Equal(t, []string{"p", "c"}, attrKeys(t, childRec))

	// Parent: no trace, attrs ["p"] only.
	parent.Info("parent-msg")
	parentRec := sink.last(t)
	require.Nil(t, traceIDOf(t, parentRec), "parent must not inherit child trace")
	require.Equal(t, []string{"p"}, attrKeys(t, parentRec))
}
