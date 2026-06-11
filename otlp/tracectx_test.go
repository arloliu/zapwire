package otlp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// testSpanContext returns a valid, sampled span context and a ctx carrying it.
func testSpanContext(t *testing.T) (trace.SpanContext, context.Context) {
	t.Helper()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:     trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
		TraceFlags: trace.FlagsSampled,
	})
	require.True(t, sc.IsValid())

	return sc, trace.ContextWithSpanContext(context.Background(), sc)
}

// typedNilCtx implements context.Context on a pointer receiver, so a nil
// *typedNilCtx stored in an interface is a typed-nil context (pass-3 P2).
type typedNilCtx struct{}

func (*typedNilCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*typedNilCtx) Done() <-chan struct{}       { return nil }
func (*typedNilCtx) Err() error                  { return nil }
func (*typedNilCtx) Value(any) any               { return nil }

func TestSpanContextField(t *testing.T) {
	sc, ctx := testSpanContext(t)
	f := SpanContext(ctx)
	require.Equal(t, "span_context", f.Key)
	require.Equal(t, zapcore.ReflectType, f.Type)
	require.Equal(t, sc, f.Interface)

	// No span / nil ctx → field still returned, zero (invalid) span context.
	f = SpanContext(context.Background())
	require.False(t, f.Interface.(trace.SpanContext).IsValid())
	f = SpanContext(nil) //nolint:staticcheck // deliberate nil-safety check
	require.False(t, f.Interface.(trace.SpanContext).IsValid())
}

func TestSpanContextFromField(t *testing.T) {
	sc, ctx := testSpanContext(t)

	// Eager helper payload.
	got, ok := spanContextFromField(SpanContext(ctx))
	require.True(t, ok)
	require.Equal(t, sc, got)

	// Raw ctx via zap.Any — zap may classify ctx as StringerType, but
	// Field.Interface preserves the concrete value regardless of Type (design §3.4).
	f := zap.Any("context", ctx)
	got, ok = spanContextFromField(f)
	require.True(t, ok)
	require.Equal(t, sc, got)

	// Ordinary fields never match.
	_, ok = spanContextFromField(zap.String("trace_id", "deadbeef"))
	require.False(t, ok)
	_, ok = spanContextFromField(zap.Int("n", 1))
	require.False(t, ok)

	// Typed-nil ctx: matched (consumed) but resolves to zero span context —
	// must NOT panic (otel guards only interface-nil).
	var tn *typedNilCtx
	got, ok = spanContextFromField(zap.Field{Key: "context", Type: zapcore.ReflectType, Interface: tn})
	require.True(t, ok)
	require.False(t, got.IsValid())
}

func TestInjectTraceFieldsCloneSemantics(t *testing.T) {
	_, ctx := testSpanContext(t)

	out := InjectTraceFields(ctx, zap.String("k", "v"))
	require.Len(t, out, 2)
	require.Equal(t, "k", out[0].Key)
	require.Equal(t, "span_context", out[1].Key)

	// Pass-4 P1 pin: a caller slice with spare capacity must NOT be mutated.
	base := make([]zap.Field, 1, 2)
	base[0] = zap.String("k", "v")
	probe := base[:2]
	probe[1] = zap.String("sentinel", "untouched")
	out = InjectTraceFields(ctx, base...)
	require.Len(t, out, 2)
	require.Equal(t, "sentinel", probe[1].Key, "caller backing array was mutated")

	// Zero fields: still returns the span context field.
	out = InjectTraceFields(ctx)
	require.Len(t, out, 1)
}

func TestInjectTraceKVs(t *testing.T) {
	_, ctx := testSpanContext(t)

	out := InjectTraceKVs(ctx, "url", "https://x", "attempt", 3)
	require.Len(t, out, 5)
	f, isField := out[0].(zap.Field)
	require.True(t, isField)
	require.Equal(t, "span_context", f.Key)
	require.Equal(t, "url", out[1])

	// Zero kvs: lone typed field, no dangling key (sugared contract pinned in
	// the encoder integration test, Task 6).
	out = InjectTraceKVs(ctx)
	require.Len(t, out, 1)
}

func TestTraceCorrelationFields(t *testing.T) {
	_, ctx := testSpanContext(t)

	fields := TraceCorrelationFields(ctx)
	require.Len(t, fields, 2)
	require.Equal(t, "trace_id", fields[0].Key)
	require.Equal(t, "5b8efff798038103d269b633813fc60c", fields[0].String)
	require.Equal(t, "span_id", fields[1].Key)
	require.Equal(t, "eee19b7ec3c1b174", fields[1].String)

	// No span → nil (no empty-string pollution).
	require.Nil(t, TraceCorrelationFields(context.Background()))
	require.Nil(t, TraceCorrelationFields(nil)) //nolint:staticcheck

	// Non-sampled valid span: IsValid does not require FlagsSampled.
	sc2 := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:  trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
	})
	ctx2 := trace.ContextWithSpanContext(context.Background(), sc2)
	fields2 := TraceCorrelationFields(ctx2)
	require.Len(t, fields2, 2, "non-sampled valid span must still return 2 fields")
	require.True(t, SpanContext(ctx2).Interface.(trace.SpanContext).IsValid())

	// Remote span: ContextWithRemoteSpanContext must also be recognised.
	ctx3 := trace.ContextWithRemoteSpanContext(context.Background(), sc2.WithRemote(true))
	fields3 := TraceCorrelationFields(ctx3)
	require.Len(t, fields3, 2, "remote span must still return 2 fields")
}
