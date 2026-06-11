package otlp

import (
	"context"
	"reflect"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// spanContextKey is the eager helper's field key. It is part of the API
// contract: non-OTLP cores in a tee render the field under this key.
const spanContextKey = "span_context"

// SpanContext eagerly captures the active span context from ctx as a zap
// field. It always returns a field so call sites stay branch-free: the OTLP
// core silently drops an invalid (no active span) context, while tee'd
// non-OTLP cores render the zero-value struct. The payload type
// trace.SpanContext implements json.Marshaler, so tee'd JSON/console cores
// render it legibly (design §3.4).
func SpanContext(ctx context.Context) zap.Field {
	return zap.Field{Key: spanContextKey, Type: zapcore.ReflectType, Interface: spanContextOrZero(ctx)}
}

// InjectTraceFields returns fields plus the eager span-context field.
// It ALWAYS allocates a fresh slice (clone-before-append): with the
// `existingSlice...` call form Go passes the slice unchanged, so a bare
// append could mutate the caller's backing array (plan-review pass-4 P1).
func InjectTraceFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	out := make([]zap.Field, 0, len(fields)+1)
	out = append(out, fields...)

	return append(out, SpanContext(ctx))
}

// InjectTraceKVs prepends the eager span-context field to a sugared
// keysAndValues list. zap's SugaredLogger consumes strongly-typed Fields
// mixed into keysAndValues before pair processing, so the prepended field
// never splits a key/value pair. An odd number of user kvs still triggers
// the SugaredLogger's usual dangling-key handling for the trailing element,
// consistent with zap's sugar contract.
func InjectTraceKVs(ctx context.Context, kvs ...any) []any {
	out := make([]any, 0, len(kvs)+1)
	out = append(out, SpanContext(ctx))

	return append(out, kvs...)
}

// TraceCorrelationFields returns flat lowercase-hex "trace_id"/"span_id"
// string fields for NON-OTLP sinks (ndjson/fluent/syslog → Loki derived
// fields, Datadog log parsing). With the OTLP core these land in attributes,
// NOT the LogRecord trace fields — use SpanContext / the Inject helpers for
// OTLP correlation. Returns nil when ctx carries no valid span.
func TraceCorrelationFields(ctx context.Context) []zap.Field {
	sc := spanContextOrZero(ctx)
	if !sc.IsValid() {
		return nil
	}

	return []zap.Field{
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	}
}

// spanContextOrZero extracts the span context with full nil-safety:
// trace.SpanContextFromContext guards only interface-nil before calling
// ctx.Value, so typed-nil contexts must be rejected here (pass-3 P2).
func spanContextOrZero(ctx context.Context) trace.SpanContext {
	if ctx == nil || isNilValue(ctx) {
		return trace.SpanContext{}
	}

	return trace.SpanContextFromContext(ctx)
}

// spanContextFromField reports whether f carries trace context (the eager
// trace.SpanContext payload or any context.Context value, regardless of how
// zap classified the field) and resolves it. A matched field is consumed by
// callers — never encoded as an attribute — even when the resolved span
// context is invalid (design §3.4).
func spanContextFromField(f zapcore.Field) (trace.SpanContext, bool) {
	return spanContextFromValue(f.Interface)
}

// spanContextFromValue is the value-level form, shared with the encoder's
// AddReflected hook (Task 4/5), which receives bare values rather than Fields.
func spanContextFromValue(val any) (trace.SpanContext, bool) {
	switch v := val.(type) {
	case nil:
		return trace.SpanContext{}, false
	case trace.SpanContext:
		return v, true
	case context.Context:
		return spanContextOrZero(v), true
	default:
		return trace.SpanContext{}, false
	}
}

// isNilValue reports whether v is a typed-nil pointer/map/etc. boxed in a
// non-nil interface.
func isNilValue(v any) bool {
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}
