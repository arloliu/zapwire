package otlp

import (
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func benchFields() []zapcore.Field {
	return []zapcore.Field{
		zap.String("method", "GET"), zap.Int("status", 200),
		zap.Duration("latency", 1500*time.Microsecond),
		zap.String("path", "/api/v1/things"), zap.Bool("cache", true),
	}
}

func BenchmarkEncodeEntry(b *testing.B) {
	enc := NewEncoder()
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "request handled"}
	fields := benchFields()
	b.ReportAllocs()
	for b.Loop() {
		buf, err := enc.EncodeEntry(ent, fields)
		if err != nil {
			b.Fatal(err)
		}
		buf.Free()
	}
}

func BenchmarkEncodeEntryWithTrace(b *testing.B) {
	enc := NewEncoder()
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "request handled"}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	})
	fields := append(benchFields(), zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: sc})
	b.ReportAllocs()
	for b.Loop() {
		buf, err := enc.EncodeEntry(ent, fields)
		if err != nil {
			b.Fatal(err)
		}
		buf.Free()
	}
}

func BenchmarkEndToEndLogger(b *testing.B) {
	// Encoder + custom core into a discard sink (no HTTP): the hot path zap sees.
	enc := newEncoder(applyOptions(nil))
	core := newOTLPCore(enc, zapcore.AddSync(io.Discard), zapcore.InfoLevel)
	logger := zap.New(core)
	fields := benchFields()
	b.ReportAllocs()
	for b.Loop() {
		logger.Info("request handled", fields...)
	}
}
