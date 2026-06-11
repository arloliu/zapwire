// Command otlp-trace-correlation shows how to ship zap logs as OTLP/HTTP
// binary protobuf with trace context from context.Context.
//
// The example is safe to run without a collector: when nothing is listening on
// the endpoint every record is counted as a drop, and the final lines report
// that count. No panic, no hang.
//
// Run it from the examples directory with:
//
//	go run ./otlp-trace-correlation
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire/otlp"
)

// infoCtx is a thin application-layer wrapper over InjectTraceFields.  It
// shows the InfoCtx-style boundary: the application decides whether to build
// helpers like this; zapwire deliberately leaves that to the caller.
func infoCtx(logger *zap.Logger, ctx context.Context, msg string, fields ...zap.Field) {
	logger.Info(msg, otlp.InjectTraceFields(ctx, fields...)...)
}

func main() {
	// Endpoint: prefer the standard env vars, fall back to localhost.
	// Any OTLP receiver works here — OTel Collector, Vector's opentelemetry
	// source, Loki ≥3.0.
	endpoint := otlp.EndpointFromEnv()
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4318"
	}

	core, w, err := otlp.NewCore(
		endpoint,
		zapcore.InfoLevel,
		otlp.WithServiceName("checkout"),
		otlp.WithFlushInterval(200*time.Millisecond),
		otlp.WithErrorHandler(func(err error) {
			// Terminal ship-path events land here (exhausted retries, partial
			// success rejections).  Print to stderr and keep going.
			fmt.Fprintf(os.Stderr, "otlp export: %v\n", err)
		}),
	)
	if err != nil {
		log.Fatalf("otlp.NewCore: %v", err)
	}

	// Build a sample trace context the way the package tests do: literal IDs +
	// FlagsSampled, wrapped in a context.Context.  In a real service ctx comes
	// from your HTTP/gRPC middleware — you would not construct it by hand.
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:     trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger := zap.New(core)
	sugar := logger.Sugar()

	// Form 1: per-call eager helper — always works regardless of core type.
	logger.Info("order placed", otlp.SpanContext(ctx), zap.String("order_id", "ord-1"))

	// Form 2: sticky context — works on otlp.NewCore because the custom core
	// pre-scans With fields before zap's field dispatch can stringify ctx.
	reqLog := logger.With(zap.Any("context", ctx))
	reqLog.Info("payment authorised", zap.Float64("amount", 19.99))
	reqLog.Info("inventory reserved", zap.Int("sku", 42))

	// Form 3: sugared Infow with InjectTraceKVs — prepends the span-context
	// field to the keysAndValues list.
	sugar.Infow("email queued", otlp.InjectTraceKVs(ctx, "recipient", "buyer@example.com")...)

	// Application-layer wrapper: infoCtx (defined above) uses InjectTraceFields.
	infoCtx(logger, ctx, "shipment dispatched", zap.String("carrier", "fedex"))

	// Graceful shutdown: Sync flushes everything enqueued before the call;
	// Close drains with a single attempt then tears down the exporter.
	// With no receiver running, records are counted as drops — the example is
	// safe to run without a collector.
	if err := w.Sync(); err != nil {
		log.Printf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Printf("close: %v", err)
	}

	fmt.Printf("\ndropped logs (expected > 0 when no collector is running): %d\n", w.DroppedLogs())
}
