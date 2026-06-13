// Package otlp ships zap logs as native OTLP/HTTP binary protobuf
// (ExportLogsServiceRequest, POST /v1/logs) to any OTLP receiver — the OTel
// Collector, Grafana Loki ≥ 3.0, Elastic, Datadog Agent — with logs↔traces
// correlation populated from context.Context as LogRecord proto fields
// (trace_id, span_id, flags), not string attributes.
//
// # Quick start
//
//	core, w, err := otlp.NewHTTPCore("http://collector:4318", zapcore.InfoLevel,
//	    otlp.WithServiceName("checkout"))
//	if err != nil { ... }
//	defer w.Close()
//	logger := zap.New(core)
//
//	logger.Info("payment ok", otlp.SpanContext(ctx))            // eager
//	logger.Info("payment ok", zap.Any("context", ctx))          // bridge-compatible
//	reqLog := logger.With(zap.Any("context", ctx))               // sticky
//	sugar.Infow("payment ok", otlp.InjectTraceKVs(ctx, "k", 1)...)
//
// # Trace-context compatibility matrix
//
// The encoder consumes any field whose value is a context.Context (the
// official contrib/bridges/otelzap convention) or the eager SpanContext
// payload. Sticky With behavior depends on the core:
//
//	form                                  | otlp core | stock zapcore.NewCore
//	per-call zap.Any("context", ctx)      | yes       | yes
//	per-call otlp.SpanContext(ctx)        | yes       | yes
//	With(otlp.SpanContext(ctx))           | yes       | yes
//	With(zap.Any("context", ctx))         | yes       | NO — stringified attribute
//
// "otlp core" is either NewHTTPCore or NewGRPCCore — both wrap the same
// trace-aware core. The stock-core limitation is structural: zap classifies
// contexts as fmt.Stringer, so ioCore.With erases the value before any encoder
// hook. Use otlp.NewHTTPCore/NewGRPCCore (recommended) or the eager helper.
//
// A second stock-core caveat: transactional rollback for FAILING zap.Inline
// marshalers applied via With. ioCore.With dispatches InlineMarshalerType
// straight into the encoder with no interception point, so a failing inline
// marshaler's partial writes persist in the With-state — zap's own JSON and
// console encoders behave identically there. The otlp cores roll such
// failures back cleanly; per-call fields are transactional on every core.
//
// In zapcore.NewTee setups, the OTHER cores receive trace-context fields as
// ordinary fields; the eager helper renders legibly (span_context JSON) and
// is the recommended form there. TraceCorrelationFields produces flat
// trace_id/span_id hex strings for non-OTLP sinks; on THIS core they are
// plain attributes, not correlation.
//
// # Transports
//
// Two OTLP transports are provided. NewHTTPWriter (and NewHTTPCore) speak
// OTLP/HTTP — binary protobuf POSTed to /v1/logs, default port 4318, the OTel
// spec's default protocol. NewGRPCWriter (and NewGRPCCore) speak OTLP/gRPC — a
// unary LogsService/Export call, default
// port 4317, implemented with a hand-rolled stdlib HTTP/2 client (no grpc-go
// dependency). ProtocolFromEnv reads OTEL_EXPORTER_OTLP_[LOGS_]PROTOCOL for
// env-driven dispatch between them.
//
// # Delivery semantics
//
// Async-only, at-most-once: bounded queue, count/byte/interval batching,
// OTLP retry (429/502/503/504 with backoff and Retry-After), partial-success
// accounting, counted drops, never blocks the application goroutine, no WAL.
// Sync flushes; Close drains with single attempts. See DroppedLogs.
//
// # Boundary
//
// zapwire provides the foundation (encoder, core, injector helpers); the
// application layer decides whether to build wrapper methods such as
// InfoCtx(ctx, msg, ...) — they are trivial over InjectTraceFields and
// InjectTraceKVs.
package otlp
