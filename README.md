# zapwire

[![CI](https://github.com/arloliu/zapwire/actions/workflows/ci.yml/badge.svg)](https://github.com/arloliu/zapwire/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/arloliu/zapwire.svg)](https://pkg.go.dev/github.com/arloliu/zapwire)
[![Go Reference](https://pkg.go.dev/badge/github.com/arloliu/zapwire/otlp.svg)](https://pkg.go.dev/github.com/arloliu/zapwire/otlp)
[![Go Report Card](https://goreportcard.com/badge/github.com/arloliu/zapwire)](https://goreportcard.com/report/github.com/arloliu/zapwire)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

High-performance [zap](https://github.com/uber-go/zap) `WriteSyncer` that ships structured
logs to log processors over UDS or TCP, with never-block drop-on-stall semantics.

```bash
go get github.com/arloliu/zapwire             # fluent, ndjson, syslog
go get github.com/arloliu/zapwire/otlp        # OTLP/HTTP + gRPC (separate module)
```

> **New here?** Read the **[User Guide](docs/guide.md)** for a full walkthrough — architecture,
> formats, time codecs, sync vs async tuning, graceful shutdown, and troubleshooting.
> Prefer code? The **[`examples/`](examples)** folder has runnable, self-contained programs.
> This README is the quick reference.

## How it fits together

zapwire sits behind a normal `zap.Logger`. A log entry flows through five pieces:

```
zap.Logger
  └─ zapcore.Core ── zap encoder ─▶ bytes
       └─ zapwire.Encoder ─▶ per-entry wire payload
            └─ zapwire.Framer ─▶ one wire frame
                 └─ zapwire.Writer ─▶ Transport (UDS / TCP, auto-reconnecting) ─▶ processor
```

- **Transport** — `zapwire.UDS(path)` or `zapwire.TCP(addr)`. Reconnects in the background.
- **Encoder / Framer** — per-processor wire format. Subpackages wire the right ones.
- **Writer** — the connection manager. Bounded, never-blocking writes (drop-on-stall).
- **Core** — `NewCore` glues encoder + Writer into a `zapcore.Core` ready for `zap.New`.

## Formats & processors

| Format | Reaches | Subpackage | Module |
|---|---|---|---|
| Fluent Forward (msgpack) | Fluentd, Fluent Bit, Vector | `fluent` | root |
| NDJSON | Vector, Logstash, OTel Collector, any line-oriented sink | `ndjson` | root |
| Syslog (RFC5424) | rsyslog, syslog-ng, Vector, Logstash | `syslog` | root |
| OTLP/gRPC + HTTP (protobuf / JSON) | OTel Collector, Loki ≥3.0, Elastic, Datadog, Fluent Bit | `otlp` | `zapwire/otlp` |

## Quick starts

### Fluent Forward over UDS

```go
core, writer, err := fluent.NewCore(
    zapwire.UDS("/var/run/fluent.sock"),
    zap.InfoLevel,
    zap.NewProductionEncoderConfig(),
    fluent.WithTag("app.logs"),
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

logger := zap.New(core)
logger.Info("started", zap.String("version", "1.0.0"))
```

`fluent` has three paths: **transcode** (above — JSON → msgpack), **native** (`NewNativeCore` —
direct msgpack, faster, exact numeric types, recommended for new code), and **bring-your-own
core** (`NewWriter`). See [Fluent: three encoding paths](docs/guide.md#fluent-three-encoding-paths).

### NDJSON over TCP (async)

```go
core, writer, _ := ndjson.NewCore(zapwire.TCP("collector:9000"), zap.InfoLevel,
    zap.NewProductionEncoderConfig(), zapwire.WithAsyncMode())
defer writer.Close()

logger := zap.New(core)
```

### Syslog RFC5424 over TCP

```go
core, writer, err := syslog.NewCore(
    zapwire.TCP("rsyslog:514"),
    zap.InfoLevel,
    zap.NewProductionEncoderConfig(),
    syslog.WithAppName("myapp"),
    syslog.WithFacility(syslog.LOCAL0),
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

logger := zap.New(core)
```

Each log entry becomes a full RFC5424 message — `PRI VERSION TIMESTAMP HOSTNAME APP-NAME
PROCID MSGID MSG` — where MSG is the zap JSON object. See
[Syslog (RFC5424)](docs/guide.md#syslog-rfc5424) for framing, severity mapping, BOM, and
header-field options.

### OTLP/HTTP (protobuf)

```go
core, w, err := otlp.NewHTTPCore("http://collector:4318", zapcore.InfoLevel,
    otlp.WithServiceName("checkout"))
if err != nil {
    log.Fatal(err)
}
defer w.Close()

logger := zap.New(core)
logger.Info("payment ok", otlp.SpanContext(ctx)) // trace correlation
```

### OTLP/gRPC

```go
core, w, err := otlp.NewGRPCCore("collector:4317",
    otlp.WithInsecure(),               // h2c plaintext; omit for TLS (default)
    otlp.WithServiceName("checkout"))
if err != nil {
    log.Fatal(err)
}
defer w.Close()
```

Accepts bare `host:port` (TLS by default; `WithInsecure` for h2c), `http://` (h2c), or
`https://` (TLS; `WithTLSConfig` for custom CA/mTLS).

### OTLP/JSON

```go
core, w, _ := otlp.NewHTTPCore("http://collector:4318", zapcore.InfoLevel,
    otlp.WithEncoding(otlp.JSON))     // spec JSON Protobuf Encoding (HTTP only)
defer w.Close()
```

See [Choosing a protocol](docs/guide.md#choosing-a-protocol) for when to use JSON and the
`WithMaxRequestBytes` caveat.

### Env-driven protocol dispatch

```go
switch otlp.ProtocolFromEnv() {
case otlp.ProtocolGRPC:
    w, err = otlp.NewGRPCWriter(otlp.EndpointFromEnv())
case otlp.ProtocolHTTPJSON:
    w, err = otlp.NewHTTPWriter(otlp.EndpointFromEnv(), otlp.WithEncoding(otlp.JSON))
default: // "", http/protobuf
    w, err = otlp.NewHTTPWriter(otlp.EndpointFromEnv())
}
```

Reads `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL` (and corresponding
endpoint vars). Explicit and opt-in — zapwire never reads env variables behind the caller's back.

### Trace correlation

```go
// Per-call eager helper — works on any core:
logger.Info("order placed", otlp.SpanContext(ctx))

// Sticky — all calls on reqLog carry the span (otlp cores only):
reqLog := logger.With(zap.Any("context", ctx))
reqLog.Info("payment authorised")

// Sugar:
sugar.Infow("email queued", otlp.InjectTraceKVs(ctx, "recipient", "buyer@x")...)
```

Trace context is promoted to `trace_id`/`span_id`/`flags` LogRecord proto fields —
first-class correlation, not string attributes. See [Trace correlation](docs/guide.md#trace-correlation).

### Cost-control tee (OTel Warn+ only)

```go
otelCore, w, _ := otlp.NewHTTPCore(endpoint, zapcore.WarnLevel,
    otlp.WithServiceName("checkout"))
defer w.Close()

logger = logger.WithOptions(zap.WrapCore(func(orig zapcore.Core) zapcore.Core {
    return zapcore.NewTee(orig, otelCore)
}))
```

`Info`/`Debug` entries flow to the original core only; `Warn+` go to both. A
`zap.AtomicLevel` turns the gate into a runtime knob. See
[Cost control](docs/guide.md#cost-control-send-only-warn-to-otel).

## Examples

Runnable, self-contained programs in [`examples/`](examples). Run with `go run ./<name>`:

| Example | Shows |
|---|---|
| [`ndjson-tcp`](examples/ndjson-tcp) | NDJSON over TCP, sync mode, end-to-end delivery |
| [`fluent-native-uds`](examples/fluent-native-uds) | Fluent native msgpack over UDS, frame decoding |
| [`async-observability`](examples/async-observability) | Async tuning + `DroppedLogs`/`ReconnectCount`/`IsConnected` |
| [`tee-console`](examples/tee-console) | `zapcore.NewTee` fan-out with per-core levels |
| [`otlp-trace-correlation`](examples/otlp-trace-correlation) | OTLP/HTTP + trace correlation, three correlation forms |
| [`otlp-tee-cost-control`](examples/otlp-tee-cost-control) | Cost-control tee: console at Info + OTLP gated to Warn+ |
| [`otlp-grpc`](examples/otlp-grpc) | OTLP/gRPC with env-driven protocol dispatch |

## Delivery modes

- **Sync (default):** each log is written inline on the caller's goroutine with a bounded
  deadline (`WithWriteTimeout`, default 100ms). No connection → immediate drop + background
  reconnect. Stalled consumer → drop after timeout.
- **Async (`zapwire.WithAsyncMode()`):** logs are enqueued and flushed in batches by a
  background goroutine; `Write` never blocks on I/O. Call `logger.Sync()` at checkpoints
  and `writer.Close()` at exit to drain the queue.

Both modes drop rather than block indefinitely. See [Sync vs Async](docs/guide.md#sync-vs-async).

## Options & defaults

Core options (for `ndjson` and `zapwire.New`, pass directly; for `fluent`, wrap in
`fluent.WithZapwireOptions(...)`; for `syslog`, wrap in `syslog.WithZapwireOptions(...)`):

| Option | Applies | Default | Purpose |
|---|---|---|---|
| `WithSyncMode()` | both | **default** | inline write-per-log |
| `WithAsyncMode()` | both | — | buffered, batched background delivery |
| `WithWriteTimeout(d)` | both | `100ms` | per-socket-write deadline |
| `WithBufferSize(n)` | async | `4096` | queue capacity, in logs |
| `WithBatchSize(n)` | async | `128` | max logs per flushed frame |
| `WithFlushInterval(d)` | async | `200ms` | max time a log waits before a flush |
| `WithDropPolicy(p)` | async | `DropNewest` | `DropNewest` or `DropOldest` when the queue is full |
| `WithMaxRetries(n)` | both | `30` | reconnect attempts per burst |
| `WithReconnect(initial, max)` | both | `100ms` / `3s` | reconnect backoff bounds |
| `WithErrorHandler(fn)` | both | stderr | transport-error callback |

`fluent`-only: `WithTag` (default `"app.logs"`), `WithTimeCodec` (default `AutoEpochCodec("ts")`),
`WithTimeKey`, `WithZapwireOptions`.

`syslog`-only: `WithFacility` (default `LOCAL0`), `WithSeverityMapper`, `WithHostname`,
`WithAppName`, `WithProcID`, `WithMsgID`, `WithBOM`, `WithFraming` (default `OctetCounting`),
`WithZapwireOptions`.

`otlp`-only (selected; full list at [pkg.go.dev](https://pkg.go.dev/github.com/arloliu/zapwire/otlp)):

| Option | Default | Purpose |
|---|---|---|
| `WithServiceName(s)` | `"unknown_service:<exe>"` | Resource `service.name` |
| `WithResource(fields...)` | — | Extra Resource attributes |
| `WithEncoding(e)` | `Protobuf` | `Protobuf` or `JSON` (HTTP only) |
| `WithCompression(c)` | `NoCompression` | `NoCompression` or `Gzip` |
| `WithQueueSize(n)` | `2048` | Ingest queue capacity, in records |
| `WithBatchSize(n)` | `512` | Records per export request |
| `WithFlushInterval(d)` | `1s` | Max batch latency |
| `WithTimeout(d)` | `10s` | Per-export-attempt deadline |
| `WithRetry(rc)` | 5s / 30s / 60s | Retry initial / max interval / budget |
| `WithHeaders(h)` | — | Auth headers added to every request |
| `WithInsecure()` | — | gRPC h2c for bare `host:port` endpoints |
| `WithTLSConfig(c)` | — | gRPC custom CA / mTLS |
| `WithTraceCorrelationAttributes(on)` | `false` | Also emit flat hex string attrs (non-OTLP pipelines) |
| `WithErrorHandler(fn)` | no-op | Ship-path error callback (`*ExportError`) |

## Observability

```go
writer.DroppedLogs()    // uint64 — logs dropped (no connection or full buffer)
writer.ReconnectCount() // uint64 — successful background (re)connections
writer.IsConnected()    // bool   — a connection object is held (not a TCP liveness check)
```

`otlp.Writer` exposes `DroppedLogs()` with the same semantics (queue overflow + retry
exhaustion + partial-success rejections).

## Semantics

zapwire is **at-most-once**: logs buffered in async mode are lost on a hard crash, and a
stalled or absent consumer causes counted drops rather than an unbounded block.

See the **[User Guide](docs/guide.md)** for the full treatment, and
[`docs/design/`](docs/design/) for the design rationale.

## Versioning & stability

The repository hosts two modules that version independently under
[SemVer](https://semver.org):

- `github.com/arloliu/zapwire` — root module (`fluent`, `ndjson`, `syslog`)
- `github.com/arloliu/zapwire/otlp` — OTLP exporter

**Compatibility promise** (from each module's v1.0.0 onward): the exported Go API of every
non-`internal` package. **Not frozen:** wire-level defaults (called out in CHANGELOG on
change), anything under `internal/`, and the minimum supported Go version (tracks the two
most recent releases).

See [CHANGELOG.md](CHANGELOG.md).
