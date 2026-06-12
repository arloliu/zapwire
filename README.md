# zapwire

High-performance [zap](https://github.com/uber-go/zap) `WriteSyncer` that ships structured
logs to log processors over UDS or TCP, with never-block drop-on-stall semantics.

```bash
go get github.com/arloliu/zapwire
```

> **New here?** Read the **[User Guide](docs/guide.md)** for a full walkthrough — architecture,
> the three Fluent encoding paths, time codecs, sync vs async tuning, graceful shutdown, and
> troubleshooting. Prefer code? The **[`examples/`](examples)** folder has runnable, self-contained
> programs. This README is the quick reference.

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
- **Encoder / Framer** — per-processor wire format (msgpack vs newline-delimited). You rarely
  touch these directly; the `fluent` and `ndjson` subpackages wire the right ones.
- **Writer** — the connection manager. Bounded, never-blocking writes (drop-on-stall).
- **Core** — `NewCore` glues a zap encoder + the Writer into a `zapcore.Core` ready for `zap.New`.

The `NewCore` / `NewWriter` constructors in each subpackage assemble all of this for you.

## Processors & formats

| Format | Reaches | Subpackage |
|---|---|---|
| Fluent Forward (msgpack) | Fluentd, Fluent-bit, Vector | `fluent` |
| NDJSON | Vector, Logstash, OTel Collector, generic | `ndjson` |
| Syslog (RFC5424) | rsyslog, syslog-ng, Vector, Logstash | `syslog` |
| OTLP/gRPC + HTTP protobuf/JSON logs | OTel Collector, Fluent-bit, Loki ≥3.0, Elastic, Datadog | `otlp` (own module; trace correlation from `context.Context`) |

## Quick start

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

`fluent` offers three paths: **transcode** (`NewCore`, JSON→msgpack), **native** (`NewNativeCore`,
direct msgpack — faster, exact numeric types, recommended for new code), and **bring-your-own
core** (`NewWriter` / `NewMsgpackEncoder` for tees and samplers). See
[Choosing a Fluent path](docs/guide.md#fluent-three-encoding-paths).

### NDJSON over TCP (async)

```go
core, writer, _ := ndjson.NewCore(zapwire.TCP("collector:9000"), zap.InfoLevel,
    zap.NewProductionEncoderConfig(), zapwire.WithAsyncMode())
defer writer.Close()
logger := zap.New(core)
```

## Examples

Runnable, self-contained programs in [`examples/`](examples) — each spins up a local sink, ships
logs to it, and prints what arrived (no external processor needed). Run with `go run ./<name>`:

- [`ndjson-tcp`](examples/ndjson-tcp) — NDJSON over TCP, sync mode, end-to-end
- [`fluent-native-uds`](examples/fluent-native-uds) — Fluent native msgpack over UDS, with frame decoding
- [`async-observability`](examples/async-observability) — async tuning + the health counters
- [`tee-console`](examples/tee-console) — `zapcore.NewTee` fan-out with per-core levels

## Delivery modes

- **Sync (default):** each log is written inline with a bounded deadline. A sync caller waits
  for its own write up to `WithWriteTimeout`; concurrent sync callers serialize on the single
  in-flight write. The wait is bounded, never unbounded.
- **Async (`zapwire.WithAsyncMode()`):** logs are buffered and flushed in batches; `Write`
  enqueues and returns without blocking. Call `logger.Sync()` to force a flush.

Both drop-on-stall rather than block indefinitely. See
[Sync vs Async](docs/guide.md#sync-vs-async) for how to choose and tune.

## Options & defaults

All options are passed to the subpackage constructors. For `fluent`, wrap core options in
`fluent.WithZapwireOptions(...)`; for `ndjson` and the root `zapwire.New`, pass them directly.

| Option | Applies | Default | Purpose |
|---|---|---|---|
| `WithSyncMode()` | both | **default** | inline write-per-log |
| `WithAsyncMode()` | both | — | buffered, batched background delivery |
| `WithWriteTimeout(d)` | both | `100ms` | per-socket-write deadline |
| `WithBufferSize(n)` | async | `4096` | queue capacity, in logs |
| `WithBatchSize(n)` | async | `128` | max logs per flushed frame |
| `WithFlushInterval(d)` | async | `200ms` | max time a log waits before a flush |
| `WithDropPolicy(p)` | async | `DropNewest` | what to discard when the buffer is full |
| `WithMaxRetries(n)` | both | `30` | reconnect attempts per burst |
| `WithReconnect(initial, max)` | both | `100ms` / `3s` | reconnect backoff bounds |
| `WithErrorHandler(fn)` | both | stderr | transport-error callback (encode errors return from `Write`) |

`fluent`-only: `WithTag` (default `"app.logs"`), `WithTimeCodec` (default `AutoEpochCodec("ts")`),
`WithTimeKey`, `WithZapwireOptions`.

The dial timeout is **3s** (not configurable) and only ever applies on the background reconnect
path — never on a log-write call.

## Observability

```go
writer.DroppedLogs()    // uint64 — logs dropped due to connection/buffer pressure
writer.ReconnectCount() // uint64 — successful background (re)connections
writer.IsConnected()    // bool   — a connection object is held (not a TCP liveness check; see guide)
```

## Semantics

zapwire is **at-most-once**: buffered logs are lost on a hard crash, and a stalled or absent
consumer causes counted drops rather than an unbounded block (sync mode waits only up to its
bounded `WithWriteTimeout`; async mode never blocks on enqueue).

See the **[User Guide](docs/guide.md)** for the full treatment, and
[`docs/design/2026-06-07-zapwire-design.md`](docs/design/2026-06-07-zapwire-design.md) for the
design rationale.

## Versioning & stability

The repository hosts two modules that version independently under
[SemVer](https://semver.org): `github.com/arloliu/zapwire` (root, with the
`fluent`, `ndjson`, and `syslog` subpackages) and
`github.com/arloliu/zapwire/otlp`. See [CHANGELOG.md](CHANGELOG.md).

**What the compatibility promise covers** (from each module's v1.0.0 onward):
the exported Go API of every non-`internal` package. **What it does not
freeze:** wire-level *defaults* may be adjusted in minor releases when a spec
or receiver ecosystem moves (always called out in the changelog); anything
under `internal/`; and the minimum supported Go version, which tracks the two
most recent Go releases.
