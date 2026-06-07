# zapwire

High-performance [zap](https://github.com/uber-go/zap) `WriteSyncer` that ships structured
logs to log processors over UDS or TCP, with never-block drop-on-stall semantics.

## Install

```bash
go get github.com/arloliu/zapwire
```

## Processors & formats

| Format | Reaches | Subpackage |
|---|---|---|
| Fluent Forward (msgpack) | Fluentd, Fluent-bit, Vector | `fluent` |
| NDJSON | Vector, Logstash, OTel Collector, generic | `ndjson` |

Transports: `zapwire.UDS(path)` and `zapwire.TCP(addr)`.

## Quick start (Fluent Forward over UDS)

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

### Timestamps

`fluent.NewCore` wires both ends of the time round-trip from a `TimeCodec`, so the JSON
encoder and the decoder always agree. Choose a built-in or supply your own:

```go
core, w, _ := fluent.NewCore(zapwire.UDS(path), zap.InfoLevel, cfg,
    fluent.WithTimeCodec(fluent.RFC3339NanoCodec("ts")))
```

Built-ins: `AutoEpochCodec` (default — magnitude-tolerant numeric epoch decoder),
`EpochNanosCodec`, `EpochMillisCodec`, `EpochSecondsCodec`, `RFC3339NanoCodec`, `RFC3339Codec`,
`ISO8601Codec`. Override just the key with `fluent.WithTimeKey("timestamp")`, or pass a custom
`fluent.TimeCodec{Key, ZapEncoder, Decode}`.

If you build your own `zapcore.Core` (via `fluent.NewWriter`) instead of `NewCore`, align the
encode end yourself: `codec.ApplyTo(&encoderConfig)`.

### Numeric fields

The Fluent transcode path decodes JSON record numbers with `UseNumber` and normalizes
integral values to msgpack `int`/`uint`, preserving `int64`/`uint64` precision above 2⁵³.
Side effect: because zap encodes a whole-number `float64` as an integer JSON literal
(`zap.Float64("x", 3.0)` → `3`), such a field arrives as a msgpack integer, indistinguishable
from `zap.Int`. Values with a fractional part are unaffected. Exact preservation of zap's
original numeric kind requires the v2 native encoder (no JSON round-trip).

## NDJSON over TCP

```go
core, writer, _ := ndjson.NewCore(zapwire.TCP("collector:9000"), zap.InfoLevel,
    zap.NewProductionEncoderConfig(), zapwire.WithAsyncMode())
defer writer.Close()
logger := zap.New(core)
```

## Delivery modes

- **Sync (default):** each log is written inline with a bounded deadline, so a sync caller
  waits for its own write up to `WithWriteTimeout` and concurrent sync callers serialize on the
  single in-flight write. The wait is bounded, never unbounded.
- **Async (`zapwire.WithAsyncMode()`):** logs are buffered and flushed in batches; `Write`
  enqueues and returns without blocking. Call `logger.Sync()` to flush.

Both drop-on-stall rather than block indefinitely. Tune with `WithBufferSize`, `WithBatchSize`,
`WithFlushInterval`, `WithDropPolicy`, `WithWriteTimeout`, `WithReconnect`, `WithMaxRetries`,
`WithErrorHandler`. Introspect with `Writer.DroppedLogs()`, `ReconnectCount()`,
`IsConnected()`.

## Semantics

zapwire is **at-most-once**: buffered logs are lost on a hard crash, and a stalled or absent
consumer causes counted drops rather than an unbounded block (sync mode waits only up to its
bounded `WithWriteTimeout`; async mode never blocks on enqueue). See
[`docs/design/2026-06-07-zapwire-design.md`](docs/design/2026-06-07-zapwire-design.md).
