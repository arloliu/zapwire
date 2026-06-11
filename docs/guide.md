# zapwire User Guide

zapwire is a [zap](https://github.com/uber-go/zap) `WriteSyncer` that ships structured logs to a
log processor (Fluentd, Fluent-bit, Vector, Logstash, the OpenTelemetry Collector, …) over a Unix
domain socket or TCP. It reconnects in the background and never blocks your application
indefinitely: a stalled or absent consumer causes *counted drops*, not a hung logging call.

This guide walks through every concept and every knob. For a one-page reference, see the
[README](../README.md); for runnable, self-contained programs, see [`examples/`](../examples).

## Contents

- [Architecture & data flow](#architecture--data-flow)
- [Choosing a format](#choosing-a-format)
- [Encoder config & log level](#encoder-config--log-level)
- [Fluent: three encoding paths](#fluent-three-encoding-paths)
- [NDJSON](#ndjson)
- [Time codecs](#time-codecs)
- [Sync vs Async](#sync-vs-async)
- [Reconnect, drops & observability](#reconnect-drops--observability)
- [Composing with other cores](#composing-with-other-cores)
- [Graceful shutdown](#graceful-shutdown)
- [Options reference](#options-reference)
- [Building a Writer by hand](#building-a-writer-by-hand)
- [Delivery semantics](#delivery-semantics)
- [Troubleshooting](#troubleshooting)

---

## Architecture & data flow

A log entry flows through five layers. zapwire owns the bottom three; zap owns the top two.

```
zap.Logger.Info(...)
  │
  ▼
zapcore.Core              ── applies level filter, runs the zap encoder ──▶ []byte
  │                          (JSON on the transcode path; msgpack on the native path)
  ▼
zapwire.Encoder           ── []byte ─▶ one per-entry wire payload
  │                          (transcode: parse JSON → msgpack; ndjson: trim newline;
  │                           native: Passthrough — the bytes are already final)
  ▼
zapwire.Framer            ── 1..N payloads ─▶ one wire frame
  │                          (fluent: PackedForward; ndjson: newline-terminated)
  ▼
zapwire.Writer            ── bounded, reconnecting, drop-on-stall write
  │
  ▼
Transport (UDS | TCP)     ── auto-reconnecting byte stream ─▶ the processor
```

The two small interfaces in the root package are the extension points:

```go
type Encoder interface { Encode(dst, record []byte) ([]byte, error) }       // bytes → payload
type Framer  interface { Frame(dst []byte, payloads [][]byte) ([]byte, error) } // payloads → frame
```

You almost never implement these yourself — the `fluent` and `ndjson` subpackages provide them
and wire everything together through their `NewCore` / `NewWriter` constructors. You only reach for
the raw pieces when [building a Writer by hand](#building-a-writer-by-hand).

**Transport** is created with `zapwire.UDS(path)` or `zapwire.TCP(addr)`. Each dial is bounded by a
3s timeout and runs only on the background (re)connect path — never on a log-write call.

---

## Choosing a format

| You're sending to | Use | Wire format |
|---|---|---|
| Fluentd, Fluent-bit, Vector (Fluent input) | `fluent` | Fluent Forward, msgpack `PackedForward` |
| Vector, Logstash, OTel Collector, anything line-oriented | `ndjson` | newline-delimited JSON |
| rsyslog, syslog-ng, Vector, Logstash | `syslog` | RFC5424 syslog (JSON body) |

If your processor speaks the Fluent Forward protocol, prefer `fluent` — it is more compact
(msgpack) and carries an exact event timestamp. Otherwise `ndjson` is the universal option.

---

## Encoder config & log level

Every `NewCore` takes two zap arguments alongside the transport: a `zapcore.EncoderConfig` and a
`zapcore.LevelEnabler`.

**EncoderConfig** controls the field names and value formats of the wire record. The keys you set
flow straight through to the processor:

```go
encCfg := zap.NewProductionEncoderConfig()
encCfg.MessageKey = "msg"        // the log message field
encCfg.LevelKey = "level"        // "info", "warn", …
encCfg.NameKey = "logger"        // logger name (zap.Named)
encCfg.CallerKey = "caller"      // file:line, if zap.AddCaller() is set
encCfg.EncodeLevel = zapcore.LowercaseLevelEncoder
```

`zap.NewProductionEncoderConfig()` is a sensible base. On the Fluent transcode path, the time key
and encoder are managed by the [TimeCodec](#time-codecs); on every other path control time through
`TimeKey` / `EncodeTime` here. (On the Fluent **native** path the encoder config's *time* settings
are ignored — time is structural — but all other keys still apply.)

**LevelEnabler** is the minimum level an entry must meet. A plain `zap.InfoLevel` is fixed for the
life of the logger. For a level you can flip at runtime, pass a `zap.AtomicLevel`:

```go
lvl := zap.NewAtomicLevelAt(zap.InfoLevel)
core, writer, _ := fluent.NewNativeCore(zapwire.UDS(path), lvl, encCfg)
// later, from anywhere:
lvl.SetLevel(zap.DebugLevel)
```

---

## Fluent: three encoding paths

The `fluent` subpackage offers three ways to produce Fluent Forward frames. They differ in how the
log record becomes msgpack and where the timestamp comes from.

### 1. Transcode (default) — `fluent.NewCore` / `fluent.NewWriter`

zap encodes each entry to **JSON**, then zapwire **parses that JSON and re-encodes it to msgpack**.

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
logger.Info("ready", zap.Int("port", 8080))
```

- The timestamp is read from a record field per the [TimeCodec](#time-codecs). `NewCore` wires
  *both* ends of the codec for you, so encode and decode always agree.
- **Numeric caveat:** because zap serializes a whole-number `float64` as an integer JSON literal
  (`zap.Float64("x", 3.0)` → `3`), such a field arrives at the processor as a msgpack *integer*,
  indistinguishable from `zap.Int`. Values with a fractional part are unaffected. Integer fields
  keep full `int64`/`uint64` precision above 2⁵³ (zapwire decodes JSON numbers with `UseNumber`).
- This path does a JSON round-trip per log, so it is the slowest of the three.

### 2. Native (recommended for new code) — `fluent.NewNativeCore` / `fluent.NewNativeWriter`

zap encodes **directly to msgpack** with no JSON in the middle.

```go
core, writer, err := fluent.NewNativeCore(
    zapwire.TCP("fluentd:24224"),
    zap.InfoLevel,
    zap.NewProductionEncoderConfig(),
    fluent.WithTag("app.logs"),
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

logger := zap.New(core)
```

- **Faster, fewer allocations** — no parse/re-encode step.
- **Exact numeric types** — `zap.Float64("x", 3.0)` stays a float; ints stay ints. The transcode
  caveat above does not apply.
- **Structural timestamp** — the event time comes from `zapcore.Entry.Time` directly, so it is
  exact. `WithTimeCodec` / `WithTimeKey` are accepted but are **no-ops** on this path; the encoder
  config's time settings are also ignored.

Internally the native path pairs `fluent.NewMsgpackEncoder` (a `zapcore.Encoder`) with a
`zapwire.Passthrough()` encoder and the PackedForward framer.

### 3. Bring-your-own core — `fluent.NewWriter` + your own `zapcore.Core`

Use this when you need to compose with `zapcore.NewTee`, a sampler, or multiple outputs, and want
to build the `zapcore.Core` yourself.

```go
codec := fluent.RFC3339NanoCodec("ts")

encCfg := zap.NewProductionEncoderConfig()
codec.ApplyTo(&encCfg) // align the ENCODE end with the decoder — see note below

writer, err := fluent.NewWriter(
    zapwire.UDS("/var/run/fluent.sock"),
    fluent.WithTag("app.logs"),
    fluent.WithTimeCodec(codec), // the DECODE end
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

core := zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), writer, zap.InfoLevel)
logger := zap.New(core)
```

> **Important:** on the transcode path, `NewWriter` only sets up the *decode* end of the timestamp
> round-trip. You must align the *encode* end yourself with `codec.ApplyTo(&encCfg)`, or the
> timestamp is misread. (`NewCore` does this for you; that's the difference.) The default
> `AutoEpochCodec` is magnitude-tolerant, so even an unaligned encode end usually decodes to ~now —
> but don't rely on that with an explicit non-Auto codec.

### Quick comparison

| | Transcode (`NewCore`) | Native (`NewNativeCore`) | BYO core (`NewWriter`) |
|---|---|---|---|
| zap encoder | JSON | msgpack (direct) | yours (usually JSON) |
| JSON round-trip | yes | no | yes |
| Numeric types preserved | whole floats coerced to int | exact | whole floats coerced to int |
| Timestamp source | record field via TimeCodec | `Entry.Time` (exact) | record field via TimeCodec |
| TimeCodec wiring | both ends, automatic | n/a (ignored) | decode auto; **encode is on you** |
| Relative speed | slowest | fastest | slowest |

---

## NDJSON

NDJSON is newline-delimited JSON: each log is zap's JSON object on its own line. It is the right
choice for Vector, Logstash, the OTel Collector, or any consumer that reads line-oriented JSON.

```go
core, writer, err := ndjson.NewCore(
    zapwire.TCP("collector:9000"),
    zap.InfoLevel,
    zap.NewProductionEncoderConfig(),
    zapwire.WithAsyncMode(),
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

logger := zap.New(core)
```

NDJSON has no timestamp codec to configure — the time is whatever zap's encoder writes (control it
through the standard `EncoderConfig.TimeKey` / `EncoderConfig.EncodeTime`). `ndjson.NewWriter`
exists too, for the bring-your-own-core pattern, and takes core zapwire options directly.

---

## Time codecs

*(Fluent transcode path only — skip this on the native path, where time is structural.)*

A `TimeCodec` bundles **both ends** of the timestamp round-trip so they cannot drift:

```go
type TimeCodec struct {
    Key        string                            // the JSON field holding the timestamp
    ZapEncoder zapcore.TimeEncoder               // how zap WRITES it (encode end)
    Decode     func(value any) (time.Time, bool) // how zapwire READS it back (decode end)
}
```

`fluent.NewCore` applies both ends from the one codec you pass, so they always match. On the
bring-your-own-core path you set the decode end via `WithTimeCodec` and the encode end via
`codec.ApplyTo(&encCfg)`.

### Built-in codecs

| Codec | Wire form | Notes |
|---|---|---|
| `AutoEpochCodec(key)` | numeric epoch | **default** (`key = "ts"`). Auto-detects unit (s/ms/µs/ns) by magnitude on decode; encodes nanoseconds. Most forgiving — good when you don't control the encode end. |
| `EpochNanosCodec(key)` | integer epoch ns | JSON numbers decode as `float64`, so ~tens of ns of precision is lost. Use `RFC3339NanoCodec` if exact ns matter. |
| `EpochMillisCodec(key)` | integer epoch ms | |
| `EpochSecondsCodec(key)` | float epoch seconds | matches zap's out-of-the-box `EpochTimeEncoder`. |
| `RFC3339NanoCodec(key)` | RFC3339 string, ns | exact to the nanosecond. |
| `RFC3339Codec(key)` | RFC3339 string, s | |
| `ISO8601Codec(key)` | ISO8601 string, ms | e.g. `2006-01-02T15:04:05.000Z0700`. |

Override just the field name (keeping the format) with `fluent.WithTimeKey("timestamp")`, or supply
a fully custom `fluent.TimeCodec{Key, ZapEncoder, Decode}`. An absent or unparseable timestamp
falls back to `time.Now()` at encode time.

```go
core, w, _ := fluent.NewCore(zapwire.UDS(path), zap.InfoLevel, cfg,
    fluent.WithTimeCodec(fluent.RFC3339NanoCodec("ts")))
```

---

## Sync vs Async

This is the most important tuning decision. It is set with `zapwire.WithSyncMode()` (the default)
or `zapwire.WithAsyncMode()`.

### Sync (default)

Each log is encoded, framed, and written to the socket **inline**, on the goroutine that called
the logger, with a deadline of `WithWriteTimeout` (default 100ms) on the socket write.

- A logging call returns as soon as its bytes are handed to the kernel.
- If there is **no live connection**, the log is dropped immediately (counted) and a background
  reconnect is triggered — the call does not block.
- If the write **stalls** (the consumer's socket buffer is full), the call blocks up to
  `WithWriteTimeout`, then the log is dropped and the connection is recycled.
- Concurrent sync callers serialize on a single in-flight write, but a single caller's wait stays
  bounded by ~one `WithWriteTimeout` (a failed write recycles the connection, so others fail fast).

**Choose sync when** you want the simplest model, the lowest latency to the wire, no extra
goroutine, and no buffered logs to lose on a crash. It is the right default for most apps.

### Async

`Write` **enqueues** the encoded payload into a bounded channel and returns immediately — it never
blocks on I/O. A background goroutine drains the queue, batches payloads into one frame, and writes
them.

```go
core, writer, _ := ndjson.NewCore(zapwire.TCP("collector:9000"), zap.InfoLevel, cfg,
    zapwire.WithAsyncMode(),
    zapwire.WithBufferSize(8192),       // queue capacity in logs (default 4096)
    zapwire.WithBatchSize(256),         // max logs per frame (default 128)
    zapwire.WithFlushInterval(100*time.Millisecond), // max wait before a flush (default 200ms)
    zapwire.WithDropPolicy(zapwire.DropOldest),      // default DropNewest
)
```

A batch is flushed when it reaches `WithBatchSize`, when `WithFlushInterval` elapses, on
`logger.Sync()`, or on `Close`. When the queue is full, `WithDropPolicy` decides what to discard:

- `DropNewest` (default) — drop the incoming log, keep the buffered backlog.
- `DropOldest` — evict the oldest queued log to make room for the new one.

**Choose async when** you want the highest throughput and the logging call to never wait on the
socket, and you can tolerate losing buffered logs on a hard crash. Trade-offs to size:

- Larger `WithBufferSize` absorbs longer consumer stalls before dropping, at the cost of memory and
  more logs lost on a crash.
- Larger `WithBatchSize` improves throughput (fewer, bigger frames) but adds a little latency.
- Shorter `WithFlushInterval` lowers delivery latency for low-volume logs.

> In async mode, call `logger.Sync()` at meaningful checkpoints (and always before exit) to force a
> flush — see [Graceful shutdown](#graceful-shutdown).

---

## Reconnect, drops & observability

The Writer manages the connection for you:

- On startup it dials immediately so logs flow at once if the endpoint is already up; otherwise it
  starts disconnected and reconnects in the background.
- A write failure recycles the dead connection and triggers a reconnect.
- Reconnect uses exponential backoff starting at `WithReconnect`'s initial interval (100ms),
  doubling up to its max (3s), for up to `WithMaxRetries` attempts per burst (30).
- **Bursts are re-armed by writes.** If a burst exhausts all `WithMaxRetries` attempts without
  reconnecting, the loop goes idle — but the next log write that finds no connection triggers a
  fresh burst. So a busy service keeps retrying indefinitely, while a fully idle logger won't
  reconnect until it next tries to write. (Raise `WithMaxRetries` if a long outage shouldn't pause
  retries between writes.)

The Writer is safe for concurrent use: multiple goroutines may call into the same `zap.Logger`
(and thus the Writer) without external locking.

Inspect runtime health at any time:

```go
writer.DroppedLogs()    // uint64 — cumulative logs dropped (no connection, or full buffer)
writer.ReconnectCount() // uint64 — cumulative successful background (re)connections
writer.IsConnected()    // bool   — a connection object is held (see TCP caveat below)
```

Route transport errors somewhere structured instead of stderr:

```go
fluent.WithZapwireOptions(zapwire.WithErrorHandler(func(err error) {
    metrics.Incr("log_ship_error")
    // ...
}))
```

**Two error channels.** Know which errors land where:

- `WithErrorHandler` receives the **initial connect failure** and **socket-write errors**, plus, in
  async mode, **batch-framing errors**. This is the callback for "shipping is unhealthy."
- **Background reconnect dial failures are silent** — they do *not* reach the handler. During a
  sustained outage you'll get the initial failure once and then nothing more from the callback; lean
  on `ReconnectCount()` / `IsConnected()` / `DroppedLogs()` to observe an ongoing outage.
- **Encode errors** (and, in sync mode, framing errors) are *returned from the `Write` call* and
  surface through zap's own internal error handling — they do not reach `WithErrorHandler`. These
  are rare and indicate a malformed record or encoder bug, not a transport problem.

A rising `DroppedLogs()` is your signal that the consumer can't keep up (or is down). Pair it with
`ReconnectCount()` to distinguish "consumer is flapping" from "consumer is slow."

> **Caveat for TCP:** `IsConnected()` reports whether a connection object is *held*, not whether the
> peer is actually alive. A half-open TCP connection (peer crashed without sending a reset) still
> reports connected until a write finally fails. See [Troubleshooting](#troubleshooting).

---

## Composing with other cores

A `*NewCore` constructor returns a `zapcore.Core`, so it slots into any zap composition.

### Ship to a processor *and* the console (Tee)

Use `zapcore.NewTee` to fan one logger out to several cores — e.g. native msgpack to Fluentd plus
human-readable lines on stdout:

```go
encCfg := zap.NewProductionEncoderConfig()
lvl := zap.InfoLevel

// One core ships native msgpack to Fluentd...
wireCore, writer, err := fluent.NewNativeCore(zapwire.UDS("/var/run/fluent.sock"), lvl, encCfg)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

// ...another writes to stdout.
console := zapcore.NewCore(zapcore.NewConsoleEncoder(encCfg), zapcore.AddSync(os.Stdout), lvl)

logger := zap.New(zapcore.NewTee(wireCore, console))
```

> Runnable version: [`examples/tee-console`](../examples/tee-console).

Each core in the tee keeps its own level and encoder, so you can, say, ship `Info`+ to the
processor while logging `Debug`+ to the console. (For finer control you can build the wire core
yourself from `fluent.NewMsgpackEncoder` + `fluent.NewNativeWriter` and pass it to
`zapwire.NewCore` — `NewNativeCore` is just that assembled for you.)

### Add zapwire to a logger built from `zap.Config`

If you already build your logger the standard way (`zap.NewProductionConfig().Build()`), graft the
zapwire core on with `zap.WrapCore`:

```go
wireCore, writer, err := fluent.NewNativeCore(zapwire.UDS("/var/run/fluent.sock"), zap.InfoLevel,
    zap.NewProductionEncoderConfig())
if err != nil {
    log.Fatal(err)
}
defer writer.Close() // you still own the writer's lifecycle

cfg := zap.NewProductionConfig()
logger, err := cfg.Build(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
    return zapcore.NewTee(c, wireCore) // keep cfg's default core, add zapwire alongside
}))
if err != nil {
    log.Fatal(err)
}
```

`cfg.Build` constructs and manages its own core, but it does **not** know about the zapwire writer —
you remain responsible for calling `writer.Close()` on shutdown.

---

## Graceful shutdown

Always `Close` the Writer. `Close` stops the background goroutines, **flushes any buffered async
entries**, and closes the connection. It is idempotent.

```go
core, writer, err := fluent.NewNativeCore(/* ... */)
if err != nil {
    log.Fatal(err)
}
defer writer.Close() // flushes async buffer, then tears down

logger := zap.New(core)
// ... run the app ...

logger.Sync() // optional: force a flush at a checkpoint; a no-op in sync mode
```

Notes:

- In **async** mode, `Close` performs a final flush of the whole queue before closing the socket,
  so a deferred `Close` is sufficient to drain on exit. `logger.Sync()` is for flushing at
  checkpoints *during* the run.
- In **sync** mode, `logger.Sync()` / `writer.Sync()` is a no-op — there is no buffer.
- After `Close`, further log calls are silently dropped (they return success to honor zap's
  multi-writer contract), not written; `Sync()` after `Close` is a no-op. `Close` is idempotent, so
  a repeat call (e.g. a defer plus an explicit call) is harmless.
- Delivery is best-effort: a flush bounded by `WithWriteTimeout` against a dead consumer will drop
  rather than hang. `Close` will not block indefinitely even if the consumer is gone.

---

## Options reference

Core options live in the root package. With `fluent`, pass them via
`fluent.WithZapwireOptions(...)`; with `ndjson` and `zapwire.New`, pass them directly.

| Option | Applies | Default | Effect |
|---|---|---|---|
| `WithSyncMode()` | both | **default** | inline write-per-log |
| `WithAsyncMode()` | both | — | buffered, batched background delivery |
| `WithWriteTimeout(d)` | both | `100ms` | deadline on each socket write |
| `WithBufferSize(n)` | async | `4096` | queue capacity, in logs |
| `WithBatchSize(n)` | async | `128` | max logs framed together per flush |
| `WithFlushInterval(d)` | async | `200ms` | max time a log waits before a flush |
| `WithDropPolicy(p)` | async | `DropNewest` | `DropNewest` or `DropOldest` when the queue is full |
| `WithMaxRetries(n)` | both | `30` | reconnect attempts per burst |
| `WithReconnect(initial, max)` | both | `100ms` / `3s` | reconnect backoff floor / ceiling |
| `WithErrorHandler(fn)` | both | stderr | callback for transport errors (see [Two error channels](#reconnect-drops--observability)) |

`fluent`-only options: `WithTag(tag)` (default `"app.logs"`), `WithTimeCodec(c)`
(default `AutoEpochCodec("ts")`), `WithTimeKey(key)`, `WithZapwireOptions(opts...)`.

Every numeric/duration option clamps a non-positive value back to its default rather than erroring.
The dial timeout is a fixed **3s** and is not an option; it applies only on the background reconnect
path, never on a log-write call.

---

## Building a Writer by hand

`zapwire.New` is the low-level constructor the subpackages build on. Reach for it to plug in a
custom wire format (your own `Encoder` / `Framer`) or to control assembly fully.

```go
w, err := zapwire.New(
    zapwire.TCP("host:9000"),
    myEncoder, // implements zapwire.Encoder
    myFramer,  // implements zapwire.Framer
    zapwire.WithAsyncMode(),
)
if err != nil { // ErrNoTransport / ErrNoEncoder / ErrNoFramer on nil args
    log.Fatal(err)
}
defer w.Close()

core := zapwire.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), w, zap.InfoLevel)
logger := zap.New(core)
```

If your `zapcore.Encoder` already emits the final per-entry wire payload (like
`fluent.NewMsgpackEncoder`), pair it with `zapwire.Passthrough()` as the `Encoder` so the bytes pass
through unchanged. `zapwire.NewCore` is a thin convenience over `zapcore.NewCore` so callers need
not import `zapcore` for the common case.

---

## Delivery semantics

zapwire is an **at-most-once** shipper, not a write-ahead log:

- **No durable buffer.** Logs buffered in async mode are lost on a hard crash.
- **Drop-on-stall, not block.** A stalled or absent consumer causes counted drops
  (`DroppedLogs()`), never an unbounded block. Sync mode waits only up to `WithWriteTimeout`; async
  mode never blocks on enqueue.
- **No delivery acknowledgement.** A successful socket write means the bytes reached the kernel send
  buffer, not that the processor received or persisted them.

If you need at-least-once or end-to-end acknowledgement, terminate at a local collector (e.g.
Fluent-bit / Vector with a persistent buffer over UDS) and let *that* handle durable forwarding.

---

## Troubleshooting

### Logs aren't arriving, but `DroppedLogs()` is 0 and there are no errors

This is the classic **half-open TCP** case. When the peer crashes without sending a reset, small
writes keep succeeding into the kernel send buffer and return *immediately with no error* — the
write deadline never fires because the write never blocks. The log is counted as sent. The loss
stays silent until the send buffer fills (then writes start timing out and dropping) or TCP's own
retransmission timeout (seconds to minutes) finally surfaces an error.

What to do:

- **Prefer UDS for a local collector.** On a Unix socket the kernel knows immediately when the peer
  process is gone: the next write fails fast with `EPIPE`, so the connection is recycled and a
  reconnect kicks in promptly. TCP cannot offer that for a half-open peer.
- Don't treat `IsConnected() == true` as proof the peer is alive on TCP — it only means a connection
  object is held.
- If you must use TCP and need prompt detection, rely on the consumer side / a local collector, or
  add application-level health checks; `WithWriteTimeout` alone won't catch a half-open peer until
  the buffer fills.

### Logs are dropped under load (async)

The consumer can't keep up and the queue fills. Options, in order of preference: speed up / scale
the consumer; raise `WithBufferSize` to absorb longer stalls; raise `WithBatchSize` for throughput;
switch to `WithDropPolicy(DropOldest)` if recent logs matter more than old ones. Watch
`DroppedLogs()` to confirm the change helped.

### Timestamps are wrong (off by years, or all ~now)

You're on the Fluent **transcode** path with a custom `zapcore.Core` and the encode end doesn't
match the decoder. Either use `fluent.NewCore` (which wires both ends) or call
`codec.ApplyTo(&encCfg)` on your encoder config. See
[bring-your-own core](#fluent-three-encoding-paths). The native path is immune — its time is
structural.

### Float fields arrive as integers

On the transcode path, zap renders a whole-number `float64` as an integer JSON literal, so it
arrives as a msgpack integer. Switch to the **native** path (`NewNativeCore`) for exact numeric
type preservation.

### `New` returns an error immediately

`zapwire.New` returns `ErrNoTransport`, `ErrNoEncoder`, or `ErrNoFramer` only when the corresponding
argument is nil. A *connection* failure at startup is **not** an error — the Writer starts
disconnected and reconnects in the background (check `IsConnected()` / `ReconnectCount()`).
