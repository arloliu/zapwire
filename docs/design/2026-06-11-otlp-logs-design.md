# OTLP logs subpackage — design (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 3) · **Date:** 2026-06-11 · **Branch:** `worktree-otel-otlp`

**Review history:** codex plan-review pass 1 (`tmp/2026-06-11-otlp-logs-design_pass1_review.md`)
raised 2×P0 (EncodeEntry error semantics; sticky `zap.Any("context", ctx)` impossible through
stock `ioCore.With` because `zap.Any` wraps contexts as `StringerType`) + 4×P1 (retry/Sync/Close
lifecycle, partial-success warnings, request-size policy, field-coverage gaps) + 2×P2 — all
resolved (§12). Pass 2 (`tmp/2026-06-11-otlp-logs-design_pass2_review.md`) confirmed the
custom-core direction and wire mapping, audited all pass-1 findings as resolved (two partially),
and raised 1×P0 (inline marshalers cannot be transactional without frame-stack rollback) +
1×P1 (Sync/Close bounds undercounted multi-batch / byte-aware drains) + 1×P2 (eager-helper key
and scanner edge cases) — all resolved (§13). Pass 3
(`tmp/2026-06-11-otlp-logs-design_pass3_review.md`) found **no P0/P1** and returned
**implementation-ready**; its lone P2 (typed-nil contexts are unsafe to hand to
`trace.SpanContextFromContext`, which guards only interface-nil — `otel/trace` `context.go:33-44`)
is folded into §3.4's shared `spanContextFromField` nil-guard. Pass 4
(`tmp/2026-06-11-otlp-logs-design_pass4_review.md`, scoped to the injector-helper addendum)
verified the SugaredLogger typed-Field claim, scanner non-collision, and the
attributes-vs-proto caveat against source, and raised 1×P1 (variadic `existingSlice...`
aliasing — fixed by specifying clone-before-append) + 2×P2 (helper renamed
`TraceCorrelationFields`; injector edge-case test rows added). Verdict: addendum preserves
implementation-readiness with those fixes applied.

**Relates to:** the v1 design `docs/design/2026-06-07-zapwire-design.md` §11 (processor roadmap —
OTLP is #1 by reach; "heavy deps → own module"); the v2 native encoder design
`docs/design/2026-06-08-native-msgpack-encoder-design.md` (the frame-stack `zapcore.Encoder`
pattern this encoder reuses); and the syslog design `docs/design/2026-06-10-syslog-rfc5424-design.md`
(the `NewEncoder`/`NewWriter`/`NewCore` triplet convention and per-encoder immutable config).

**Decisions locked in brainstorming (2026-06-11):**

1. **Scope: full subpackage** — native OTLP encoder + self-contained HTTP shipping, like
   `syslog/`, not an encoder-only or OTel-SDK-bridge design.
2. **Trace correlation: ctx-field + eager helper** — any zap field whose value is a
   `context.Context` (the official `go.opentelemetry.io/contrib/bridges/otelzap` convention)
   is consumed for trace correlation, plus an eager `otlp.SpanContext(ctx)` helper.
   Dependency: `go.opentelemetry.io/otel/trace` (stable v1.x). **Addendum (post-pass-3):**
   functional injector helpers `InjectTraceFields`/`InjectTraceKVs` (structural sugar) and
   `TraceCorrelationFields` (flat hex strings for non-OTLP sinks) — §3.4 — approved in a follow-up
   brainstorming round referencing the uptrace otelzap guide; deliberately functional, not
   a wrapper logger type.
3. **Wire: OTLP/HTTP binary protobuf, hand-rolled** — proto wire format encoded by hand,
   shipped via `net/http` POST to `/v1/logs`. No gRPC, no OTel SDK, no
   `google.golang.org/protobuf`. Prior art: VictoriaMetrics/easyproto, VictoriaTraces
   (OTLP without grpc-go).

**Empirical claims, qualified (pass-1 P2):** the "+209 KB / 2 go.mod lines for `otel/trace`"
measurement and the "hand-rolled encoder byte-identical to `proto.Marshal`" PoC are prior
empirical observations (2026-06-11, Go 1.26.x, otel v1.44.0; PoC at
`/tmp/otlp-dep-test/proto/handroll_test.go`, not checked in). They motivated the strategy but
are **not** load-bearing for correctness — the §9 conformance suite is the binding gate.

**Spec versions targeted:** opentelemetry-proto **v1.10.0** (logs package Stable; field
numbers frozen, additions-only within 1.x), OTLP spec **1.10.0** (Stable for logs).

---

## 1. Goal

Ship zap logs as native **OTLP/HTTP** (`ExportLogsServiceRequest`, binary protobuf) to any
OTLP receiver — OTel Collector, Grafana Loki ≥ 3.0, Elastic managed OTLP, Datadog Agent —
with first-class **logs↔traces correlation**: `trace_id`/`span_id`/`flags` populated as
LogRecord *proto fields* (not string attributes) from the caller's `context.Context`.

Binary protobuf over HTTP is the one encoding every OTLP receiver MUST accept and the
spec-recommended default protocol (`http/protobuf`). gRPC adds grpc-go for zero reach and is
out of scope.

zapwire's delivery philosophy carries over unchanged: bounded buffering, never block the
application goroutine, at-most-once delivery, counted drops, no WAL.

## 2. Architecture

### 2.1 Why the existing `Writer` is not reused

`zapwire.Writer` ships frames over a `net.Conn` (`Transport.Dial`). OTLP/HTTP is
request/response: TLS, status codes, retry classes (429/502/503/504 + `Retry-After`),
partial-success response bodies. None of that fits a stream-conn seam, so `otlp/` carries its
own small HTTP exporter (§5) that mirrors the core Writer's *semantics* (bounded queue, batch,
flush interval, drop policy, counted drops) while replacing conn-write with HTTP POST. There
is no reconnect loop — `http.Client` owns connection lifecycle.

Consequently `otlp/` does **not** import the root `zapwire` module at all. It is a sibling
module sharing the design philosophy, not the Writer. (The small `DropPolicy` enum is
deliberately re-declared locally rather than importing the root module for two constants.)

### 2.2 Why there is a custom `zapcore.Core` (pass-1 P0 resolution)

The sticky form `logger.With(zap.Any("context", ctx))` **cannot** be intercepted from inside
a `zapcore.Encoder` behind the stock `zapcore.NewCore`: `ioCore.With` applies fields to the
encoder clone via `Field.AddTo` (`zapcore/core.go:81-84`), and `zap.Any` classifies every
stdlib context as **`StringerType`** (`zap/field.go:618-624` — contexts implement `String()`,
`context/context.go:201,542,673,762`), so `AddTo` reaches `encodeStringer` → `AddString` with
the *stringified* context (`zapcore/field.go:173-176,214-232`); the original value never
reaches any `Add*` hook. The official contrib bridge intercepts in its own `Core` for exactly
this reason.

So `otlp/` ships a thin custom core (returned by `otlp.NewCore`) that is `ioCore` plus one
behavior: **`With(fields)` pre-scans the raw `[]Field` before zap's dispatch can erase types**
— trace-context fields (§3.4) are consumed and stashed on the cloned encoder; the remaining
fields are applied normally. `Write`/`Check`/`Enabled`/`Sync` delegate exactly like `ioCore`.
Per-call fields need no core support: `EncodeEntry` receives the raw `[]Field` and scans them
itself, so per-call interception works behind *any* core, including a BYO stock
`zapcore.NewCore`.

Compatibility matrix (documented in the package docs):

| Form | via `otlp.NewCore` | via BYO stock `zapcore.NewCore(otlp.NewEncoder(…), …)` |
|---|---|---|
| per-call `zap.Any("context", ctx)` | ✅ | ✅ (encoder scans raw fields) |
| per-call `otlp.SpanContext(ctx)` | ✅ | ✅ |
| sticky `logger.With(otlp.SpanContext(ctx))` | ✅ | ✅ (the helper field is `ReflectType` → `AddReflected` hook) |
| sticky `logger.With(zap.Any("context", ctx))` | ✅ (core pre-scan) | ❌ — degrades to a stringified `context` attribute; documented, with the fix being `otlp.NewCore` or the eager helper |

### 2.3 Data path

```
zap logger
  → otlp.Core                           With(): raw-field pre-scan → trace-context stash
  → otlp zapcore.Encoder.EncodeEntry    bare LogRecord proto bytes (one per entry)  §3
  → otlp.Writer.Write(record)           copy → bounded queue (never blocks)         §5
      → flush loop (single goroutine)   batch by count/bytes/interval
      → envelope assembly               ExportLogsServiceRequest wrapping            §4
      → optional gzip → HTTP POST       {endpoint}/v1/logs, application/x-protobuf
      → retry / partial-success         OTLP spec semantics                          §5.3
```

The per-entry/per-batch split is exactly the core `Encoder`/`Framer` shape: proto `repeated`
fields are concatenation-friendly, so the envelope can wrap N independently-encoded
`LogRecord` payloads without re-encoding them.

## 3. The encoder

A custom `zapcore.Encoder` (the native pattern: same conclusion as fluent-native and syslog —
only the `zapcore.Encoder` layer sees `Entry` + `[]Field`; everything below is bytes). Output
per entry: the **bare `LogRecord` message bytes** (no outer tag/length — the envelope adds
`log_records` tag 2 + varint length per record at batch time).

### 3.1 LogRecord field mapping

Targets `opentelemetry.proto.logs.v1.LogRecord` (proto field numbers below; tag 4 is
`reserved` and must never be emitted):

| Proto field (#, wire type) | Source |
|---|---|
| `time_unix_nano` (1, fixed64) | `ent.Time.UnixNano()` |
| `severity_number` (2, varint) | severity mapping table (§3.2) |
| `severity_text` (3, len) | `ent.Level.String()` verbatim (`"info"`, `"warn"`, …) |
| `body` (5, len → AnyValue) | `AnyValue{string_value: ent.Message}` |
| `attributes` (6, repeated KeyValue) | all remaining zap fields (§3.3) + entry metadata (§3.5) |
| `dropped_attributes_count` (7) | never set (we drop nothing) |
| `flags` (8, fixed32) | W3C trace flags byte from span context; only set with a valid `trace_id` (§3.4) |
| `trace_id` (9, bytes) | 16 bytes from span context; **omitted entirely** when invalid (§3.4) |
| `span_id` (10, bytes) | 8 bytes from span context; omitted when invalid (§3.4) |
| `observed_time_unix_nano` (11, fixed64) | same value as field 1 (spec guidance for in-process emitters) |
| `event_name` (12) | never set (zap has no event concept; future option if needed) |

In `AnyValue`/`KeyValue`, only fields 1–7 / 1–2 are used; the Development-status
`*_strindex` fields (AnyValue 8, KeyValue 3) are profiling-only and never emitted.

### 3.2 Severity mapping

Default table = the official contrib bridge's canonical mapping (`bridges/otelzap`
`convertLevel`), so backends see the same numbers they already expect from otelzap:

| zapcore.Level | SeverityNumber |
|---|---|
| Debug | 5 (DEBUG) |
| Info | 9 (INFO) |
| Warn | 13 (WARN) |
| Error | 17 (ERROR) |
| DPanic | 21 (FATAL) |
| Panic | 22 (FATAL2) |
| Fatal | 23 (FATAL3) |
| unknown | 0 (UNSPECIFIED) |

Overridable via `WithSeverityMapper(func(zapcore.Level) SeverityNumber)`; results are clamped
to `0..24` per entry (out-of-range → 0/24), mirroring syslog's clamp-at-use discipline.

### 3.3 Field encoding: a proto-writing `ObjectEncoder`

Zap fields become `KeyValue{key, AnyValue}` entries under `attributes`, encoded directly to
proto wire bytes via the frame-stack design proven in `fluent/msgpack_encoder.go`: nested
containers (`zap.Namespace` → `kvlist_value`, `AddArray` → `array_value`, `AddObject` →
`kvlist_value`) open a frame with its **own pooled buffer**; sealing a frame writes
`tag + varint(len) + child bytes` into the parent. Per-frame buffers sidestep proto's
length-prefix problem (varint lengths are variable-width, so no in-place backpatching).

**Error semantics — degradable, never entry-fatal (pass-1 P0 resolution).** zap's contract
(`zapcore/field.go:117-185`) converts errors returned by `AddArray`/`AddObject`/
`AddReflected`/stringer paths into a `<key>Error` string attribute and continues; an error
returned from `EncodeEntry` instead aborts the **whole entry** (`zapcore/core.go:94-100`).
Therefore every fallible method is **transactional** — no partial bytes survive a failure —
and `EncodeEntry` returns non-nil **only** for fatal assembly failures (none exist in
practice: buffer appends cannot fail), never for field-level problems. `AddReflected`
JSON-marshals to a scratch buffer first and writes only on success (local precedent
`fluent/msgpack_encoder.go:317-327`).

**Transactionality is implemented as snapshot/rollback, not just child-frame discard
(pass-2 P0 resolution).** Discarding a child frame is insufficient for two cases: (a)
`InlineMarshalerType` (`zap.Inline`) writes into the **current** level — `Field.AddTo` calls
`MarshalLogObject(enc)` directly with no child frame (`zapcore/field.go:122-124`,
`zap/field.go:407-414`); (b) any `ObjectMarshaler`/`ArrayMarshaler` may call `OpenNamespace`
or nest further before failing, leaving frames open *above* the one being discarded. So
before invoking **any** user marshaler (`AddObject`, `AddArray`, `InlineMarshalerType`
dispatch, and `AddReflected`'s post-marshal write), the encoder snapshots
`{stack depth, current frame buffer length, current frame element count}`; on error it pops
and discards (to the pool) every frame above the snapshot depth, truncates the current
frame's buffer to the snapshot length, and restores the count — then returns the error so
zap emits `<key>Error` (for `zap.Inline`, whose key is empty, zap emits `"Error"`). The §9
degradation tests assert byte-exact equality with the same entry logged *without* the
failing field (modulo the `<key>Error` attribute).

**Scope note (impl-plan review):** the snapshot wraps dispatch this package controls —
per-call fields in `EncodeEntry`, the custom core's `With`, and resource encoding. Behind a
**stock** `zapcore.NewCore`, a failing `zap.Inline` applied via `With` follows zap's
standard partial-write behavior: `ioCore.With` dispatches `InlineMarshalerType` directly
into the encoder with no interception point, and zap's own JSON/console encoders behave
identically there. Documented in the §2.2 compatibility matrix and package docs; pinned by
a test. (`AddObject`/`AddArray`/`AddReflected` remain transactional on every core — those
are encoder methods.)

**Complete `zapcore.ObjectEncoder` surface** (every method, not just common constructors —
pass-1 P1 resolution):

| ObjectEncoder method (zap source) | AnyValue |
|---|---|
| `AddString`, stringers (`StringerType`) | `string_value` |
| `AddInt64/32/16/8`, `AddInt`, `AddUint8/16/32`, `AddUintptr` | `int_value` |
| `AddUint64`/`AddUint` | `int_value`; values > MaxInt64 → decimal `string_value` (OTel attribute model has no uint) |
| `AddFloat64/32` | `double_value` |
| `AddBool` | `bool_value` |
| `AddBinary` (`zap.Binary`, `zap.Any([]byte)`) | `bytes_value` (arbitrary blob) |
| `AddByteString` (`zap.ByteString`) | `string_value` — zap's documented semantic is "UTF-8 encoded bytes"; mirrors the JSON encoder's string-vs-base64 distinction. Caller owns UTF-8 validity (proto3 `string` requirement), per zap's own contract |
| `AddTime` | `int_value` UnixNano (matches contrib bridge) |
| `AddDuration` | `int_value` nanoseconds (matches contrib bridge) |
| `AddComplex128/64` | `string_value` (zap JSON convention `"a+bi"`) |
| `AddArray` | `array_value` (frame) |
| `AddObject`, `InlineMarshalerType` | `kvlist_value` (frame); inline marshals into the current level |
| `OpenNamespace` | opens a `kvlist_value` frame that stays open for subsequent fields, sealed at entry end (zap namespace semantics) |
| `AddReflected` (`zap.Any` fallback) | JSON-marshal → `string_value` (documented lossy fallback; transactional) |

**Errors** (`ErrorType`) are *not* special-cased: they delegate to zap's standard
`encodeError` dispatch, which may emit the base `<key>` string plus `<key>Verbose`
(`fmt.Formatter` errors) and a `<key>Causes` array (grouped errors) through the
ObjectEncoder surface (`zap/error.go:37-49`, `zapcore/error.go:64-78`) — all arrive as
ordinary attributes via the methods above.

Attribute keys MUST be unique per the spec; like zap's own encoders we do not deduplicate
(zap itself permits duplicates) — documented as caller responsibility.

### 3.4 Trace context (the headline decision)

Two ways in, both **consumed** (never emitted as attributes):

```go
// 1. Lazy — official-bridge convention; any field whose value IS a context.Context,
//    key irrelevant ("context" by convention):
logger.Info("payment ok", zap.Any("context", ctx))
reqLog := logger.With(zap.Any("context", ctx))          // sticky — requires otlp.NewCore (§2.2)

// 2. Eager — explicit helper; extracts trace.SpanContext at the call site:
logger.Info("payment ok", otlp.SpanContext(ctx))
reqLog := logger.With(otlp.SpanContext(ctx))            // sticky — works behind any core
```

Mechanics:

- **Per-call fields** (`EncodeEntry`'s raw `fields` slice) are scanned before dispatch.
  Detection is by `Field.Interface` type, independent of `Field.Type` (so it works whether
  zap classified the value as `StringerType`, `ReflectType`, or anything else — `Stringer`
  and `Reflect` both preserve the original value in `Interface`): a `context.Context` is
  resolved via `trace.SpanContextFromContext` (one ctx-value lookup; zero `SpanContext` on
  miss); a `trace.SpanContext` (the eager helper's payload) is used directly. The field is
  skipped during attribute dispatch. Last match wins (contrib behavior).
- **Sticky `With` fields** are intercepted in the custom core's `With` pre-scan (§2.2) for
  the ctx form, or via the `AddReflected` hook for the eager helper's `ReflectType` field on
  a stock core. Either way the resolved `trace.SpanContext` is stashed on the encoder clone
  **at `With` time, before the clone is shared** — never mutated afterwards (§3.7). A
  stashed `context.Context` is resolved at `With` time too; this is semantically identical
  to the contrib bridge's stored-ctx behavior (a ctx's span association is immutable), and
  the "span started after `With` is not seen" caveat is documented.
- **Precedence:** a per-call trace field overrides the `With`-stash for that entry.
- **Emission:** `sc.IsValid()` gates everything. Valid → `trace_id` = 16 raw bytes,
  `span_id` = 8 raw bytes, `flags` = `uint32(sc.TraceFlags())` (low 8 bits, sampled bit).
  Invalid/absent → all three fields omitted entirely (spec: absent ≠ 16 zero bytes). Silent
  degradation matches every OTel SDK.
- **`otlp.SpanContext(ctx) zap.Field`** extracts immediately and returns a `ReflectType`
  field carrying the `trace.SpanContext` value, under the fixed key **`"span_context"`**
  (pass-2 P2: the key is part of the API contract because non-OTLP cores render the field).
  It always returns a field (even with no active span — the encoder omits invalid
  contexts), so call sites stay unconditional. `trace.SpanContext` implements
  `json.Marshaler`, so tee'd JSON/console cores render it legibly. **Tee caveat
  (documented):** other cores in a `zapcore.NewTee` still receive trace-context fields as
  ordinary fields (a stringified ctx, or the JSON-rendered span context under
  `"span_context"`) — the eager helper is the recommended form for tee setups.
- **Convenience injectors (brainstorming addendum, 2026-06-11 — uptrace-otelzap-inspired,
  but functional instead of a wrapper type):**

  ```go
  // Structural — sugar over SpanContext(ctx); OTLP proto-field correlation:
  func InjectTraceFields(ctx context.Context, fields ...zap.Field) []zap.Field
      // clone-before-append: out := append(append(make([]zap.Field, 0,
      // len(fields)+1), fields...), SpanContext(ctx)) — exactly one allocation,
      // NEVER appends in place. (Pass-4 P1: the `existingSlice...` call form
      // passes the slice unchanged per the Go spec, so a bare append with spare
      // capacity would mutate the caller's backing array.)
  func InjectTraceKVs(ctx context.Context, kvs ...any) []any
      // = [SpanContext(ctx), kvs...] built into a fresh slice; for the sugared
      // logger — zap's SugaredLogger consumes strongly-typed Fields mixed into
      // keysAndValues before any odd-pair/non-string-key checks (sugar.go
      // sweetenFields), so prepending never splits a k/v pair and a lone
      // injected Field (zero user kvs) triggers no dangling-key warning

  // Flat strings — for NON-OTLP sinks (ndjson/fluent/syslog, Loki derived
  // fields, Datadog log parsing); fixed keys "trace_id"/"span_id", lowercase
  // hex (32/16 chars); returns nil when no valid span (no empty-string
  // pollution, apmzap behavior):
  func TraceCorrelationFields(ctx context.Context) []zap.Field
  ```

  Usage: `logger.Info("msg", otlp.InjectTraceFields(ctx, zap.String("k","v"))...)`,
  `sugar.Infow("msg", otlp.InjectTraceKVs(ctx, "url", url)...)`,
  `ndjsonLogger.Info("msg", otlp.TraceCorrelationFields(ctx)...)`.

  The `Inject*` pair appends the eager field **unconditionally** (no-span → encoder omits;
  call sites stay branch-free); inline `otlp.SpanContext(ctx)` remains the zero-extra-alloc
  form and `Inject*` is documented as sugar. **`TraceCorrelationFields` is deliberately a
  separate, explicitly-named helper** (pass-4 P2: named for its purpose — log↔trace
  correlation on string-parsing backends — and plural because it emits both IDs, unlike
  uptrace's singular `WithTraceIDField`): its output lands in OTLP *attributes* (not the
  proto trace fields) if used with the OTLP core — it exists for tee'd/non-OTLP cores, and
  its docs say so. It lives in `otlp/` regardless because extracting from ctx requires
  `otel/trace`. All four helpers share the §-below `spanContextFromField`/nil-guard
  discipline for nil and typed-nil contexts.
- **Scanner edge cases (pinned, pass-3 P2):** a nil `Field.Interface` never matches (type
  assertions on a nil interface fail). A **typed-nil** concrete context is *not* safe to
  hand to `trace.SpanContextFromContext` — otel's `SpanFromContext` guards only
  interface-nil before calling `ctx.Value` (`otel/trace@v1.44.0/context.go:33-44`), so a
  typed-nil receiver would panic. All three intake points (per-call field scan, custom-core
  `With` pre-scan, the `SpanContext(ctx)` eager helper) therefore share one
  `spanContextFromField` helper that rejects interface-nil **and** reflective-nil values
  before extraction; rejected values resolve to the zero `SpanContext` → fields omitted,
  field still consumed. Multiple trace-context fields in one call: last match wins, all are
  consumed.

Dependency note: the ctx key for the active span is unexported in `go.opentelemetry.io/otel/trace`;
there is no dep-free extraction. The module is the **stable v1.x trace API** (not the SDK,
not the v0.x logs SDK).

### 3.5 Entry metadata → attributes

| Entry datum | Attribute (semconv) | Control |
|---|---|---|
| `ent.Caller` | `code.function.name`, `code.file.path`, `code.line.number` | `WithCallerAttributes(bool)`, default **on** (emitted only when `Caller.Defined`) |
| `ent.Stack` | `code.stacktrace` | emitted when non-empty |
| `ent.LoggerName` | `logger` key (no stable semconv exists; key overridable via `WithLoggerNameKey`, empty disables) | emitted when non-empty |

The `InstrumentationScope` (§4) is per-encoder static — per-record logger names ride as
attributes instead, because scope lives in the batch envelope, not the record.

### 3.6 No `zapcore.EncoderConfig` (deliberate deviation)

`NewEncoder` takes only `...Option`. Every slot `EncoderConfig` would configure (time/level/
message/caller *keys* and *formats*) is structurally defined by the OTLP data model — there
is nothing for the config to control. This deviates from the fluent/syslog triplet signature
deliberately; `NewCore` likewise drops the `encCfg` parameter.

### 3.7 Concurrency & buffer lifecycle

Same contract as syslog §3.6 / fluent-native: `Clone` and `EncodeEntry` may run concurrently
and must not mutate the receiver. Receiver state is (a) immutable config, (b) the
`trace.SpanContext` stash — written **only** during `With` (by the custom core's pre-scan or
the `AddReflected` hook), i.e. on a freshly-created clone not yet visible to any other
goroutine (`ioCore.With` clones before `addFields`; our core does the same), and read-only
thereafter, (c) pre-encoded `With`-attribute bytes — appended under the same
fresh-clone-only rule; `EncodeEntry` only **reads** them, copying into per-call working
buffers from the pool. Per-call trace-context resolution lives in locals, never the receiver.
`EncodeEntry` returns a `buffer.Buffer` from zap's pool; the Writer copies before returning
(§5.1), so the buffer is freed safely by `ioCore`/our core.

## 4. Envelope assembly

Per batch, the flush loop wraps N record payloads as:

```
ExportLogsServiceRequest          (collector/logs/v1; wire-identical to LogsData)
  resource_logs (1):  ResourceLogs
    resource (1):     Resource        ← precomputed bytes (construction time)
    scope_logs (2):   ScopeLogs
      scope (1):      InstrumentationScope  ← precomputed bytes
      log_records (2): N × LogRecord  ← the per-entry payloads, tag+len each
```

`Resource` and `InstrumentationScope` are immutable per-writer: their proto bytes are built
once at construction from options and reused for every batch. Assembly is two passes of size
arithmetic (all child lengths known) + appends into a pooled buffer — no re-encoding, no
backpatching, no per-batch allocation beyond the output buffer.

Resource contents: `service.name` (default `unknown_service:<basename of os.Args[0]>`, the
SDK convention) + `WithResource(fields ...zap.Field)` for arbitrary extra attributes, encoded
through the same proto `ObjectEncoder` as §3.3. We deliberately do **not** claim
`telemetry.sdk.name = opentelemetry` (reserved for official SDKs); scope defaults to
`name = "github.com/arloliu/zapwire/otlp"`, `version` = module version via
`debug.ReadBuildInfo` (empty if unavailable).

## 5. The HTTP exporter (`otlp.Writer`)

A `zapcore.WriteSyncer`. Async-only: per-entry HTTP POSTs are pathological, so there is no
sync mode (deviation from the core Writer, documented); `Sync()` provides the flush barrier.

### 5.1 Ingest path (hot, never blocks)

`Write(p []byte)` copies `p` into a queue-owned slice (zap frees the encoder buffer after
`Write` returns — same ownership rule as the core async path) and does a non-blocking enqueue
into a bounded channel. Full queue → drop per policy (`DropNewest` default / `DropOldest`),
increment the drop counter, return `(len(p), nil)` to honor zap's multi-writer contract.

Two `Write`-time guards:
- **Oversized single record** (pass-1 P1): if `len(p)` + worst-case envelope overhead exceeds
  `WithMaxRequestBytes`, the record is dropped immediately (counted + error handler) — it
  could never be exported.
- **Post-close writes** are silently discarded **without** counting, matching the core
  Writer's contract exactly (`writer.go:138-140`, `docs/guide.md` "post-close writes are
  silently dropped") — pass-1 P2 resolution; the earlier "counted drops" text was wrong.

### 5.2 Flush loop & batching

A **single flush goroutine** owns dequeue, batching, envelope assembly, and export (the same
shape as the core writer's `flushLoop`, `writer.go:431-462`: one `select` over lifecycle
`done`, `Sync` requests, the ticker, and the queue).

A batch closes when the **first** of these is reached:
- `WithBatchSize` records (default **512**),
- adding the next record would push the assembled request past `WithMaxRequestBytes`
  (default **4 MiB**, uncompressed body; envelope overhead accounted) — byte-aware batching,
  pass-1 P1 resolution,
- `WithFlushInterval` elapses (default **1 s**).

Queue capacity is `WithQueueSize` (default **2048**). Count/interval defaults mirror the OTel
SDK BatchLogRecordProcessor so behavior is unsurprising to OTel operators. Export: assemble
envelope (§4) → optional gzip (`WithCompression(otlp.Gzip)`, default **off**) →
`POST {endpoint}/v1/logs`, `Content-Type: application/x-protobuf`, `WithHeaders` applied,
per-attempt timeout `WithTimeout` (default **10 s**, the spec's exporter default).

**Head-of-line consequence (stated plainly):** exports run inline in the flush goroutine, so
a retrying batch delays subsequent batches. The bounded queue absorbs the backlog; sustained
backend failure surfaces as counted drops at the queue boundary — never as application
backpressure. This is the deliberate at-most-once trade.

### 5.3 Response handling (OTLP spec semantics)

| Response | Action |
|---|---|
| 200, no `partial_success` (absent or empty message) | success |
| 200 + `partial_success.rejected_log_records > 0` | count as drops, notify `WithErrorHandler`, **no retry** (retrying duplicates the accepted part) |
| 200 + `rejected_log_records == 0` + non-empty `error_message` | **warning**: notify `WithErrorHandler` (typed as warning), no drop accounting, no retry — pass-1 P1 resolution |
| 429 / 502 / 503 / 504 | retry: exponential backoff + jitter (initial 5 s, max 30 s, max elapsed 60 s — OTel retry defaults), honoring `Retry-After` (seconds or HTTP-date); a `Retry-After` beyond the remaining retry budget → give up immediately (drop + notify) |
| 400 | **never retried** — drop batch, notify handler |
| 413 | non-retryable — drop batch, notify handler; prevention is the byte-aware batching above (no split-and-retry: keeps the at-most-once story simple) |
| other 4xx/5xx, transport errors | non-retryable — drop + notify (only the four spec codes retry; predictable at-most-once) |
| retry budget exhausted | drop batch, count, notify |

Retry sleeps `select` on the writer's lifecycle context, so `Close` interrupts them (§5.4).
Parsing the partial-success body needs a ~40 LOC proto decoder for two fields
(`rejected_log_records = 1` varint, `error_message = 2` string) inside `partial_success = 1`
— the only decoding in the package.

### 5.4 Lifecycle (pass-1 P1 resolution — explicit state machine)

Mirrors the core writer's discipline (`Close` barrier + `flushDone` wait, `writer.go:175-188`;
`Sync` ack, `writer.go:411-428`), with HTTP-specific bounds:

- **Pending-batch arithmetic (pass-2 P1 resolution).** Records awaiting resolution at any
  moment form `pending_batches` = the flush goroutine's current partial batch + the queued
  records as they will be cut by **both** the count limit (`WithBatchSize`) and the byte
  limit (`WithMaxRequestBytes`). With pathological record sizes, byte cutting can make
  `pending_batches` as large as the number of queued records (one record per request) — the
  bounds below are therefore stated per batch, not per queue-capacity division.
- **`Sync()`** sends a flush request to the flush goroutine and waits until every record
  enqueued *before the call* has been resolved (exported, or dropped after its retry budget).
  **Bound (documented):** worst case = `pending_batches × (retry budget + attempt timeout)`
  under the inline-export model (each batch may consume its own retry budget serially) —
  with defaults and count-only cutting, ≤ 5 × (60 s + 10 s). `Sync` is a barrier with a
  configuration-dependent worst case, **not** a hard deadline; callers needing one should
  wrap it. `Sync` after `Close` returns nil (no-op).
- **`Close()`** is idempotent: (1) stop intake (subsequent `Write`s discarded, uncounted);
  (2) cancel the lifecycle context — an in-progress backoff sleep aborts immediately and the
  retrying batch is counted as dropped + notified (its current HTTP attempt, if one is
  actually in flight, is allowed to finish within its per-attempt timeout to avoid
  cancelling a request the server may have already accepted); (3) drain with **one attempt
  per remaining batch, no retries**, each bounded by `WithTimeout` — failures are counted
  drops; (4) release resources. **Bound (documented):** ≤ one in-flight attempt timeout +
  `drain_batches × attempt timeout`, where `drain_batches` is the §-above pending-batch
  count after byte-aware cutting (defaults with count-only cutting: ≤ 10 s + 5×10 s).
- Drop accounting nuance (documented): a batch cancelled mid-retry counts as dropped even if
  a previous attempt was actually accepted server-side after a lost response — drop counts
  are "not confirmed delivered", consistent with at-most-once.

### 5.5 Endpoint resolution

`NewWriter(endpoint string, ...)` — if the URL has no path (or `/`), `/v1/logs` is appended;
otherwise it is used verbatim. The `otlp.EndpointFromEnv()` helper resolves
`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` (used as-is) then `OTEL_EXPORTER_OTLP_ENDPOINT` (base —
path appended) per the exporter spec, returning `""` if neither is set; env handling stays
explicit and opt-in (zapwire never reads env behind the caller's back). Empty endpoint →
`ErrNoEndpoint` from `NewWriter`.

## 6. Options & defaults

One `...Option` list feeds encoder and writer ends (each option sets only the fields it
owns; the unused ones are documented no-ops on the other constructor — the syslog
convention).

| Option | Default | End |
|---|---|---|
| `WithServiceName(string)` | `unknown_service:<exe>` | envelope |
| `WithResource(...zap.Field)` | — | envelope |
| `WithScopeName(string)` / `WithScopeVersion(string)` | `github.com/arloliu/zapwire/otlp` / build info | envelope |
| `WithSeverityMapper(func(zapcore.Level) SeverityNumber)` | §3.2 table | encoder |
| `WithCallerAttributes(bool)` | `true` | encoder |
| `WithLoggerNameKey(string)` | `"logger"` (empty disables) | encoder |
| `WithQueueSize(int)` | 2048 | writer |
| `WithBatchSize(int)` | 512 | writer |
| `WithMaxRequestBytes(int)` | 4 MiB (uncompressed) | writer |
| `WithFlushInterval(time.Duration)` | 1 s | writer |
| `WithDropPolicy(DropPolicy)` | `DropNewest` | writer |
| `WithTimeout(time.Duration)` | 10 s | writer |
| `WithRetry(RetryConfig)` | initial 5 s / max 30 s / elapsed 60 s | writer |
| `WithHeaders(map[string]string)` | — | writer |
| `WithHTTPClient(*http.Client)` | `http.DefaultClient`-equivalent with sane transport | writer |
| `WithCompression(Compression)` | `None` | writer |
| `WithErrorHandler(func(error))` | no-op | writer |

Non-positive numeric options clamp to defaults (core `normalizeConfig` discipline).

## 7. Public API

```go
func NewEncoder(opts ...Option) zapcore.Encoder
func NewWriter(endpoint string, opts ...Option) (*Writer, error)
func NewCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error)

func SpanContext(ctx context.Context) zap.Field   // eager trace-context capture
func InjectTraceFields(ctx context.Context, fields ...zap.Field) []zap.Field // sugar: append(fields, SpanContext(ctx))
func InjectTraceKVs(ctx context.Context, kvs ...any) []any                   // sugar for SugaredLogger kvs
func TraceCorrelationFields(ctx context.Context) []zap.Field                 // flat hex trace_id/span_id for non-OTLP sinks
func EndpointFromEnv() string                     // OTEL_EXPORTER_OTLP_* resolution

func (*Writer) Sync() error
func (*Writer) Close() error
func (*Writer) DroppedLogs() uint64
```

`NewCore` returns the **custom core** (§2.2) wired to the encoder and writer — the
established triplet signature, the caller owns and must `Close` the Writer. BYO-core users
composing `NewEncoder` with stock `zapcore.NewCore` get the documented §2.2 compatibility
matrix.

## 8. Error handling

- Field-level failures (failing `ObjectMarshaler`/`ArrayMarshaler`/reflect fallback) are
  **degradable, never entry-fatal**: transactional container methods + zap's `<key>Error`
  convention (§3.3). `EncodeEntry` errors are reserved for fatal assembly failures (none in
  practice).
- Ship-path errors never reach the application goroutine: retry per §5.3, then drop + count +
  `WithErrorHandler` callback (callback receives typed errors: `*ExportError{StatusCode,
  Retryable, Rejected, Warning}` etc. for observability).
- No active span → trace fields omitted, silently (spec-standard).
- `NewWriter`/`NewCore`: `ErrNoEndpoint`, invalid URL errors.

## 9. Testing

- **Conformance (the load-bearing suite):** golden round-trip against the official
  `go.opentelemetry.io/proto/otlp` stubs — our bytes must `proto.Unmarshal` cleanly and
  re-`proto.Marshal` byte-identically, across: every `ObjectEncoder` method in the §3.3
  table (incl. `Binary` vs `ByteString` divergence, uint64 > MaxInt64, complex, namespaces,
  inline marshalers), nested namespaces/arrays/objects, >127-byte lengths (multi-byte
  varints), trace context present/absent, multi-record batches, resource/scope contents,
  and exact tags/wire types for `flags` (fixed32, field 8) / `trace_id` (bytes, field 9) /
  `span_id` (bytes, field 10). Lives in a **separate test-only module**
  (`otlp/internal/conformance` with its own go.mod) so the stubs (+grpc graph) never touch
  the `otlp/` go.mod — the established quarantine pattern from the fluent-bit integration
  test.
- **Field-degradation matrix (pass-1 P0, pass-2 P0):** `zap.Any("bad", make(chan int))`, a
  failing `ObjectMarshaler`, a failing nested `ArrayMarshaler`, and a failing `zap.Inline`
  marshaler that first writes an attribute **and opens a namespace** before erroring —
  entry still ships, `<key>Error` (or bare `Error` for inline) attribute present, and the
  decoded record is **byte-exact** with the same entry logged without the failing field
  (modulo the error attribute) — proving snapshot/rollback leaves no partial bytes.
- **Error-field expansion:** plain error, `fmt.Formatter` error (`<key>Verbose`), grouped
  errors (`<key>Causes` array).
- **Trace-context matrix:** per-call ctx field / per-call eager / sticky eager via `With`
  (both core flavors) / sticky `zap.Any` ctx via `otlp.NewCore` (works) **and** via stock
  `zapcore.NewCore` (documented degradation: stringified attribute, no trace fields) /
  per-call precedence over stash / two trace fields in one call (last wins, both consumed) /
  nil and typed-nil `Field.Interface` / no span / non-recording span / tee to a JSON core
  rendering the eager helper under `"span_context"`.
- **Injector helpers:** `InjectTraceFields` through `Logger.Info` and `InjectTraceKVs`
  through `SugaredLogger.Infow` (typed Field among kvs reaches the encoder and is consumed
  into proto trace fields); no-span variants stay branch-free (field appended, encoder
  omits); `TraceCorrelationFields` with an active span (exact `trace_id`/`span_id` hex
  fields, e.g. decoded on an ndjson oracle), without a span (returns nil, no fields
  emitted), and on the OTLP core (lands as attributes, **not** proto trace fields — pinning
  the documented caveat). Pass-4 edge rows: `InjectTraceFields(ctx, existingSlice...)`
  where the slice has spare capacity (caller's backing array must remain untouched —
  pinning clone-before-append); `InjectTraceKVs(ctx)` with zero user kvs (no sugared
  dangling-key warning); `InjectTraceKVs(ctx, zap.String("typed","field"), "k", "v")`
  (typed fields mixed with pairs); nil and typed-nil ctx through all four helpers
  (`SpanContext`, both `Inject*`, `TraceCorrelationFields`).
- **Exporter (`httptest`):** batching by count, by bytes (`WithMaxRequestBytes` split), and
  by interval; oversized single record dropped at `Write`; gzip round-trip;
  partial-success matrix (absent / empty message / rejected > 0 / rejected == 0 with
  warning message); 429 + `Retry-After` honored; `Retry-After` beyond budget → immediate
  drop; 400 and 413 not retried; retry-budget exhaustion → drops; queue-full drop policies;
  `Sync` while a batch is in retry backoff (returns within the documented bound); `Sync`
  with **multiple pending batches** under a tiny `WithMaxRequestBytes` forcing one record
  per request against a retrying backend (documented multi-batch bound + accounting);
  `Close` during a `Retry-After` sleep (aborts, counts the batch dropped exactly once);
  `Close` with a partial in-memory batch plus a full queue under byte-aware one-record
  batches (documented drain bound); `Close` while an HTTP request is in flight; sustained
  backend failure with concurrent writes; post-close writes (discarded, **uncounted**).
- **Concurrency:** `-race` through one shared core + `With`/`Namespace` clones (§3.7), and
  concurrent `Write`/`Sync`/`Close` on the exporter.
- **Opt-in integration:** real `otel-collector` binary behind a build tag + `make` target
  (mirroring the fluent-bit pattern): file-exporter output asserted for body, attributes,
  and intact trace/span IDs end-to-end.
- **Benchmarks:** `EncodeEntry` (with/without trace ctx, field-type spread) and end-to-end
  async, reporting `ns/op` + `allocs/op`, with budgets per the v1 design §8 discipline.

## 10. Scope

**In scope:** OTLP/HTTP binary protobuf to `/v1/logs`; hand-rolled proto encoder; custom
core for ctx-based trace correlation + eager helper; resource/scope configuration; gzip;
OTLP retry semantics; partial-success accounting; byte-aware batching; conformance suite;
opt-in collector integration test.

**Out of scope (future):**
- OTLP/gRPC (grpc-go dependency; no receiver requires it — all listen on HTTP 4318).
- OTLP/JSON (spec-MAY for servers; cheap to add behind the same encoder seam later).
- Traces/metrics signals.
- At-least-once delivery / WAL (core philosophy: at-most-once, counted drops).
- 413 split-and-retry (byte-aware batching is the prevention; splitting complicates the
  at-most-once story).
- **Encoder-side** string-field lifting (`zap.String("trace_id", …)` → proto fields) —
  rejected in brainstorming as magic-name fragility; revisit only on demand. (Caller-side
  *emission* of flat string fields for non-OTLP sinks is in scope —
  `TraceCorrelationFields`, §3.4 — the rejection applies only to the encoder silently
  reinterpreting attribute strings.)
- A wrapper logger type with `Ctx()`/`InfoContext()` methods (the uptrace-otelzap shape) —
  the functional injectors cover the ergonomics without a second logger type. **Boundary
  (decided):** zapwire provides the foundation (encoder, core, injector helpers); the
  application layer makes its own call on wrapper methods such as `InfoCtx(ctx, msg, …)` /
  `InfowCtx(ctx, msg, …)` — trivially buildable on `InjectTraceFields`/`InjectTraceKVs`.

## 11. Module & dependencies

`otlp/` is its own module **`github.com/arloliu/zapwire/otlp`** (v1 design §11 as planned —
though the original heavy-dep rationale largely evaporated with the hand-rolled encoder, the
own-module boundary still keeps the root graph free of *any* OTel dependency):

- Direct deps: `go.uber.org/zap`, `go.opentelemetry.io/otel/trace` (stable v1.x; pulls
  indirect `go.opentelemetry.io/otel` into go.mod only).
- **No** root-module import (§2.1), no grpc, no protobuf runtime, no OTel SDK (the logs SDK
  is still v0.x with breaking churn — explicitly avoided).
- No committed `go.work`: per the Go team's guidance, workspace files are a local dev
  convenience and stay untracked (gitignored). Each module builds standalone — the
  conformance module reaches `otlp/` via a `replace` directive. Developers who want
  cross-module editing run `go work init . ./otlp ./otlp/internal/conformance` locally.
  CI/Makefile loop over the modules directly (`go test ./...` per module, lint per module).
- Release tagging: `otlp/vX.Y.Z` tags per Go multi-module convention.

## 12. Codex plan-review pass-1 resolutions

| Finding | Resolution |
|---|---|
| **P0** `EncodeEntry` error semantics would drop degradable field failures (zap converts `Add*` errors to `<key>Error` and continues; `EncodeEntry` errors kill the entry) | §3.3 rewritten: all container/reflect methods transactional (discard frame, no partial bytes, return error → zap emits `<key>Error`); `EncodeEntry` fatal-only. §8 updated; degradation test matrix added (§9). |
| **P0** Sticky `zap.Any("context", ctx)` cannot work through stock `ioCore.With` — `zap.Any` classifies contexts as `StringerType` (contexts implement `String()`), so `AddTo` stringifies before any encoder hook | §2.2 added: `otlp.NewCore` returns a thin custom core whose `With` pre-scans raw fields (the contrib bridge's own approach). Eager helper constructed as `ReflectType` → works behind any core. Compatibility matrix documented; per-call scan unaffected (detection by `Field.Interface` type, independent of `Field.Type`). Test rows for both core flavors added (§9). |
| **P1** Retry/`Sync`/`Close` bounds contradictory; no named goroutine/cancellation model | §5.2 names the single flush goroutine + head-of-line consequence; §5.3 retry sleeps select on lifecycle ctx; §5.4 specifies the full state machine with explicit worst-case bounds for `Sync` (retry budget + attempt) and `Close` (in-flight attempt + single-attempt drain), `Retry-After` capped by remaining budget, and the drop-accounting nuance. Lifecycle tests added (§9). |
| **P1** Partial-success warnings (`rejected == 0` + message) unhandled | §5.3 three-case handling: absent/empty = success; rejected > 0 = drops + notify, no retry; rejected == 0 + message = warning notify only. Test matrix added (§9). |
| **P1** No request-size policy (aggregate 413, oversized single record) | §5.2 byte-aware batching via `WithMaxRequestBytes` (default 4 MiB); §5.1 oversized-record drop at `Write`; §5.3 413 = non-retryable drop (no split-and-retry, listed out-of-scope §10). Tests added (§9). |
| **P1** Field coverage omitted `BinaryType` vs `ByteStringType` and error expansion (`Verbose`/`Causes`) | §3.3 now enumerates the complete `ObjectEncoder` surface: `AddBinary` → `bytes_value`, `AddByteString` → `string_value` (deliberate, zap-semantic divergence documented), errors delegated to zap's `encodeError` (base + `Verbose` + `Causes`). Conformance rows added (§9). |
| **P2** Empirical PoC / binary-size claims not statically verifiable | Header note qualifies both as prior empirical observations; the conformance suite is the binding gate. |
| **P2** Post-close drop accounting contradicted the core Writer (`writer.go:138-140` returns success without counting) | §5.1/§5.4: post-close writes silently discarded **uncounted**, matching the core Writer and `docs/guide.md`. Test added (§9). |

## 13. Codex plan-review pass-2 resolutions

| Finding | Resolution |
|---|---|
| **P0** Inline marshaler failures cannot be transactional via child-frame discard — `zap.Inline` writes into the *current* level (`zapcore/field.go:122-124`), and any marshaler may open namespaces before failing | §3.3: transactionality re-specified as **snapshot/rollback** — before any user marshaler runs, snapshot `{stack depth, current frame buffer length, element count}`; on error, discard frames above the depth, truncate, restore count. Covers inline, nested-namespace, and container failures uniformly. §9 degradation test strengthened to byte-exact equality vs the entry without the failing field. |
| **P1** `Sync`/`Close` bounds undercounted multi-batch and byte-aware drains (a drain can be one batch *per record* under a tiny byte limit) | §5.4: bounds restated in pending-batch arithmetic — `Sync` ≤ `pending_batches × (retry budget + attempt timeout)`; `Close` ≤ in-flight attempt + `drain_batches × attempt timeout`, with `pending_batches`/`drain_batches` defined after **both** count and byte cutting (worst case = number of queued records). `Sync` explicitly documented as a configuration-dependent barrier, not a hard deadline. Multi-batch lifecycle tests added (§9). |
| **P2** Eager-helper field key undefined; scanner edge cases (multiple trace fields, nil/typed-nil `Field.Interface`, tee rendering) untested | §3.4: key fixed as `"span_context"` (API contract); scanner edge cases pinned (nil-guard before `trace.SpanContextFromContext`, last-match-wins with all consumed). Test rows added (§9). |
