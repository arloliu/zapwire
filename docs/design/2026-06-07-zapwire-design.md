# zapwire — Design

- **Date:** 2026-06-07
- **Module:** `github.com/arloliu/zapwire` (Go 1.25), root package `zapwire`
- **Status:** Approved (pending written-spec review)
- **Note:** the repository directory is currently `zap-writer/`; the module/package is
  `zapwire`. Directory rename is optional and cosmetic (Go does not require them to match).

## 1. Purpose

A high-performance [zap](https://github.com/uber-go/zap) logging writer that ships
structured logs to log processors (Fluentd, Fluent-bit, Vector, …) over local or
network sockets. It must:

- never block the application goroutine (drop-on-stall, like the reference example),
- minimize allocations and CPU on the hot path,
- keep third-party dependencies minimal and **quarantined per processor**,
- be extensible to new processors and transports without touching the core.

The mature reference implementation in `tmp/fluent/` (a self-healing UDS → Fluent
Forward writer) is the proven baseline for transport, reconnect, and drop semantics.

## 2. Processor & transport scope

Two wire formats reach all five major processors:

| Wire format | Processors reached natively | Transport | Ships in |
|---|---|---|---|
| **Fluent Forward (msgpack)** | Fluentd (`in_forward`/`in_unix`), Fluent-bit (`forward`), Vector (`fluent` source) | UDS, TCP | **v1** |
| **NDJSON** (newline-delimited JSON) | Vector (`socket`), Logstash (`tcp` + `json_lines`), OTel Collector (`tcp_log`), generic | UDS, TCP | **v1** |
| Syslog RFC5424 | rsyslog, syslog-ng, Vector, Logstash | UDS/TCP/UDP | v2 (see §11) |
| OTLP logs (protobuf) | OTel Collector, most vendors (Grafana, Datadog, …) | gRPC, HTTP | v2, own module (see §11) |
| UDP / unixgram | datagram receivers | UDP | deferred |

**Transports for v1: UDS + TCP.** Both are stdlib `net.Conn` byte streams and share a
single reconnect engine. UDP/unixgram is deferred because it shares neither framing nor
reconnect semantics. OTLP and syslog are v2 (§11).

### Forward-protocol carrier mode

The framer **always emits PackedForward** (`[tag, <N × [time, record]>, option]`); a
synchronous single write is the degenerate `size = 1` case. This is safe because both
Fluentd `out_forward` and Fluent-bit default to PackedForward, and Vector's `fluent`
source ingests from those agents. **Residual risk:** Vector's docs don't *explicitly*
enumerate accepted carrier modes. Mitigation: the framer can fall back to single
`Message` mode for `size = 1` if a Vector incompatibility is ever observed — this is a
one-line branch, not a structural change.

### Why not Vector's native protocol / Avro?

Vector's `native`/`native_json` codecs and the gRPC `vector` source target
**Vector-to-Vector** topologies. For a log *producer*, **Fluent Forward (msgpack)** is
the sweet spot: compact binary, no schema registry, and it serves three processors with
one encoder. `native` (protobuf) only helps Vector and couples us to Vector's versioned
proto schema; `native_json` stringifies timestamps (a downgrade); `avro` needs a schema.
Decision: **Forward is the Vector path.** Revisit `native` only for a Vector-exclusive,
bandwidth-bound user (rare) — and if so, as its own module (§11).

## 3. Architecture

zap's `Core → WriteSyncer` boundary is **bytes-only**: `ioCore.Write` calls
`enc.EncodeEntry(ent, fields)` then `out.Write(buf.Bytes())`. The WriteSyncer never sees
the entry, fields, timestamp, or tag. Therefore the two performance paths live at
*different layers*:

- **JSON transcode** (the proven `tmp/fluent` path, **v1**) → lives in the WriteSyncer:
  parse zap's JSON line, build the `[time, record]` payload.
- **Native zero-round-trip** (**v2**, deferred) → a custom `zapcore.Encoder` (the only
  layer with structured field access). Emits the msgpack `[time, record]` payload
  directly, skipping the JSON parse/re-encode.

Both produce the **same per-entry payload shape**, so the framing layer is identical for
both. v1 ships the transcode path with the seam ready; v2 slots the native encoder in
without touching the framer or core.

### Package layout

```
zapwire/  (repo root = package `zapwire`)   stdlib-only core — no third-party deps
├── Writer                  io.Writer + zapcore.WriteSyncer (Write, Sync, Close)
├── connection manager      UDS+TCP dial, background reconnect + backoff
├── buffer/dispatch         sync & async modes, sync.Pool buffers, ring buffer
├── Encoder interface       produces a per-entry payload []byte
├── Framer interface        wraps [][]byte payloads → one wire frame
├── metrics                 atomic DroppedLogs / ReconnectCount counters
│
├── fluent/                 OWNS github.com/tinylib/msgp  (msgpack — quarantined)
│   ├── Framer              PackedForward [tag, [N×[time,record]], option]
│   ├── Encoder (transcode) JSON bytes → msgpack [time, record] payload   (v1)
│   ├── Encoder (native)    zapcore.Encoder → msgpack [time, record]      (v2)
│   └── NewWriter / NewCore preset helpers returning WriteSyncer / zapcore.Core
├── ndjson/                 dedicated, dependency-free (shared by Vector/Logstash/OTel/generic)
│   ├── Framer              newline-delimited frames
│   ├── Encoder             passthrough (zap JSON) / native line encoder
│   └── NewWriter / NewCore preset helpers
│
├── syslog/   (v2)          stdlib only — RFC5424 framing; same module
├── otlp/     (v2)          OWN go.mod (grpc + protobuf) — promoted module (§11)
│
└── docs/, Makefile, CLAUDE.md, AGENTS.md, .agents/, .golangci-lint.go.mod, .golangci.yaml
```

**Dependency isolation:** the only v1 third-party runtime dependency is `tinylib/msgp`,
quarantined in `fluent/`. An NDJSON-only consumer compiles zero third-party packages.

**`ndjson` is dedicated** (not folded into root) because NDJSON is a *shared* format
across multiple processors, matching the "if shared, keep it dedicated" rule.

### Core interfaces (sketch — finalized during planning)

```go
// Encoder produces the per-entry wire payload (NOT the full frame).
//   Fluent:  msgpack 2-array [EventTime ext, record map]
//   NDJSON:  the JSON line (no trailing newline; the Framer adds it)
type Encoder interface {
    // Transcode path (v1): input is zap's encoded bytes (JSON). dst is a pooled buffer.
    Encode(dst []byte, record []byte) ([]byte, error)
}
// Native path (v2) is a zapcore.Encoder implemented in the fluent subpackage; it emits
// the same payload shape so the Framer is path-agnostic.

// Framer wraps one or more per-entry payloads into a single wire frame.
// len(payloads) == 1 in sync mode; N in async/batched mode.
type Framer interface {
    Frame(dst []byte, payloads [][]byte) ([]byte, error)
}

// Transport is a reconnectable byte-stream (UDS or TCP).
type Transport interface {
    Dial(ctx context.Context) (net.Conn, error)
    String() string
}
```

## 4. Delivery model

Configurable per writer (both modes):

- **Sync mode** — `Write` encodes, then does a deadline-bounded `conn.Write`; on stall
  the log is dropped and counted. Never blocks the app. (Today's example behaviour.)
  Framer flushes at `size = 1`.
- **Async mode** — `Write` enqueues the payload into a `sync.Pool`-backed ring buffer; a
  background goroutine batches payloads and flushes them as one PackedForward frame on a
  size or time trigger. Framer flushes at `size = N`.

Sync vs async collapses to **"flush at 1" vs "flush at N-or-timer"** over one framing
path.

### Async semantics (deliberate, not accidental)

- **`Sync()`** flushes the buffer synchronously (honors zap's flush contract).
- **Full-buffer policy** is configurable: `drop-new` (default) or `drop-oldest`. Both
  preserve never-block. Drops increment `DroppedLogs`.
- **Crash semantics:** buffered-but-unflushed logs are lost on a hard crash. This is an
  **at-most-once** shipper, not a write-ahead log. Stated explicitly so callers choose
  the mode knowingly.

## 5. Performance principles

- **Buffer pooling is first-class**, not a later optimization. The reference example's
  `MarshalMsg(nil)` allocates per write; the core pools encode and write buffers via
  `sync.Pool` and passes a `dst []byte` into `Encode`/`Frame`.
- **The native encoder (v2) avoids the JSON round-trip** entirely on the hot path. v1's
  transcode path is the proven baseline; benchmarks compare the two once v2 lands.
- `alloc/op` and `ns/op` are tracked benchmark metrics (see Testing).

## 6. Error handling & resilience

Carried over from the proven example, generalized to UDS+TCP:

- background reconnect goroutine with exponential backoff (≈100ms → 3s), bounded retry
  bursts re-armed on demand;
- per-write deadline; on any write error the connection is discarded and rebuilt;
- stale-connection guard (only the goroutine that removes the current conn closes it);
- `Close()` is idempotent and cancels in-flight dials;
- `Write` returns a non-nil error **only** on encode failure; drops return
  `(len(p), nil)` to satisfy zap's multi-writer contract;
- atomic `DroppedLogs()` / `ReconnectCount()` / `IsConnected()` introspection.

## 7. Public API surface

Per your choice — **WriteSyncer + helpers**:

- Core: `zapwire.New(transport, encoder, framer, ...Option) (*Writer, error)` where
  `*Writer` is a `zapcore.WriteSyncer` (`Write`, `Sync`, `Close`).
- Functional options: transport selection (UDS path / TCP addr), sync|async mode, buffer
  size, full-buffer policy, write timeout, reconnect tunables, tag (Fluent).
- Per-processor presets that wire encoder+framer for you:
  - `fluent.NewWriter(...) (*zapwire.Writer, error)` and `fluent.NewCore(...)`,
  - `ndjson.NewWriter(...)` and `ndjson.NewCore(...)`.

## 8. Testing strategy

- Table-driven unit tests per Encoder and Framer (golden wire bytes checked against the
  Forward spec).
- Mock UDS/TCP servers (the example's `mock_fluentd.go` is the starting point) for
  end-to-end encode→frame→ship assertions.
- Reconnect / drop / concurrency tests run under `-race`.
- Benchmarks for the hot path with `alloc/op` budgets.
- Optional Fluent-bit integration test (the example has one) gated behind a build tag /
  env flag.

## 9. Repository setup (tasks 2 & 3)

Modeled on `~/projects/parti` — the reference for a professional Go package's agent
config and tooling.

### Agent configuration

- **`CLAUDE.md`** — tiny: `@AGENTS.md` import + a skill-invocation note (mirrors parti).
- **`AGENTS.md`** — authoritative entrypoint for all coding agents: project identity,
  pointer to `.agents/rules/`, the always-on contract, and the pre-commit/validation gate.
- **`.agents/rules/`** — numbered rule files with a trigger-map `AGENTS.md` index,
  adapted from parti and scoped to this package:
  - `000-agent-contract.md` (always-on: don't guess, keep changes small, surface
    conflicts, test intent, fail loud)
  - `100-project-map.md` (identity, the package layout from §3, **dependency policy: no
    third-party deps in root; `tinylib/msgp` only in `fluent/`; hybrid module policy §11**)
  - `200-go-style.md`, `300-testing.md`, `400-docs.md`,
    `500-validation-and-workflow.md`, `550-git-conventions.md`,
    `600-go-after-write.md`, `700-performance-security.md` (hot-path allocation
    discipline is central here)
  - (design/review-loop rules `800`/`850` and `.agents/skills/` optional — add if we
    want the review-skill workflow.)

### Tooling

- **Isolated linter** — `.golangci-lint.go.mod` as a separate tool module (`tool`
  directive, version-pinned golangci-lint v2), invoked via
  `go tool -modfile=.golangci-lint.go.mod golangci-lint run`. `.golangci.yaml` adapted
  from the parti config.
- **`Makefile`** — `lint`, `test` (`-race`), `bench`, `coverage`, `gomod-tidy`,
  `linter-update`, `linter-version`, `clean-linter-cache` targets mirroring parti.

### Conventions (carried from parti, enforced)

- Conventional Commits + branch prefixes (`feat/`, `fix/`, …); **never** add
  `Co-Authored-By`/attribution trailers (also per global CLAUDE.md).
- `any` over `interface{}`; error wrap `%w` + `errors.Is/As`; sentinels `Err*`, error
  types `*Error`; standard file-layout order.
- After editing Go: `go fix` on touched packages, then `make lint` until clean.
- Design specs live under `docs/design/`, plans under `docs/plans/<name>/`.

## 10. Scope summary

**v1 (this effort):** core (sync+async, UDS+TCP, pooled buffers, reconnect); Encoder/
Framer seam; Fluent subpackage with the **transcode** encoder + PackedForward framer +
presets; dedicated NDJSON subpackage + presets; WriteSyncer + helper API; full repo/agent
setup (CLAUDE.md, AGENTS.md, `.agents/rules/`, isolated linter, Makefile); unit/race/bench
tests.

**v2 (next):** native msgpack `zapcore.Encoder` (zero round-trip fast path); **syslog
RFC5424** (same module); **OTLP logs** (own module, §11).

**Deferred:** UDP/unixgram transport; CompressedPackedForward (gzip); Vector native
protocol; Loki push API.

## 11. Future roadmap & module policy

### Processor roadmap (recommended priority)

1. **OTLP logs** (protobuf over gRPC + HTTP) — vendor-neutral standard; reaches the OTel
   Collector and nearly every modern backend directly. Highest marginal reach. **Heavy
   deps** → own module.
2. **Syslog RFC5424** (UDS/TCP/UDP) — ubiquitous legacy; reuses the stream transport;
   stdlib only → stays in the main module.
3. *(conditional)* **Loki push API** — only if Grafana Loki is a direct target; otherwise
   OTLP→Loki covers it.

Skipped: Vector native / Avro / Cap'n Proto (no benefit for a log producer, see §2);
broker transports Kafka/Redis/NATS (different product scope).

### Module policy — hybrid (decided)

- **Single module for v1/v2** (`fluent` + `ndjson` + `syslog`). Modern Go already gives
  most isolation for free: per-package compilation means unimported subpackages aren't
  built, and module-graph pruning (Go ≥1.17) keeps unrelated transitive deps out of a
  consumer's graph. The only residual for a light dep like `tinylib/msgp` is its `require`
  line in the root `go.mod` — not worth the multi-module tax.
- **Promote a subpackage to its own `go.mod` only when it pulls a heavy dependency tree.**
  Concretely: **`otlp`** (grpc + protobuf) and any future **`vectornative`** become
  `github.com/arloliu/zapwire/otlp` etc. with their own `go.mod`, so non-OTLP users never
  see grpc/protobuf in their module graph.
- **Design clean boundaries now** so promotion is a `go.mod`-add + tag, not a refactor:
  subpackages depend only on the small exported core interfaces (`Encoder`, `Framer`,
  `Transport`, `Writer`, `Option`); no cross-seam shared mutable state; no subpackage
  imports another subpackage. When the first module is promoted, add a `go.work` for local
  development.

## 12. Configurable time handling — `fluent.TimeCodec` (v1 refinement)

### Problem

The fluent transcode path has an implicit contract with two independently-configured
halves that can silently drift:

- **Encode** (zap → JSON): the time *key* (`EncoderConfig.TimeKey`) and *value format*
  (`EncoderConfig.EncodeTime`).
- **Decode** (JSON → `EventTime`): how `extractTime` reads that key and parses that value.

The original v1 hard-coded both ends (`"ts"` + `EpochNanos`). That is inflexible (callers
can't choose their key or format) and the hard-coding was a *patch* for a drift bug
(seconds-vs-nanos → 1970). Any flexibility we add must make drift **structurally
impossible**, not merely configurable.

### Design: a bundled codec

`fluent.TimeCodec` bundles the key, the zap encoder, and the decoder as one unit, so the
two ends are defined together and cannot diverge:

```go
type TimeCodec struct {
    Key        string                                 // JSON field, e.g. "ts"
    ZapEncoder zapcore.TimeEncoder                    // encode end (how zap writes it)
    Decode     func(value any) (t time.Time, ok bool) // decode end (how we read it back)
}
func (c TimeCodec) ApplyTo(cfg *zapcore.EncoderConfig) // sets cfg.TimeKey + cfg.EncodeTime
```

- **Built-in codecs** (each maps to the matching `zapcore.*TimeEncoder`):
  `AutoEpochCodec` (default — magnitude-tolerant numeric epoch decoder), the exact
  `EpochNanosCodec` / `EpochMillisCodec` / `EpochSecondsCodec` (float seconds, zap's default
  `EpochTimeEncoder`), and the string `RFC3339NanoCodec` / `RFC3339Codec` / `ISO8601Codec`.
  Each is a constructor taking the key: `EpochNanosCodec("ts")`.
- **Custom** codec: construct a `TimeCodec{}` with your own `ZapEncoder` + `Decode`.
- **Options:** `WithTimeCodec(c)` sets the whole codec; `WithTimeKey(k)` overrides just the
  key on the active codec (order-independent — resolved at build time).
- **`NewCore` auto-wires both ends** from the codec (`codec.ApplyTo(&encCfg)` + the encoder
  uses `codec.Decode`/`codec.Key`) → the caller picks one codec and both sides are
  guaranteed to agree.
- **BYO-core (`NewWriter`) path:** the caller passes `WithTimeCodec(c)` for the decode side
  and calls `c.ApplyTo(&theirEncoderConfig)` to align the encode side with one call.
- **Default** = `AutoEpochCodec("ts")` — magnitude-tolerant numeric epoch decoder
  (auto-detects s/ms/µs/ns) so a bring-your-own-core caller using zap's default float-seconds
  encoder decodes correctly instead of to ~1970 (this resolves post-impl Finding 3). Explicit
  codecs parse their one format exactly.

### Constraints & caveats

- **Scope:** fluent-only. `ndjson` passes zap's JSON through untouched (the collector parses
  the timestamp), so it needs no codec. The v2 native `zapcore.Encoder` sidesteps the
  round-trip entirely.
- **Precision:** JSON numbers decode to `float64`, so epoch-*nanos* lose ~tens-of-ns
  precision; epoch-millis/seconds and the string codecs are exact. Documented per codec. The
  *record* fields are decoded with `json.UseNumber` and then normalized (int64/uint64 when
  integral and exactly representable, else float64) so integer fields above 2^53 keep their
  type and full precision on the msgpack wire; the time value is converted back to float64
  before `codec.Decode`, so the codec precision note above is unchanged.
- **Fallback:** an absent/unparseable time field falls back to `time.Now()` (unchanged).
