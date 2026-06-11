# Syslog RFC5424 subpackage — design (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 3) · **Date:** 2026-06-10 · **Branch:** `feat/zapwire-syslog`

**Review history:** codex plan-review pass 1 (`tmp/…_pass1_review.md`) raised 2×P0 / 1×P1 + 3
test additions — resolved (§11). Pass 2 (`tmp/…_pass2_review.md`) confirmed those and caught
1 residual P0 (the `LineEnding=""` fix was ineffective; corrected to `SkipLineEnding=true`) —
resolved (§12). Pass 3 (`tmp/…_pass3_review.md`) found **no P0/P1** and returned
**implementation-ready**; its lone P2 (JSON-parity test oracle must use the same
`SkipLineEnding=true` config) is folded into §8.

**Relates to:** the v1 design `docs/design/2026-06-07-zapwire-design.md` §3 (the
`Encoder`/`Framer` seam), §10 (syslog is v2 scope, same module), §11 (processor roadmap —
syslog is #2 by reach, stdlib-only); and the v2 native encoder design
`docs/design/2026-06-08-native-msgpack-encoder-design.md`, whose **native path** structure
(custom `zapcore.Encoder` → `Passthrough` → `Framer`) this subpackage mirrors.

**Decisions locked in brainstorming (2026-06-10):** JSON message body now with a seam left
for RFC5424 STRUCTURED-DATA later; octet-counting framing by default with an LF option;
conservative zap-level → syslog-severity mapping (override via `WithSeverityMapper`); header
defaults facility `local0`, RFC3339 UTC timestamp, `os.Hostname()` / process-name / pid /
`-` for the identity fields; UTF-8 BOM off by default; architecture A (custom `zapcore.Encoder`
embedding an inner JSON encoder).

---

## 1. Goal

Ship zap logs as well-formed **RFC5424** syslog messages to rsyslog, syslog-ng, Vector, and
Logstash over the existing reconnecting `zapwire.Writer` (UDS or TCP). The subpackage is
**stdlib-only**, stays in the main module, and reuses the entire core — reconnect,
sync/async dispatch, bounded never-blocking writes, drop accounting — through the existing
`Encoder`/`Framer` seam, exactly as `fluent` and `ndjson` do.

A single message looks like:

```
<134>1 2026-06-10T22:14:15.003456Z host.example.com myapp 4123 - - {"level":"info","msg":"started","version":"1.2.3"}
└PRI┘V └──────── TIMESTAMP ────────┘ └─ HOSTNAME ─┘ APP  PROCID │ │ └──────────────── MSG (JSON body) ───────────────┘
                                                              MSGID SD="-"
```

`PRI = facility*8 + severity` (here `16*8 + 6 = 134`: `local0` + Informational).

## 2. Architecture

The RFC5424 **header** needs per-entry data that lives only on the `zapcore.Entry`:
`severity` (from `Entry.Level`) and `TIMESTAMP` (from `Entry.Time`). The core `Encoder` and
`Framer` see only bytes, never the Entry — so the header cannot be built there. It is built
in a custom `zapcore.Encoder`, the same conclusion the `fluent` native path reached.

The data path (identical in shape to `fluent.NewNativeCore`):

```
zap logger
  → syslog zapcore.Encoder.EncodeEntry   builds one full SYSLOG-MSG (header + JSON body)
  → zapwire.Writer.Write(syslogMsg)
      → zapwire.Passthrough().Encode      copies the final bytes into a pooled scratch
      → syslog.Framer.Frame               octet-counting (default) or LF
      → transport (UDS/TCP, reconnecting)
```

Three building blocks — two new in `syslog/`, one reused from the core:

- **`Encoder`** (new) — a `zapcore.Encoder` that produces the `SYSLOG-MSG` (§3).
- **`Framer`** (new) — wraps each `SYSLOG-MSG` for the stream (§4).
- **`Passthrough`** — reused verbatim from the core (`zapwire.Passthrough()`); the encoder's
  output is already the final wire bytes, so the core `Encoder` is a no-op copy.

## 3. The encoder

### 3.1 Structure

`NewEncoder` returns a `zapcore.Encoder` that **embeds an inner JSON encoder**. It first
copies the caller's `encCfg` (a value copy, leaving the caller's struct untouched) and sets
`encCfg.SkipLineEnding = true` on the copy, then builds `zapcore.NewJSONEncoder(copy)`.
Suppressing the line ending is load-bearing: zap's JSON encoder otherwise appends the
configured `LineEnding` to every entry. Note that setting `LineEnding = ""` does **not**
work — zap normalizes an empty line ending back to the default `"\n"`
(`zapcore/json_encoder.go`: `if cfg.SkipLineEnding { cfg.LineEnding = "" } else if
cfg.LineEnding == "" { cfg.LineEnding = DefaultLineEnding }`); only the `SkipLineEnding` flag
actually emits no terminator (resolves pass-1 P0 / pass-2 residual P0). With it set, the inner
body bytes are the bare JSON object with **no** trailing terminator regardless of the caller's
`LineEnding`/`SkipLineEnding`, so the MSG body is exactly valid JSON and no trailing-byte
trimming is needed. Embedding the interface
means every `ObjectEncoder` method (`AddString`, `AddInt64`, `OpenNamespace`, `AddArray`,
`AddObject`, …) is inherited and produces byte-for-byte the same field encoding as a plain
zap JSON encoder. Only two methods are overridden:

```go
type encoder struct {
    zapcore.Encoder        // embedded inner JSON encoder — all field handling inherited
    cfg  header            // sanitized, immutable header config (§3.3)
    pool buffer.Pool       // zap buffer pool for the assembled line
}

func (e *encoder) Clone() zapcore.Encoder {
    return &encoder{Encoder: e.Encoder.Clone(), cfg: e.cfg, pool: e.pool}
}

func (e *encoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
    body, err := e.Encoder.EncodeEntry(ent, fields) // bare JSON object (inner SkipLineEnding=true)
    if err != nil {
        return nil, err
    }
    defer body.Free()

    line := e.pool.Get()
    sev := clampSeverity(e.cfg.severityOf(ent.Level)) // 0..7 (§3.4)
    pri := int(e.cfg.facility)*8 + int(sev)           // facility already 0..23 (§3.4)

    line.AppendByte('<')
    line.AppendInt(int64(pri))
    line.AppendString(">1 ")            // PRI + VERSION(1) + SP
    appendRFC3339Micros(line, ent.Time) // UTC, ≤6-digit fractional (§3.2)
    line.AppendByte(' ')
    line.AppendString(e.cfg.hostname)
    line.AppendByte(' ')
    line.AppendString(e.cfg.appName)
    line.AppendByte(' ')
    line.AppendString(e.cfg.procID)
    line.AppendByte(' ')
    line.AppendString(e.cfg.msgID)
    line.AppendString(" - ")            // STRUCTURED-DATA = "-" (JSON-body mode) + SP
    if e.cfg.bom {
        line.AppendString("\uFEFF")     // optional UTF-8 BOM (§5)
    }
    _, _ = line.Write(body.Bytes())     // MSG = bare JSON body (no trailing terminator)

    return line, nil
}
```

`EncodeEntry` returns a buffer from `e.pool`; zap's `ioCore` frees it after the
`WriteSyncer.Write` returns, which is the idiomatic contract (`consoleEncoder` does the
same). The inner JSON body buffer is freed via `defer`.

### 3.2 Timestamp

`Entry.Time` is rendered in UTC as RFC3339 with **up to 6 fractional digits** — RFC5424
caps `TIME-SECFRAC` at 6 (`1*6DIGIT`), so `time.RFC3339Nano` (9 digits) is non-compliant.
Format: `2006-01-02T15:04:05.000000Z`. In practice the core always supplies a real
`Entry.Time`; as a guard, a zero `Entry.Time` emits the NILVALUE `-` for the timestamp
(RFC5424 permits it) rather than a bogus year-1 date.

### 3.3 Header sanitization (once, at construction)

RFC5424 constrains the header fields to printable US-ASCII (`PRINTUSASCII`, codes 33–126)
with length caps. All identity fields are validated and normalized **once** in `NewEncoder`,
so `EncodeEntry` only appends pre-cleaned strings:

| Field     | Cap (chars) | Empty/invalid → |
|-----------|-------------|-----------------|
| HOSTNAME  | 255         | `-`             |
| APP-NAME  | 48          | `-`             |
| PROCID    | 128         | `-`             |
| MSGID     | 32          | `-`             |

Normalization: any byte outside 33–126 is dropped (not replaced), the result is truncated
to the cap, and an empty result becomes the NILVALUE `-`. This guarantees every emitted
header is grammatically valid regardless of caller input or hostname contents.

### 3.4 Severity mapping

`severityOf func(zapcore.Level) Severity` is configurable (`WithSeverityMapper`). The
default is **conservative** — it never auto-emits Emergency/Alert (0–1), which carry
"system unusable / page now" semantics:

| zap level                  | syslog severity      |
|----------------------------|----------------------|
| Debug                      | 7 Debug              |
| Info                       | 6 Informational      |
| Warn                       | 4 Warning            |
| Error                      | 3 Error              |
| DPanic / Panic / Fatal     | 2 Critical           |

**PRI range normalization (resolves pass-1 P0).** `PRI = facility*8 + severity` is only valid
for `facility ∈ 0..23` and `severity ∈ 0..7` (so `PRI ∈ 0..191`). Named types and exported
constants do not prevent a caller constructing an out-of-range typed value
(`WithFacility(Facility(99))`) or a custom `WithSeverityMapper` returning `Severity(9)`, so
both are bounded structurally:

- **Facility** is validated **once** in `NewEncoder`: if outside `0..23` it falls back to the
  default `LOCAL0` (16). (Considered rejecting via an error return, but `NewEncoder` returns a
  bare `zapcore.Encoder` like `fluent.NewMsgpackEncoder`; clamping to the default keeps that
  signature and is consistent with how header identity fields degrade to `-`.)
- **Severity** is clamped **per entry** by `clampSeverity`: a mapper result outside `0..7` is
  coerced into range (values `>7 → 7` Debug, `<0 → 0` Emergency) so the default and every
  custom mapper always yield a syntactically valid PRI.

With both bounded, the §7 claim "header assembly cannot fail" holds: PRI is arithmetic over
pre-validated operands.

### 3.5 Body-format seam (future STRUCTURED-DATA)

The STRUCTURED-DATA section (`" - "`) and the MSG section are produced behind a small,
unexported `bodyFormat` indirection. Only `jsonBody` is implemented (SD = `-`, MSG = the
inner JSON line). A future `structuredData` mode would populate the SD-ELEMENT(s) and move
the human message to MSG, changing only that one seam — the header assembly and framer are
untouched. **Not built now** (YAGNI); the seam exists so adding it is additive.

### 3.6 Concurrency (corrected per pass-1 P1)

zap's actual encoder contract (`zapcore/encoder.go`): `Clone` and `EncodeEntry` **may be
called concurrently** and must **not modify the receiver**. `ioCore.With` clones for
accumulated context, but `ioCore.Write` calls `EncodeEntry` on the core's *shared* encoder —
so `EncodeEntry` is not guaranteed to run on a private clone. zap's own JSON encoder is safe
because its `EncodeEntry` immediately makes a private working clone before adding the entry's
fields, leaving the receiver untouched.

The syslog wrapper inherits that safety **as long as it never mutates receiver-owned state in
`EncodeEntry`**, which the §3.1 sketch satisfies:

- The only mutable field-encoding state lives inside the **embedded** JSON encoder, and the
  wrapper reaches it only through `e.Encoder.EncodeEntry(...)` — which clones internally, so
  the shared receiver is not mutated.
- `cfg` is immutable after construction (read-only).
- Each call allocates a **fresh** output buffer from `pool` (a `sync.Pool`-backed
  `buffer.Pool`, safe for concurrent `Get`); nothing is reused across calls.
- `Clone()` deep-clones the inner encoder and copies `cfg`/`pool`, so `With`-derived encoders
  share no field state.

This differs from `fluent`'s native msgpack encoder, which had to deep-copy its *own*
persistent `stack`/`scratch` into per-call working state precisely because it does not
delegate to an inner encoder (`fluent/msgpack_encoder.go`; native design §3.1). The syslog
wrapper has no such receiver-owned scratch, so delegation + immutability + per-call buffers
are sufficient. A `-race` test logging concurrently through one shared core **and** through
`logger.With(...)` / `zap.Namespace(...)` clones guards this.

## 4. The framer

```go
type Framer struct{ octetCounting bool }

func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
    for _, p := range payloads {
        if f.octetCounting {
            dst = strconv.AppendInt(dst, int64(len(p)), 10) // RFC 6587 §3.4.1
            dst = append(dst, ' ')
            dst = append(dst, p...)
        } else {
            dst = append(dst, p...) // RFC 6587 §3.4.2 (non-transparent)
            dst = append(dst, '\n')
        }
    }
    return dst, nil
}
```

- **Octet-counting (default):** `MSG-LEN SP SYSLOG-MSG`, where `MSG-LEN` is the byte length
  of the `SYSLOG-MSG`. Unambiguous and robust to any payload byte — the expected mode for
  hardened TCP syslog pipelines.
- **LF-terminated (option):** `SYSLOG-MSG \n`, like `ndjson`'s framer. Relies on the message
  containing no literal `LF`; a single-line JSON body satisfies that (zap escapes `\n`), so
  it is safe in practice but structurally weaker. Useful for ad-hoc/legacy receivers.

Batched async delivery passes N payloads in one `Frame` call → N framed messages
concatenated. The framer never errors (matches `ndjson`/`fluent`).

## 5. Options & defaults

All options follow the `fluent` convention (`func(*options)` closures; `WithZapwireOptions`
forwards core options). Defaults are chosen so `NewCore(t, level, encCfg)` with no options
emits valid, useful RFC5424 immediately.

| Option | Default | Notes |
|---|---|---|
| `WithFacility(Facility)` | `LOCAL0` (16) | Typed `Facility` constants `KERN`…`LOCAL7`, `USER`, etc. Out-of-range (`>23`) → `LOCAL0` at construction (§3.4). |
| `WithSeverityMapper(func(zapcore.Level) Severity)` | conservative table (§3.4) | Full override of the level→severity function; result clamped to `0..7` per entry (§3.4). |
| `WithHostname(string)` | `os.Hostname()` | Read once; empty → `-`. |
| `WithAppName(string)` | `filepath.Base(os.Args[0])` | Empty → `-`. |
| `WithProcID(string)` | `strconv.Itoa(os.Getpid())` | Empty → `-`. |
| `WithMsgID(string)` | `-` | |
| `WithFraming(Framing)` | `OctetCounting` | or `LFTerminated`. |
| `WithBOM(bool)` | `false` | A leading UTF-8 BOM trips naive JSON consumers; `true` = strict RFC5424 MSG-UTF8. |
| `WithZapwireOptions(...zapwire.Option)` | — | Forwards core options (async, buffer, timeouts, drop policy, reconnect…). |

`Facility` and `Severity` are small named integer types with exported constants and a
documented `PRI` relationship.

## 6. Public API

```go
// NewEncoder builds the RFC5424 zapcore.Encoder (header + JSON body).
func NewEncoder(encCfg zapcore.EncoderConfig, opts ...Option) zapcore.Encoder

// NewWriter builds a zapwire.Writer for the syslog path: Passthrough + the
// syslog Framer over t. Pair with NewEncoder to build a core yourself (e.g.
// inside zapcore.NewTee).
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error)

// NewCore wires NewEncoder + NewWriter into a ready core plus its writer, which
// the caller must Close.
func NewCore(
    t zapwire.Transport,
    level zapcore.LevelEnabler,
    encCfg zapcore.EncoderConfig,
    opts ...Option,
) (zapcore.Core, *zapwire.Writer, error)
```

This is the same triplet as `fluent`'s native path (`NewMsgpackEncoder` / `NewNativeWriter`
/ `NewNativeCore`). Framing options apply to the framer built inside `NewWriter`/`NewCore`;
header/severity options apply to the encoder built inside `NewEncoder`/`NewCore`. A single
`opts ...Option` list feeds both (each option sets only the fields it owns), so the caller
configures the whole subpackage in one place.

Because `NewWriter` builds only `Passthrough` + the `Framer` (not the encoder), header and
severity options passed to `NewWriter` are **no-ops** — the caller building a BYO-core path
supplies those to `NewEncoder` instead. This mirrors `fluent.NewNativeWriter`, where
`WithTimeCodec`/`WithTimeKey` are likewise documented no-ops. Only `WithFraming` and
`WithZapwireOptions` affect `NewWriter`.

## 7. Error handling

- `EncodeEntry` returns the inner JSON encoder's error verbatim if body encoding fails
  (rare). Header assembly cannot fail (fields are pre-sanitized; PRI is arithmetic).
- `Frame` never returns an error.
- Delivery: inherited from the core `Writer` — never blocks; a stalled/absent consumer
  causes counted drops (`DroppedLogs`), never a blocked goroutine.
- `NewWriter`/`NewCore` surface `zapwire.New` errors (e.g. `ErrNoTransport`).
- The async path copies the encoded bytes into a queue-owned slice before zap frees the
  entry buffer, so there is no use-after-free across the async boundary.

## 8. Testing

- **Header/golden:** exact RFC5424 line per zap level (PRI/severity), facility arithmetic,
  timestamp format (UTC, ≤6 fractional digits), NILVALUE handling.
- **PRI range (pass-1 P0):** boundary facilities (`KERN`=0, `LOCAL7`=23) and severities
  (Emergency=0, Debug=7) emit correct PRIs; an out-of-range `WithFacility(Facility(99))`
  falls back to `LOCAL0`; a custom mapper returning `Severity(9)`/`Severity(-1)` is clamped to
  `7`/`0`.
- **Line-ending independence (pass-1/pass-2 P0):** across caller `encCfg` with `LineEnding`
  `"\n"`, `"\r\n"`, `""`, a custom suffix (`"END\n"`), and `SkipLineEnding` both true and
  false, the emitted MSG is byte-for-byte valid JSON with **no** trailing terminator (no stray
  `\n`/`\r`/suffix), confirming the encoder's forced `SkipLineEnding=true` overrides the
  caller's config; the octet-count length equals `len(SYSLOG-MSG)` over those final bytes.
- **JSON-body parity:** after stripping the header (and optional BOM), MSG matches the body
  of an oracle `zapcore.NewJSONEncoder` built from the **same `SkipLineEnding=true` copied
  config** (not the raw caller `encCfg`, which would append `DefaultLineEnding` and never
  match), byte-for-byte, including `With` fields and `zap.Namespace` nesting.
- **Sanitization:** over-length and illegal-byte inputs for each identity field → truncated
  / stripped / `-`.
- **Severity mapper:** default table and a custom override (within range).
- **Framer:** octet-count byte-length correctness (incl. BOM-on and multi-byte UTF-8 field
  values — `MSG-LEN` is byte length, not rune count), LF termination, multi-payload batch.
- **`NewWriter` option applicability:** `WithFraming`/`WithZapwireOptions` take effect on
  `NewWriter`; header/severity/BOM options are no-ops there (§6).
- **RFC5424 validity oracle:** parse emitted lines back with a strict grammar matcher and
  assert MSG is valid JSON with the expected fields.
- **Concurrency:** `-race` test logging concurrently through one shared core **and** through
  `logger.With(...)` / `zap.Namespace(...)` clones (§3.6).
- **In-module end-to-end:** a loopback UDS listener (like `ndjson_test.go`) — strip the
  frame, assert the RFC5424 header + decoded JSON fields. No external daemon.
- **Benchmarks:** `EncodeEntry` plus end-to-end sync/async, reporting `ns/op` and
  `allocs/op`, using `ndjson` as the JSON-body baseline (syslog adds only the header).

## 9. Scope

**In scope:** RFC5424 over UDS/TCP streams; JSON message body; octet-counting (default) and
LF framing; configurable header (facility, hostname, app-name, procid, msgid) with the
conservative severity default; full reuse of the core writer.

**Out of scope (future):**
- UDP / unixgram datagram transport (v1 design §10 deferred — different framing semantics:
  one datagram per message, no octet-counting).
- RFC5424 STRUCTURED-DATA message body (the §3.5 seam is left clean for it).
- RFC3164 (legacy BSD) format.
- A real-daemon opt-in integration test against rsyslog/syslog-ng (same `//go:build`-gated
  pattern as the fluent-bit integration test) — tracked as a separate follow-up task, not
  built in this plan.

## 10. Module & dependencies

Stays in the **main module**; **stdlib-only**. Imports only `go.uber.org/zap/zapcore`,
`go.uber.org/zap/buffer`, and `github.com/arloliu/zapwire` — the same dependency footprint
as `ndjson`. Adds nothing to the root module graph and keeps the root `msgp`-free guarantee
intact (verified the same way as `ndjson`: `go list -deps ./syslog | grep -c tinylib/msgp`
→ 0).

## 11. Codex plan-review pass-1 resolutions

| Finding | Resolution |
|---|---|
| **P0** PRI range not bounded — caller can construct out-of-range `Facility`/`Severity` → invalid PRI | §3.4 adds explicit normalization: facility validated to `0..23` once in `NewEncoder` (else `LOCAL0`); severity clamped to `0..7` per entry via `clampSeverity` (`>7→7`, `<0→0`). §5 option notes and the §7 "cannot fail" claim updated; boundary + invalid-input tests added (§8). |
| **P0** MSG assembly ignored zap's configurable `LineEnding` (trimmed only `\n`; `\r\n`/custom suffix corrupted MSG) | §3.1 now copies `encCfg` and sets `SkipLineEnding = true` before building the inner JSON encoder, so the body is the bare JSON object and is appended with **no** trim (`line.Write(body.Bytes())`). (See §12 — the pass-1 `LineEnding=""` form was wrong and was corrected in pass 2.) Line-ending-independence test added across `"\n"`/`"\r\n"`/`""`/custom (§8). |
| **P1** Concurrency rationale wrong — claimed `EncodeEntry` runs on a clone; zap may call it on the shared receiver | §3.6 rewritten to zap's real contract (`Clone`/`EncodeEntry` may run concurrently and must not mutate the receiver; `ioCore.Write` uses the shared encoder; safety comes from the inner JSON encoder's internal per-call clone). Race test now covers a shared core **and** `With`/`Namespace` clones. |
| Tests: JSON-body parity, `NewWriter` option applicability, RFC6587 byte-length with non-ASCII/BOM | All three folded into §8. |

Buffer-lifecycle and Writer/Passthrough claims were confirmed correct by the reviewer
(`passthrough.go`, `writer.go` sync/async paths) — no change required.

## 12. Codex plan-review pass-2 resolutions

| Finding | Resolution |
|---|---|
| **P0 (residual)** The pass-1 line-ending fix (`encCfg.LineEnding = ""`) does **not** suppress the terminator: zap normalizes an empty `LineEnding` back to `DefaultLineEnding` (`"\n"`), so the MSG would still carry a trailing newline and the octet count would be wrong. | §3.1 corrected to set `encCfg.SkipLineEnding = true` (the flag zap added for exactly this; `zapcore/json_encoder.go:82-85` shows `SkipLineEnding` ⇒ `LineEnding=""` while a bare `LineEnding==""` ⇒ `DefaultLineEnding`). Verified against `go.uber.org/zap v1.28.0` (the module's pinned version). Resolutions table (§11) and the §8 line-ending test updated to the `SkipLineEnding` form. |

Pass-2 also confirmed RESOLVED: the P0 PRI-range normalization (§3.4) and the P1 concurrency
rewrite (§3.6). No other P0/P1 raised.
