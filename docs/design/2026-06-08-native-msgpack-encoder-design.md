# Native msgpack zap encoder — design (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 3) · **Date:** 2026-06-08 · **Branch:** `feat/zapwire-v2`

**Relates to:** the v1 design `docs/design/2026-06-07-zapwire-design.md` §3 (two encode
paths), §10–§11 (v2 roadmap). This document specifies the v2 **native msgpack encoder**:
the first item on that roadmap.

**Review history:** codex plan-review pass 1 (`tmp/…_pass1_review.md`) raised 3×P0 / 3×P1 /
1×P2 (resolved — §9); pass 2 (`tmp/…_pass2_review.md`) raised 2×P0 / 2×P1 / 1×P2 (resolved —
§10); pass 3 (`tmp/…_pass3_review.md`) found **no P0/P1** and returned
**implementation-ready** — its 3×P2 wording-precision items are folded in.

---

## 1. Goal & motivation

v1 ships logs to Fluent processors via a **transcode** path: zap marshals each entry to a
JSON line, then `fluent.Encoder` parses that JSON and re-marshals it as a msgpack
`[time, record]` Forward entry. That is two serializations and one parse per log on the hot
path.

The **native encoder** is a custom `zapcore.Encoder` that emits the msgpack
`[time, record]` payload **directly** from zap's structured fields — one serialization, no
JSON, no parse.

Beyond raw speed, operating at the `zapcore.Encoder` layer dissolves two problems the
transcode path had to engineer around:

- **Exact timestamps for free.** `zapcore.Entry.Time` is a native `time.Time`. The native
  encoder writes it straight into the Forward `EventTime` extension. The entire `TimeCodec`
  round-trip — magnitude detection, key configuration, float64 precision caveats — is
  **bypassed** (v1 design §12, line 341 anticipated this).
- **Exact integer types for free.** Field types are known at the `AddInt64` / `AddUint64` /
  `AddFloat64` call site, so there is no JSON-number ambiguity and no `>2^53` precision loss.
  The transcode path needed `json.Number` + `normalizeNumbers` to recover this; native never
  loses it.

### Non-goals

- **Not** a replacement for transcode. Transcode stays the **default** and the proven
  baseline. Native is **additive, opt-in**.
- **Not** an NDJSON "native" encoder — NDJSON is already native (zap emits JSON, the writer
  ships JSON). No round-trip exists there to eliminate.
- **Not** in scope: CompressedPackedForward/gzip, syslog, otlp (separate roadmap items).

### Success criteria

1. Output is **wire-equivalent** to the transcode path for the same logical record, for
   **unique keys** and the standard scalar/container types, **modulo the documented
   divergences in §3.8**.
2. Benchmarks show **fewer allocs/op and lower ns/op** than transcode, sync and async.
3. The full `zapcore.Encoder` surface is supported — including `With`, namespaces, nested
   objects/arrays, and `zap.Any` of arbitrary structs — with **no entry ever dropped** due to
   a single un-encodable field (it degrades to a `<key>Error` field, exactly as zap does).
4. Zero new third-party runtime dependencies (`tinylib/msgp`, `go.uber.org/zap` already
   present in `fluent/`). Dependency isolation (root = 0 third-party) is preserved.

---

## 2. Where it sits (architecture)

zap's `Core → WriteSyncer` boundary is **bytes-only**: `ioCore.Write` calls
`enc.EncodeEntry(ent, fields)`, then `out.Write(buf.Bytes())`. The WriteSyncer never sees the
entry, fields, timestamp, or tag. The structured fields exist **only** at the
`zapcore.Encoder` layer — which is therefore the only place a native encoder can live.

The native encoder emits the **complete per-entry payload** `[EventTime, {record}]`. The
zapwire `Writer` is unchanged: it runs a **passthrough** `zapwire.Encoder` (a no-op that
copies the already-final bytes) followed by the **existing** `fluent.Framer` (PackedForward).
Reconnect, sync/async dispatch, drop accounting, buffer pooling — all reused verbatim.

```
                           ┌─ zapcore layer (structured) ─┐   ┌──── WriteSyncer layer (bytes) ────┐
logger ─▶ zapcore.Core ─▶  msgpackEncoder.EncodeEntry  ─▶  Writer.Write(p) ─▶ Passthrough.Encode ─▶ fluent.Framer ─▶ socket
                           emits [time, record] msgpack       (copies p)         (PackedForward; unchanged)
```

### Buffer lifetime / copy semantics (already satisfied by the current Writer)

`EncodeEntry` returns a `*buffer.Buffer` from zap's pool; **`ioCore` frees it after `Write`
returns** (`zapcore/core.go`). We therefore **must not** free the returned buffer ourselves —
only our internal scratch/frame buffers (§3.1).

- **Sync mode:** `Writer.writeSync` (`writer.go:129-147`) calls `enc.Encode(scratch, p)` and
  `framer.Frame(...)` then writes — all **before** `Write` returns, while `p` is still valid.
  `Passthrough` appends `p` into the pooled scratch buffer.
- **Async mode:** `Writer.writeAsync` (`writer.go:332-353`) calls `enc.Encode(nil, p)`; the
  returned slice is **queued** and outlives the call. `Passthrough`'s `append(nil, p...)`
  makes the required owning copy, so the freed zap buffer can't corrupt the queued payload.

`Passthrough` = `func (passthrough) Encode(dst, p []byte) ([]byte, error) { return
append(dst, p...), nil }` — correct for both modes with no special-casing.

---

## 3. The encoder (`fluent/msgpack_encoder.go`)

A single type implementing the **entire** `zapcore.Encoder` interface (which embeds
`zapcore.ObjectEncoder`) **and** `zapcore.ArrayEncoder` (which embeds
`zapcore.PrimitiveArrayEncoder`). Structurally it mirrors zap's own `jsonEncoder` —
constructor, `Clone`, `EncodeEntry`, the per-type `Add*`/`Append*` methods, namespace
handling — swapping JSON tokens for `msgp.Append*` calls plus **element counting**.

> A compile-time assertion `var _ zapcore.Encoder = (*msgpackEncoder)(nil)` guarantees the
> full method set is implemented; the implementation cross-checks against the interface in the
> module cache (`go.uber.org/zap/zapcore/encoder.go`) so no method is missed.

> **The count machinery is net-new with no `jsonEncoder` analog.** JSON delimits with
> commas/braces and needs no element counts; msgpack maps and arrays carry an explicit
> element count in their header. Everything subtle in this design (§3.2–§3.5) exists because
> of that single difference. "Mirror jsonEncoder" applies to *structure, method set, and the
> envelope/nil-fallback matrix* (§3.4) — **not** to the counting.

### 3.1 State, the frame stack, and the two clones

The encoder keeps its in-progress structure as a **stack of frames**, each frame being one
open msgpack container:

```
type frameKind uint8
const ( kindMap frameKind = iota; kindArray )

type frame struct {
    kind  frameKind   // kindMap | kindArray
    buf   []byte      // this container's CONTENTS only (no header); pooled
    count int         // map: number of key/value PAIRS; array: number of ELEMENTS
}

type msgpackEncoder struct {
    cfg     zapcore.EncoderConfig
    stack   []frame     // stack[0] = the record map (root); top = current open container
    scratch []byte      // per-instance scratch for transactional reflected encoding (§3.7)
    // frame-buffer pool + encoder pool
}
```

`stack[0]` (the **root** frame) is always a `kindMap`: it is the record map's contents. Its
header (`map N`) is written in `EncodeEntry` once the final count is known.

There is **no `err` field** and **no error accumulation** (see §3.9). Field-level errors are
returned from the relevant `Add*`/`Append*` method and zap converts them to a `<key>Error`
field; the entry still ships.

**Concurrency rule (zap requires `Clone` and `EncodeEntry` to be safe to call concurrently and
to not mutate the receiver — `zapcore/encoder.go:447-465`).** `stack` and `scratch` are
**per-instance mutable state, never shared**:
- `Clone()` returns a **new instance** with its own deep-copied `stack` and its own `scratch`
  (`nil`, grown lazily) — it does **not** alias the receiver's `scratch`.
- `EncodeEntry` operates **entirely on a per-call working clone** (a fresh instance from the
  encoder pool, with its own `stack` and `scratch`). It **never mutates** the receiver's
  `stack`/`scratch`; it **reads** the receiver's `stack` only to deep-copy the immutable
  persistent `With` frames into the working clone (§3.4 step 2), and it **never** touches the
  receiver's `scratch`.

So no mutable buffer is shared across concurrent `Clone`/`EncodeEntry` calls (zap permits
immutable reads of the receiver; it forbids only concurrent receiver *mutation*) — closing the
pass-2 shared-`scratch` data race.

Two **distinct** clone operations; keeping them separate is essential to correctness and
thread-safety:

1. **`Clone() zapcore.Encoder` (the interface method).** Called by `logger.With(fields…)`.
   Returns a new encoder that **deep-copies** the entire frame stack (each frame's `buf`
   bytes and `count`). `With` fields are then added to the clone via the normal `Add*`
   methods, accumulating into the clone's frames. The original is untouched. A namespace
   opened at the **top level** inside `With` (and any fields under it) is left **open** on the
   clone's stack and survives to `EncodeEntry` (§3.5).

2. **internal working clone, at the *start* of `EncodeEntry`.** Multiple goroutines may call
   `EncodeEntry` on the *same* core concurrently. As zap's `jsonEncoder` does
   (`final := enc.clone()`), our `EncodeEntry` first makes a private working copy of the
   stack from pooled buffers, writes the envelope + call-site fields into **that**, seals it,
   frees its buffers, and returns. **`EncodeEntry` never mutates the receiver's persistent
   state.**

### 3.2 Headers written at seal time — `sealDownTo`, no backpatching

msgpack containers need their element count in the header, **before** the contents. zap
delivers fields incrementally, so the count is unknown when a container opens. Strategy
**(chosen): per-level buffers, sealed on close.** Each open container writes into its own
pooled `buf`; the count lives in the frame; when the container closes we know the count, so we
emit the header then the contents — **no placeholder, no backpatch, no offset arithmetic.**

A single primitive drives all closing:

```
// sealDownTo collapses frames until len(stack) == depth, sealing each popped frame
// into the frame below it: parent.buf = appendMapHeader|appendArrayHeader(parent.buf,
// popped.count); parent.buf = append(parent.buf, popped.buf...); recycle popped.buf.
func (e *msgpackEncoder) sealDownTo(depth int)
```

- **`AddObject`/`AddArray` and object/array array-elements** seal **immediately and
  scope-bounded**: record `d := len(stack)`; (for `Add*`) write the key; push the
  object/array frame; run the marshaler (which may open namespaces, nested objects, arrays —
  all at depth `> d`); then `sealDownTo(d+1)` (collapse everything the marshaler opened —
  **including namespaces opened *inside* the object** — back into the object/array frame);
  then `sealDownTo(d)` (seal the object/array frame itself into its parent). This makes
  namespaces opened inside an `ObjectMarshaler` **scoped to that object**, matching
  `zapcore/json_encoder.go:218-229`.
- **Only top-level namespaces stay open** until `EncodeEntry` finalization — i.e. namespaces
  opened directly on the record (via `With` or at the call site), not inside an object.
  `EncodeEntry` finalizes with `sealDownTo(1)` (collapse all remaining open namespaces into
  the root), then assembles the payload.

**Header helpers (P2 fix).** `frame.count` is `int`, but `msgp.AppendMapHeader`/
`AppendArrayHeader` take `uint32`. Wrap them:

```
// appendMapHeader/appendArrayHeader cast count to uint32. count is a per-entry element
// count and cannot realistically exceed math.MaxUint32 (a single log with >4e9 fields is
// impossible); the cast is annotated //nolint:gosec and the helper documents the cap.
func appendMapHeader(b []byte, count int) []byte   // msgp.AppendMapHeader(b, uint32(count))
func appendArrayHeader(b []byte, count int) []byte // msgp.AppendArrayHeader(b, uint32(count))
```

**Field order is unspecified** (msgpack maps are unordered). For **unique** keys this is
irrelevant; for duplicate keys see §3.8.

### 3.3 The count invariant (state verbatim in code comments and tests)

The encode hooks (`EncodeLevel`/`EncodeName`/`EncodeCaller`/`EncodeDuration`/`EncodeTime`)
receive the encoder **itself** as a `PrimitiveArrayEncoder` and call `Append*` to write a
value **right after** we've written its key. So `Append*` runs in *two* contexts: as an array
element, and as a map value. The invariant that reconciles them:

- **`AddXxx(key, val)` and `OpenNamespace(key)`** bump the current (top) map frame's `count`
  **once, on the key side**. The value write must **not** bump.
- **`AppendXxx(val)`** bumps the top frame's `count` **only when the top frame is an array**.
  In map-value context (driven by an encode hook), the top frame is a map → it writes the
  value bytes and does **not** bump.

Get this wrong and every entry carrying a level/name/caller/time/duration field has an
off-by-one map count → corrupt msgpack. This invariant is a primary test target (§5.4).

### 3.4 `EncodeEntry(ent, fields)` — two-phase assembly, the envelope nil/fallback matrix, and guards

Mirrors `jsonEncoder.EncodeEntry`'s ordering, **minus the line ending and the time field**.
The critical structural point (pass-2 P0): **envelope fields must land at the record root, not
inside a `With`-opened namespace.** zap writes the envelope to a fresh root *first*, *then*
splices the persistent `With` content (whose namespace is still open) *after*
(`zapcore/json_encoder.go:361-421`). A single shared stack would route envelope fields into an
open `With` namespace. We therefore use **two phases against the working clone `final`** (a
fresh pooled instance with its own `stack` and `scratch`, §3.1):

1. **Envelope phase → a transient root frame.** Set `final.stack = [freshMapFrame]` (no
   namespaces — no envelope field opens one). Write the envelope fields through `final`'s
   normal `Add*` machinery. Capture this frame as `env` (its `buf` and `count`). Each field
   honors the **empty-key omission** convention (`key == "" ⇒ skip`) and the matrix below, with
   a **no-op guard** on every hook path (record `len(top.buf)` before the hook; if unchanged
   after, append the documented fallback) so **a key is never written without a value**:

   | Envelope field | Written when | Hook | nil-hook / no-op fallback |
   |---|---|---|---|
   | **Level** | `LevelKey != "" && EncodeLevel != nil` (**omit** if `EncodeLevel == nil`) | `EncodeLevel(ent.Level, final)` | `AppendString(ent.Level.String())` (json_encoder.go:365-374) |
   | **Message** | `MessageKey != ""` | — | `AppendString(ent.Message)` (always) |
   | **Name** | `NameKey != "" && ent.LoggerName != ""` | `EncodeName`, or `FullNameEncoder` if nil | `AppendString(ent.LoggerName)` (json_encoder.go:378-394) |
   | **Caller** | `CallerKey != "" && ent.Caller.Defined` | `EncodeCaller(ent.Caller, final)` | `AppendString(ent.Caller.String())` — see note ‡ |
   | **Function** | `FunctionKey != "" && ent.Caller.Defined` | — | `AppendString(ent.Caller.Function)` — value may be `""` |
   | **Stack** | `StacktraceKey != "" && ent.Stack != ""` | — | `AppendString(ent.Stack)` |

   Matches `zapcore/json_encoder.go:396-421` for caller/function: caller and function are both
   gated by `ent.Caller.Defined`; **function is written whenever `FunctionKey != ""` even if
   `ent.Caller.Function == ""`** (zap emits an empty string — do **not** add a non-empty
   guard). ‡ **Caller nil-hook = deliberate hardening divergence:** zap calls `EncodeCaller`
   directly and *panics* if it is nil (it is non-optional in `EncoderConfig`); native instead
   falls back to `ent.Caller.String()` when `EncodeCaller == nil`, so a misconfigured config
   degrades gracefully rather than panicking. Documented and tested as such.

   **Time is intentionally NOT an envelope field** (§3.8); `TimeKey`/`EncodeTime` are not
   consulted for the envelope.

2. **`With` + call-site phase → the working copy of the persistent stack.** Set
   `final.stack = deepCopy(receiver.stack)` (the carried `With` structure; §3.1 #2,
   concurrency-safe). Add call-site `fields` via `zapcore.Field.AddTo(final)` (zap's standard
   dispatch) — they land in the **current top frame**, i.e. inside a still-open `With`
   namespace if one exists (matching zap). `Field.AddTo` is also what converts a method error
   into a `<key>Error` field (§3.9). Then **`sealDownTo(1)`** closes any remaining open
   top-level namespaces into the root, leaving `withRoot = final.stack[0]`.

3. **Assemble** into a `*buffer.Buffer` from zap's pool. Concatenation order is `env` then
   `withRoot`. For **unique keys this order is immaterial** (msgpack maps are unordered, §3.2);
   we make **no last-wins guarantee** for duplicate keys (native preserves all pairs with
   unspecified order, §3.8 — note our root-level field order does *not* exactly match zap's,
   which interleaves message after caller and writes stacktrace last). Each namespace key
   inside `withRoot` is immediately followed by its sealed map value (no stray bytes interleave,
   because once a namespace is open nothing else is appended to its parent — see §3.5), so the
   concatenation is valid msgpack:
   - `out = msgp.AppendArrayHeader(out, 2)` — the `[time, record]` 2-tuple (`0x92`).
   - `out, err = msgp.AppendExtension(out, &et)`, `et := EventTime(ent.Time)` — the **same**
     call generated `Entry.MarshalMsg` uses (`fluent/proto_gen.go:59`); byte-identical to the
     tested path. A non-nil `err` here is **fatal** (§3.9).
   - `out = appendMapHeader(out, env.count + withRoot.count)`.
   - `out = append(out, env.buf...)`; `out = append(out, withRoot.buf...)`.
   - write `out` into the buffer; recycle the working frame/scratch buffers; **return the
     buffer (do not free it — zap's `ioCore` frees it after `Write`)**.
4. **No `cfg.LineEnding`** is appended (a reflexive `jsonEncoder` copy bug to avoid).

### 3.5 `With` + `Clone` + namespaces — the carried-open-frame case

`logger.With(zap.Namespace("ns"), zap.String("a","1"))` adds fields through `Add*`/
`OpenNamespace` on a `Clone()`d encoder **before** any `EncodeEntry`, leaving the **top-level**
`ns` frame open on the cloned stack with its `count` reflecting the `With` fields so far. At
`EncodeEntry`, the **`With`+call-site phase** (§3.4 step 2) deep-copies that stack, call-site
fields increment the same open `ns` frame, and `sealDownTo(1)` seals it.

**Crucially, the envelope (level, message, name, caller, function, stack) is written in the
separate envelope phase (§3.4 step 1) into its own root frame — never into the open `ns`
namespace.** This is the pass-2 P0 fix: envelope fields stay at the record root, exactly as
zap's `jsonEncoder` keeps them at root and splices the `With` namespace after. Once a namespace
is open, nothing is appended to its parent except (eventually) the sealed namespace value, so
each `ns` key is immediately adjacent to its map value — valid msgpack. Namespaces opened
**inside** an object are a different case, sealed within `AddObject`/`AddArray` (§3.2).

Mandatory tests (§5.4): (a) `logger.With(zap.Namespace("ns"), zap.Int("a",1)).Info("m",
zap.Int("b",2))` ⇒ `{level, msg, … at root; "ns": {"a":1, "b":2}}` — envelope at **root**,
`With` and call-site fields inside `ns`;
(b) an `ObjectMarshaler` that calls `OpenNamespace("inner")` then returns, followed by a
sibling call-site field ⇒ the sibling is **outside** the object's namespace (object-scoped
closure).

### 3.6 Type mapping

| zap method(s) | msgpack output | `msgp` call | note |
|---|---|---|---|
| `AddString`/`AppendString`, `AddByteString`/`AppendByteString` | str | `AppendString` / `AppendStringFromBytes` | bytes written as-is (§3.8 UTF-8 note) |
| `AddBool`/`AppendBool` | bool | `AppendBool` | |
| `AddInt*`/`AddInt`, `Append*` | int | `AppendInt64` (widen) | exact; no float64 truncation |
| `AddUint*`/`AddUint`/`AddUintptr`, `Append*` | uint | `AppendUint64` (widen) | exact, incl. `> 2^63` |
| `AddFloat64`/`AddFloat32`, `Append*` | float64/float32, **or str for NaN/±Inf** | `AppendFloat*` / `AppendString` | NaN/Inf stringified to match zap (§3.8) |
| `AddBinary` | **bin** | `AppendBytes` | real msgpack bin, **not** base64 (§3.8) |
| `AddDuration`/`AppendDuration` | per `EncodeDuration` | hook → `Append*` | nil/no-op ⇒ int nanos (§3.8) |
| `AddTime`/`AppendTime` (a *field*) | per `EncodeTime` | hook → `Append*` | nil/no-op ⇒ int unix-nanos; envelope time excluded (§3.8) |
| `AddComplex128`/`64`, `Append*` | str `"r+ii"` | `AppendString` | matches zap's representation (§3.8) |
| `AddReflected`/`AppendReflected`, and `zap.Any` of arbitrary structs | nested msgpack | scratch-encode (§3.7) | transactional; on total failure → `<key>Error` |

`OpenNamespace(key)` → bump top map count, write `key`, push a `kindMap` frame.
`AddObject`/`AppendObject` → §3.2 depth-scoped push/run/`sealDownTo`. `AddArray`/`AppendArray`
→ §3.2 depth-scoped, `kindArray`. (Arrays never open namespaces directly; only the
`ObjectEncoder` handed to a nested `AppendObject` can.)

### 3.7 `AddReflected` / arbitrary values — transactional, never corrupt, never drop

`zap.Any(struct{…}{})` and `zap.Reflect(...)` route to `AddReflected(key, value any)`. Two
hazards must both be handled (codex P0):

- `msgp.AppendIntf` **writes partial container bytes** (map/array header + earlier elements)
  before returning an error on a nested unsupported value
  (`tinylib/msgp/write_bytes.go:372-383, 453-462, 465-500`). Appending a fallback after that
  yields corrupt/duplicated msgpack.
- A returned error must **not** abort the entry (§3.9).

**Therefore encode the value into `e.scratch` first, commit only on success** (this mirrors
zap's own `jsonEncoder.AddReflected`, which encodes to bytes before writing the key). `e` here
is the **per-call working clone** (or the per-`With` clone), so `e.scratch` is private and never
shared across concurrent encodes (§3.1 concurrency rule):

```
encodeReflected(value any) ([]byte, error):
    b := msgp.AppendIntf(e.scratch[:0], value)         // Tier 1 (fast)
    if err == nil { e.scratch = b; return b, nil }
    j, err := json.Marshal(value)                       // Tier 2 (fidelity == transcode)
    if err != nil { return nil, err }                   //   → caller returns err (§3.9)
    var v any; dec := json.NewDecoder(bytes.NewReader(j)); dec.UseNumber(); dec.Decode(&v)
    v = normalizeNumber(v)                               // NOTE: normalizeNumber, not …Numbers —
                                                         // handles a TOP-LEVEL json.Number (e.g. a
                                                         // MarshalJSON returning a bare uint64 > 2^63)
                                                         // AND recurses into maps/slices. normalizeNumbers
                                                         // alone (fluent/encoder.go:77-90) skips the
                                                         // top-level scalar, so msgp.AppendIntf →
                                                         // AppendJSONNumber would drop it to float64.
    b = msgp.AppendIntf(e.scratch[:0], v); e.scratch = b // structs → nested maps == transcode
    return b, err

AddReflected(key, value):
    b, err := encodeReflected(value)
    if err != nil { return err }       // NOTHING written; zap adds <key>Error, entry ships
    bump top map count; write key; append b to top.buf
    return nil

AppendReflected(value):  // array element
    b, err := encodeReflected(value)
    if err != nil { return err }       // element omitted; error propagates up to <key>Error
    bump top array count; append b to top.buf
    return nil
```

The scratch buffer ensures **no partial bytes** ever reach a frame on the error path. Tier 2
reuses the existing `normalizeNumber` (singular — it handles a top-level `json.Number` and
recurses; `normalizeNumbers` would skip a top-level scalar, §3.7 note) so structs become the
**same** nested maps the transcode path produces (integer precision preserved). The exact set of values `AppendIntf`
rejects (arbitrary structs reject statically at `write_bytes.go:388-397, 499-500`; nested
unsupported values are the partial-write case) is **verified by a focused test during
planning**.

> **Performance guidance — structured vs reflected nesting (important for users).** There are
> two ways nested data reaches the encoder, and only one is native:
>
> - **Structured nesting → native, arbitrary depth, no JSON.** `zap.Object` / `zap.Dict` /
>   `zap.Inline` / `zap.Array` / `zap.Objects` / `zap.Namespace`, and any type implementing
>   `zapcore.ObjectMarshaler` / `ArrayMarshaler` (including a struct that implements
>   `ObjectMarshaler`, even when passed via `zap.Any`). These build through the frame stack
>   (§3.2) with zero serialization round-trip — the full native win.
> - **Reflected nesting → correct but JSON-fallback, no speed-up.** A **plain Go struct with no
>   marshaler**, logged via `zap.Any`/`zap.Reflect` (or an unrecognized slice/map of such),
>   routes to `AddReflected` and takes the Tier-2 `json.Marshal` round-trip *per field*. Output
>   is **transcode-equivalent and correct**, but those fields are **no faster than the v1
>   transcode path**.
>
> **Recommendation:** for hot-path nested data, implement `ObjectMarshaler` (or use
> `zap.Dict`/`zap.Inline`) rather than `zap.Any(struct)`. A reflect-based struct→msgpack
> encoder that would make reflected structs native is deferred (§8): it would either
> reimplement `encoding/json`'s reflection or add a dependency, and risks silent divergence
> from transcode. Note: a struct implementing `fmt.Stringer` or `error` is routed by zap to a
> **string** field (not a nested object) on **both** paths — consistent, by design.

### 3.8 Time semantics and the documented divergences from transcode

- **Envelope time = `ent.Time`, exact.** Written to the `EventTime` extension; sub-second
  precision; no parsing, no `TimeCodec`. `WithTimeCodec`/`WithTimeKey` and
  `EncoderConfig.TimeKey`/`EncodeTime` **do not affect the envelope** (not consulted; not an
  error — see §4). **`EncodeTime` still applies to user-added `time.Time` *fields*** via
  `AddTime`. This is not new: the transcode path already lifts the `ts` field into the
  envelope; native takes it from `ent.Time` instead.

The native wire is **identical to transcode for unique keys** across all standard scalar and
container types, with these **deliberate, documented exceptions**:

1. **`AddBinary` → real msgpack `bin`** (not base64-in-a-string as zap JSON does). Fluent
   consumers handle `bin` natively; strictly better on the wire.
2. **Complex numbers → string `"r+ii"`**, matching zap's own JSON representation (no native
   msgpack complex type).
3. **Duplicate keys are preserved** on the wire (native does not deduplicate), matching zap's
   *own* JSON encoder (`zapcore/json_encoder.go:65-76` documents that zap keeps duplicates and
   relies on consumers). The **transcode** path collapses duplicates to last-wins via its
   intermediate `map[string]any` (`fluent/encoder.go:38-49`) — that collapse is transcode's
   artifact. Native field order is unspecified, so we make **no guarantee** about which
   duplicate a consumer's last-wins resolution selects. Equivalence tests exclude duplicates
   (§5.3); a dedicated test asserts native preserves all duplicate pairs.
4. **Invalid UTF-8 in strings/byte-strings passes through unchanged** into the msgpack `str`
   object. zap's JSON encoder replaces invalid bytes with `�`
   (`zapcore/json_encoder.go:527-542`); native does **not** scan/transform strings (a
   deliberate hot-path choice — skipping the per-string scan is part of the speed win).
   Revisit if a downstream consumer rejects non-UTF-8 `str`.

**Matched (NOT divergences), to be explicit:**

- **NaN / ±Inf floats are stringified** to `"NaN"` / `"+Inf"` / `"-Inf"`, **matching zap**
  (`zapcore/json_encoder.go:471-481`): `AddFloat64`/`AddFloat32` (and the `Append*`
  equivalents) special-case `math.IsNaN`/`math.IsInf` and emit a msgpack `str`; finite values
  use `AppendFloat*`. So native == transcode for all float values.
- **Negative zero and `uint64 > 2^63`** are emitted exactly (`AppendFloat64`/`AppendUint64`)
  and round-trip equal to transcode.

### 3.9 Error handling — the entry always ships unless assembly is fatal

zap's `ioCore.Write` **aborts and does not write** if `EncodeEntry` returns a non-nil error
(`zapcore/core.go:94-100`). zap's normal per-field degradation happens *earlier*:
`Field.AddTo` calls the error-returning encoder method and, on error, adds a
`<key>Error` string field and **continues** (`zapcore/field.go:117-185`). Therefore:

- **Field-level errors are returned from the method**, never accumulated and never returned
  from `EncodeEntry`. zap's `Field.AddTo` converts only the errors from the methods **it
  directly calls** — `AddArray`/`AddObject`/`AddReflected`, inline marshaling, stringer, and
  error fields (`zapcore/field.go:117-185`) — into `<key>Error`. The **nested** `Append*`
  methods (`AppendArray`/`AppendObject`/`AppendReflected`) are invoked by a user's
  `ArrayMarshaler`/`ObjectMarshaler`, so their error becomes `<key>Error` **only if that
  marshaler returns it** up through the enclosing `AddArray`/`AddObject`. Native returns such
  errors faithfully; it cannot (and does not promise to) synthesize `<key>Error` when a user
  marshaler **swallows** the error. This matches the transcode path, where an unencodable
  `zap.Any` also becomes `<key>Error` (zap's JSON encoder, upstream, does the same).
- **`EncodeEntry` returns a non-nil error only for a genuinely fatal assembly failure** — in
  practice only `msgp.AppendExtension` on the fixed 8-byte `EventTime` (not expected to fail).
  Such an entry legitimately cannot be encoded. There is no degradable-error path that returns
  from `EncodeEntry` (this resolves the pass-1 P0 contradiction between §3.7 and §3.9).
- **`With`-time field errors do not poison later entries:** a bad `With` field returns its
  error to `Field.AddTo` during `logger.With(...)`, which adds `<key>Error` to the cloned
  encoder's persistent buffer — a normal field, not retained error state.

---

## 4. API surface

### 4.1 New, root package — `zapwire/passthrough.go`

```go
// Passthrough returns an Encoder that copies the per-entry payload through unchanged.
// Use it when the bytes handed to the Writer are ALREADY the final per-entry wire
// payload — e.g. a native zapcore.Encoder that emits the framed entry directly.
func Passthrough() Encoder
```

Exported as a small generic primitive so external processors can wire their own native
`zapcore.Encoder` onto `zapwire.New(t, zapwire.Passthrough(), theirFramer)`. For fluent users
it is hidden behind the helpers below.

### 4.2 New, fluent package — `fluent/msgpack_encoder.go` + `fluent/native.go`

```go
// NewMsgpackEncoder returns a zapcore.Encoder that emits Fluent Forward
// [EventTime, record] msgpack payloads directly from zap's structured fields —
// no JSON round-trip. Envelope time comes from zapcore.Entry.Time (exact);
// TimeCodec / EncoderConfig time settings do not affect it (see design §3.8).
func NewMsgpackEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder

// NewNativeWriter builds a zapwire.Writer for the native path: a Passthrough
// encoder + the PackedForward Framer. Pair it with NewMsgpackEncoder to build a
// zapcore.Core yourself (e.g. inside zapcore.NewTee or a sampler).
func NewNativeWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error)

// NewNativeCore wires NewMsgpackEncoder + NewNativeWriter into a ready zapcore.Core
// plus its Writer (which the caller must Close).
func NewNativeCore(
    t zapwire.Transport,
    level zapcore.LevelEnabler,
    encCfg zapcore.EncoderConfig,
    opts ...Option,
) (zapcore.Core, *zapwire.Writer, error)
```

**Option reuse and applicability:**

| Option | Native applicability |
|---|---|
| `WithTag` | ✅ frame tag (Framer) |
| `WithZapwireOptions` | ✅ mode/buffer/timeouts (Writer) |
| `WithTimeCodec` / `WithTimeKey` | ⛔ not consulted (envelope time is structural). **Accepted without error**, documented as a no-op on the native path. |

**Why no-op rather than error for `WithTimeCodec`/`WithTimeKey`:** `Option` is the shared
fluent options type consumed by both transcode and native helpers; returning an error (or
panicking) would break ergonomic option reuse and make `WithZapwireOptions`-style bundling
fragile. The no-op is documented prominently on `NewMsgpackEncoder`/`NewNativeCore`. (A future
`go vet`-style lint could warn, but that is out of scope.)

`NewNativeWriter`/`NewNativeCore` resolve `WithTag` (default `"app.logs"`) via the shared
writer-build path (refactored to accept the chosen `zapwire.Encoder`).

### 4.3 Usage

```go
// Preset (most users)
core, w, err := fluent.NewNativeCore(t, zap.InfoLevel, encCfg, fluent.WithTag("app.logs"),
    fluent.WithZapwireOptions(zapwire.WithAsyncMode()))
logger := zap.New(core); defer w.Close()

// BYO core (Tee / sampling) — no passthrough knowledge needed
w, _   := fluent.NewNativeWriter(t, fluent.WithTag("app.logs"))
enc    := fluent.NewMsgpackEncoder(encCfg)
core   := zapcore.NewTee(stdoutCore, zapcore.NewCore(enc, w, zap.InfoLevel))

// External processor with its own native encoder
w2, _  := zapwire.New(t, zapwire.Passthrough(), myFramer)
```

**Transcode `NewWriter`/`NewCore` are unchanged and remain the default.**

---

## 5. Testing & benchmarks

Three independent oracles so the encoder is never validated against itself.

### 5.1 Oracle 1 — golden hex vectors (library-independent)
Hand-computed msgpack bytes for the critical structures, asserted exactly:
- empty record map header; the `fixmap`→`map16` boundary (the 16th field) — exercises header
  sizing;
- the `[time, record]` array header `0x92` + the 8-byte `EventTime` extension for a known
  instant (cross-checked against `EventTime.MarshalBinaryTo`);
- one nested namespace (one inner + one outer field) — count correctness;
- **envelope byte-identity:** build native `[EventTime, {}]` and compare **byte-for-byte** to
  `Entry{Time: et, Record: map[string]any{}}.MarshalMsg` (`fluent/proto_gen.go:54-65`),
  proving the EventTime envelope path is identical to the generated one.

### 5.2 Oracle 2 — decode + structural compare
Decode native output with `msgp`'s **reader** path (`UnmarshalMsg`/`ReadIntfBytes` — a
distinct code path from the generated `MarshalMsg` writer) and assert the decoded
`map[string]any` equals the expected record. **Logical** comparison, not byte equality
(msgpack map key order is unspecified; Go map iteration is randomized).

### 5.3 Oracle 3 — equivalence vs the tested transcode path
For the **unique-key scalar subset** (strings of valid UTF-8, bools, ints/uints, finite
floats **and** NaN/±Inf), feed identical fields through both `fluent.NewEncoder` (transcode)
and `NewMsgpackEncoder`, decode both, and assert equal records. **Excluded by design** (the
§3.8 divergences): `AddBinary` (bin vs base64), nanosecond envelope-time precision, duplicate
keys, invalid-UTF-8 strings, and reflected structs.

### 5.4 Behavior cases (each its own test)
- every type in §3.6 round-trips;
- **the envelope nil/fallback matrix (§3.4):** for level, name, caller, time-field, and
  duration-field — nil hook and no-op hook — decode and assert exact map counts and the
  documented fallback value; assert level is **omitted** when `EncodeLevel == nil`; assert
  **caller** falls back to `Caller.String()` when `EncodeCaller == nil` (the documented ‡
  hardening divergence) and uses `EncodeCaller` otherwise; assert **function** is emitted even
  when `ent.Caller.Function == ""` (empty string), gated on `ent.Caller.Defined`;
- **envelope-at-root parity (pass-2 P0):** `logger.With(zap.Namespace("ns"),
  zap.Int("a",1)).Info("m", zap.Int("b",2))` ⇒ `level`/`msg`/caller/function/stack (as
  configured) at the **root**, and only `a`,`b` under `ns`;
- empty-key omission for each envelope field; all keys set vs all empty;
- the §3.5 object-scoped-namespace case (an `ObjectMarshaler` opening `OpenNamespace("inner")`,
  then a sibling call-site field lands **outside** the object's namespace);
- nested objects/arrays, including arrays of objects;
- `zap.Any` of: a primitive, a `map[string]any`, a `[]int`, an **arbitrary struct** (Tier 2 ⇒
  transcode-equivalent nested map), a **custom `MarshalJSON` returning a bare unsigned number
  `> 2^63`** (Tier 2 ⇒ `uint64`, **not** float — exercises the top-level `normalizeNumber`
  path §3.7), a **nested-bad value** `map[string]any{"bad": struct{}{}}` and
  `[]any{1, struct{}{}}` (Tier-1 partial-write rollback ⇒ clean output, no duplicate/partial
  container), and an **unencodable value** like `make(chan int)` (total failure ⇒ `<key>Error`,
  **entry still ships** — assert via a recording WriteSyncer that `Write` was called);
- **nested `Append*` error propagation:** an `ArrayMarshaler`/`ObjectMarshaler` that *returns*
  an `AppendReflected`/`AppendObject` error ⇒ `<key>Error` added (and a control case where the
  marshaler *swallows* the error ⇒ no `<key>Error`, per §3.9 wording);
- **duplicate keys** across envelope/`With`/namespace/call-site ⇒ assert native preserves all
  pairs (documented §3.8 behavior);
- NaN/±Inf ⇒ string `"NaN"`/`"+Inf"`/`"-Inf"`; negative zero and `uint64 > 2^63` exact;
- large integers `> 2^53` preserved exactly (regression vs transcode's old caveat);
- deep nesting that pushes payload size across the framer's bin8→bin16→bin32 boundaries; and a
  **framer integration** test: two native payloads concatenated in one PackedForward bin ⇒
  bin length == sum of payload lengths, `size` option == payload count (`fluent/framer.go:21-31`);
- the count invariant (§3.3): an entry whose only fields come from encode hooks (level+caller)
  ⇒ map count == number of pairs (off-by-one canary).

### 5.5 Concurrency
`-race` test: N goroutines logging through one `NewNativeCore`-built core concurrently
(exercises working-clone-per-`EncodeEntry`, §3.1 #2). A `logger.With(...)` shared across
goroutines, each adding distinct call-site fields, asserting no cross-talk and that a bad
`With` field does not poison later entries (§3.9). **A dedicated reflected-scratch race test
(pass-2 P0):** many goroutines logging `zap.Any(...)` values that force both Tier 1 and Tier 2
through the same core, asserting `-race` is clean — proving the working-clone owns its
`scratch` (§3.1 concurrency rule).

### 5.6 `Passthrough` (root) — `zapwire/passthrough_test.go`
Sync mode copies into a provided `dst`; async mode (`dst == nil`) returns an owning copy that
survives mutation/reuse of the source slice (grounds `writer.go:129-147` vs `332-353`).

### 5.7 Benchmarks (`fluent/bench_test.go`, extend existing)
Native vs transcode, **sync and async**, reporting `ns/op` and **`allocs/op`**. Field mixes: a
small scalar record (typical) and a larger record with a nested object. The v1-vs-v2
comparison v1 design §3 (line 163) promised. Acceptance: native shows strictly fewer
allocs/op on the scalar hot path; no correctness regressions.

---

## 6. Files

| File | Responsibility |
|---|---|
| `zapwire/passthrough.go` | `Passthrough()` identity encoder |
| `zapwire/passthrough_test.go` | copy semantics (sync `dst`, async `nil`) |
| `fluent/msgpack_encoder.go` | encoder type, frame stack, `sealDownTo`, header helpers, `Clone`, `EncodeEntry`, `Add*`/`OpenNamespace`, reflected scratch-encode |
| `fluent/msgpack_array.go` | `Append*` (ArrayEncoder) methods |
| `fluent/native.go` | `NewMsgpackEncoder`, `NewNativeWriter`, `NewNativeCore`; shared writer-build refactor |
| `fluent/msgpack_encoder_test.go` | golden vectors, type round-trips, behavior/matrix/error cases |
| `fluent/msgpack_equiv_test.go` | transcode-equivalence (unique-key scalar subset) + decode oracle |
| `fluent/bench_test.go` | extend with native sync/async benchmarks |

(If the array methods are trivial they may fold into `msgpack_encoder.go` — decided during
planning.)

---

## 7. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Count off-by-one from the Add/Append duality (§3.3) | Invariant stated verbatim in code + an off-by-one canary test (§5.4) |
| Dropping a log on a degradable field error (pass-1 P0) | Field errors returned from methods → zap `<key>Error`; `EncodeEntry` returns error only for fatal assembly (§3.9); recording-WriteSyncer test |
| Corrupt msgpack from `AppendIntf` partial writes (pass-1 P0) | Transactional scratch-encode-then-commit (§3.7); nested-bad-value tests |
| Object-scoped namespace leaking to the record (pass-1 P0) | `sealDownTo(d+1)` inside `AddObject`/`AddArray` (§3.2); object-namespace test (§5.4) |
| `With`-namespace count finalized late (§3.5) | Frame stack carried across `Clone`, sealed in `EncodeEntry`; mandatory test |
| Key-without-value from a nil/omitted hook (pass-1 P1) | Full envelope nil/fallback matrix + no-op guard (§3.4); matrix tests |
| Undocumented wire divergence | §3.8 enumerates every divergence (binary, complex, duplicates, UTF-8) and every match (NaN/Inf, ±0, big uint); golden + equivalence tests pin the rest |
| Envelope fields landing inside a `With` namespace (pass-2 P0) | Two-phase `EncodeEntry`: envelope → separate root frame; `With`+call-site → working stack; concatenated (§3.4); envelope-at-root parity test (§5.4) |
| Concurrent `EncodeEntry` data race incl. shared `scratch` (pass-2 P0) | Working clone owns its `stack` **and** `scratch`; `Clone` allocates fresh `scratch`; concurrency rule (§3.1); reflected-scratch `-race` test (§5.5) |
| Caller/function envelope fidelity (pass-2 P1) | Matrix matches zap (function emitted even when empty; caller gated on `Caller.Defined`); caller nil-hook documented as hardening ‡ (§3.4); matrix tests |
| Tier-2 reflected drops top-level big uint (pass-2 P1) | Use `normalizeNumber` (handles top-level `json.Number`), not `normalizeNumbers` (§3.7); MarshalJSON-uint64 test (§5.4) |
| Header cast won't compile / overflow (pass-1 P2) | `appendMapHeader`/`appendArrayHeader` cast helpers with documented cap (§3.2) |
| Dependency creep | No new runtime deps; golden vectors add none; isolation re-verified (root 0 / fluent 1) |

---

## 8. Out of scope (future roadmap, unchanged)

CompressedPackedForward/gzip; native NDJSON encoder (unnecessary — already native); `syslog/`
(RFC5424, same module); `otlp/` (own module). Tracked in v1 design §11.

**Native reflected-struct encoder (deferred, profiling-gated).** A `reflect`-based
struct→msgpack walker would make plain `zap.Any(struct)` / `zap.Reflect` fields native instead
of taking the Tier-2 JSON round-trip (§3.7 performance guidance). Deferred because it must
either reimplement `encoding/json` reflection semantics (json tags, embedded fields, `omitempty`,
`MarshalJSON`) to stay transcode-equivalent, or adopt a msgpack-reflection dependency that uses
different tag conventions — both costly and divergence-prone. Revisit only if profiling shows a
struct-heavy hot path where implementing `ObjectMarshaler` is not viable.

---

## 9. Codex pass-1 resolutions

| Finding | Resolution |
|---|---|
| **P0** EncodeEntry error drops entry | §3.9 rewritten: field errors → `<key>Error` via method return; `EncodeEntry` errors only on fatal assembly. Tier-3 placeholder removed. |
| **P0** reflected partial-write corruption | §3.7 rewritten: transactional scratch-encode-then-commit; nothing written on error. |
| **P0** object-scoped namespaces | §3.2 `sealDownTo(d+1)` collapses namespaces opened inside an object back into it; only top-level namespaces carry to `EncodeEntry`. §3.5 distinguishes the two. |
| **P1** duplicate keys / order | §3.8 divergence #3: native preserves duplicates (like zap), order unspecified; transcode collapse is its artifact. Excluded from equivalence; dedicated test. |
| **P1** NaN/Inf & invalid UTF-8 | §3.8: NaN/Inf **stringified to match zap** (not a divergence); invalid UTF-8 documented as divergence #4 (pass-through). |
| **P1** encode-hook nil/fallback | §3.4 full matrix (level omitted if `EncodeLevel==nil`; name→`FullNameEncoder`; time/duration→int nanos), no-op guard ensures no key-without-value. |
| **P2** count uint32 cast/overflow | §3.2 `appendMapHeader`/`appendArrayHeader` helpers with documented cap. |

## 10. Codex pass-2 resolutions

| Finding | Resolution |
|---|---|
| **P0** envelope fields written into carried `With` namespace | §3.4 rewritten to a **two-phase** `EncodeEntry`: envelope → a separate transient root frame (never the open namespace); `With`+call-site → working copy of the persistent stack; the two are concatenated (order matches zap). §3.5 updated. |
| **P0** shared `scratch` data race | §3.1 concurrency rule added: `stack` **and** `scratch` are per-instance; `Clone` allocates a fresh `scratch`; the `EncodeEntry` working clone owns both; nothing shared. §3.7 notes `e` is the working clone. New reflected-scratch `-race` test (§5.5). |
| **P1** caller/function rows didn't match zap | §3.4 matrix fixed: function gated on `ent.Caller.Defined` and emitted even when `Caller.Function == ""`; caller nil-hook documented as a deliberate hardening divergence (zap would panic). |
| **P1** Tier-2 drops top-level big unsigned number | §3.7 uses `normalizeNumber(v)` (handles a top-level `json.Number`, recurses) instead of `normalizeNumbers(v)`; test added (§5.4). |
| **P2** error-contract overstated `Append*` → `<key>Error` | §3.9 clarified: nested `Append*` errors reach `<key>Error` only if the user marshaler returns them; native makes no promise for swallowed errors. Propagation test added (§5.4). |
