# Native msgpack Encoder — Implementation Plan (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 4) · **Date:** 2026-06-08 ·
**Branch:** `feat/zapwire-v2` · **Spec:** `docs/design/2026-06-08-native-msgpack-encoder-design.md`

**Review history:** codex plan-review pass 1 (`tmp/…_pass1_review.md`) raised 2×P0 / 2×P1 / 1×P2;
pass 2 (`tmp/…_pass2_review.md`) confirmed those resolved, raised 3×P1; pass 3
(`tmp/…_pass3_review.md`) confirmed those resolved, raised 1×P1; pass 4
(`tmp/…_pass4_review.md`) found **no P0/P1/P2** and returned **implementation-ready**.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Execute tasks **in order** (1 → 11): each task leaves
> the tree compiling with all prior tests green.

**Goal:** Add an opt-in `zapcore.Encoder` that emits Fluent Forward `[EventTime, record]`
msgpack payloads directly from zap's structured fields — one serialization, no JSON round-trip —
shipped through the existing `Writer` via a no-op `Passthrough` encoder + the existing
PackedForward `Framer`.

**Architecture:** A single `msgpackEncoder` type implements the whole `zapcore.Encoder` +
`zapcore.ArrayEncoder` surface, mirroring zap's own `jsonEncoder` (constructor, `Clone`,
`EncodeEntry`, per-type `Add*`/`Append*`, namespaces) but swapping JSON tokens for `msgp.Append*`
plus **explicit msgpack element counting**. Containers are buffered per level and sealed on close
(`sealDownTo`, no backpatching). `EncodeEntry` is two-phase: the entry envelope writes to a fresh
root frame, the persistent `With` context + call-site fields write to a deep copy of the carried
stack, and the two are concatenated under one map header. The result is the complete per-entry
wire payload; `Passthrough()` copies it through to the unchanged `Writer`/`Framer`.

**Tech Stack:** Go 1.25, `go.uber.org/zap/zapcore` + `go.uber.org/zap/buffer` (integration
target, already present), `github.com/tinylib/msgp/msgp` (already present in `fluent/`),
`github.com/stretchr/testify` (tests). **No new runtime dependencies; the root `zapwire` package
stays free of `tinylib/msgp`.**

**Design source:** `docs/design/2026-06-08-native-msgpack-encoder-design.md` (codex plan-review
consensus, pass 3). Read it first — section refs below (e.g. §3.4) point into it.

---

## Dependency policy (unchanged from v1)

| Package | Allowed imports | This plan adds |
|---|---|---|
| `zapwire` (root) | stdlib + `go.uber.org/zap/zapcore` | `passthrough.go` (stdlib only) |
| `fluent/` | stdlib + `zapcore` + `zap/buffer` + `tinylib/msgp` | the encoder + native presets |

`Passthrough()` lives in the **root** package and imports nothing beyond stdlib, so an NDJSON-only
or BYO-encoder consumer can wire a native core without pulling in `tinylib/msgp`.

## File structure (created / modified by this plan)

| File | Status | Responsibility |
|---|---|---|
| `passthrough.go` | **create** | `Passthrough()` identity `zapwire.Encoder` |
| `passthrough_test.go` | **create** | copy semantics (sync `dst`, async `nil`) |
| `fluent/msgpack_encoder.go` | **create** | type, frame stack, pools, `sealDownTo`, header helpers, `addKey`, `Clone`, `EncodeEntry`, `writeEnvelope`, all `Add*` / `OpenNamespace` / `AddObject` / `AddArray` / `AddReflected`, `encodeReflected` |
| `fluent/msgpack_array.go` | **create** | all `Append*` (`ArrayEncoder`), `appendFloat`, `appendComplex`, `AppendObject` / `AppendArray` / `AppendReflected` |
| `fluent/native.go` | **create** | `NewMsgpackEncoder`, `NewNativeWriter`, `NewNativeCore` |
| `fluent/fluent.go` | **modify** | refactor `buildWriter` to accept the chosen `zapwire.Encoder` |
| `fluent/msgpack_encoder_test.go` | **create** | golden vectors, type round-trips, envelope matrix, containers, reflected, error contract |
| `fluent/msgpack_equiv_test.go` | **create** | transcode-equivalence (unique-key scalar subset) + the decode oracle |
| `fluent/bench_test.go` | **modify** | add native sync/async benchmarks |

**Decision (resolves spec §6's "may fold"):** `Append*` methods go in their **own file**
`fluent/msgpack_array.go`; `Add*` + machinery go in `fluent/msgpack_encoder.go`. Both are
`package fluent`, so cross-file calls are free.

---

## Shared invariants & contracts (referenced by every task)

These are stated once here; each task that touches them restates the relevant code. Encode the
**bold** ones verbatim as code comments where indicated.

1. **Uniform delegation (collapses the count off-by-one surface, design §3.3).** Every `Add*`
   method is exactly `addKey(key)` then the matching `Append*(val)` — identical to zap's
   `jsonEncoder`. There are **only two count-bump sites**:
   - **`addKey(key)`** bumps the top **map** frame's pair count by one and writes the key. Used by
     every `Add*` **and** `OpenNamespace`.
   - **`Append*(val)`** writes the value and bumps the count **only when the top frame is an
     array** (`kindArray`). In map-value context — whether driven by an `Add*` value-write or by an
     encode hook (`EncodeLevel`/`EncodeCaller`/…) — it writes the value and does **not** bump.
2. **Seal on close, no backpatch (§3.2).** Each open container buffers its **contents only** (no
   header) in a pooled `buf`; the element count lives in the frame. `sealDownTo(depth)` pops frames
   until `len(stack) == depth`, writing each popped frame's header (from its count) + contents into
   the frame below, then recycling the popped buffer.
3. **Object-scoped namespaces (§3.2).** `AddObject`/`AppendObject`/`AddArray`/`AppendArray` record
   `d := len(stack)` before pushing the container frame, run the marshaler, then `sealDownTo(d+1)`
   (collapse anything the marshaler opened — including namespaces — into the container) then
   `sealDownTo(d)` (seal the container into its parent). Only **top-level** namespaces survive to
   `EncodeEntry`, which finalizes with `sealDownTo(1)`.
4. **Two-phase `EncodeEntry` (§3.4, the pass-2 P0 fix).** Envelope fields write to a **fresh root
   frame** (never into a carried `With` namespace); `With` + call-site fields write to a **deep
   copy** of the persistent stack; the two roots are concatenated under one map header. **Time is
   not an envelope field** — it is the `[time, record]` extension, taken from `ent.Time`.
5. **Concurrency (§3.1).** `stack` and `scratch` are per-instance mutable state, never shared.
   `Clone()` returns a new instance with a deep-copied stack and its own (nil) scratch.
   `EncodeEntry` runs entirely on per-call working clones (from a pool) and **never mutates** the
   receiver — it only **reads** the receiver's stack to deep-copy it.
6. **Entry always ships unless assembly is fatal (§3.9).** Field-level errors are **returned from
   the method**; zap's `Field.AddTo` turns them into a `<key>Error` string field and continues.
   `EncodeEntry` returns a non-nil error **only** for a fatal assembly failure (in practice only
   `msgp.AppendExtension` on the 8-byte `EventTime`). No log is ever dropped for a single bad field.

### Conventions for every task

- **TDD:** write the failing test → run it red → implement minimally → run it green → commit.
- **Test command:** `go test ./... -race` (the Makefile's `make test` is already `-race`). Scope
  to the package under work with `go test ./fluent/ -race -run <Name>` while iterating.
- **Before every commit:** `make lint` (and `make fmt` if it reports formatting) until clean. The
  `uint32` header casts and big-endian packing require the `//nolint:gosec` annotations shown in
  the code below — gosec will fail the build without them.
- **Commits:** Conventional Commits, present tense, **no attribution trailers** (global CLAUDE.md).

---

## Task 1: `Passthrough()` identity encoder (root package)

Independent of the encoder; ship it first so `native.go` (Task 9) can wire it. Proves the copy
semantics both `Writer` paths rely on: sync appends into a pooled `dst` (`writer.go:133`), async
passes `dst == nil` and the result is queued and outlives the call (`writer.go:335`).

**Files:**
- Create: `passthrough.go`
- Test: `passthrough_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// passthrough_test.go
package zapwire

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPassthrough_SyncAppendsIntoDst(t *testing.T) {
	dst := make([]byte, 0, 16)
	out, err := Passthrough().Encode(dst, []byte("abc"))
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), out)
}

func TestPassthrough_AsyncReturnsOwningCopy(t *testing.T) {
	src := []byte("hello")
	out, err := Passthrough().Encode(nil, src)
	require.NoError(t, err)
	src[0] = 'J' // mutate the source after encoding (simulates buffer reuse)
	require.Equal(t, []byte("hello"), out, "async copy (dst==nil) must not alias the source")
}

func TestPassthrough_EmptyRecord(t *testing.T) {
	out, err := Passthrough().Encode(nil, nil)
	require.NoError(t, err)
	require.Empty(t, out)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./ -run TestPassthrough -race`
Expected: FAIL — `undefined: Passthrough`.

- [ ] **Step 3: Implement `Passthrough`**

```go
// passthrough.go
package zapwire

// passthrough copies an already-final per-entry payload through unchanged. Use it when the
// bytes handed to the Writer are ALREADY the final per-entry wire payload — e.g. a native
// zapcore.Encoder (see fluent.NewMsgpackEncoder) that emits the framed entry directly.
type passthrough struct{}

// Passthrough returns an Encoder that copies the per-entry payload through unchanged.
//
// It is correct for both Writer modes with no special-casing: in sync mode the caller passes a
// pooled dst and append reuses it; in async mode the caller passes dst == nil, so append(nil,
// record...) allocates a fresh owning copy that survives the source buffer being freed/reused.
func Passthrough() Encoder { return passthrough{} }

func (passthrough) Encode(dst, record []byte) ([]byte, error) {
	return append(dst, record...), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./ -run TestPassthrough -race`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add passthrough.go passthrough_test.go
git commit -m "feat: add Passthrough identity encoder for native zapcore encoders"
```

---

## Task 2: Encoder skeleton — frame stack, machinery, two-phase `EncodeEntry`

Lay down the full type, all 50 interface methods (real machinery + `addKey` + `AppendString`/
`AddString` + `OpenNamespace`; the rest stubbed), pools, `sealDownTo`, `Clone`, and the **two-phase
`EncodeEntry`** with a **message-only** envelope. After this task the package compiles, the
`zapcore.Encoder`/`ArrayEncoder` compile-assertions hold, and an empty/message-only record encodes
byte-identically to the generated `Entry.MarshalMsg`.

Scalars (Task 3), hooks/envelope matrix (Task 4), containers (Task 5), reflected (Task 6) replace
their stubs in later tasks. Stubbed scalar `Add*`/`Append*` are **no-ops**; stubbed container/
reflected methods return `errNotImplemented`; stubbed time/duration are no-ops. None are exercised
by this task's tests.

**Files:**
- Create: `fluent/msgpack_encoder.go`
- Create: `fluent/msgpack_array.go`
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// fluent/msgpack_encoder_test.go
package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// minimalCfg has every entry-level key empty, so EncodeEntry writes a bare record map.
func minimalCfg() zapcore.EncoderConfig { return zapcore.EncoderConfig{} }

func TestNative_EmptyRecord_GoldenAndByteIdentity(t *testing.T) {
	et := time.Unix(1692959400, 123456789)
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{Time: et}, nil)
	require.NoError(t, err)
	out := buf.Bytes()

	// Golden structural bytes: [0x92] [fixext8 0xd7 type 0x00 + 8 bytes] [empty map 0x80].
	require.Len(t, out, 12)
	require.Equal(t, byte(0x92), out[0], "2-element [time, record] array header")
	require.Equal(t, byte(0xd7), out[1], "fixext8 marker")
	require.Equal(t, byte(0x00), out[2], "EventTime extension type 0")
	require.Equal(t, byte(0x80), out[11], "empty fixmap")

	// Byte-identity with the generated marshaler for the same instant + empty record.
	want, err := Entry{Time: EventTime(et), Record: map[string]any{}}.MarshalMsg(nil)
	require.NoError(t, err)
	require.Equal(t, want, out, "native empty entry must equal Entry.MarshalMsg")
}

func TestNative_MessageOnly_RoundTrips(t *testing.T) {
	cfg := zapcore.EncoderConfig{MessageKey: "msg"}
	enc := newMsgpackEncoder(cfg)
	buf, err := enc.EncodeEntry(zapcore.Entry{Message: "hello"}, nil)
	require.NoError(t, err)

	e := decodeEntry(t, buf.Bytes())
	rec := e.Record.(map[string]any)
	assert.Equal(t, "hello", rec["msg"])
	assert.Len(t, rec, 1, "exactly one pair — proves the map count matches the body")
}

func TestNative_CallSiteStringField_RoundTrips(t *testing.T) {
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{
		{Key: "k", Type: zapcore.StringType, String: "v"},
	})
	require.NoError(t, err)
	rec := decodeEntry(t, buf.Bytes()).Record.(map[string]any)
	assert.Equal(t, "v", rec["k"])
	assert.Len(t, rec, 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run TestNative -race`
Expected: FAIL — `undefined: newMsgpackEncoder`.

- [ ] **Step 3: Write `fluent/msgpack_encoder.go` (machinery + skeleton)**

```go
// fluent/msgpack_encoder.go
package fluent

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Compile-time proof the full method set is implemented (cross-checked against
// go.uber.org/zap/zapcore/encoder.go in the module cache).
var (
	_ zapcore.Encoder      = (*msgpackEncoder)(nil)
	_ zapcore.ArrayEncoder = (*msgpackEncoder)(nil)
)

var errNotImplemented = errors.New("fluent: msgpack encoder method not implemented")

// frameKind distinguishes the two msgpack container types we open.
type frameKind uint8

const (
	kindMap frameKind = iota
	kindArray
)

// frame is one open msgpack container. buf holds the container's CONTENTS ONLY (no header);
// the header is written from count when the frame is sealed (sealDownTo).
type frame struct {
	kind  frameKind
	buf   []byte // pooled
	count int    // map: number of key/value PAIRS; array: number of ELEMENTS
}

// msgpackEncoder implements the entire zapcore.Encoder + zapcore.ArrayEncoder surface, emitting
// a Fluent Forward [EventTime, record] msgpack payload directly from zap's structured fields.
//
// stack[0] (the root) is always a kindMap: the record map's contents. stack and scratch are
// per-instance mutable state and are NEVER shared across goroutines (see Clone / EncodeEntry).
type msgpackEncoder struct {
	cfg     zapcore.EncoderConfig
	stack   []frame
	scratch []byte // transactional reflected-encode buffer (Task 6); per-instance, private
}

// newMsgpackEncoder builds a persistent encoder with an empty root map frame. With-fields added
// via Clone()+Add* accumulate into this stack until EncodeEntry.
func newMsgpackEncoder(cfg zapcore.EncoderConfig) *msgpackEncoder {
	e := &msgpackEncoder{cfg: cfg}
	e.stack = append(e.stack, frame{kind: kindMap, buf: getBytes()})
	return e
}

func (e *msgpackEncoder) top() *frame { return &e.stack[len(e.stack)-1] }

// addKey bumps the current map frame's pair count and writes the key. Every Add* method and
// OpenNamespace funnel through here, so the map count is bumped EXACTLY ONCE per key — never on
// the value side (the count invariant, design §3.3).
func (e *msgpackEncoder) addKey(key string) {
	t := e.top()
	t.count++
	t.buf = msgp.AppendString(t.buf, key)
}

// sealDownTo collapses frames until len(stack) == depth, sealing each popped frame into the frame
// below it: write the popped frame's header (from its count) then its contents, then recycle its
// buffer. No placeholder, no backpatch (design §3.2).
func (e *msgpackEncoder) sealDownTo(depth int) {
	for len(e.stack) > depth {
		top := e.stack[len(e.stack)-1]
		e.stack = e.stack[:len(e.stack)-1]
		parent := &e.stack[len(e.stack)-1]
		if top.kind == kindMap {
			parent.buf = appendMapHeader(parent.buf, top.count)
		} else {
			parent.buf = appendArrayHeader(parent.buf, top.count)
		}
		parent.buf = append(parent.buf, top.buf...)
		putBytes(top.buf)
	}
}

func (e *msgpackEncoder) OpenNamespace(key string) {
	e.addKey(key) // bump parent map count + write the namespace key
	e.stack = append(e.stack, frame{kind: kindMap, buf: getBytes()})
}

// --- Clone -----------------------------------------------------------------------------------

// Clone returns a NEW persistent encoder with a deep copy of the frame stack, so With-fields
// added to the copy never touch the receiver. Its scratch starts nil (grown lazily). Not pooled:
// the returned encoder is long-lived (held by the logger).
func (e *msgpackEncoder) Clone() zapcore.Encoder {
	c := &msgpackEncoder{cfg: e.cfg}
	c.stack = cloneStack(make([]frame, 0, len(e.stack)), e.stack)
	return c
}

// cloneStack appends deep copies of src's frames (fresh pooled buffers) onto dst.
func cloneStack(dst, src []frame) []frame {
	for i := range src {
		b := getBytes()
		b = append(b, src[i].buf...)
		dst = append(dst, frame{kind: src[i].kind, buf: b, count: src[i].count})
	}
	return dst
}

// --- EncodeEntry (two-phase, design §3.4) ----------------------------------------------------

func (e *msgpackEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	// Phase 1: envelope into a fresh transient root frame (no namespaces — no envelope field
	// opens one). NEVER routes into a carried With namespace.
	env := getEncoder(e.cfg)
	env.stack = append(env.stack, frame{kind: kindMap, buf: getBytes()})
	env.writeEnvelope(ent)

	// Phase 2: With + call-site fields into a deep copy of the persistent stack (concurrency-safe:
	// we only READ the receiver's stack here). Call-site fields land in the current top frame —
	// inside a still-open top-level With namespace if one exists, matching zap.
	final := getEncoder(e.cfg)
	final.stack = cloneStack(final.stack, e.stack)
	for i := range fields {
		fields[i].AddTo(final) // zap's standard dispatch; converts method errors to <key>Error
	}
	final.sealDownTo(1) // close any remaining open top-level namespaces into the root

	// Phase 3: assemble [time, record] into a pooled zap buffer (freed by ioCore after Write).
	buf := bufferPool.Get()
	hdr := getBytes()
	hdr = msgp.AppendArrayHeader(hdr, 2)
	et := EventTime(ent.Time)
	var err error
	if hdr, err = msgp.AppendExtension(hdr, &et); err != nil {
		putBytes(hdr)
		buf.Free()
		putEncoder(env)
		putEncoder(final)
		return nil, fmt.Errorf("fluent: marshal event time: %w", err) // fatal (design §3.9)
	}
	hdr = appendMapHeader(hdr, env.stack[0].count+final.stack[0].count)
	_, _ = buf.Write(hdr)
	_, _ = buf.Write(env.stack[0].buf)
	_, _ = buf.Write(final.stack[0].buf)

	putBytes(hdr)
	putEncoder(env)
	putEncoder(final)
	return buf, nil // do NOT free buf — zap's ioCore frees it after Write
}

// writeEnvelope writes the entry-level fields into the current (root) map frame. Task 2 writes
// only the message; Task 4 fills the full level/name/caller/function/stack matrix. Time is
// intentionally excluded — it is the [time, record] extension (design §3.8).
func (e *msgpackEncoder) writeEnvelope(ent zapcore.Entry) {
	if e.cfg.MessageKey != "" {
		e.addKey(e.cfg.MessageKey)
		e.AppendString(ent.Message)
	}
}

// --- scalar Add* (real: String; stubs: the rest, filled in Task 3) ---------------------------

func (e *msgpackEncoder) AddString(key, val string) { e.addKey(key); e.AppendString(val) }

func (e *msgpackEncoder) AddBool(string, bool)               {}
func (e *msgpackEncoder) AddByteString(string, []byte)       {}
func (e *msgpackEncoder) AddBinary(string, []byte)           {}
func (e *msgpackEncoder) AddComplex128(string, complex128)   {}
func (e *msgpackEncoder) AddComplex64(string, complex64)     {}
func (e *msgpackEncoder) AddFloat64(string, float64)         {}
func (e *msgpackEncoder) AddFloat32(string, float32)         {}
func (e *msgpackEncoder) AddInt(string, int)                 {}
func (e *msgpackEncoder) AddInt64(string, int64)             {}
func (e *msgpackEncoder) AddInt32(string, int32)             {}
func (e *msgpackEncoder) AddInt16(string, int16)             {}
func (e *msgpackEncoder) AddInt8(string, int8)               {}
func (e *msgpackEncoder) AddUint(string, uint)               {}
func (e *msgpackEncoder) AddUint64(string, uint64)           {}
func (e *msgpackEncoder) AddUint32(string, uint32)           {}
func (e *msgpackEncoder) AddUint16(string, uint16)           {}
func (e *msgpackEncoder) AddUint8(string, uint8)             {}
func (e *msgpackEncoder) AddUintptr(string, uintptr)         {}
func (e *msgpackEncoder) AddDuration(string, time.Duration)  {}
func (e *msgpackEncoder) AddTime(string, time.Time)          {}

// --- container / reflected Add* (stubs; filled in Tasks 5–6) ---------------------------------

func (e *msgpackEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return errNotImplemented }
func (e *msgpackEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return errNotImplemented }
func (e *msgpackEncoder) AddReflected(string, any) error                  { return errNotImplemented }

// --- pools & header helpers ------------------------------------------------------------------

var bufferPool = buffer.NewPool()

var bytesPool = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

func getBytes() []byte {
	bp, _ := bytesPool.Get().(*[]byte)
	return (*bp)[:0]
}

func putBytes(b []byte) { bytesPool.Put(&b) }

var encPool = sync.Pool{New: func() any { return &msgpackEncoder{} }}

func getEncoder(cfg zapcore.EncoderConfig) *msgpackEncoder {
	w, _ := encPool.Get().(*msgpackEncoder)
	w.cfg = cfg
	w.stack = w.stack[:0]
	return w // w.scratch (if any) is retained for reuse
}

func putEncoder(w *msgpackEncoder) {
	for i := range w.stack {
		putBytes(w.stack[i].buf)
		w.stack[i].buf = nil
	}
	w.stack = w.stack[:0]
	encPool.Put(w)
}

// appendMapHeader / appendArrayHeader cast the int count to uint32. count is a per-entry element
// count and cannot realistically exceed math.MaxUint32 (a single log with >4e9 fields is
// impossible).
//
//nolint:gosec // count is a bounded, non-negative per-entry element count; see doc above
func appendMapHeader(b []byte, count int) []byte { return msgp.AppendMapHeader(b, uint32(count)) }

//nolint:gosec // see appendMapHeader
func appendArrayHeader(b []byte, count int) []byte { return msgp.AppendArrayHeader(b, uint32(count)) }
```

- [ ] **Step 4: Write `fluent/msgpack_array.go` (Append skeleton)**

```go
// fluent/msgpack_array.go
package fluent

import (
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap/zapcore"
)

// AppendString writes a string value into the top frame, counting it only in array context (the
// count invariant, design §3.3). It is the one real Append* in the skeleton; the rest are filled
// in Tasks 3–6.
func (e *msgpackEncoder) AppendString(v string) {
	t := e.top()
	t.buf = msgp.AppendString(t.buf, v)
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendBool(bool)             {}
func (e *msgpackEncoder) AppendByteString([]byte)     {}
func (e *msgpackEncoder) AppendComplex128(complex128) {}
func (e *msgpackEncoder) AppendComplex64(complex64)   {}
func (e *msgpackEncoder) AppendFloat64(float64)       {}
func (e *msgpackEncoder) AppendFloat32(float32)       {}
func (e *msgpackEncoder) AppendInt(int)               {}
func (e *msgpackEncoder) AppendInt64(int64)           {}
func (e *msgpackEncoder) AppendInt32(int32)           {}
func (e *msgpackEncoder) AppendInt16(int16)           {}
func (e *msgpackEncoder) AppendInt8(int8)             {}
func (e *msgpackEncoder) AppendUint(uint)             {}
func (e *msgpackEncoder) AppendUint64(uint64)         {}
func (e *msgpackEncoder) AppendUint32(uint32)         {}
func (e *msgpackEncoder) AppendUint16(uint16)         {}
func (e *msgpackEncoder) AppendUint8(uint8)           {}
func (e *msgpackEncoder) AppendUintptr(uintptr)       {}
func (e *msgpackEncoder) AppendDuration(time.Duration) {}
func (e *msgpackEncoder) AppendTime(time.Time)        {}

func (e *msgpackEncoder) AppendArray(zapcore.ArrayMarshaler) error   { return errNotImplemented }
func (e *msgpackEncoder) AppendObject(zapcore.ObjectMarshaler) error { return errNotImplemented }
func (e *msgpackEncoder) AppendReflected(any) error                  { return errNotImplemented }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -run TestNative -race`
Expected: PASS (3 tests). The package compiles (compile-assertions satisfied).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder.go fluent/msgpack_array.go fluent/msgpack_encoder_test.go
git commit -m "feat(fluent): add native msgpack encoder skeleton with two-phase EncodeEntry"
```

---

## Task 3: Scalar types — `Add*` / `Append*` with the count invariant and the §3.8 divergences

Replace the scalar no-op stubs with real implementations. Every `Add*` delegates `addKey` +
`Append*`; floats stringify NaN/±Inf to match zap; binary writes a real msgpack `bin`; complex
writes zap's `"r+ii"` string form. (Time/Duration stay stubbed — they need the hook machinery,
done in Task 4.)

**Files:**
- Modify: `fluent/msgpack_encoder.go` (scalar `Add*`)
- Modify: `fluent/msgpack_array.go` (scalar `Append*`, `appendFloat`, `appendComplex`)
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// add to fluent/msgpack_encoder_test.go
import (
	"math"
	"strings"
	// ...existing imports
)

// encodeFields builds a native entry from fields with an empty cfg and decodes the record.
func encodeFields(t *testing.T, fields ...zapcore.Field) map[string]any {
	t.Helper()
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{}, fields)
	require.NoError(t, err)
	return decodeEntry(t, buf.Bytes()).Record.(map[string]any)
}

func TestNative_ScalarTypes_RoundTrip(t *testing.T) {
	const bigInt = int64(9007199254740993) // 2^53 + 1
	const bigUint = uint64(math.MaxUint64)
	rec := encodeFields(t,
		zapcore.Field{Key: "s", Type: zapcore.StringType, String: "hi"},
		zapcore.Field{Key: "b", Type: zapcore.BoolType, Integer: 1},
		zapcore.Field{Key: "i", Type: zapcore.Int64Type, Integer: bigInt},
		zapcore.Field{Key: "u", Type: zapcore.Uint64Type, Integer: int64(bigUint)},
		zapcore.Field{Key: "f", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(1.5))},
	)
	assert.Equal(t, "hi", rec["s"])
	assert.Equal(t, true, rec["b"])
	assert.Equal(t, bigInt, rec["i"], "int64 above 2^53 exact")
	assert.Equal(t, bigUint, rec["u"], "uint64 above MaxInt64 exact")
	assert.Equal(t, 1.5, rec["f"])
}

func TestNative_FloatNaNInf_Stringified(t *testing.T) {
	rec := encodeFields(t,
		zapcore.Field{Key: "nan", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(math.NaN()))},
		zapcore.Field{Key: "pinf", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(math.Inf(1)))},
		zapcore.Field{Key: "ninf", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(math.Inf(-1)))},
		zapcore.Field{Key: "negz", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(math.Copysign(0, -1)))},
	)
	assert.Equal(t, "NaN", rec["nan"])
	assert.Equal(t, "+Inf", rec["pinf"])
	assert.Equal(t, "-Inf", rec["ninf"])
	assert.Equal(t, math.Copysign(0, -1), rec["negz"], "negative zero is exact, not stringified")
	require.True(t, math.Signbit(rec["negz"].(float64)), "the IEEE sign bit must survive the wire")
}

func TestNative_Binary_IsRealMsgpackBin(t *testing.T) {
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{
		{Key: "raw", Type: zapcore.BinaryType, Interface: []byte{0x00, 0x01, 0xff}},
	})
	require.NoError(t, err)
	rec := decodeEntry(t, buf.Bytes()).Record.(map[string]any)
	assert.Equal(t, []byte{0x00, 0x01, 0xff}, rec["raw"], "AddBinary emits a msgpack bin, not base64")
}

func TestNative_Complex_MatchesZapStringForm(t *testing.T) {
	rec := encodeFields(t,
		zapcore.Field{Key: "c", Type: zapcore.Complex128Type, Interface: complex(1.5, -2)},
	)
	assert.Equal(t, "1.5-2i", rec["c"])
}

func TestNative_FixmapToMap16Boundary(t *testing.T) {
	fields := make([]zapcore.Field, 0, 16)
	for i := range 16 {
		fields = append(fields, zapcore.Field{
			Key: "k" + strings.Repeat("x", i), Type: zapcore.Int64Type, Integer: int64(i),
		})
	}
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{}, fields)
	require.NoError(t, err)
	out := buf.Bytes()
	// out[0]=array, out[1..10]=fixext8 EventTime (10 bytes), out[11]=record map header.
	assert.Equal(t, byte(0xde), out[11], "16 fields must use a map16 header, not fixmap")
	assert.Len(t, decodeEntry(t, out).Record.(map[string]any), 16)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run TestNative_ -race`
Expected: FAIL — scalar fields decode as absent/wrong (no-op stubs).

- [ ] **Step 3: Implement scalar `Append*` + `appendFloat` + `appendComplex` in `msgpack_array.go`**

Replace the scalar no-op stubs with:

```go
import (
	"math"
	"strconv"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap/zapcore"
)

func (e *msgpackEncoder) AppendBool(v bool) {
	t := e.top()
	t.buf = msgp.AppendBool(t.buf, v)
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendInt64(v int64) {
	t := e.top()
	t.buf = msgp.AppendInt64(t.buf, v)
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendUint64(v uint64) {
	t := e.top()
	t.buf = msgp.AppendUint64(t.buf, v)
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendByteString(v []byte) {
	t := e.top()
	t.buf = msgp.AppendStringFromBytes(t.buf, v) // UTF-8 bytes as a msgpack str
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendFloat64(v float64) { e.appendFloat(v, 64) }
func (e *msgpackEncoder) AppendFloat32(v float32) { e.appendFloat(float64(v), 32) }

// appendFloat stringifies NaN/±Inf to "NaN"/"+Inf"/"-Inf" (matching zapcore/json_encoder.go
// appendFloat); finite values use a native msgpack float. Negative zero is finite → exact.
func (e *msgpackEncoder) appendFloat(v float64, bits int) {
	t := e.top()
	switch {
	case math.IsNaN(v):
		t.buf = msgp.AppendString(t.buf, "NaN")
	case math.IsInf(v, 1):
		t.buf = msgp.AppendString(t.buf, "+Inf")
	case math.IsInf(v, -1):
		t.buf = msgp.AppendString(t.buf, "-Inf")
	case bits == 32:
		t.buf = msgp.AppendFloat32(t.buf, float32(v))
	default:
		t.buf = msgp.AppendFloat64(t.buf, v)
	}
	if t.kind == kindArray {
		t.count++
	}
}

func (e *msgpackEncoder) AppendComplex128(v complex128) { e.appendComplex(v, 64) }
func (e *msgpackEncoder) AppendComplex64(v complex64)   { e.appendComplex(complex128(v), 32) }

// appendComplex writes zap's "r+ii" string form (no native msgpack complex type). Real/imag use
// strconv.FormatFloat(_, 'f', -1, bits) — matching zap/buffer.Buffer.AppendFloat.
func (e *msgpackEncoder) appendComplex(v complex128, bits int) {
	r := strconv.FormatFloat(real(v), 'f', -1, bits)
	im := strconv.FormatFloat(imag(v), 'f', -1, bits)
	s := r
	if imag(v) >= 0 {
		s += "+"
	}
	s += im + "i"
	t := e.top()
	t.buf = msgp.AppendString(t.buf, s)
	if t.kind == kindArray {
		t.count++
	}
}

// Width converters widen to the int64/uint64 workhorses (mirrors zap's jsonEncoder).
func (e *msgpackEncoder) AppendInt(v int)         { e.AppendInt64(int64(v)) }
func (e *msgpackEncoder) AppendInt32(v int32)     { e.AppendInt64(int64(v)) }
func (e *msgpackEncoder) AppendInt16(v int16)     { e.AppendInt64(int64(v)) }
func (e *msgpackEncoder) AppendInt8(v int8)       { e.AppendInt64(int64(v)) }
func (e *msgpackEncoder) AppendUint(v uint)       { e.AppendUint64(uint64(v)) }
func (e *msgpackEncoder) AppendUint32(v uint32)   { e.AppendUint64(uint64(v)) }
func (e *msgpackEncoder) AppendUint16(v uint16)   { e.AppendUint64(uint64(v)) }
func (e *msgpackEncoder) AppendUint8(v uint8)     { e.AppendUint64(uint64(v)) }
func (e *msgpackEncoder) AppendUintptr(v uintptr) { e.AppendUint64(uint64(v)) }
```

- [ ] **Step 4: Implement scalar `Add*` in `msgpack_encoder.go`**

Replace the scalar no-op `Add*` stubs (keep `AddString` as-is; `AddDuration`/`AddTime` stay
stubbed until Task 4) with:

```go
func (e *msgpackEncoder) AddBool(k string, v bool)             { e.addKey(k); e.AppendBool(v) }
func (e *msgpackEncoder) AddByteString(k string, v []byte)     { e.addKey(k); e.AppendByteString(v) }
func (e *msgpackEncoder) AddComplex128(k string, v complex128) { e.addKey(k); e.AppendComplex128(v) }
func (e *msgpackEncoder) AddComplex64(k string, v complex64)   { e.addKey(k); e.AppendComplex64(v) }
func (e *msgpackEncoder) AddFloat64(k string, v float64)       { e.addKey(k); e.AppendFloat64(v) }
func (e *msgpackEncoder) AddFloat32(k string, v float32)       { e.addKey(k); e.AppendFloat32(v) }
func (e *msgpackEncoder) AddInt64(k string, v int64)           { e.addKey(k); e.AppendInt64(v) }
func (e *msgpackEncoder) AddUint64(k string, v uint64)         { e.addKey(k); e.AppendUint64(v) }

// AddBinary writes a REAL msgpack bin (not base64-in-a-string as zap's JSON does); design §3.8 #1.
func (e *msgpackEncoder) AddBinary(k string, v []byte) {
	e.addKey(k)
	t := e.top()
	t.buf = msgp.AppendBytes(t.buf, v)
	// map-value context: no element count (the key was counted by addKey).
}

func (e *msgpackEncoder) AddInt(k string, v int)         { e.AddInt64(k, int64(v)) }
func (e *msgpackEncoder) AddInt32(k string, v int32)     { e.AddInt64(k, int64(v)) }
func (e *msgpackEncoder) AddInt16(k string, v int16)     { e.AddInt64(k, int64(v)) }
func (e *msgpackEncoder) AddInt8(k string, v int8)       { e.AddInt64(k, int64(v)) }
func (e *msgpackEncoder) AddUint(k string, v uint)       { e.AddUint64(k, uint64(v)) }
func (e *msgpackEncoder) AddUint32(k string, v uint32)   { e.AddUint64(k, uint64(v)) }
func (e *msgpackEncoder) AddUint16(k string, v uint16)   { e.AddUint64(k, uint64(v)) }
func (e *msgpackEncoder) AddUint8(k string, v uint8)     { e.AddUint64(k, uint64(v)) }
func (e *msgpackEncoder) AddUintptr(k string, v uintptr) { e.AddUint64(k, uint64(v)) }
```

Remove the now-unused `msgp` import guard issues by ensuring `msgp` is imported in both files
(it already is). Delete the scalar no-op stub block this replaces.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -run TestNative_ -race`
Expected: PASS (all scalar tests).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder.go fluent/msgpack_array.go fluent/msgpack_encoder_test.go
git commit -m "feat(fluent): implement native scalar types with msgpack-bin and NaN/Inf parity"
```

---

## Task 4: Hook-driven fields — envelope matrix + `AddTime`/`AddDuration` + the count canary

Fill `writeEnvelope` with the full level/name/caller/function/stack matrix (design §3.4) and
implement the hook-driven `AppendTime`/`AppendDuration` (+ their `Add*`), all sharing the no-op
guard (measure top-buf length before/after the hook; on no-op, append the documented fallback).
This is where `Append*` runs in **map-value context** driven by an encode hook — the count
invariant's other half.

**Files:**
- Modify: `fluent/msgpack_encoder.go` (`writeEnvelope`, `AddTime`/`AddDuration`)
- Modify: `fluent/msgpack_array.go` (`AppendTime`/`AppendDuration`)
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// add to fluent/msgpack_encoder_test.go
import (
	"go.uber.org/zap"
	// ...existing
)

func envCfg() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:    "msg",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "caller",
		FunctionKey:   "func",
		StacktraceKey: "stack",
		EncodeLevel:   zapcore.LowercaseLevelEncoder,
		EncodeCaller:  zapcore.ShortCallerEncoder,
		EncodeTime:    zapcore.EpochNanosTimeEncoder,
		EncodeDuration: zapcore.NanosDurationEncoder,
	}
}

func TestNative_EnvelopeMatrix_AllSet(t *testing.T) {
	enc := newMsgpackEncoder(envCfg())
	caller := zapcore.NewEntryCaller(0, "server/handler.go", 142, true)
	ent := zapcore.Entry{
		Level: zapcore.InfoLevel, Message: "done", LoggerName: "svc",
		Caller: caller, Stack: "goroutine 1 ...",
	}
	rec := decodeEntryRecord(t, enc, ent, nil)
	assert.Equal(t, "info", rec["level"])
	assert.Equal(t, "done", rec["msg"])
	assert.Equal(t, "svc", rec["logger"])
	assert.Equal(t, caller.TrimmedPath(), rec["caller"])
	assert.Equal(t, caller.Function, rec["func"]) // "" is fine; key present
	assert.Equal(t, "goroutine 1 ...", rec["stack"])
}

func TestNative_EnvelopeMatrix_Omissions(t *testing.T) {
	cfg := envCfg()
	cfg.EncodeLevel = nil // level must be OMITTED when EncodeLevel == nil
	enc := newMsgpackEncoder(cfg)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"}, nil)
	_, hasLevel := rec["level"]
	assert.False(t, hasLevel, "level omitted when EncodeLevel is nil")
	// Caller not Defined → no caller/func keys.
	_, hasCaller := rec["caller"]
	_, hasFunc := rec["func"]
	assert.False(t, hasCaller)
	assert.False(t, hasFunc)
}

// Empty-key omission for EVERY envelope field at once (design §3.4 / §5.4): with all keys empty,
// the record map is empty even though the Entry carries level/name/caller/message/stack.
func TestNative_EnvelopeMatrix_AllKeysEmpty(t *testing.T) {
	enc := newMsgpackEncoder(zapcore.EncoderConfig{}) // every entry-level key empty
	ent := zapcore.Entry{
		Level: zapcore.InfoLevel, Message: "m", LoggerName: "n",
		Caller: zapcore.NewEntryCaller(0, "a.go", 1, true), Stack: "trace",
	}
	rec := decodeEntryRecord(t, enc, ent, nil)
	assert.Empty(t, rec, "every envelope field is omitted when its key is empty")
}

func TestNative_CallerNilHook_FallsBackNotPanic(t *testing.T) {
	cfg := envCfg()
	cfg.EncodeCaller = nil // zap would panic; native hardens to Caller.String()
	enc := newMsgpackEncoder(cfg)
	caller := zapcore.NewEntryCaller(0, "a/b.go", 5, true)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{Caller: caller, Message: "m"}, nil)
	assert.Equal(t, caller.String(), rec["caller"])
}

func TestNative_NoOpHook_FallsBackToString(t *testing.T) {
	cfg := envCfg()
	cfg.EncodeLevel = func(zapcore.Level, zapcore.PrimitiveArrayEncoder) {} // writes nothing
	enc := newMsgpackEncoder(cfg)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{Level: zapcore.WarnLevel, Message: "m"}, nil)
	assert.Equal(t, zapcore.WarnLevel.String(), rec["level"], "no-op hook → Level.String() fallback")
}

// CANARY: an entry whose only fields come from encode hooks (level + caller). The record map
// count must equal the number of pairs — an off-by-one from double-counting hook values corrupts.
func TestNative_CountInvariant_HookOnlyEntry(t *testing.T) {
	cfg := zapcore.EncoderConfig{
		LevelKey: "level", CallerKey: "caller",
		EncodeLevel: zapcore.LowercaseLevelEncoder, EncodeCaller: zapcore.ShortCallerEncoder,
	}
	enc := newMsgpackEncoder(cfg)
	ent := zapcore.Entry{Level: zapcore.ErrorLevel, Caller: zapcore.NewEntryCaller(0, "x.go", 1, true)}
	rec := decodeEntryRecord(t, enc, ent, nil) // decodeEntry already asserts no trailing bytes
	assert.Len(t, rec, 2, "exactly {level, caller}: map count == body pairs")
}

func TestNative_DurationAndTimeFields(t *testing.T) {
	enc := newMsgpackEncoder(envCfg())
	rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{
		zap.Duration("d", 1500),
		zap.Time("t", time.Unix(0, 12345)),
	})
	assert.Equal(t, int64(1500), rec["d"], "NanosDurationEncoder → int64 nanos")
	assert.Equal(t, int64(12345), rec["t"], "EpochNanosTimeEncoder → int64 unix nanos")
}

func TestNative_TimeField_NoOpHookFallsBackToUnixNanos(t *testing.T) {
	cfg := minimalCfg()
	cfg.EncodeTime = func(time.Time, zapcore.PrimitiveArrayEncoder) {} // no-op
	enc := newMsgpackEncoder(cfg)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{zap.Time("t", time.Unix(0, 777))})
	assert.Equal(t, int64(777), rec["t"])
}

// Remaining nil/no-op matrix cells (design §3.4 / §5.4): name no-op → LoggerName; caller no-op →
// Caller.String(); duration nil AND no-op → int64 nanos; time nil → unix nanos. Each decodes and
// asserts the fallback value (and that exactly one key is present — no key-without-value).
func TestNative_NameNoOpHook_FallsBackToLoggerName(t *testing.T) {
	cfg := envCfg()
	cfg.EncodeName = func(string, zapcore.PrimitiveArrayEncoder) {} // no-op
	enc := newMsgpackEncoder(cfg)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{LoggerName: "svc"}, nil)
	assert.Equal(t, "svc", rec["logger"])
}

func TestNative_CallerNoOpHook_FallsBackToCallerString(t *testing.T) {
	cfg := envCfg()
	cfg.EncodeCaller = func(zapcore.EntryCaller, zapcore.PrimitiveArrayEncoder) {} // no-op
	enc := newMsgpackEncoder(cfg)
	caller := zapcore.NewEntryCaller(0, "a/b.go", 5, true)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{Caller: caller}, nil)
	assert.Equal(t, caller.String(), rec["caller"])
}

func TestNative_DurationField_NilAndNoOpFallBackToNanos(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*zapcore.EncoderConfig)
	}{
		{"nil", func(c *zapcore.EncoderConfig) { c.EncodeDuration = nil }},
		{"noop", func(c *zapcore.EncoderConfig) {
			c.EncodeDuration = func(time.Duration, zapcore.PrimitiveArrayEncoder) {}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := minimalCfg() // empty envelope keys, so the record holds only the duration field
			tc.mod(&cfg)
			enc := newMsgpackEncoder(cfg)
			rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{zap.Duration("d", 1500)})
			assert.Equal(t, int64(1500), rec["d"], "duration falls back to int64 nanos")
			assert.Len(t, rec, 1, "exactly one pair — no key-without-value")
		})
	}
}

func TestNative_TimeField_NilHookFallsBackToUnixNanos(t *testing.T) {
	cfg := minimalCfg()
	cfg.EncodeTime = nil
	enc := newMsgpackEncoder(cfg)
	rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{zap.Time("t", time.Unix(0, 4242))})
	assert.Equal(t, int64(4242), rec["t"])
	assert.Len(t, rec, 1)
}
```

Add the shared helper to `msgpack_encoder_test.go`:

```go
func decodeEntryRecord(t *testing.T, enc *msgpackEncoder, ent zapcore.Entry, fields []zapcore.Field) map[string]any {
	t.Helper()
	buf, err := enc.EncodeEntry(ent, fields)
	require.NoError(t, err)
	return decodeEntry(t, buf.Bytes()).Record.(map[string]any)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run 'TestNative_(Envelope|Caller|Name|NoOp|Count|Duration|Time)' -race`
Expected: FAIL — envelope writes only the message; time/duration are no-ops.

- [ ] **Step 3: Implement the full `writeEnvelope` (replace the message-only version)**

```go
// writeEnvelope writes the entry-level fields into the current (root) map frame, mirroring
// jsonEncoder.EncodeEntry's field handling (design §3.4). Order differs from zap (we make no
// duplicate-key last-wins guarantee, §3.8) but every field lands at the record ROOT. Time is
// excluded — it is the [time, record] extension. Each hook path uses a no-op guard so a key is
// never written without a value.
func (e *msgpackEncoder) writeEnvelope(ent zapcore.Entry) {
	cfg := e.cfg

	// Level: omitted entirely when EncodeLevel == nil (design §3.4 matrix).
	if cfg.LevelKey != "" && cfg.EncodeLevel != nil {
		e.addKey(cfg.LevelKey)
		before := len(e.top().buf)
		cfg.EncodeLevel(ent.Level, e)
		if len(e.top().buf) == before {
			e.AppendString(ent.Level.String()) // no-op hook fallback
		}
	}

	if cfg.NameKey != "" && ent.LoggerName != "" {
		e.addKey(cfg.NameKey)
		nameEnc := cfg.EncodeName
		if nameEnc == nil {
			nameEnc = zapcore.FullNameEncoder // zap's backwards-compat default
		}
		before := len(e.top().buf)
		nameEnc(ent.LoggerName, e)
		if len(e.top().buf) == before {
			e.AppendString(ent.LoggerName)
		}
	}

	if ent.Caller.Defined {
		if cfg.CallerKey != "" {
			e.addKey(cfg.CallerKey)
			if cfg.EncodeCaller == nil {
				// Hardening divergence (design §3.4 ‡): zap calls EncodeCaller directly and
				// panics if nil; native falls back so a misconfigured config degrades.
				e.AppendString(ent.Caller.String())
			} else {
				before := len(e.top().buf)
				cfg.EncodeCaller(ent.Caller, e)
				if len(e.top().buf) == before {
					e.AppendString(ent.Caller.String())
				}
			}
		}
		if cfg.FunctionKey != "" {
			e.addKey(cfg.FunctionKey)
			e.AppendString(ent.Caller.Function) // emitted even when "" (matches zap)
		}
	}

	if cfg.MessageKey != "" {
		e.addKey(cfg.MessageKey)
		e.AppendString(ent.Message)
	}

	if cfg.StacktraceKey != "" && ent.Stack != "" {
		e.addKey(cfg.StacktraceKey)
		e.AppendString(ent.Stack)
	}
}
```

- [ ] **Step 4: Implement `AppendTime`/`AppendDuration` (array file) and `AddTime`/`AddDuration`**

In `msgpack_array.go`, replace the time/duration no-op stubs:

```go
// AppendDuration runs EncodeDuration (which calls back into Append* to write + count the value);
// a no-op hook falls back to int64 nanoseconds. The inner Append* owns the count (invariant §3.3),
// so AppendDuration never counts directly.
func (e *msgpackEncoder) AppendDuration(v time.Duration) {
	before := len(e.top().buf)
	if e.cfg.EncodeDuration != nil {
		e.cfg.EncodeDuration(v, e)
	}
	if len(e.top().buf) == before {
		e.AppendInt64(int64(v))
	}
}

func (e *msgpackEncoder) AppendTime(v time.Time) {
	before := len(e.top().buf)
	if e.cfg.EncodeTime != nil {
		e.cfg.EncodeTime(v, e)
	}
	if len(e.top().buf) == before {
		e.AppendInt64(v.UnixNano())
	}
}
```

In `msgpack_encoder.go`, replace the `AddTime`/`AddDuration` stubs:

```go
func (e *msgpackEncoder) AddDuration(k string, v time.Duration) { e.addKey(k); e.AppendDuration(v) }
func (e *msgpackEncoder) AddTime(k string, v time.Time)         { e.addKey(k); e.AppendTime(v) }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -race`
Expected: PASS (all prior + new tests; the count canary green).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder.go fluent/msgpack_array.go fluent/msgpack_encoder_test.go
git commit -m "feat(fluent): add native envelope matrix and hook-driven time/duration fields"
```

---

## Task 5: Containers — objects, arrays, and object-scoped namespaces

Replace the container stubs. `AddObject`/`AddArray` delegate `addKey` + `Append{Object,Array}`;
`AppendObject`/`AppendArray` implement the depth-scoped push/run/`sealDownTo(d+1)`/`sealDownTo(d)`
algorithm (design §3.2). This makes namespaces opened **inside** an `ObjectMarshaler` scoped to
that object, and lets the **carried top-level** `With` namespace round-trip (envelope-at-root
parity, the pass-2 P0).

**Files:**
- Modify: `fluent/msgpack_encoder.go` (`AddObject`/`AddArray`)
- Modify: `fluent/msgpack_array.go` (`AppendObject`/`AppendArray`)
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// add to fluent/msgpack_encoder_test.go
import "go.uber.org/zap/zapcore" // already imported

// objMarshaler / arrMarshaler are tiny inline marshalers for container tests.
type objMarshaler func(zapcore.ObjectEncoder) error

func (f objMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error { return f(enc) }

type arrMarshaler func(zapcore.ArrayEncoder) error

func (f arrMarshaler) MarshalLogArray(enc zapcore.ArrayEncoder) error { return f(enc) }

func TestNative_NestedObject(t *testing.T) {
	rec := encodeFields(t, zap.Object("o", objMarshaler(func(enc zapcore.ObjectEncoder) error {
		enc.AddString("a", "1")
		enc.AddInt64("b", 2)
		return nil
	})))
	inner := rec["o"].(map[string]any)
	assert.Equal(t, "1", inner["a"])
	assert.Equal(t, int64(2), inner["b"])
	assert.Len(t, inner, 2)
}

func TestNative_ArrayOfObjects(t *testing.T) {
	rec := encodeFields(t, zap.Array("xs", arrMarshaler(func(enc zapcore.ArrayEncoder) error {
		for i := range 3 {
			_ = enc.AppendObject(objMarshaler(func(o zapcore.ObjectEncoder) error {
				o.AddInt64("i", int64(i))
				return nil
			}))
		}
		return nil
	})))
	xs := rec["xs"].([]any)
	require.Len(t, xs, 3)
	assert.Equal(t, int64(2), xs[2].(map[string]any)["i"])
}

// Object-scoped namespace: a namespace opened INSIDE an ObjectMarshaler must close with the
// object; a sibling call-site field lands OUTSIDE it (design §3.2 / §5.4).
func TestNative_ObjectScopedNamespace(t *testing.T) {
	rec := encodeFields(t,
		zap.Object("obj", objMarshaler(func(enc zapcore.ObjectEncoder) error {
			enc.OpenNamespace("inner")
			enc.AddInt64("deep", 9)
			return nil
		})),
		zap.Int("sibling", 7),
	)
	obj := rec["obj"].(map[string]any)
	innerNS := obj["inner"].(map[string]any)
	assert.Equal(t, int64(9), innerNS["deep"])
	assert.Equal(t, int64(7), rec["sibling"], "sibling is at root, OUTSIDE the object's namespace")
	_, leaked := rec["inner"]
	assert.False(t, leaked, "the object-scoped namespace must not leak to the record root")
}

// Envelope-at-root parity (the pass-2 P0): With(Namespace+field).Info(msg, field).
func TestNative_EnvelopeAtRoot_WithCarriedNamespace(t *testing.T) {
	base := newMsgpackEncoder(envCfg())
	cloned := base.Clone()
	zap.Namespace("ns").AddTo(cloned)
	zap.Int("a", 1).AddTo(cloned)
	buf, err := cloned.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zapcore.Field{zap.Int("b", 2)})
	require.NoError(t, err)
	rec := decodeEntry(t, buf.Bytes()).Record.(map[string]any)

	assert.Equal(t, "info", rec["level"], "envelope at ROOT, not inside ns")
	assert.Equal(t, "m", rec["msg"])
	ns := rec["ns"].(map[string]any)
	assert.Equal(t, int64(1), ns["a"], "With field inside ns")
	assert.Equal(t, int64(2), ns["b"], "call-site field inside the carried-open ns")
	assert.Len(t, ns, 2)
}

// Golden: a single nested namespace, exact element counts via decode.
func TestNative_NamespaceGolden(t *testing.T) {
	base := newMsgpackEncoder(minimalCfg())
	cloned := base.Clone()
	zap.Namespace("ns").AddTo(cloned)
	zap.String("x", "y").AddTo(cloned)
	buf, err := cloned.EncodeEntry(zapcore.Entry{}, nil)
	require.NoError(t, err)
	rec := decodeEntry(t, buf.Bytes()).Record.(map[string]any)
	assert.Len(t, rec, 1)
	assert.Equal(t, map[string]any{"x": "y"}, rec["ns"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run 'TestNative_(Nested|ArrayOf|Object|Envelope|Namespace)' -race`
Expected: FAIL — `AddObject`/`AddArray` return `errNotImplemented` → fields become `<key>Error`.

- [ ] **Step 3: Implement `AppendObject`/`AppendArray` in `msgpack_array.go`**

```go
// AppendObject pushes a map frame, runs the marshaler, then seals: sealDownTo(d+1) collapses any
// namespaces the marshaler opened INTO the object frame (object-scoped, design §3.2), and
// sealDownTo(d) seals the object into its parent. In array context the object is an element, so
// count it on the parent now (the only count site for a nested container).
func (e *msgpackEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	d := len(e.stack)
	if e.stack[d-1].kind == kindArray {
		e.stack[d-1].count++
	}
	e.stack = append(e.stack, frame{kind: kindMap, buf: getBytes()})
	err := obj.MarshalLogObject(e)
	e.sealDownTo(d + 1)
	e.sealDownTo(d)
	return err
}

// AppendArray mirrors AppendObject for a kindArray frame. (Arrays cannot open namespaces — the
// ArrayEncoder has no OpenNamespace — but a nested AppendObject seals itself, so after the
// marshaler the stack is back to d+1; sealDownTo(d+1) is a defensive no-op.)
func (e *msgpackEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	d := len(e.stack)
	if e.stack[d-1].kind == kindArray {
		e.stack[d-1].count++
	}
	e.stack = append(e.stack, frame{kind: kindArray, buf: getBytes()})
	err := arr.MarshalLogArray(e)
	e.sealDownTo(d + 1)
	e.sealDownTo(d)
	return err
}
```

- [ ] **Step 4: Implement `AddObject`/`AddArray` in `msgpack_encoder.go`**

```go
func (e *msgpackEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	e.addKey(key)
	return e.AppendObject(obj)
}

func (e *msgpackEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	e.addKey(key)
	return e.AppendArray(arr)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -race`
Expected: PASS (all container + parity tests).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder.go fluent/msgpack_array.go fluent/msgpack_encoder_test.go
git commit -m "feat(fluent): add native objects, arrays and object-scoped namespaces"
```

---

## Task 6: Reflected values — transactional two-tier `AddReflected`/`AppendReflected`

Replace the reflected stubs with the transactional scratch-encode-then-commit algorithm (design
§3.7): Tier 1 `msgp.AppendIntf`; on error Tier 2 `json.Marshal` → `UseNumber` decode →
`normalizeNumber` → `msgp.AppendIntf`. Encode into the per-instance `scratch` first; commit (write
key + bytes) only on success, so a partial-write from `AppendIntf` never reaches a frame. On total
failure return the error → zap adds `<key>Error`, entry ships.

**Files:**
- Modify: `fluent/msgpack_encoder.go` (`AddReflected`, `encodeReflected`)
- Modify: `fluent/msgpack_array.go` (`AppendReflected`)
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// add to fluent/msgpack_encoder_test.go — add "strconv" and "sync" to the import block
// ("go.uber.org/zap/zapcore", "go.uber.org/zap", and "math" are already imported from Tasks 2–4).
import (
	"strconv"
	"sync"
)

type bareUint64 uint64 // MarshalJSON returns a bare unsigned number > 2^63

func (b bareUint64) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatUint(uint64(b), 10)), nil
}

// recordingWS is an in-memory zapcore.WriteSyncer that captures every Write, used to prove zap's
// ioCore.Write was actually called (it aborts the write on a non-nil EncodeEntry error).
type recordingWS struct {
	mu     sync.Mutex
	writes [][]byte
}

func (r *recordingWS) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := make([]byte, len(p))
	copy(b, p)
	r.writes = append(r.writes, b)
	return len(p), nil
}

func (r *recordingWS) Sync() error { return nil }

func TestNative_Reflected_PrimitivesMapsSlices(t *testing.T) {
	rec := encodeFields(t,
		zap.Any("p", 42),
		zap.Any("m", map[string]any{"k": "v"}),
		zap.Any("s", []int{1, 2, 3}),
	)
	assert.EqualValues(t, 42, rec["p"])
	assert.Equal(t, map[string]any{"k": "v"}, rec["m"])
	assert.Equal(t, []any{int64(1), int64(2), int64(3)}, rec["s"])
}

func TestNative_Reflected_ArbitraryStruct_Tier2(t *testing.T) {
	type inner struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	rec := encodeFields(t, zap.Any("o", inner{A: "x", B: 5}))
	assert.Equal(t, map[string]any{"a": "x", "b": int64(5)}, rec["o"])
}

func TestNative_Reflected_TopLevelBigUint(t *testing.T) {
	const big = uint64(math.MaxUint64) // > 2^63
	rec := encodeFields(t, zap.Any("u", bareUint64(big)))
	assert.Equal(t, big, rec["u"], "Tier-2 normalizeNumber preserves a top-level json.Number > 2^63")
}

// Tier-1 writes a PARTIAL container (header + earlier elements) then fails on the nested
// struct{}{} (AppendIntf rejects arbitrary structs); Tier-2 (json.Marshal → {}) succeeds. The
// committed bytes must be ONLY the Tier-2 result — no duplicated/partial header. encodeFields'
// decodeEntry asserts no trailing bytes, so a corrupt rollback fails to decode. (Prior-P0
// partial-write case, design §3.7; robust to Go's randomized map order.)
func TestNative_Reflected_PartialWriteRolledBack_Map(t *testing.T) {
	rec := encodeFields(t, zap.Any("m", map[string]any{"ok": 1, "bad": struct{}{}}))
	assert.Equal(t, map[string]any{"ok": int64(1), "bad": map[string]any{}}, rec["m"])
	assert.NotContains(t, rec, "mError", "Tier-2 succeeded — no <key>Error")
}

func TestNative_Reflected_PartialWriteRolledBack_Slice(t *testing.T) {
	rec := encodeFields(t, zap.Any("s", []any{1, struct{}{}}))
	assert.Equal(t, []any{int64(1), map[string]any{}}, rec["s"])
	assert.NotContains(t, rec, "sError")
}

// Total failure: a value both tiers reject (chan) → field becomes <key>Error, entry intact.
func TestNative_Reflected_TotalFailure_BecomesKeyError(t *testing.T) {
	rec := encodeFields(t, zap.Any("m", map[string]any{"ok": 1, "bad": make(chan int)}))
	_, hasM := rec["m"]
	assert.False(t, hasM)
	assert.Contains(t, rec, "mError")
}

// Entry-always-ships proven THROUGH zap's ioCore (not by calling EncodeEntry directly): a
// recording WriteSyncer must receive the framed entry even though one field is unencodable. zap
// aborts the write on a non-nil EncodeEntry error (zapcore/core.go:94-100), so a recorded Write
// proves EncodeEntry returned nil and degraded the bad field to <key>Error (design §3.9 / §5.4).
func TestNative_Unencodable_EntryStillShipsThroughCore(t *testing.T) {
	ws := &recordingWS{}
	core := zapcore.NewCore(newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"}), ws, zapcore.InfoLevel)
	zap.New(core).Info("keep", zap.Any("c", make(chan int)))
	require.Len(t, ws.writes, 1, "ioCore.Write must be called — a bad field must not abort the entry")
	rec := decodeEntry(t, ws.writes[0]).Record.(map[string]any)
	assert.Equal(t, "keep", rec["msg"])
	assert.Contains(t, rec, "cError")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run 'TestNative_(Reflected|Unencodable)' -race`
Expected: FAIL — `AddReflected` returns `errNotImplemented` (every reflected field becomes
`<key>Error`).

- [ ] **Step 3: Implement `encodeReflected` + `AddReflected` in `msgpack_encoder.go`**

Add imports `bytes`, `encoding/json`:

```go
// encodeReflected serializes value into e.scratch transactionally (design §3.7). e is the per-call
// working clone or the per-With clone, so e.scratch is private — never shared across concurrent
// encodes. On success it returns the committed scratch slice; on failure NOTHING is committed.
func (e *msgpackEncoder) encodeReflected(value any) ([]byte, error) {
	b, err := msgp.AppendIntf(e.scratch[:0], value) // Tier 1 (fast)
	if err == nil {
		e.scratch = b
		return b, nil
	}
	// Tier 2 (fidelity == transcode): JSON round-trip with integer-preserving normalization.
	j, jerr := json.Marshal(value)
	if jerr != nil {
		return nil, jerr // caller returns err → zap adds <key>Error (design §3.9)
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(j))
	dec.UseNumber()
	if derr := dec.Decode(&v); derr != nil {
		return nil, derr
	}
	v = normalizeNumber(v) // singular: handles a TOP-LEVEL json.Number too (not normalizeNumbers)
	b, err = msgp.AppendIntf(e.scratch[:0], v)
	if err != nil {
		return nil, err
	}
	e.scratch = b
	return b, nil
}

func (e *msgpackEncoder) AddReflected(key string, value any) error {
	b, err := e.encodeReflected(value)
	if err != nil {
		return err // nothing written; zap adds <key>Error, entry ships
	}
	e.addKey(key)
	t := e.top()
	t.buf = append(t.buf, b...)
	return nil
}
```

- [ ] **Step 4: Implement `AppendReflected` in `msgpack_array.go`**

```go
func (e *msgpackEncoder) AppendReflected(value any) error {
	b, err := e.encodeReflected(value)
	if err != nil {
		return err // element omitted; error propagates up to <key>Error
	}
	t := e.top()
	t.buf = append(t.buf, b...)
	if t.kind == kindArray {
		t.count++
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -race`
Expected: PASS (all reflected + prior tests).

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder.go fluent/msgpack_array.go fluent/msgpack_encoder_test.go
git commit -m "feat(fluent): add transactional two-tier reflected value encoding"
```

---

## Task 7: Error contract, duplicate keys, and `With`-error isolation

The mechanics already return errors from methods; this task **pins the contract** with tests:
entry always ships (recording WriteSyncer), nested `Append*` error propagation (returns vs.
swallows), duplicate-key preservation, and a bad `With` field isolated to its own `<key>Error`
without poisoning later entries. No production code change is expected — if a test fails, fix the
specific method, not the test.

**Files:**
- Test: `fluent/msgpack_encoder_test.go`

- [ ] **Step 1: Write the failing/locking tests**

```go
// add to fluent/msgpack_encoder_test.go (no new imports beyond those already present)

// failObj returns an error from a nested AddReflected via its marshaler (propagates → <key>Error).
type failObj struct{ swallow bool }

func (f failObj) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	err := enc.AddReflected("inner", make(chan int)) // unencodable → error
	if f.swallow {
		return nil // marshaler swallows it → no <key>Error (design §3.9)
	}
	return err
}

func TestNative_NestedError_PropagatesToKeyError(t *testing.T) {
	rec := encodeFields(t, zap.Object("o", failObj{swallow: false}))
	assert.Contains(t, rec, "oError", "a returned nested error becomes <key>Error")
}

func TestNative_NestedError_SwallowedYieldsNoKeyError(t *testing.T) {
	rec := encodeFields(t, zap.Object("o", failObj{swallow: true}))
	_, hasErr := rec["oError"]
	assert.False(t, hasErr, "a swallowed error yields no <key>Error (native makes no promise)")
	assert.Contains(t, rec, "o")
}

// failArr returns (or swallows) an AppendReflected error from inside an ArrayMarshaler. This is the
// ArrayEncoder.Append* path — distinct error-returning code from the ObjectEncoder.Add* path above
// (design §3.9 / §5.4). zap's Field.AddTo converts the returned AddArray error into <key>Error.
type failArr struct{ swallow bool }

func (f failArr) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	err := enc.AppendReflected(make(chan int)) // unencodable → AppendReflected returns an error
	if f.swallow {
		return nil // marshaler swallows it → no <key>Error
	}
	return err
}

func TestNative_NestedAppendError_PropagatesToKeyError(t *testing.T) {
	rec := encodeFields(t, zap.Array("a", failArr{swallow: false}))
	assert.Contains(t, rec, "aError", "a returned nested AppendReflected error becomes <key>Error")
}

func TestNative_NestedAppendError_SwallowedYieldsNoKeyError(t *testing.T) {
	rec := encodeFields(t, zap.Array("a", failArr{swallow: true}))
	_, hasErr := rec["aError"]
	assert.False(t, hasErr, "a swallowed AppendReflected error yields no <key>Error")
	assert.Contains(t, rec, "a", "the (empty) array value is still present")
}

// Duplicate keys are preserved on the wire (design §3.8 #3). This case forces the SAME root key
// "dup" through the envelope (MessageKey), the With phase, and two call-site fields — four root
// pairs, none deduped. Go maps dedup, so count raw msgpack pairs via recordKeys.
func TestNative_DuplicateKeys_RootAcrossPhases(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "dup"}) // envelope contributes a root "dup"
	cloned := base.Clone()
	zap.String("dup", "with").AddTo(cloned) // With phase, root
	buf, err := cloned.EncodeEntry(zapcore.Entry{Message: "envmsg"}, []zapcore.Field{
		zap.String("dup", "call1"), // call-site, root
		zap.String("dup", "call2"), // call-site, root
	})
	require.NoError(t, err)

	keys := recordKeys(t, buf.Bytes())
	require.Len(t, keys, 4, "envelope + With + two call-site 'dup' pairs — no dedup")
	dup := 0
	for _, k := range keys {
		if k == "dup" {
			dup++
		}
	}
	assert.Equal(t, 4, dup, "all four duplicate 'dup' pairs survive at the record root")
}

// Duplicate keys inside a carried top-level NAMESPACE are also preserved (the §3.5 seal path):
// a With field and a call-site field share the key "dup" inside namespace "ns". namespaceKeys
// reads the namespace map's raw pairs (Go maps would dedup them away).
func TestNative_DuplicateKeys_InsideNamespacePreserved(t *testing.T) {
	base := newMsgpackEncoder(minimalCfg())
	cloned := base.Clone()
	zap.Namespace("ns").AddTo(cloned)       // opens a top-level namespace, carried to EncodeEntry
	zap.String("dup", "with").AddTo(cloned) // inside ns (With phase)
	buf, err := cloned.EncodeEntry(zapcore.Entry{}, []zapcore.Field{
		zap.String("dup", "call"), // inside ns (call-site lands in the still-open namespace)
	})
	require.NoError(t, err)

	nsKeys := namespaceKeys(t, buf.Bytes(), "ns")
	assert.Equal(t, []string{"dup", "dup"}, nsKeys, "both duplicate pairs survive inside the namespace")
}

func TestNative_BadWithField_DoesNotPoisonLaterEntries(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	cloned := base.Clone()
	zap.Any("bad", make(chan int)).AddTo(cloned) // bad With field → badError in the clone's stack
	buf, err := cloned.EncodeEntry(zapcore.Entry{Message: "first"}, nil)
	require.NoError(t, err)
	rec := decodeEntry(t, buf.Bytes()).Record.(map[string]any)
	assert.Equal(t, "first", rec["msg"])
	assert.Contains(t, rec, "badError")
	// A second entry from the SAME clone still ships and is not corrupted.
	buf2, err := cloned.EncodeEntry(zapcore.Entry{Message: "second"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "second", decodeEntry(t, buf2.Bytes()).Record.(map[string]any)["msg"])
}
```

Add `recordKeys` (returns every record map key in wire order, WITHOUT Go-map dedup) to
`msgpack_encoder_test.go`:

```go
func recordKeys(t *testing.T, entry []byte) []string {
	t.Helper()
	_, o, err := msgp.ReadArrayHeaderBytes(entry) // [time, record]
	require.NoError(t, err)
	o, err = msgp.Skip(o) // skip the EventTime extension
	require.NoError(t, err)
	n, o, err := msgp.ReadMapHeaderBytes(o)
	require.NoError(t, err)
	keys := make([]string, 0, n)
	for range int(n) {
		var k string
		k, o, err = msgp.ReadStringBytes(o)
		require.NoError(t, err)
		o, err = msgp.Skip(o) // value
		require.NoError(t, err)
		keys = append(keys, k)
	}
	require.Empty(t, o, "no trailing bytes after the record map")
	return keys
}

// namespaceKeys returns the raw keys (no Go-map dedup) of the map stored under the root key nsKey.
func namespaceKeys(t *testing.T, entry []byte, nsKey string) []string {
	t.Helper()
	_, o, err := msgp.ReadArrayHeaderBytes(entry) // [time, record]
	require.NoError(t, err)
	o, err = msgp.Skip(o) // EventTime extension
	require.NoError(t, err)
	n, o, err := msgp.ReadMapHeaderBytes(o)
	require.NoError(t, err)
	for range int(n) {
		var k string
		k, o, err = msgp.ReadStringBytes(o)
		require.NoError(t, err)
		if k == nsKey {
			m, rest, merr := msgp.ReadMapHeaderBytes(o)
			require.NoError(t, merr)
			keys := make([]string, 0, m)
			for range int(m) {
				var ik string
				ik, rest, merr = msgp.ReadStringBytes(rest)
				require.NoError(t, merr)
				rest, merr = msgp.Skip(rest) // value
				require.NoError(t, merr)
				keys = append(keys, ik)
			}
			return keys
		}
		o, err = msgp.Skip(o) // not the target — skip its value
		require.NoError(t, err)
	}
	t.Fatalf("namespace key %q not found in record", nsKey)
	return nil
}
```

(Requires `"github.com/tinylib/msgp/msgp"` in the test imports.)

- [ ] **Step 2: Run tests to verify they pass (or surface a real gap)**

Run: `go test ./fluent/ -run 'TestNative_(Nested|Duplicate|BadWith)' -race`
Expected: PASS. If `TestNative_BadWithField...` fails, the `With`-clone path mishandles a field
error — fix in `Clone`/`AddReflected`, not the test.

- [ ] **Step 3: Lint and commit**

```bash
make lint
git add fluent/msgpack_encoder_test.go
git commit -m "test(fluent): pin native error contract, duplicate keys and With isolation"
```

---

## Task 8: Equivalence oracle — native vs the tested transcode path

For the unique-key scalar subset, feed identical fields through both `NewEncoder` (transcode) and
the native encoder, decode both, and assert equal records (design §5.3). This is the third
independent oracle (the transcode path is itself tested), catching any wire drift the golden and
decode oracles miss. **Excluded by design:** binary, nanosecond envelope precision, duplicate keys,
invalid UTF-8, reflected structs.

**Files:**
- Create: `fluent/msgpack_equiv_test.go`

- [ ] **Step 1: Write the failing test**

```go
// fluent/msgpack_equiv_test.go
package fluent

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// transcodeRecord runs fields through a real zap JSON encoder + the transcode Encoder and returns
// the decoded record (the proven baseline).
func transcodeRecord(t *testing.T, cfg zapcore.EncoderConfig, ent zapcore.Entry, fields []zapcore.Field) map[string]any {
	t.Helper()
	jsonEnc := zapcore.NewJSONEncoder(cfg)
	buf, err := jsonEnc.EncodeEntry(ent, fields)
	require.NoError(t, err)
	out, err := NewEncoderWithCodec(EpochNanosCodec(cfg.TimeKey)).Encode(nil, buf.Bytes())
	require.NoError(t, err)
	return decodeEntry(t, out).Record.(map[string]any)
}

func TestNative_EquivalentToTranscode_ScalarSubset(t *testing.T) {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "req done", Time: time.Unix(1692959400, 0)}
	fields := []zapcore.Field{
		zap.String("svc", "zapwire"),
		zap.Bool("ok", true),
		zap.Int64("id", 9007199254740993), // > 2^53
		zap.Uint64("big", math.MaxUint64),
		zap.Float64("ratio", 1.5),
		zap.Float64("nan", math.NaN()),
		zap.Float64("pinf", math.Inf(1)),
	}

	native := decodeEntryRecord(t, newMsgpackEncoder(cfg), ent, fields)
	transcoded := transcodeRecord(t, cfg, ent, fields)

	// Both lift time out of the record (native via the extension; transcode via the codec key),
	// so compare the remaining record fields.
	delete(transcoded, "ts")
	assert.Equal(t, transcoded, native, "native record must equal the transcode record for the scalar subset")
}
```

(Confirm `EpochNanosCodec` exists in `fluent/timecodec.go`; if the helper is named differently,
use the matching built-in. The codec only governs the transcode side here.)

- [ ] **Step 2: Run the test to verify it fails, then passes**

Run: `go test ./fluent/ -run TestNative_EquivalentToTranscode -race`
Expected: PASS (the encoder is complete for scalars). If the records differ, the diff localizes
the divergence — investigate before proceeding.

- [ ] **Step 3: Lint and commit**

```bash
make lint
git add fluent/msgpack_equiv_test.go
git commit -m "test(fluent): assert native encoder equivalence with the transcode path"
```

---

## Task 9: Presets — `NewMsgpackEncoder`, `NewNativeWriter`, `NewNativeCore`

Wire the encoder into the public API: refactor `buildWriter` to accept the chosen
`zapwire.Encoder` (shared by transcode and native), then add `native.go`. `WithTimeCodec`/
`WithTimeKey` are accepted but no-ops on the native path (documented; envelope time is structural).
Add an end-to-end test through a real `Writer` + `Framer` (the framer integration the spec asks for).

**Files:**
- Modify: `fluent/fluent.go` (`buildWriter` signature + callers)
- Create: `fluent/native.go`
- Create: `fluent/native_testsupport_test.go` (package-local UDS read server)
- Test: `fluent/native_test.go`

- [ ] **Step 0: Add package-local test support (`fluent/native_testsupport_test.go`)**

`randomSocketPath`/`startReadServer` exist only in the root package's `testsupport_test.go`, which
is **not importable** across packages. Add `fluent`-local equivalents (reused by Task 10):

```go
// fluent/native_testsupport_test.go
package fluent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func randomSocketPath(t testing.TB) string {
	t.Helper()
	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_native_%d.sock", time.Now().UnixNano()))
}

// nativeReadServer accepts one UDS connection and streams each read to recv.
type nativeReadServer struct {
	ln   net.Listener
	recv chan []byte
	path string
}

func startReadServer(t testing.TB, path string) *nativeReadServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &nativeReadServer{ln: ln, recv: make(chan []byte, 64), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				s.recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()
	return s
}

func (s *nativeReadServer) stop() { _ = s.ln.Close(); _ = os.Remove(s.path) }

// drainServer accepts one UDS connection and DISCARDS everything it reads — it never applies
// backpressure, so high-volume concurrency smoke tests (Task 10) cannot stall on a full channel.
type drainServer struct {
	ln   net.Listener
	path string
}

func startDrainServer(t testing.TB, path string) *drainServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &drainServer{ln: ln, path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		for {
			if _, rerr := conn.Read(buf); rerr != nil {
				return
			}
		}
	}()
	return s
}

func (s *drainServer) stop() { _ = s.ln.Close(); _ = os.Remove(s.path) }
```

- [ ] **Step 1: Write the failing tests**

```go
// fluent/native_test.go
package fluent

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	zapwire "github.com/arloliu/zapwire"
)

func TestNewMsgpackEncoder_ImplementsEncoder(t *testing.T) {
	var _ zapcore.Encoder = NewMsgpackEncoder(zap.NewProductionEncoderConfig())
}

func TestNewNativeCore_EndToEndThroughWriterAndFramer(t *testing.T) {
	path := randomSocketPath(t) // from Step 0's native_testsupport_test.go
	srv := startReadServer(t, path)
	defer srv.stop()

	cfg := zap.NewProductionEncoderConfig()
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, cfg, WithTag("app.logs"))
	require.NoError(t, err)
	defer w.Close()
	require.Eventually(t, w.IsConnected, time.Second, 5*time.Millisecond)

	logger := zap.New(core)
	logger.Info("hello", zap.String("k", "v"), zap.Int("n", 1))
	require.NoError(t, w.Sync())

	frame := <-srv.recv
	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	rec := entries[0].Record.(map[string]any)
	require.Equal(t, "hello", rec["msg"])
	require.Equal(t, "v", rec["k"])
	require.EqualValues(t, 1, rec["n"])
}

func TestNewNativeCore_TimeOptionsAreNoOpNotError(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()
	cfg := zap.NewProductionEncoderConfig()
	_, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, cfg,
		WithTimeCodec(EpochMillisCodec("whatever")), WithTimeKey("ignored"))
	require.NoError(t, err, "time options are accepted as no-ops on the native path")
	_ = w.Close()
}

// Framer integration (design §5.4): multiple native payloads concatenate into one PackedForward
// entries bin; the bin length equals the sum of payloads and size equals the payload count. Enough
// ~50-byte entries push the bin past 256 bytes into the bin16 (0xc5) branch. Reuses binHeaderOf /
// decodePackedForward from framer_test.go.
func TestNative_FramerIntegration_MultiPayloadBin16(t *testing.T) {
	enc := NewMsgpackEncoder(zap.NewProductionEncoderConfig())
	mkPayload := func(i int) []byte {
		buf, err := enc.EncodeEntry(
			zapcore.Entry{Level: zapcore.InfoLevel, Message: strings.Repeat("x", 40) + fmt.Sprint(i)}, nil)
		require.NoError(t, err)
		b := make([]byte, len(buf.Bytes())) // own the bytes (zap would free buf after Write)
		copy(b, buf.Bytes())
		return b
	}
	payloads := make([][]byte, 0, 8)
	for i := range 8 {
		payloads = append(payloads, mkPayload(i))
	}
	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	require.GreaterOrEqual(t, total, 1<<8, "must exceed bin8 to exercise bin16")
	require.Less(t, total, 1<<16)

	frame, err := NewFramer("app.logs").Frame(nil, payloads)
	require.NoError(t, err)

	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc5), marker, "entries bin must use a bin16 header")
	require.Equal(t, total, length, "bin length == sum of native payload lengths")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, len(payloads), size, "size option == payload count")
	require.Len(t, entries, len(payloads))
}
```

> **Helpers:** `decodePackedForward`/`binHeaderOf` already exist in `fluent/framer_test.go`;
> `randomSocketPath`/`startReadServer` come from Step 0's `native_testsupport_test.go`. All are
> `package fluent`, so they are shared across the test binary.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./fluent/ -run 'TestNew(Msgpack|Native)|TestNative_FramerIntegration' -race`
Expected: FAIL — `undefined: NewMsgpackEncoder` / `NewNativeCore`.

- [ ] **Step 3: Refactor `buildWriter` in `fluent/fluent.go`**

Change the signature to accept the encoder and update the two transcode callers:

```go
func buildWriter(t zapwire.Transport, o options, enc zapwire.Encoder) (*zapwire.Writer, error) {
	if o.tag == "" {
		o.tag = defaultTag
	}
	return zapwire.New(t, enc, NewFramer(o.tag), o.wireOpts...)
}
```

In `NewWriter`: `return buildWriter(t, o, NewEncoderWithCodec(o.resolveCodec()))`
In `NewCore`: `w, err := buildWriter(t, o, NewEncoderWithCodec(o.resolveCodec()))`

- [ ] **Step 4: Write `fluent/native.go`**

```go
// fluent/native.go
package fluent

import (
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// NewMsgpackEncoder returns a zapcore.Encoder that emits Fluent Forward [EventTime, record]
// msgpack payloads directly from zap's structured fields — no JSON round-trip. Envelope time comes
// from zapcore.Entry.Time (exact); TimeCodec / EncoderConfig time settings do NOT affect it
// (design §3.8).
func NewMsgpackEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder { return newMsgpackEncoder(cfg) }

// NewNativeWriter builds a zapwire.Writer for the native path: a Passthrough encoder + the
// PackedForward Framer. Pair it with NewMsgpackEncoder to build a zapcore.Core yourself (e.g.
// inside zapcore.NewTee or a sampler).
//
// WithTimeCodec / WithTimeKey are accepted but are no-ops here: native envelope time is structural
// (zapcore.Entry.Time → the EventTime extension), not read from a record field.
func NewNativeWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}
	return buildWriter(t, o, zapwire.Passthrough())
}

// NewNativeCore wires NewMsgpackEncoder + NewNativeWriter into a ready zapcore.Core plus its
// Writer (which the caller must Close). WithTimeCodec / WithTimeKey are no-ops (see above).
func NewNativeCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}
	w, err := buildWriter(t, o, zapwire.Passthrough())
	if err != nil {
		return nil, nil, err
	}
	return zapwire.NewCore(NewMsgpackEncoder(encCfg), w, level), w, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./fluent/ -race` and `go test ./ -race` (transcode `NewWriter`/`NewCore`
unaffected by the `buildWriter` refactor).
Expected: PASS across both packages.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add fluent/fluent.go fluent/native.go fluent/native_testsupport_test.go fluent/native_test.go
git commit -m "feat(fluent): add NewMsgpackEncoder, NewNativeWriter and NewNativeCore presets"
```

---

## Task 10: Concurrency — `-race` proof of per-instance state

Prove the concurrency rule (design §3.1/§5.5): N goroutines logging through one `NewNativeCore`
core concurrently; a shared `With(...)` logger with distinct call-site fields per goroutine; and a
dedicated reflected-scratch race (many goroutines forcing both Tier 1 and Tier 2 through one core).

**Files:**
- Test: `fluent/native_race_test.go`

- [ ] **Step 1: Write the race tests**

```go
// fluent/native_race_test.go
package fluent

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	zapwire "github.com/arloliu/zapwire"
)

// nativeLogger uses a DRAINING server (not the queueing startReadServer): the through-core smoke
// tests below log 16×200 entries and never read frames, so a backpressuring server would stall
// sync writes against the write deadline. The decoded correctness assertions live in the direct
// EncodeEntry tests above, which need no socket at all.
func nativeLogger(t *testing.T) (*zap.Logger, func()) {
	t.Helper()
	path := randomSocketPath(t)
	srv := startDrainServer(t, path)
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig())
	require.NoError(t, err)
	require.Eventually(t, w.IsConnected, time.Second, 5*time.Millisecond)
	return zap.New(core), func() { _ = w.Close(); srv.stop() }
}

// Direct-EncodeEntry concurrency WITH assertions: N goroutines call EncodeEntry on ONE shared
// Clone (the carried With context) with distinct call-site fields. We copy each payload, then
// after the barrier decode every one and assert no cross-talk and that the With field survived —
// proving EncodeEntry never mutates the receiver (design §3.1 / §5.5). Run with -race.
func TestNative_Concurrent_EncodeEntry_NoCrossTalk(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	shared := base.Clone()
	zap.String("shared", "ctx").AddTo(shared)

	const G, N = 16, 200
	raw := make([][][]byte, G)
	var wg sync.WaitGroup
	for g := range G {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([][]byte, 0, N)
			for i := range N {
				buf, err := shared.EncodeEntry(zapcore.Entry{Message: "m"},
					[]zapcore.Field{zap.Int("g", g), zap.Int("i", i)})
				if err != nil {
					panic(err) // t.Fatal is not goroutine-safe
				}
				b := make([]byte, len(buf.Bytes()))
				copy(b, buf.Bytes())
				out = append(out, b)
			}
			raw[g] = out
		}(g)
	}
	wg.Wait()

	for g := range G {
		for i, b := range raw[g] {
			rec := decodeEntry(t, b).Record.(map[string]any)
			require.Equal(t, "ctx", rec["shared"], "carried With field intact (receiver not mutated)")
			require.Equal(t, "m", rec["msg"])
			require.EqualValues(t, g, rec["g"], "no cross-talk between goroutines")
			require.EqualValues(t, i, rec["i"])
			require.Len(t, rec, 4)
		}
	}
}

// A bad With field shared across goroutines degrades to <key>Error on EVERY concurrent entry
// without poisoning others (design §3.9 / §5.5).
func TestNative_Concurrent_BadWith_NoPoison(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	shared := base.Clone()
	zap.Any("bad", make(chan int)).AddTo(shared) // → badError baked into the shared stack

	const G, N = 8, 100
	raw := make([][][]byte, G)
	var wg sync.WaitGroup
	for g := range G {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([][]byte, 0, N)
			for i := range N {
				buf, err := shared.EncodeEntry(zapcore.Entry{Message: "m"}, []zapcore.Field{zap.Int("i", i)})
				if err != nil {
					panic(err)
				}
				b := make([]byte, len(buf.Bytes()))
				copy(b, buf.Bytes())
				out = append(out, b)
			}
			raw[g] = out
		}(g)
	}
	wg.Wait()

	for g := range G {
		for i, b := range raw[g] {
			rec := decodeEntry(t, b).Record.(map[string]any)
			require.Contains(t, rec, "badError")
			require.EqualValues(t, i, rec["i"])
			require.Equal(t, "m", rec["msg"])
		}
	}
}

// Through-the-core -race smoke (integration path): concurrent logging through one core. The
// assertion is "no race, no panic" across the Writer/Framer/socket path.
func TestNative_Concurrent_ThroughCore(t *testing.T) {
	logger, cleanup := nativeLogger(t)
	defer cleanup()
	shared := logger.With(zap.String("shared", "ctx"))
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				shared.Info("m", zap.Int("g", g), zap.Int("i", i))
			}
		}(g)
	}
	wg.Wait()
}

func TestNative_Concurrent_ReflectedScratch(t *testing.T) {
	logger, cleanup := nativeLogger(t)
	defer cleanup()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				// Mix Tier 1 (map) and Tier 2 (struct) through the same core.
				logger.Info("m",
					zap.Any("m", map[string]any{"g": g, "i": i}),
					zap.Any("s", struct {
						A int `json:"a"`
					}{A: i}),
				)
			}
		}(g)
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run with the race detector**

Run: `go test ./fluent/ -run 'TestNative_Concurrent' -race -count=1`
Expected: PASS, no race reports. A data race here means shared mutable `stack`/`scratch` — fix
`Clone`/`getEncoder`/`encodeReflected`, not the test.

- [ ] **Step 3: Lint and commit**

```bash
make lint
git add fluent/native_race_test.go
git commit -m "test(fluent): race-prove per-instance encoder stack and reflected scratch"
```

---

## Task 11: Benchmarks + final gate

Add native sync/async benchmarks alongside the transcode ones and run the full local gate.

**Files:**
- Modify: `fluent/bench_test.go`

- [ ] **Step 1: Add native benchmarks**

```go
// add to fluent/bench_test.go
import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	// ...existing imports
)

func benchNativeLogger(b *testing.B, opts ...zapwire.Option) (*zap.Logger, func()) {
	b.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_native_bench_%d.sock", time.Now().UnixNano()))
	ln := drainingServer(b, path)
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig(),
		WithZapwireOptions(opts...))
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)
	return zap.New(core), func() { _ = w.Close(); _ = ln.Close(); _ = os.Remove(path) }
}

func benchFields() []zapcore.Field {
	return []zapcore.Field{
		zap.String("service", "zapwire"),
		zap.Int("status", 200),
		zap.String("caller", "server/handler.go:142"),
	}
}

func BenchmarkNativeWriter_Sync(b *testing.B) {
	logger, cleanup := benchNativeLogger(b)
	defer cleanup()
	fields := benchFields()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed", fields...)
	}
}

func BenchmarkNativeWriter_Async(b *testing.B) {
	logger, cleanup := benchNativeLogger(b, zapwire.WithAsyncMode())
	defer cleanup()
	fields := benchFields()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed", fields...)
	}
}

func BenchmarkNativeWriter_SyncNestedObject(b *testing.B) {
	logger, cleanup := benchNativeLogger(b)
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed",
			zap.String("service", "zapwire"),
			zap.Object("http", objMarshaler(func(enc zapcore.ObjectEncoder) error {
				enc.AddInt("status", 200)
				enc.AddString("method", "GET")
				return nil
			})),
		)
	}
}
```

> The existing transcode `BenchmarkFluentWriter_Sync`/`_Async` use a raw JSON byte slice through
> `w.Write`; the native benches log through `zap.Logger` (the realistic comparison the v1 design
> §3 promised). Both report `allocs/op`.

- [ ] **Step 2: Run benchmarks (no race) and compare allocs**

Run: `go test ./fluent/ -run='^$' -bench='Writer_(Sync|Async)' -benchmem`
Expected: native shows **strictly fewer `allocs/op`** than transcode on the scalar hot path, and
lower `ns/op`. Record the numbers in the commit message. (Not a hard CI assertion — a guided
acceptance check.)

- [ ] **Step 3: Full local gate**

Run: `make lint && make vet && make test`
Expected: all green, no race reports.

- [ ] **Step 4: Commit**

```bash
git add fluent/bench_test.go
git commit -m "test(fluent): add native sync/async benchmarks vs transcode"
```

- [ ] **Step 5: Final review handoff**

After all tasks pass: per memory `[[post-impl-review-via-codex]]`, run an independent
post-implementation review with the codex plugin (`codex:codex-rescue`) against the design spec
before merging `feat/zapwire-v2`. Then use `superpowers:finishing-a-development-branch`.

---

## Self-review checklist (run before plan-review)

- **Spec coverage:** §2 architecture → Task 1+9; §3.1 state/clone/concurrency → Task 2+10; §3.2
  sealDownTo/headers/object-scope → Task 2+5; §3.3 count invariant → Task 2+3+4 (canary);
  §3.4 two-phase + envelope matrix → Task 2+4; §3.5 With/namespaces → Task 5; §3.6 type mapping →
  Task 3+4+5+6; §3.7 reflected → Task 6; §3.8 divergences → Task 3 (binary/complex/NaN/Inf) + Task
  7 (duplicates) + Task 8 (equivalence exclusions); §3.9 error contract → Task 6+7; §4 API →
  Task 1+9; §5 testing oracles → Task 2 (golden) + Task 8 (equivalence) + all (decode); §5.5
  concurrency → Task 10; §5.7 benchmarks → Task 11.
- **No placeholders:** every code step shows real Go; stubs are explicit and scheduled for
  replacement (Task 2 → 3/4/5/6).
- **Type consistency:** `frame`/`frameKind`/`msgpackEncoder`/`newMsgpackEncoder`/`addKey`/
  `sealDownTo`/`getBytes`/`putBytes`/`getEncoder`/`putEncoder`/`appendMapHeader`/
  `appendArrayHeader`/`writeEnvelope`/`encodeReflected`/`cloneStack` are defined in Task 2 and used
  with the same signatures throughout. `buildWriter(t, o, enc)` (Task 9) matches both callers.
- **Golden validity across tasks:** the Task 2 empty-record golden uses `minimalCfg()` (empty
  keys); later tasks never change that config, so the golden stays valid at every step.
