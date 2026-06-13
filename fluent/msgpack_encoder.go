// fluent/msgpack_encoder.go
package fluent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// maxEncodeDepth caps container nesting (objects/arrays). A zapcore marshaler
// that recurses unboundedly (self-referential or attacker-shaped) would grow the
// goroutine stack to exhaustion — an UNCATCHABLE fatal throw on the logging path.
// The limit is far above any real log (legitimate nesting is single digits); it
// guards only runaway recursion. A flat OpenNamespace chain grows the heap, not
// the stack, so it is intentionally not bounded here.
const maxEncodeDepth = 1000

// errMaxEncodeDepth is returned by AppendObject/AppendArray at the depth cap. It
// surfaces through zap's <key>Error convention; the entry still ships.
var errMaxEncodeDepth = errors.New("fluent: max container nesting depth exceeded")

// Compile-time proof the full method set is implemented (cross-checked against
// go.uber.org/zap/zapcore/encoder.go in the module cache).
var (
	_ zapcore.Encoder      = (*msgpackEncoder)(nil)
	_ zapcore.ArrayEncoder = (*msgpackEncoder)(nil)
)

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
	// The header is written straight into buf — a 2-element array, the EventTime extension, and
	// the record map header — so there is no pooled header buffer and the EventTime is not boxed
	// into an interface (design §3.4; byte-identity is pinned by the golden tests).
	buf := bufferPool.Get()

	buf.AppendByte(0x92)                // fixarray, 2 elements: [time, record]
	buf.AppendByte(0xd7)                // msgpack fixext8: an 8-byte extension payload
	buf.AppendByte(byte(extensionType)) // EventTime extension type

	var ts [length]byte
	et := EventTime(ent.Time)
	if err := et.MarshalBinaryTo(ts[:]); err != nil {
		buf.Free()
		putEncoder(env)
		putEncoder(final)

		return nil, fmt.Errorf("fluent: marshal event time: %w", err) // fatal (design §3.9)
	}
	buf.AppendBytes(ts[:])

	appendMapHeaderTo(buf, env.stack[0].count+final.stack[0].count) // record map header
	_, _ = buf.Write(env.stack[0].buf)
	_, _ = buf.Write(final.stack[0].buf)

	putEncoder(env)
	putEncoder(final)

	return buf, nil // do NOT free buf — zap's ioCore frees it after Write
}

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
				// panics if nil; native falls back so a misconfigured config degrades gracefully.
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

// --- scalar Add* (real: String + all scalars in Task 3; Duration/Time stubbed until Task 4) ----

func (e *msgpackEncoder) AddString(key, val string) { e.addKey(key); e.AppendString(val) }

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

func (e *msgpackEncoder) AddDuration(k string, v time.Duration) { e.addKey(k); e.AppendDuration(v) }
func (e *msgpackEncoder) AddTime(k string, v time.Time)         { e.addKey(k); e.AppendTime(v) }

// --- container / reflected Add* (stubs; filled in Tasks 5–6) ---------------------------------

func (e *msgpackEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	// Check the depth cap BEFORE addKey: addKey writes the key and bumps the map
	// pair count, so a cap error raised inside AppendObject would otherwise seal
	// a map with a keyed-but-valueless pair (malformed msgpack). AppendObject
	// re-checks at the same stack depth, so the value is always written.
	if len(e.stack) >= maxEncodeDepth {
		return errMaxEncodeDepth
	}
	e.addKey(key)

	return e.AppendObject(obj)
}

func (e *msgpackEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	if len(e.stack) >= maxEncodeDepth {
		return errMaxEncodeDepth // before addKey: keep the map pair balanced
	}
	e.addKey(key)

	return e.AppendArray(arr)
}

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

// --- pools & header helpers ------------------------------------------------------------------

var bufferPool = buffer.NewPool()

var bytesPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 256)

	return &b
}}

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

// appendMapHeaderTo writes a msgpack map header for count entries straight into buf, mirroring
// msgp.AppendMapHeader's fixmap/map16/map32 selection so the wire bytes are identical. Writing
// into buf (rather than a temporary slice) keeps the EncodeEntry header off the heap.
//
//nolint:gosec // count is a bounded, non-negative per-entry element count; see appendMapHeader
func appendMapHeaderTo(buf *buffer.Buffer, count int) {
	switch c := uint32(count); {
	case c < 16:
		buf.AppendByte(0x80 | byte(c)) // fixmap
	case c <= 0xffff:
		buf.AppendByte(0xde) // map16
		buf.AppendByte(byte(c >> 8))
		buf.AppendByte(byte(c))
	default:
		buf.AppendByte(0xdf) // map32
		buf.AppendByte(byte(c >> 24))
		buf.AppendByte(byte(c >> 16))
		buf.AppendByte(byte(c >> 8))
		buf.AppendByte(byte(c))
	}
}
