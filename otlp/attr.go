package otlp

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

type frameKind uint8

const (
	frameRoot   frameKind = iota // KeyValue entries tagged 0x32 (LogRecord.attributes)
	frameKVList                  // KeyValue entries tagged 0x0a (KeyValueList.values)
	frameArray                   // AnyValue entries tagged 0x0a (ArrayValue.values)
)

type frame struct {
	kind  frameKind
	nsKey string // KeyValue key this frame seals into (unused for frameRoot)
	buf   []byte // already-tagged entries for this container (pooled)
}

var frameBufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 1024)

	return &b
}}

func getFrameBuf() []byte  { return (*frameBufPool.Get().(*[]byte))[:0] }
func putFrameBuf(b []byte) { frameBufPool.Put(&b) }

// encState is the proto-writing zapcore.ObjectEncoder. It is the working
// state of EncodeEntry, the persistent With-state of the encoder (embedded),
// and the Resource attribute builder (Task 7).
type encState struct {
	stack   []frame
	scratch []byte // reused AnyValue assembly buffer
	// trace sink: AddReflected stores resolved span contexts here instead of
	// encoding them (design §3.4). Nil sink → trace values are still consumed
	// (never encoded) but not stored.
	scSink    *trace.SpanContext
	scSinkSet *bool
}

func newEncState() *encState {
	return &encState{stack: []frame{{kind: frameRoot, buf: getFrameBuf()}}}
}

// free returns all frame buffers to the pool. The state is unusable after.
func (s *encState) free() {
	for i := range s.stack {
		putFrameBuf(s.stack[i].buf)
	}
	s.stack = nil
}

// clone deep-copies the state for per-call work (design §3.7): every frame
// buffer is copied into a pooled slice so EncodeEntry never mutates the
// receiver-owned persistent state.
func (s *encState) clone() *encState { //nolint:unused // used in Task 5 (encoder.go)
	c := &encState{stack: make([]frame, len(s.stack))}
	for i := range s.stack {
		buf := getFrameBuf()
		buf = append(buf, s.stack[i].buf...)
		c.stack[i] = frame{kind: s.stack[i].kind, nsKey: s.stack[i].nsKey, buf: buf}
	}

	return c
}

func (s *encState) cur() *frame { return &s.stack[len(s.stack)-1] }

type snapshot struct {
	depth  int
	bufLen int
}

func (s *encState) snap() snapshot {
	return snapshot{depth: len(s.stack), bufLen: len(s.cur().buf)}
}

// rollback restores the state captured by snap: frames opened above the
// snapshot depth are discarded and the then-current frame is truncated, so
// no partial bytes survive a failed marshaler (design §3.3, pass-2 P0).
func (s *encState) rollback(sn snapshot) {
	for len(s.stack) > sn.depth {
		top := s.stack[len(s.stack)-1]
		s.stack = s.stack[:len(s.stack)-1]
		putFrameBuf(top.buf)
	}
	f := s.cur()
	f.buf = f.buf[:sn.bufLen]
}

// entryTag returns the tag byte for entries of this container.
func (f *frame) entryTag() byte {
	if f.kind == frameRoot {
		return 0x32 // LogRecord.attributes
	}

	return 0x0a // KeyValueList.values / ArrayValue.values
}

// addKV appends KeyValue{key, av} as a tagged entry of the current frame.
func (s *encState) addKV(key string, av []byte) {
	f := s.cur()
	kvLen := 1 + uvarintLen(uint64(len(key))) + len(key) +
		1 + uvarintLen(uint64(len(av))) + len(av)
	f.buf = append(f.buf, f.entryTag())
	f.buf = appendUvarint(f.buf, uint64(kvLen)) //nolint:gosec
	f.buf = appendTaggedString(f.buf, 0x0a, key)
	f.buf = appendTaggedBytes(f.buf, 0x12, av)
}

// addAV appends a bare AnyValue entry (array containers only).
func (s *encState) addAV(av []byte) {
	f := s.cur()
	f.buf = appendTaggedBytes(f.buf, f.entryTag(), av)
}

// addValue routes an AnyValue to the current container: arrays take bare
// AnyValues, root/kvlist take KeyValue-wrapped ones.
func (s *encState) addValue(key string, av []byte) {
	if s.cur().kind == frameArray {
		s.addAV(av)

		return
	}

	s.addKV(key, av)
}

// AnyValue builders (into s.scratch; valid until the next any* call).
func (s *encState) anyString(v string) []byte {
	s.scratch = appendTaggedString(s.scratch[:0], 0x0a, v)

	return s.scratch
}

func (s *encState) anyBool(v bool) []byte {
	b := byte(0)
	if v {
		b = 1
	}

	s.scratch = append(s.scratch[:0], 0x10, b)

	return s.scratch
}

func (s *encState) anyInt(v int64) []byte {
	s.scratch = appendTaggedVarint(s.scratch[:0], 0x18, v)

	return s.scratch
}

func (s *encState) anyDouble(v float64) []byte {
	s.scratch = appendTaggedFixed64(s.scratch[:0], 0x21, math.Float64bits(v))

	return s.scratch
}

func (s *encState) anyBytes(v []byte) []byte {
	s.scratch = appendTaggedBytes(s.scratch[:0], 0x3a, v)

	return s.scratch
}

// --- container open/seal ---

func (s *encState) openFrame(kind frameKind, key string) {
	s.stack = append(s.stack, frame{kind: kind, nsKey: key, buf: getFrameBuf()})
}

// sealTop closes the top frame into its parent as kvlist/array AnyValue.
func (s *encState) sealTop() {
	top := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	avTag := byte(0x32) // AnyValue.kvlist_value
	if top.kind == frameArray {
		avTag = 0x2a // AnyValue.array_value
	}
	avLen := 1 + uvarintLen(uint64(len(top.buf))) + len(top.buf)
	parent := s.cur()
	if parent.kind == frameArray {
		// bare AnyValue element
		parent.buf = append(parent.buf, parent.entryTag())
		parent.buf = appendUvarint(parent.buf, uint64(avLen)) //nolint:gosec
	} else {
		kvLen := 1 + uvarintLen(uint64(len(top.nsKey))) + len(top.nsKey) +
			1 + uvarintLen(uint64(avLen)) + avLen //nolint:gosec
		parent.buf = append(parent.buf, parent.entryTag())
		parent.buf = appendUvarint(parent.buf, uint64(kvLen)) //nolint:gosec
		parent.buf = appendTaggedString(parent.buf, 0x0a, top.nsKey)
		parent.buf = append(parent.buf, 0x12)
		parent.buf = appendUvarint(parent.buf, uint64(avLen)) //nolint:gosec
	}
	parent.buf = append(parent.buf, avTag)
	parent.buf = appendUvarint(parent.buf, uint64(len(top.buf)))
	parent.buf = append(parent.buf, top.buf...)
	putFrameBuf(top.buf)
}

// sealAll closes every open namespace down to the root frame (entry end).
func (s *encState) sealAll() {
	for len(s.stack) > 1 {
		s.sealTop()
	}
}

// --- zapcore.ObjectEncoder ---

func (s *encState) AddString(k, v string)      { s.addValue(k, s.anyString(v)) }
func (s *encState) AddBool(k string, v bool)   { s.addValue(k, s.anyBool(v)) }
func (s *encState) AddInt(k string, v int)     { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt64(k string, v int64) { s.addValue(k, s.anyInt(v)) }
func (s *encState) AddInt32(k string, v int32) { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt16(k string, v int16) { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt8(k string, v int8)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint(k string, v uint)   { s.AddUint64(k, uint64(v)) }
func (s *encState) AddUint64(k string, v uint64) {
	if v > math.MaxInt64 {
		// OTel attribute model has no uint64 (design §3.3): decimal string.
		s.addValue(k, s.anyString(strconv.FormatUint(v, 10)))

		return
	}

	s.AddInt64(k, int64(v))
}

func (s *encState) AddUint32(k string, v uint32)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint16(k string, v uint16)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint8(k string, v uint8)     { s.AddInt64(k, int64(v)) }
func (s *encState) AddUintptr(k string, v uintptr) { s.AddUint64(k, uint64(v)) }
func (s *encState) AddFloat64(k string, v float64) { s.addValue(k, s.anyDouble(v)) }
func (s *encState) AddFloat32(k string, v float32) { s.AddFloat64(k, float64(v)) }
func (s *encState) AddBinary(k string, v []byte)   { s.addValue(k, s.anyBytes(v)) }

func (s *encState) AddByteString(k string, v []byte) {
	// zap semantic: UTF-8 text bytes → string_value (design §3.3).
	s.addValue(k, s.anyString(string(v)))
}

func (s *encState) AddComplex128(k string, v complex128) {
	s.addValue(k, s.anyString(formatComplex(v)))
}

func (s *encState) AddComplex64(k string, v complex64)    { s.AddComplex128(k, complex128(v)) }
func (s *encState) AddDuration(k string, v time.Duration) { s.AddInt64(k, v.Nanoseconds()) }
func (s *encState) AddTime(k string, v time.Time)         { s.AddInt64(k, v.UnixNano()) }

func (s *encState) OpenNamespace(k string) { s.openFrame(frameKVList, k) }

func (s *encState) AddObject(k string, m zapcore.ObjectMarshaler) error {
	sn := s.snap()
	s.openFrame(frameKVList, k)

	if err := m.MarshalLogObject(s); err != nil {
		s.rollback(sn)

		return err
	}

	s.sealDownTo(sn.depth)

	return nil
}

func (s *encState) AddArray(k string, m zapcore.ArrayMarshaler) error {
	sn := s.snap()
	s.openFrame(frameArray, k)

	if err := m.MarshalLogArray(arrayEnc{s}); err != nil {
		s.rollback(sn)

		return err
	}

	s.sealDownTo(sn.depth)

	return nil
}

// addKVRoot appends KeyValue{key, av} to the ROOT frame regardless of open
// namespaces — entry-metadata attributes are never namespaced (design §3.5,
// pinned attribute order).
func (s *encState) addKVRoot(key string, av []byte) {
	f := &s.stack[0]
	kvLen := 1 + uvarintLen(uint64(len(key))) + len(key) +
		1 + uvarintLen(uint64(len(av))) + len(av)
	f.buf = append(f.buf, f.entryTag())
	f.buf = appendUvarint(f.buf, uint64(kvLen)) //nolint:gosec
	f.buf = appendTaggedString(f.buf, 0x0a, key)
	f.buf = appendTaggedBytes(f.buf, 0x12, av)
}

// sealDownTo seals frames (incl. namespaces the marshaler opened and never
// closed — zap permits that) until the stack is back at depth.
func (s *encState) sealDownTo(depth int) {
	for len(s.stack) > depth {
		s.sealTop()
	}
}

func (s *encState) AddReflected(k string, v any) error {
	if sc, ok := spanContextFromValue(v); ok {
		// Trace-context value: consume, never encode (design §3.4). Stored
		// only when a sink is armed (With on a fresh clone / EncodeEntry
		// locals) — receiver-safe by construction (§3.7).
		if s.scSink != nil {
			*s.scSink, *s.scSinkSet = sc, true
		}

		return nil
	}

	sn := s.snap()
	js, err := json.Marshal(v)
	if err != nil {
		s.rollback(sn) // nothing was written; keeps the invariant explicit

		return err
	}

	s.addValue(k, s.anyString(string(js)))

	return nil
}

func formatComplex(v complex128) string {
	// zap JSON convention "a+bi" (strips the parentheses of strconv).
	str := strconv.FormatComplex(v, 'f', -1, 128)

	return str[1 : len(str)-1]
}

// --- zapcore.ArrayEncoder (elements of the current frameArray) ---

type arrayEnc struct{ s *encState }

func (a arrayEnc) AppendBool(v bool)             { a.s.addAV(a.s.anyBool(v)) }
func (a arrayEnc) AppendByteString(v []byte)     { a.s.addAV(a.s.anyString(string(v))) }
func (a arrayEnc) AppendComplex128(v complex128) { a.s.addAV(a.s.anyString(formatComplex(v))) }
func (a arrayEnc) AppendComplex64(v complex64)   { a.AppendComplex128(complex128(v)) }
func (a arrayEnc) AppendFloat64(v float64)       { a.s.addAV(a.s.anyDouble(v)) }
func (a arrayEnc) AppendFloat32(v float32)       { a.AppendFloat64(float64(v)) }
func (a arrayEnc) AppendInt(v int)               { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt64(v int64)           { a.s.addAV(a.s.anyInt(v)) }
func (a arrayEnc) AppendInt32(v int32)           { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt16(v int16)           { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt8(v int8)             { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendString(v string)         { a.s.addAV(a.s.anyString(v)) }
func (a arrayEnc) AppendUint(v uint)             { a.AppendUint64(uint64(v)) }
func (a arrayEnc) AppendUint64(v uint64) {
	if v > math.MaxInt64 {
		a.s.addAV(a.s.anyString(strconv.FormatUint(v, 10)))

		return
	}

	a.AppendInt64(int64(v))
}

func (a arrayEnc) AppendUint32(v uint32)          { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUint16(v uint16)          { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUint8(v uint8)            { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUintptr(v uintptr)        { a.AppendUint64(uint64(v)) }
func (a arrayEnc) AppendDuration(v time.Duration) { a.AppendInt64(v.Nanoseconds()) }
func (a arrayEnc) AppendTime(v time.Time)         { a.AppendInt64(v.UnixNano()) }

func (a arrayEnc) AppendArray(m zapcore.ArrayMarshaler) error {
	sn := a.s.snap()
	a.s.openFrame(frameArray, "")

	if err := m.MarshalLogArray(a); err != nil {
		a.s.rollback(sn)

		return err
	}

	a.s.sealDownTo(sn.depth)

	return nil
}

func (a arrayEnc) AppendObject(m zapcore.ObjectMarshaler) error {
	sn := a.s.snap()
	a.s.openFrame(frameKVList, "")

	if err := m.MarshalLogObject(a.s); err != nil {
		a.s.rollback(sn)

		return err
	}

	a.s.sealDownTo(sn.depth)

	return nil
}

func (a arrayEnc) AppendReflected(v any) error {
	sn := a.s.snap()
	js, err := json.Marshal(v)
	if err != nil {
		a.s.rollback(sn)

		return err
	}

	a.s.addAV(a.s.anyString(string(js)))

	return nil
}

var (
	_ zapcore.ObjectEncoder = (*encState)(nil)
	_ zapcore.ArrayEncoder  = arrayEnc{}
)
