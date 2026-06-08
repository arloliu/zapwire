// fluent/msgpack_array.go
package fluent

import (
	"math"
	"strconv"
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
