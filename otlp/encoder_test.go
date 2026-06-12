package otlp

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func testEntry() zapcore.Entry {
	return zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Unix(0, 1),
		Message: "m",
	}
}

func encodeRecord(t *testing.T, e zapcore.Encoder, ent zapcore.Entry, fields ...zapcore.Field) []byte {
	t.Helper()
	buf, err := e.EncodeEntry(ent, fields)
	require.NoError(t, err)
	defer buf.Free()

	return append([]byte(nil), buf.Bytes()...)
}

// attrKeys decodes the top-level attribute keys of a LogRecord, in order.
func attrKeys(t *testing.T, rec []byte) []string {
	t.Helper()

	var keys []string

	b := rec
	for len(b) > 0 {
		tag, n := uvarint(b) // see test helper below
		require.Positive(t, n)
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)

		switch wt {
		case 0:
			_, vn := uvarint(b)
			b = b[vn:]
		case 1:
			b = b[8:]
		case 2:
			l, ln := uvarint(b)
			payload := b[ln : ln+int(l)]
			if num == 6 {
				key, err := findField(payload, 1)
				require.NoError(t, err)
				keys = append(keys, string(key))
			}
			b = b[ln+int(l):]
		case 5:
			b = b[4:]
		}
	}

	return keys
}

func uvarint(b []byte) (uint64, int) {
	var v uint64
	for i := range b {
		v |= uint64(b[i]&0x7f) << (7 * i)
		if b[i] < 0x80 {
			return v, i + 1
		}
	}

	return 0, 0
}

func TestEncodeEntryMinimalGolden(t *testing.T) {
	e := NewEncoder()
	rec := encodeRecord(t, e, testEntry())
	want := []byte{
		0x09, 1, 0, 0, 0, 0, 0, 0, 0, // time_unix_nano = 1
		0x10, 0x09, // severity_number = 9 (INFO)
		0x1a, 0x04, 'i', 'n', 'f', 'o', // severity_text
		0x2a, 0x03, 0x0a, 0x01, 'm', // body = AnyValue{"m"}
		0x59, 1, 0, 0, 0, 0, 0, 0, 0, // observed_time_unix_nano = 1
	}
	require.Equal(t, want, rec)
}

// topLevelFieldNums walks a record's top-level proto fields in order.
func topLevelFieldNums(t *testing.T, rec []byte) []int {
	t.Helper()

	var nums []int

	b := rec
	for len(b) > 0 {
		tag, n := uvarint(b)
		require.Positive(t, n)
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		nums = append(nums, num)

		switch wt {
		case 0:
			_, vn := uvarint(b)
			b = b[vn:]
		case 1:
			b = b[8:]
		case 2:
			l, ln := uvarint(b)
			b = b[ln+int(l):]
		case 5:
			b = b[4:]
		default:
			t.Fatalf("unexpected wire type %d", wt)
		}
	}

	return nums
}

func TestEncodeEntryEpochZeroOmitsTimes(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()
	ent.Time = time.Unix(0, 0)
	rec := encodeRecord(t, e, ent)
	nums := topLevelFieldNums(t, rec)
	require.NotContains(t, nums, 1, "time_unix_nano must be omitted at epoch zero")
	require.NotContains(t, nums, 11, "observed_time_unix_nano must be omitted at epoch zero")
}

func TestUnsampledSpanOmitsFlags(t *testing.T) {
	// Valid IDs, TraceFlags 0: trace_id/span_id emitted, flags OMITTED
	// (proto.Marshal drops a zero fixed32 — byte-identity rule).
	scUnsampled := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})
	e := NewEncoder()
	rec := encodeRecord(t, e, testEntry(),
		zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scUnsampled})
	nums := topLevelFieldNums(t, rec)
	require.Contains(t, nums, 9)
	require.Contains(t, nums, 10)
	require.NotContains(t, nums, 8, "zero flags must be omitted")
}

func TestEncodeEntryTraceFields(t *testing.T) {
	sc, ctx := testSpanContext(t)
	e := NewEncoder()

	for _, f := range []zapcore.Field{SpanContext(ctx), zap.Any("context", ctx)} {
		rec := encodeRecord(t, e, testEntry(), f, zap.String("k", "v"))
		tid, err := findField(rec, 9)
		require.NoError(t, err)
		wantTID := sc.TraceID()
		require.Equal(t, wantTID[:], tid)
		sid, err := findField(rec, 10)
		require.NoError(t, err)
		wantSID := sc.SpanID()
		require.Equal(t, wantSID[:], sid)
		// flags: fixed32 tag 0x45 followed by 01 00 00 00 (sampled).
		require.Contains(t, string(rec), string([]byte{0x45, 0x01, 0x00, 0x00, 0x00}))
		// consumed: not an attribute.
		require.Equal(t, []string{"k"}, attrKeys(t, rec))
	}

	// No span → all three omitted.
	rec := encodeRecord(t, e, testEntry(), SpanContext(nil)) //nolint:staticcheck
	tid, err := findField(rec, 9)
	require.NoError(t, err)
	require.Nil(t, tid)
}

// attrValueOf decodes the string_value of the top-level attribute with the
// given key (returns "" if absent). It mirrors attrKeys but descends into the
// matched KeyValue to read AnyValue.string_value (field 1).
func attrValueOf(t *testing.T, rec []byte, key string) string {
	t.Helper()

	b := rec
	for len(b) > 0 {
		tag, n := uvarint(b)
		require.Positive(t, n)
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)

		switch wt {
		case 0:
			_, vn := uvarint(b)
			b = b[vn:]
		case 1:
			b = b[8:]
		case 2:
			l, ln := uvarint(b)
			payload := b[ln : ln+int(l)]
			if num == 6 {
				k, err := findField(payload, 1)
				require.NoError(t, err)
				if string(k) == key {
					av, err := findField(payload, 2) // KeyValue.value (AnyValue)
					require.NoError(t, err)
					sv, err := findField(av, 1) // AnyValue.string_value
					require.NoError(t, err)

					return string(sv)
				}
			}
			b = b[ln+int(l):]
		case 5:
			b = b[4:]
		}
	}

	return ""
}

func TestTraceCorrelationAttributes(t *testing.T) {
	sc, ctx := testSpanContext(t)

	// Option ON + per-call SpanContext: BOTH the proto trace fields AND the two
	// flat string attributes are present, with correct lowercase-hex values.
	e := NewEncoder(WithTraceCorrelationAttributes(true))
	rec := encodeRecord(t, e, testEntry(), SpanContext(ctx), zap.String("k", "v"))

	tid, err := findField(rec, 9)
	require.NoError(t, err)
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], tid, "proto trace_id still emitted")
	sid, err := findField(rec, 10)
	require.NoError(t, err)
	wantSID := sc.SpanID()
	require.Equal(t, wantSID[:], sid, "proto span_id still emitted")

	require.Equal(t, []string{"k", "trace_id", "span_id"}, attrKeys(t, rec),
		"flat attributes land after per-call attrs, in trace_id/span_id order")
	require.Equal(t, sc.TraceID().String(), attrValueOf(t, rec, "trace_id"))
	require.Equal(t, sc.SpanID().String(), attrValueOf(t, rec, "span_id"))

	// Option ON + no valid span: no attributes added.
	rec = encodeRecord(t, e, testEntry(), SpanContext(nil), zap.String("k", "v")) //nolint:staticcheck
	require.Equal(t, []string{"k"}, attrKeys(t, rec))

	// Default OFF + valid span: no attributes (regression pin) — proto fields only.
	off := NewEncoder()
	rec = encodeRecord(t, off, testEntry(), SpanContext(ctx), zap.String("k", "v"))
	require.Equal(t, []string{"k"}, attrKeys(t, rec))
	tid, err = findField(rec, 9)
	require.NoError(t, err)
	require.Equal(t, wantTID[:], tid, "proto trace_id unaffected by default-off option")
}

func TestTracePrecedenceAndLastWins(t *testing.T) {
	scA, ctxA := testSpanContext(t)
	scB := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})

	// With-stash seeds; per-call overrides (Clone + AddTo simulates the
	// stock-core With path with the EAGER field, which is ReflectType).
	e := NewEncoder().Clone()
	SpanContext(ctxA).AddTo(e.(zapcore.ObjectEncoder))
	rec := encodeRecord(t, e, testEntry())
	tid, _ := findField(rec, 9)
	wantA := scA.TraceID()
	require.Equal(t, wantA[:], tid)

	// Per-call beats the stash; two per-call trace fields → last wins, both consumed.
	fB := zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scB}
	rec = encodeRecord(t, e, testEntry(), SpanContext(ctxA), fB)
	tid, _ = findField(rec, 9)
	wantB := scB.TraceID()
	require.Equal(t, wantB[:], tid)
	require.Empty(t, attrKeys(t, rec))
}

func TestMetaAttributes(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()
	ent.LoggerName = "svc.sub"
	ent.Caller = zapcore.EntryCaller{Defined: true, File: "/a/b.go", Line: 7, Function: "pkg.F"}
	ent.Stack = "stack..."
	rec := encodeRecord(t, e, ent, zap.String("k", "v"))
	require.Equal(t,
		[]string{"logger", "code.function.name", "code.file.path", "code.line.number", "code.stacktrace", "k"},
		attrKeys(t, rec))

	// Disabled variants.
	e = NewEncoder(WithCallerAttributes(false), WithLoggerNameKey(""))
	rec = encodeRecord(t, e, ent, zap.String("k", "v"))
	require.Equal(t, []string{"code.stacktrace", "k"}, attrKeys(t, rec))
}

func TestMetaStaysAtRootUnderWithNamespace(t *testing.T) {
	// With opens a namespace; meta attrs must still land at the root.
	e := NewEncoder().Clone()
	zap.Namespace("req").AddTo(e.(zapcore.ObjectEncoder))
	ent := testEntry()
	ent.LoggerName = "L"
	rec := encodeRecord(t, e, ent, zap.String("in", "ns"))
	require.Equal(t, []string{"logger", "req"}, attrKeys(t, rec))
}

func TestFieldDegradation(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()

	// Failing object marshaler — entry ships, <key>Error attr, no partial bytes.
	recBad := encodeRecord(t, e, ent, zap.String("good", "1"), zap.Object("bad", failObj{partial: true}))
	require.Equal(t, []string{"good", "badError"}, attrKeys(t, recBad))

	// Failing zap.Inline (empty key → "Error").
	recInline := encodeRecord(t, e, ent, zap.String("good", "1"), zap.Inline(failObj{partial: true}))
	require.Equal(t, []string{"good", "Error"}, attrKeys(t, recInline))

	// zap.Any with an unmarshalable channel.
	recChan := encodeRecord(t, e, ent, zap.Any("ch", make(chan int)))
	require.Equal(t, []string{"chError"}, attrKeys(t, recChan))
}

// verboseError implements fmt.Formatter so zap's encodeError emits <key>Verbose.
type verboseError struct{ msg string }

func (e verboseError) Error() string { return e.msg }
func (e verboseError) Format(s fmt.State, verb rune) {
	if verb == 'v' && s.Flag('+') {
		fmt.Fprintf(s, "%s\nwith stack", e.msg)

		return
	}

	fmt.Fprint(s, e.msg)
}

func TestErrorFieldExpansion(t *testing.T) {
	e := NewEncoder()

	// Plain error → single string attribute under the field key.
	rec := encodeRecord(t, e, testEntry(), zap.Error(errors.New("boom")))
	require.Equal(t, []string{"error"}, attrKeys(t, rec))

	// fmt.Formatter error → zap's encodeError adds <key>Verbose (design §3.3:
	// errors delegate to zap's standard expansion through the ObjectEncoder).
	rec = encodeRecord(t, e, testEntry(), zap.Error(verboseError{msg: "boom"}))
	require.Equal(t, []string{"error", "errorVerbose"}, attrKeys(t, rec))

	// Grouped errors (multierr) → <key>Causes array attribute.
	rec = encodeRecord(t, e, testEntry(),
		zap.Error(multierr.Combine(errors.New("a"), errors.New("b"))))
	require.Equal(t, []string{"error", "errorCauses"}, attrKeys(t, rec))
}

// traceCarrier is a zapcore.ObjectMarshaler whose MarshalLogObject calls
// AddReflected with a trace.SpanContext value.  This exercises the sink path
// in EncodeEntry (work.scSink re-point, encoder.go §3.7): a top-level
// SpanContext field is intercepted before applyField, but a nested
// AddReflected call reaches the sink directly.  With the re-point line
// present, the sink points at per-call locals so the span context lands in
// the LogRecord; without it, scSink is nil and the span context is silently
// dropped — caught by the deterministic assertion below.
type traceCarrier struct{ sc trace.SpanContext }

func (c traceCarrier) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return enc.AddReflected("sc", c.sc)
}

func TestEncoderConcurrency(t *testing.T) {
	e := NewEncoder()
	core := zapcore.NewCore(e, zapcore.AddSync(io.Discard), zapcore.DebugLevel)
	logger := zap.New(core)
	sc, ctx := testSpanContext(t)

	// Deterministic bite: verify nested AddReflected routes through the sink.
	// With encoder.go's re-point line present, scSink → per-call local → span
	// fields emitted.  With it removed, scSink is nil (encState.clone zeroes
	// it) → silent drop → findField returns nil → require.Equal fails.
	rec := encodeRecord(t, e, testEntry(), zap.Inline(traceCarrier{sc}))
	wantSID := sc.SpanID()
	sid, err := findField(rec, 10)
	require.NoError(t, err)
	require.Equal(t, wantSID[:], sid,
		"nested AddReflected trace must reach per-call locals (encoder.go sink re-point)")

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			l := logger.With(SpanContext(ctx), zap.Namespace("ns"), zap.String("w", "1"))

			for j := range 200 {
				logger.Info("shared", zap.Int("j", j))
				logger.Info("traced", zap.Inline(traceCarrier{sc}))
				l.Info("cloned", zap.Int("j", j))
			}
		})
	}

	wg.Wait()
}
