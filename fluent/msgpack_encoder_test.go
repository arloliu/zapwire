// fluent/msgpack_encoder_test.go
package fluent

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap"
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
	bigUint := uint64(math.MaxUint64)
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

// decodeEntryRecord is a convenience wrapper for tests that only need the record map.
func decodeEntryRecord(t *testing.T, enc *msgpackEncoder, ent zapcore.Entry, fields []zapcore.Field) map[string]any {
	t.Helper()
	buf, err := enc.EncodeEntry(ent, fields)
	require.NoError(t, err)

	return decodeEntry(t, buf.Bytes()).Record.(map[string]any)
}

func envCfg() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    "func",
		StacktraceKey:  "stack",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeTime:     zapcore.EpochNanosTimeEncoder,
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

// Empty-key omission for EVERY envelope field at once: with all keys empty, the record map is
// empty even though the Entry carries level/name/caller/message/stack.
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

// Remaining nil/no-op matrix cells: name no-op → LoggerName; caller no-op → Caller.String();
// duration nil AND no-op → int64 nanos; time nil → unix nanos. Each decodes and asserts the
// fallback value (and that exactly one key is present — no key-without-value).
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

func recordKeys(t *testing.T, entry []byte) []string {
	t.Helper()
	count, o, err := msgp.ReadArrayHeaderBytes(entry) // [time, record]
	require.NoError(t, err)
	require.Equal(t, uint32(2), count, "entry is a 2-element [time, record] array")
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
// It fully consumes the [time, record] payload (asserting no trailing bytes) so it cannot pass
// vacuously on a corrupt namespace-seal wire.
func namespaceKeys(t *testing.T, entry []byte, nsKey string) []string {
	t.Helper()
	count, o, err := msgp.ReadArrayHeaderBytes(entry) // [time, record]
	require.NoError(t, err)
	require.Equal(t, uint32(2), count, "entry is a 2-element [time, record] array")
	o, err = msgp.Skip(o) // EventTime extension
	require.NoError(t, err)
	n, o, err := msgp.ReadMapHeaderBytes(o)
	require.NoError(t, err)
	var found []string
	gotIt := false
	for range int(n) {
		var k string
		k, o, err = msgp.ReadStringBytes(o)
		require.NoError(t, err)
		if k == nsKey && !gotIt {
			var m uint32
			m, o, err = msgp.ReadMapHeaderBytes(o)
			require.NoError(t, err)
			keys := make([]string, 0, m)
			for range int(m) {
				var ik string
				ik, o, err = msgp.ReadStringBytes(o)
				require.NoError(t, err)
				o, err = msgp.Skip(o) // value
				require.NoError(t, err)
				keys = append(keys, ik)
			}
			found = keys
			gotIt = true

			continue
		}
		o, err = msgp.Skip(o) // not the target (or a later dup of nsKey) — skip its value
		require.NoError(t, err)
	}
	require.True(t, gotIt, "namespace key %q not found in record", nsKey)
	require.Empty(t, o, "no trailing bytes after the record map")

	return found
}

// =============================================================================================
// Parity with zap's zapcore JSON-encoder test matrix, ADAPTED to the native msgpack divergences.
// Each test below mirrors a case (or group) from json_encoder_impl_test.go / json_encoder_test.go
// but asserts the native msgpack behavior (raw str, real bin, complex/NaN→string, int64/uint64
// round-trips) rather than the JSON text. Decoded Go TYPES were verified empirically.
// =============================================================================================

// TestNative_IntegerWidths_Boundaries mirrors TestJSONEncoderObjectFields' int/uint width matrix
// (int8..int64, uint8..uint64, uintptr) at min/max boundaries — the area-A truncation guard. Every
// width converter widens to AppendInt64/AppendUint64, so a truncating converter would change the
// decoded VALUE. msgp picks the smallest wire encoding, so small positives decode as int64 while
// large unsigned values decode as uint64; EqualValues handles the mid-range, and exact-type Equal
// pins the two extremes (MaxUint64 / MinInt64) where EqualValues could mask a sign/overflow bug.
func TestNative_IntegerWidths_Boundaries(t *testing.T) {
	rec := encodeFields(t,
		zap.Int("i", 42), zap.Int("imin", math.MinInt), zap.Int("imax", math.MaxInt),
		zap.Int8("i8min", math.MinInt8), zap.Int8("i8max", math.MaxInt8),
		zap.Int16("i16min", math.MinInt16), zap.Int16("i16max", math.MaxInt16),
		zap.Int32("i32min", math.MinInt32), zap.Int32("i32max", math.MaxInt32),
		zap.Int64("i64min", math.MinInt64), zap.Int64("i64max", math.MaxInt64),
		zap.Uint("u", 42), zap.Uint("umin", uint(0)), zap.Uint("umax", math.MaxUint),
		zap.Uint8("u8max", math.MaxUint8),
		zap.Uint16("u16max", math.MaxUint16),
		zap.Uint32("u32max", math.MaxUint32),
		zap.Uint64("u64", 42), zap.Uint64("u64max", math.MaxUint64),
		zap.Uintptr("uptr", 42), zap.Uintptr("uptrmax", ^uintptr(0)),
	)

	assert.EqualValues(t, 42, rec["i"])
	// MinInt==MinInt64 on 64-bit: pin exact type so a sign-flip in AddInt→AddInt64 cannot pass.
	assert.Equal(t, int64(math.MinInt), rec["imin"])
	// MaxInt==MaxInt64 on 64-bit: EqualValues because a large positive may decode as uint64.
	assert.EqualValues(t, int64(math.MaxInt), rec["imax"])
	assert.EqualValues(t, math.MinInt8, rec["i8min"])
	assert.EqualValues(t, math.MaxInt8, rec["i8max"])
	assert.EqualValues(t, math.MinInt16, rec["i16min"])
	assert.EqualValues(t, math.MaxInt16, rec["i16max"])
	assert.EqualValues(t, math.MinInt32, rec["i32min"])
	assert.EqualValues(t, math.MaxInt32, rec["i32max"])
	// MinInt64: pin the exact type — EqualValues against a uint64 could mask a sign flip.
	assert.Equal(t, int64(math.MinInt64), rec["i64min"])
	assert.EqualValues(t, int64(math.MaxInt64), rec["i64max"])
	assert.EqualValues(t, 42, rec["u"])
	// uint(0): small positive decodes as int64(0); EqualValues is type-agnostic.
	assert.EqualValues(t, 0, rec["umin"])
	// MaxUint==MaxUint64 on 64-bit: exact-type Equal so overflow-to-negative cannot pass.
	assert.Equal(t, uint64(math.MaxUint), rec["umax"])
	assert.EqualValues(t, math.MaxUint8, rec["u8max"])
	assert.EqualValues(t, math.MaxUint16, rec["u16max"])
	assert.EqualValues(t, math.MaxUint32, rec["u32max"])
	assert.EqualValues(t, 42, rec["u64"])
	// MaxUint64 > MaxInt64: exact-type Equal so an overflow-to-negative bug cannot pass.
	assert.Equal(t, uint64(math.MaxUint64), rec["u64max"])
	assert.EqualValues(t, 42, rec["uptr"])
	// ^uintptr(0)==MaxUint64 on 64-bit: exact-type Equal catches truncation in AddUintptr→AddUint64.
	assert.Equal(t, uint64(^uintptr(0)), rec["uptrmax"])
}

// TestNative_AppendScalars_InArray mirrors TestJSONEncoderArrays' per-type Append* matrix: it is
// the area-G count-bump guard. Every Append* must bump the array frame's element count exactly
// once; a single miscount corrupts the whole frame, so require.Len on the decoded slice is the
// load-bearing assertion. With minimalCfg, AppendTime→int64 unix-nanos and AppendDuration→int64
// nanos (incl. a NEGATIVE duration, mirroring zap's duration/negative case).
func TestNative_AppendScalars_InArray(t *testing.T) {
	negDur := time.Duration(-1)
	tm := time.Unix(1, 0)

	t.Run("full matrix", func(t *testing.T) {
		rec := encodeFields(t, zap.Array("a", arrMarshaler(func(enc zapcore.ArrayEncoder) error {
			enc.AppendBool(true)
			enc.AppendInt(42)
			enc.AppendInt8(math.MinInt8)
			enc.AppendInt16(math.MaxInt16)
			enc.AppendInt32(math.MinInt32)
			enc.AppendInt64(math.MaxInt64)
			enc.AppendUint(42)
			enc.AppendUint8(math.MaxUint8)
			enc.AppendUint16(math.MaxUint16)
			enc.AppendUint32(math.MaxUint32)
			enc.AppendUint64(math.MaxUint64)
			enc.AppendUintptr(7)
			enc.AppendFloat32(2.71)
			enc.AppendFloat64(3.14)
			enc.AppendComplex128(1 + 2i)
			enc.AppendComplex64(complex64(1 + 2i))
			enc.AppendByteString([]byte("bs"))
			enc.AppendString("s")
			enc.AppendTime(tm)
			enc.AppendDuration(negDur)

			return nil
		})))

		const appended = 20
		arr := rec["a"].([]any)
		require.Len(t, arr, appended, "every Append* must bump the array count exactly once")

		// Spot-check several element values + decoded TYPES.
		assert.Equal(t, true, arr[0])
		assert.EqualValues(t, math.MinInt8, arr[2])
		assert.Equal(t, uint64(math.MaxUint64), arr[10], "MaxUint64 element keeps its type/value")
		assert.Equal(t, float32(2.71), arr[12], "AppendFloat32 round-trips as float32, not float64")
		assert.InDelta(t, 3.14, arr[13].(float64), 0)
		assert.Equal(t, "1+2i", arr[14], "complex128 → string")
		assert.Equal(t, "1+2i", arr[15], "complex64 → string")
		assert.Equal(t, "bs", arr[16], "AppendByteString → msgpack str (string)")
		assert.Equal(t, "s", arr[17])
		assert.Equal(t, tm.UnixNano(), arr[18], "AppendTime → int64 unix-nanos under minimalCfg")
		assert.Equal(t, int64(-1), arr[19], "negative AppendDuration → int64 nanos")
	})

	// Same scalar twice → a 2-element array (mirrors zap's "expect f to be called twice").
	t.Run("same scalar twice", func(t *testing.T) {
		rec := encodeFields(t, zap.Array("a", arrMarshaler(func(enc zapcore.ArrayEncoder) error {
			enc.AppendInt64(42)
			enc.AppendInt64(42)

			return nil
		})))
		arr := rec["a"].([]any)
		require.Len(t, arr, 2)
		assert.EqualValues(t, 42, arr[0])
		assert.EqualValues(t, 42, arr[1])
	})
}

// TestNative_Float32_FiniteRoundTrips mirrors the finite float32/float64 cases of
// TestJSONEncoderObjectFields. float32 fields decode as float32 (the DECODED-TYPE gotcha); float64
// fields decode as float64.
func TestNative_Float32_FiniteRoundTrips(t *testing.T) {
	rec := encodeFields(t,
		zap.Float32("f271", 2.71),
		zap.Float32("f01", 0.1),
		zap.Float32("f1e10", 1e10),
		zap.Float64("d1e10", 1e10),
		zap.Float64("dpi", math.Pi),
	)
	assert.Equal(t, float32(2.71), rec["f271"])
	assert.Equal(t, float32(0.1), rec["f01"])
	assert.Equal(t, float32(1e10), rec["f1e10"])
	assert.InDelta(t, 1e10, rec["d1e10"].(float64), 0)
	assert.Equal(t, math.Pi, rec["dpi"])
}

// TestNative_Complex_Variants mirrors the complex128/complex64 cases (incl. negative imaginary and
// the 2.71+3.14i float32-formatting case). Each decodes as a "r±ii" string — no native complex.
func TestNative_Complex_Variants(t *testing.T) {
	rec := encodeFields(t,
		zap.Complex128("c128p", 1+2i),
		zap.Complex128("c128n", 1-2i),
		zap.Complex64("c64p", complex64(1+2i)),
		zap.Complex64("c64f", complex64(2.71+3.14i)),
	)
	assert.Equal(t, "1+2i", rec["c128p"])
	assert.Equal(t, "1-2i", rec["c128n"])
	assert.Equal(t, "1+2i", rec["c64p"])
	assert.Equal(t, "2.71+3.14i", rec["c64f"])
}

// TestNative_StringRawBytes_NoEscapingNoReplacement is THE divergence test, mirroring
// TestJSONEscaping (String + ByteString sub-tests) and key-escaping. Where zap JSON emits
// \uXXXX / \\ / �, the native encoder writes RAW msgpack str bytes: special chars, control
// bytes, astral unicode, and INVALID UTF-8 all pass through BYTE-EXACT — including in KEYS — with
// NO escaping and NO U+FFFD replacement. (Area-D passthrough guard: confirms AppendString /
// AppendStringFromBytes do not validate/replace.)
func TestNative_StringRawBytes_NoEscapingNoReplacement(t *testing.T) {
	inputs := []string{
		"", "foo", `"`, `\`, "a\nb", "a\tb",
		string([]byte{0x07}), // ASCII bell — zap JSON would emit 
		"☃",                  // astral-plane unicode
		"\xed\xa0\x80",       // invalid UTF-8 (RuneError) — zap JSON → ���
		"foo\xed\xa0\x80",    // valid prefix + invalid tail
	}

	for _, v := range inputs {
		t.Run("string/"+strconv.Quote(v), func(t *testing.T) {
			rec := encodeFields(t, zapcore.Field{Key: "k", Type: zapcore.StringType, String: v})
			assert.Equal(t, v, rec["k"], "AddString writes raw bytes — no escaping/replacement")
		})
		t.Run("byteString/"+strconv.Quote(v), func(t *testing.T) {
			rec := encodeFields(t, zapcore.Field{
				Key: "k", Type: zapcore.ByteStringType, Interface: []byte(v),
			})
			assert.Equal(t, v, rec["k"], "AddByteString → msgpack str, raw bytes byte-exact")
		})
	}

	// KEYS with special characters and invalid UTF-8 round-trip byte-exact too (no key escaping).
	// Uses recordKeys (raw msgpack parse) to avoid Go-map dedup and to assert byte-exactness.
	keyCorpus := []string{
		`weird"\` + "\nkey", "tab\tkey", "uni☃key",
		"\xed\xa0\x80", "foo\xed\xa0\x80",
	}
	for _, k := range keyCorpus {
		enc := newMsgpackEncoder(minimalCfg())
		buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{
			{Key: k, Type: zapcore.StringType, String: "v"},
		})
		require.NoError(t, err)
		require.Equal(t, []string{k}, recordKeys(t, buf.Bytes()),
			"record key must round-trip byte-exact (no escaping / no UTF-8 replacement): %q", k)
	}
}

// TestNative_ByteStringAndBinary_EmptyAndNil mirrors the byteString empty/nil cases of
// TestJSONEncoderObjectFields (and the binary case). AddByteString → msgpack str → "" (string);
// AddBinary → real msgpack bin → empty []byte. Empty and nil inputs are indistinguishable on wire.
func TestNative_ByteStringAndBinary_EmptyAndNil(t *testing.T) {
	rec := encodeFields(t,
		zapcore.Field{Key: "bsEmpty", Type: zapcore.ByteStringType, Interface: []byte{}},
		zapcore.Field{Key: "bsNil", Type: zapcore.ByteStringType, Interface: []byte(nil)},
		zapcore.Field{Key: "binEmpty", Type: zapcore.BinaryType, Interface: []byte{}},
		zapcore.Field{Key: "binNil", Type: zapcore.BinaryType, Interface: []byte(nil)},
	)

	// ByteString → string "" (decoded type is string).
	assert.Equal(t, "", rec["bsEmpty"])
	assert.IsType(t, "", rec["bsEmpty"])
	assert.Equal(t, "", rec["bsNil"])
	assert.IsType(t, "", rec["bsNil"])

	// Binary → []byte, empty (decoded type is []byte).
	assert.IsType(t, []byte(nil), rec["binEmpty"])
	assert.Empty(t, rec["binEmpty"].([]byte))
	assert.IsType(t, []byte(nil), rec["binNil"])
	assert.Empty(t, rec["binNil"].([]byte))
}

// TestNative_Reflected_NilAndNilElements mirrors json_encoder_test.go's null_value /
// array_with_null_elements cases (Tier-1/Tier-2 reflected nil handling, design §3.7). A nil
// reflected value decodes to nil; an array of {&struct{}{}, nil, (*struct{})(nil), 2} decodes to
// [{}, nil, nil, int64(2)] — the empty struct → {}, both nils → null.
func TestNative_Reflected_NilAndNilElements(t *testing.T) {
	rec := encodeFields(t,
		zap.Reflect("n", nil),
		zap.Reflect("a", []any{&struct{}{}, nil, (*struct{})(nil), 2}),
	)
	assert.Nil(t, rec["n"])
	assert.Equal(t, []any{map[string]any{}, nil, nil, int64(2)}, rec["a"])
}

// TestNative_NestedArrayInArray mirrors TestJSONEncoderArrays' arrays(success)/arrays(error).
// Success: an inner AppendArray nested in an outer array yields [[bool,bool]]. Error: the inner
// MarshalLogArray returns an error which the OUTER marshaler returns → zap converts the AddArray
// error to <key>Error. The partial inner frame is still sealed (mirrors zap keeping [true]).
func TestNative_NestedArrayInArray(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rec := encodeFields(t, zap.Array("a", arrMarshaler(func(outer zapcore.ArrayEncoder) error {
			return outer.AppendArray(arrMarshaler(func(inner zapcore.ArrayEncoder) error {
				inner.AppendBool(true)
				inner.AppendBool(false)

				return nil
			}))
		})))
		outer := rec["a"].([]any)
		require.Len(t, outer, 1, "one nested array element")
		inner := outer[0].([]any)
		require.Len(t, inner, 2)
		assert.Equal(t, true, inner[0])
		assert.Equal(t, false, inner[1])
	})

	t.Run("error propagates to keyError", func(t *testing.T) {
		rec := encodeFields(t, zap.Array("a", arrMarshaler(func(outer zapcore.ArrayEncoder) error {
			return outer.AppendArray(arrMarshaler(func(inner zapcore.ArrayEncoder) error {
				inner.AppendBool(true)

				return errors.New("fail")
			}))
		})))
		// The OUTER marshaler returned the error → Field.AddTo converts it to <key>Error.
		assert.Contains(t, rec, "aError", "a returned nested-array error becomes <key>Error")
		// The partial inner frame is still sealed (mirrors zap keeping [true]).
		assert.Equal(t, []any{[]any{true}}, rec["a"], "the partial nested array survives")
	})
}

// TestNative_Time_StringEncoder mirrors TestJSONEncoderTimeFormats: with a string time hook the
// value is whatever the hook writes. ISO8601TimeEncoder → an ISO8601 STRING (AddTime). NOTE the
// divergence from the task brief: EpochMillisTimeEncoder calls AppendFloat64, so AppendTime in an
// array decodes as a float64 number of millis (NOT int64) — the native encoder faithfully emits
// whatever the hook writes, exactly as zap's own JSON encoder does.
func TestNative_Time_StringEncoder(t *testing.T) {
	date := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("ISO8601 string", func(t *testing.T) {
		cfg := minimalCfg()
		cfg.EncodeTime = zapcore.ISO8601TimeEncoder
		cfg.EncodeDuration = zapcore.NanosDurationEncoder
		enc := newMsgpackEncoder(cfg)
		rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{zap.Time("k", date)})
		assert.Equal(t, "2000-01-02T03:04:05.000Z", rec["k"], "AddTime → ISO8601 string")
	})

	t.Run("EpochMillis float in array", func(t *testing.T) {
		cfg := minimalCfg()
		cfg.EncodeTime = zapcore.EpochMillisTimeEncoder
		enc := newMsgpackEncoder(cfg)
		rec := decodeEntryRecord(t, enc, zapcore.Entry{}, []zapcore.Field{
			zap.Array("a", arrMarshaler(func(ae zapcore.ArrayEncoder) error {
				ae.AppendTime(date)

				return nil
			})),
		})
		arr := rec["a"].([]any)
		require.Len(t, arr, 1)
		// Identical-formula expectation (EpochMillisTimeEncoder: float64(nanos)/float64(Millisecond)).
		assert.Equal(t, float64(date.UnixNano())/float64(time.Millisecond), arr[0])
	})
}

// TestNative_Clone_Isolation mirrors TestJSONClone: fields added to one clone never leak into a
// sibling clone or the base. Each clone is encoded independently (decodeEntry asserts no trailing
// bytes), proving the per-clone frame stacks are isolated.
func TestNative_Clone_Isolation(t *testing.T) {
	base := newMsgpackEncoder(minimalCfg())

	c1 := base.Clone()
	zap.String("a", "1").AddTo(c1)
	c2 := base.Clone()
	zap.String("b", "2").AddTo(c2)

	encodeClone := func(enc zapcore.Encoder) map[string]any {
		buf, err := enc.EncodeEntry(zapcore.Entry{}, nil)
		require.NoError(t, err)

		return decodeEntry(t, buf.Bytes()).Record.(map[string]any)
	}

	rec1 := encodeClone(c1)
	assert.Equal(t, "1", rec1["a"])
	assert.NotContains(t, rec1, "b", "c1 must not see c2's field")

	rec2 := encodeClone(c2)
	assert.Equal(t, "2", rec2["b"])
	assert.NotContains(t, rec2, "a", "c2 must not see c1's field")

	recBase := encodeClone(base)
	assert.NotContains(t, recBase, "a", "base must not see either clone's field")
	assert.NotContains(t, recBase, "b")
	assert.Empty(t, recBase, "base record is empty — clones are fully isolated")
}

// TestNative_DeepNamespaces_InObjectMarshaler mirrors TestJSONEncoderObjectFields' "multiple open
// namespaces": namespaces opened inside an ObjectMarshaler nest INSIDE the object and seal with it
// (design §3.2). The result is {foo:1, middle:{foo:2, inner:{foo:3}}}.
func TestNative_DeepNamespaces_InObjectMarshaler(t *testing.T) {
	rec := encodeFields(t, zap.Object("k", objMarshaler(func(enc zapcore.ObjectEncoder) error {
		enc.AddInt64("foo", 1)
		enc.OpenNamespace("middle")
		enc.AddInt64("foo", 2)
		enc.OpenNamespace("inner")
		enc.AddInt64("foo", 3)

		return nil
	})))

	obj := rec["k"].(map[string]any)
	assert.Equal(t, int64(1), obj["foo"])
	require.Len(t, obj, 2, "the object holds {foo, middle} — middle nests the rest")

	middle := obj["middle"].(map[string]any)
	assert.Equal(t, int64(2), middle["foo"])

	inner := middle["inner"].(map[string]any)
	assert.Equal(t, int64(3), inner["foo"])
	assert.Len(t, inner, 1)
}

// TestNative_ObjectMarshaler_PartialThenError mirrors the design's partial-container-survives
// contract (and the spirit of object(error)): a marshaler that writes a field and THEN returns an
// error keeps its PARTIAL container ({"written":"yes"}) on the wire AND yields <key>Error (the
// returned error is converted by Field.AddTo). The entry still ships.
func TestNative_ObjectMarshaler_PartialThenError(t *testing.T) {
	rec := encodeFields(t, zap.Object("o", objMarshaler(func(enc zapcore.ObjectEncoder) error {
		enc.AddString("written", "yes")

		return errors.New("boom")
	})))

	assert.Equal(t, map[string]any{"written": "yes"}, rec["o"], "the partial container survives")
	assert.Contains(t, rec, "oError", "the returned error becomes <key>Error")
}
