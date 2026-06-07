package fluent

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func decodeEntry(t *testing.T, b []byte) Entry {
	t.Helper()
	var e Entry
	rest, err := e.UnmarshalMsg(b)
	require.NoError(t, err)
	require.Empty(t, rest, "no bytes must remain after decoding the entry")

	return e
}

func TestEncoder_StripsTimestampAndKeepsFields(t *testing.T) {
	// This record carries an RFC3339 string timestamp, so use the matching string codec; the
	// default codec is the magnitude-tolerant numeric epoch decoder (string ts requires an
	// explicit string codec).
	enc := NewEncoderWithCodec(RFC3339NanoCodec("ts"))
	out, err := enc.Encode(nil, []byte(`{"level":"info","msg":"test","ts":"2023-08-26T10:30:00.000Z"}`))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	rec := e.Record.(map[string]any)
	assert.Equal(t, "info", rec["level"])
	assert.Equal(t, "test", rec["msg"])
	_, hasTS := rec["ts"]
	assert.False(t, hasTS, "ts must be lifted out of the record")

	want, _ := time.Parse(time.RFC3339Nano, "2023-08-26T10:30:00.000Z")
	assert.True(t, time.Time(e.Time).Equal(want))
}

func TestEncoder_EpochNanosTimestamp(t *testing.T) {
	enc := NewEncoder()
	want := time.Unix(1692959400, 123456789).UTC()
	out, err := enc.Encode(nil, fmt.Appendf(nil, `{"msg":"x","ts":%d}`, want.UnixNano()))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	diff := time.Time(e.Time).UnixNano() - want.UnixNano()
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, int64(1000), "epoch nanos preserved to sub-microsecond")
}

// TestEncoder_NumericEpochUnits exercises magnitude-based unit detection: zap's default
// EpochTimeEncoder emits float SECONDS, while EpochNanosTimeEncoder emits nanos. The decoder
// must classify each by magnitude so neither decodes to ~1970.
func TestEncoder_NumericEpochUnits(t *testing.T) {
	want := time.Unix(1717286400, 123456789).UTC() // 2024-06-02, sub-second precision
	tests := []struct {
		name string
		ts   string // JSON numeric literal for "ts"
		tol  int64  // allowed |diff| in nanoseconds (unit truncation)
	}{
		{"seconds", fmt.Sprintf("%.9f", float64(want.UnixNano())/1e9), 1000},
		{"millis", fmt.Sprintf("%d", want.UnixMilli()), int64(time.Millisecond)},
		{"micros", fmt.Sprintf("%d", want.UnixMicro()), int64(time.Microsecond)},
		{"nanos", fmt.Sprintf("%d", want.UnixNano()), 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := NewEncoder().Encode(nil, fmt.Appendf(nil, `{"msg":"x","ts":%s}`, tc.ts))
			require.NoError(t, err)
			e := decodeEntry(t, out)
			diff := time.Time(e.Time).UnixNano() - want.UnixNano()
			if diff < 0 {
				diff = -diff
			}
			assert.LessOrEqual(t, diff, tc.tol, "decoded %s epoch must be ~want, not ~1970", tc.name)
		})
	}
}

func TestEncoder_InvalidJSON(t *testing.T) {
	_, err := NewEncoder().Encode(nil, []byte(`{"bad":`))
	require.Error(t, err)
}

func TestEncoder_NoTimestampFallsBackToNow(t *testing.T) {
	before := time.Now().Add(-time.Second)
	out, err := NewEncoder().Encode(nil, []byte(`{"msg":"no ts"}`))
	require.NoError(t, err)
	e := decodeEntry(t, out)
	assert.False(t, time.Time(e.Time).Before(before), "zero ts falls back to ~now")
}

func TestEncoderWithCodec_CustomKeyAndFormat(t *testing.T) {
	enc := NewEncoderWithCodec(EpochMillisCodec("timestamp"))
	want := time.Unix(1692959400, 456000000).UTC()
	out, err := enc.Encode(nil, fmt.Appendf(nil, `{"msg":"x","timestamp":%d}`, want.UnixMilli()))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	require.Equal(t, want.UnixMilli(), time.Time(e.Time).UnixMilli())
	rec := e.Record.(map[string]any)
	_, hasKey := rec["timestamp"]
	require.False(t, hasKey, "the configured time key must be lifted out of the record")
}

func TestEncoderWithCodec_RFC3339NanoExact(t *testing.T) {
	enc := NewEncoderWithCodec(RFC3339NanoCodec("ts"))
	want := time.Unix(1692959400, 123456789).UTC()
	out, err := enc.Encode(nil, fmt.Appendf(nil, `{"msg":"x","ts":%q}`, want.Format(time.RFC3339Nano)))
	require.NoError(t, err)
	require.True(t, time.Time(decodeEntry(t, out).Time).Equal(want))
}

func TestEncoderWithCodec_MissingKeyFallsBackToNow(t *testing.T) {
	before := time.Now().Add(-time.Second)
	enc := NewEncoderWithCodec(EpochMillisCodec("timestamp"))
	out, err := enc.Encode(nil, []byte(`{"msg":"no time field"}`))
	require.NoError(t, err)
	require.False(t, time.Time(decodeEntry(t, out).Time).Before(before))
}

// TestEncoder_NumericFieldIntegrity drives a real zap JSON encoder into the transcode Encoder
// and asserts numeric record fields keep their integer type and full precision on the msgpack
// wire. A plain json.Unmarshal would decode every number to float64, truncating int64 above
// 2^53 (9007199254740993 -> 9007199254740992) and erasing the integer type.
func TestEncoder_NumericFieldIntegrity(t *testing.T) {
	const bigInt = int64(9007199254740993) // 2^53 + 1: not exactly representable as float64
	const bigUint = uint64(math.MaxUint64) // exceeds MaxInt64: exercises the uint64 branch
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	jsonEnc := zapcore.NewJSONEncoder(cfg)
	buf, err := jsonEnc.EncodeEntry(zapcore.Entry{Time: time.Now(), Message: "x"}, []zapcore.Field{
		zap.Int64("id", bigInt),
		zap.Uint64("big", bigUint),
		zap.Float64("ratio", 1.5), // non-integral: must stay float64
		zap.Int("count", 7),
		zap.Int64s("ids", []int64{bigInt}), // array elements must normalize too
	})
	require.NoError(t, err)

	out, err := NewEncoder().Encode(nil, buf.Bytes())
	require.NoError(t, err)
	rec := decodeEntry(t, out).Record.(map[string]any)

	assert.Equal(t, bigInt, rec["id"], "int64 above 2^53 must round-trip exactly as int64")
	assert.Equal(t, bigUint, rec["big"], "uint64 above MaxInt64 must round-trip as uint64")
	assert.Equal(t, 1.5, rec["ratio"], "non-integral float must stay float64")
	assert.Equal(t, int64(7), rec["count"], "small int normalizes to int64")
	assert.Equal(t, []any{bigInt}, rec["ids"], "array elements above 2^53 must round-trip as int64")
}

func TestNewEncoder_DefaultIsMagnitudeTolerant(t *testing.T) {
	// The zero-arg NewEncoder uses the magnitude-tolerant default: it must decode epoch
	// nanos, seconds (zap's default), AND millis to the correct instant — never 1970.
	want := time.Unix(1692959400, 123456789).UTC()
	for _, in := range []string{
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.UnixNano()),  // nanoseconds
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.Unix()),      // seconds (zap default)
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.UnixMilli()), // milliseconds
	} {
		out, err := NewEncoder().Encode(nil, []byte(in))
		require.NoError(t, err)
		got := time.Time(decodeEntry(t, out).Time)
		require.WithinDuration(t, want, got, time.Second, "input %s", in)
	}
}
