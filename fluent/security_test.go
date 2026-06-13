package fluent

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// deepObject is a zapcore.ObjectMarshaler that recurses n levels deep. It models
// a pathological or attacker-shaped marshaler used to prove the encoder caps
// nesting instead of recursing the goroutine stack to an uncatchable throw.
type deepObject struct{ n int }

func (d deepObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if d.n <= 0 {
		return nil
	}

	return enc.AddObject("child", deepObject{n: d.n - 1})
}

// deepArray is the array-side counterpart of deepObject.
type deepArray struct{ n int }

func (d deepArray) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	if d.n <= 0 {
		return nil
	}

	return enc.AppendArray(deepArray{n: d.n - 1})
}

// TestEncoderDepthCapNoCrash drives marshalers far past the depth cap through
// EncodeEntry. With the guard the over-depth field degrades to a <key>Error and
// the entry ships as VALID msgpack; without it the goroutine stack would exhaust
// (a fatal, unrecoverable throw). decodeEntryRecord fully decodes the result, so
// a malformed map (e.g. a keyed-but-valueless pair from a mis-placed guard)
// fails the decode rather than passing vacuously.
func TestEncoderDepthCapNoCrash(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field zapcore.Field
	}{
		{"object", zap.Object("root", deepObject{n: maxEncodeDepth * 10})},
		{"array", zap.Array("root", deepArray{n: maxEncodeDepth * 10})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := decodeEntryRecord(t, newMsgpackEncoder(minimalCfg()), zapcore.Entry{}, []zapcore.Field{tc.field})
			require.Contains(t, rec, "rootError", "over-depth field degrades to <key>Error in valid msgpack")
		})
	}
}

// TestEncoderNormalNestingUnaffected confirms the cap does not perturb encoding
// of legitimately shallow nesting.
func TestEncoderNormalNestingUnaffected(t *testing.T) {
	enc := newMsgpackEncoder(minimalCfg())
	buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{
		zap.Object("root", deepObject{n: 8}),
	})
	require.NoError(t, err)
	require.NotZero(t, buf.Len())
}

// TestCheckBinSize exercises the msgpack bin32 overflow guard at its boundary.
// The >4 GiB case is unreachable in a real test (it would allocate 4 GiB), so
// the guard is split into checkBinSize and tested directly.
func TestCheckBinSize(t *testing.T) {
	require.NoError(t, checkBinSize(0))
	require.NoError(t, checkBinSize(1<<16))

	if uint64(^uint(0)) != ^uint64(0) {
		t.Skip("bin32 overflow boundary is only representable on 64-bit int")
	}
	overLimit := int(uint64(0xFFFFFFFF) + 1)
	require.NoError(t, checkBinSize(overLimit-1), "0xFFFFFFFF is the largest valid bin32 length")
	require.Error(t, checkBinSize(overLimit), "one byte past the bin32 limit must be rejected")
}
