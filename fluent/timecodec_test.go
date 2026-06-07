package fluent

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// encodeTimeValue drives the codec's zap TimeEncoder through a JSON encoder and reads the time
// field back, capturing the JSON-ish value zap would write so it can be fed to codec.Decode.
func encodeTimeValue(t *testing.T, c TimeCodec, want time.Time) any {
	t.Helper()
	cfg := zapcore.EncoderConfig{
		TimeKey: c.Key, MessageKey: "msg", EncodeTime: c.ZapEncoder,
	}
	enc := zapcore.NewJSONEncoder(cfg)
	buf, err := enc.EncodeEntry(zapcore.Entry{Time: want, Message: "x"}, nil)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, jsonUnmarshal(buf.Bytes(), &m))

	return m[c.Key]
}

func TestBuiltinCodecs_RoundTrip(t *testing.T) {
	want := time.Unix(1692959400, 123000000).UTC() // ms-aligned so all codecs can match
	codecs := map[string]TimeCodec{
		"epochNanos":   EpochNanosCodec("ts"),
		"epochMillis":  EpochMillisCodec("ts"),
		"epochSeconds": EpochSecondsCodec("ts"),
		"rfc3339nano":  RFC3339NanoCodec("ts"),
		"rfc3339":      RFC3339Codec("ts"),
		"iso8601":      ISO8601Codec("ts"),
	}
	for name, c := range codecs {
		t.Run(name, func(t *testing.T) {
			v := encodeTimeValue(t, c, want)
			got, ok := c.Decode(v)
			require.True(t, ok, "codec must decode its own encoded value")
			// Allow up to 1s slop for second-precision codecs (rfc3339, epochSeconds float).
			require.WithinDuration(t, want, got, time.Second)
		})
	}
}

func TestEpochMillisCodec_ExactToMillis(t *testing.T) {
	want := time.Unix(1692959400, 123000000).UTC()
	v := encodeTimeValue(t, EpochMillisCodec("ts"), want)
	got, ok := EpochMillisCodec("ts").Decode(v)
	require.True(t, ok)
	require.Equal(t, want.UnixMilli(), got.UnixMilli())
}

func TestRFC3339NanoCodec_ExactToNanos(t *testing.T) {
	want := time.Unix(1692959400, 123456789).UTC()
	v := encodeTimeValue(t, RFC3339NanoCodec("ts"), want)
	got, ok := RFC3339NanoCodec("ts").Decode(v)
	require.True(t, ok)
	require.True(t, got.Equal(want), "RFC3339Nano is exact: got %v want %v", got, want)
}

func TestDecode_WrongType(t *testing.T) {
	_, ok := EpochNanosCodec("ts").Decode("not-a-number")
	require.False(t, ok)
	_, ok = RFC3339NanoCodec("ts").Decode(12345)
	require.False(t, ok)
}

func TestApplyTo_SetsBothEnds(t *testing.T) {
	c := EpochMillisCodec("when")
	var cfg zapcore.EncoderConfig
	c.ApplyTo(&cfg)
	require.Equal(t, "when", cfg.TimeKey)
	require.NotNil(t, cfg.EncodeTime)
}
