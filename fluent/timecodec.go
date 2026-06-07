package fluent

import (
	"time"

	"go.uber.org/zap/zapcore"
)

// TimeCodec bundles the two ends of the timestamp round-trip so they cannot drift: how zap
// writes the time field on the JSON wire (Key + ZapEncoder) and how the transcode Encoder
// reads it back into the Fluent EventTime (Key + Decode). Use a built-in (EpochNanosCodec,
// EpochMillisCodec, RFC3339NanoCodec, …) or supply your own.
type TimeCodec struct {
	// Key is the JSON field holding the timestamp (e.g. "ts").
	Key string
	// ZapEncoder is the zapcore time encoder for the encode end; NewCore (and ApplyTo)
	// wire it onto the EncoderConfig so the encode side matches Decode.
	ZapEncoder zapcore.TimeEncoder
	// Decode converts the JSON-decoded value at Key into a time. ok=false means
	// "absent or unparseable" → the Encoder falls back to time.Now().
	Decode func(value any) (time.Time, bool)
}

// ApplyTo wires this codec's encode end onto a zapcore.EncoderConfig (TimeKey + EncodeTime).
// Callers building their own zapcore.Core (instead of using NewCore) should call this so the
// encode side matches Decode.
func (c TimeCodec) ApplyTo(cfg *zapcore.EncoderConfig) {
	cfg.TimeKey = c.Key
	cfg.EncodeTime = c.ZapEncoder
}

// valid reports whether the codec is usable (a zero TimeCodec is not).
func (c TimeCodec) valid() bool { return c.Key != "" && c.ZapEncoder != nil && c.Decode != nil }

// defaultTimeCodec is used when no codec is configured: a magnitude-tolerant epoch decoder
// at key "ts", so a bring-your-own-core caller using zap's default float-seconds encoder is
// decoded correctly instead of to ~1970.
func defaultTimeCodec() TimeCodec { return AutoEpochCodec("ts") }

// AutoEpochCodec decodes a numeric epoch timestamp, auto-detecting its unit (s/ms/µs/ns) by
// magnitude — robust for log timestamps (always ~now, ~3 orders of magnitude apart per
// unit). Its encode end is EpochNanosTimeEncoder, so NewCore round-trips exactly while the
// decoder tolerates other units on the bring-your-own-core path. This is the default codec.
func AutoEpochCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochNanosTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return epochToTime(f), true
			}

			return time.Time{}, false
		},
	}
}

// Magnitude thresholds for detecting the unit of a numeric epoch timestamp. Today: seconds
// ~1.7e9, millis ~1.7e12, micros ~1.7e15, nanos ~1.7e18. Only pre-~2001 nanosecond values
// (which never occur in logs) would be misclassified.
const (
	epochMillisThreshold = 1e12 // >= this: at least milliseconds
	epochMicrosThreshold = 1e15 // >= this: at least microseconds
	epochNanosThreshold  = 1e18 // >= this: nanoseconds
)

// epochToTime converts a numeric epoch timestamp to a time, detecting its unit by magnitude.
func epochToTime(v float64) time.Time {
	switch {
	case v >= epochNanosThreshold:
		return time.Unix(0, int64(v))
	case v >= epochMicrosThreshold:
		return time.Unix(0, int64(v*1e3))
	case v >= epochMillisThreshold:
		return time.Unix(0, int64(v*1e6))
	default:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)

		return time.Unix(sec, nsec)
	}
}

// EpochNanosCodec encodes/decodes integer epoch nanoseconds (zapcore.EpochNanosTimeEncoder).
// Note: JSON numbers decode to float64, so nanosecond magnitudes lose ~tens of ns of
// precision. Use RFC3339NanoCodec when exact nanoseconds matter.
func EpochNanosCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochNanosTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return time.Unix(0, int64(f)), true
			}

			return time.Time{}, false
		},
	}
}

// EpochMillisCodec encodes/decodes integer epoch milliseconds (zapcore.EpochMillisTimeEncoder).
func EpochMillisCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochMillisTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return time.UnixMilli(int64(f)), true
			}

			return time.Time{}, false
		},
	}
}

// EpochSecondsCodec encodes/decodes floating-point epoch seconds — zap's default
// EpochTimeEncoder. Lets zap's out-of-the-box config work without misreading the time.
func EpochSecondsCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				sec := int64(f)
				nsec := int64((f - float64(sec)) * float64(time.Second))

				return time.Unix(sec, nsec), true
			}

			return time.Time{}, false
		},
	}
}

// RFC3339NanoCodec encodes/decodes RFC3339 timestamps with nanoseconds
// (zapcore.RFC3339NanoTimeEncoder). Exact to the nanosecond.
func RFC3339NanoCodec(key string) TimeCodec {
	return stringCodec(key, zapcore.RFC3339NanoTimeEncoder, time.RFC3339Nano)
}

// RFC3339Codec encodes/decodes RFC3339 timestamps at second precision
// (zapcore.RFC3339TimeEncoder).
func RFC3339Codec(key string) TimeCodec {
	return stringCodec(key, zapcore.RFC3339TimeEncoder, time.RFC3339)
}

// ISO8601Codec encodes/decodes ISO8601 millisecond-precision strings
// (zapcore.ISO8601TimeEncoder), e.g. "2006-01-02T15:04:05.000Z0700".
func ISO8601Codec(key string) TimeCodec {
	return stringCodec(key, zapcore.ISO8601TimeEncoder, "2006-01-02T15:04:05.000Z0700")
}

// stringCodec builds a codec for string timestamps. It tries the primary layout, then a few
// common variants, so minor format differences still parse.
func stringCodec(key string, enc zapcore.TimeEncoder, layout string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: enc,
		Decode: func(v any) (time.Time, bool) {
			s, ok := v.(string)
			if !ok {
				return time.Time{}, false
			}
			for _, l := range []string{layout, time.RFC3339Nano, time.RFC3339} {
				if t, err := time.Parse(l, s); err == nil {
					return t, true
				}
			}

			return time.Time{}, false
		},
	}
}
