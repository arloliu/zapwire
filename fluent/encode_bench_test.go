package fluent

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	zapwire "github.com/arloliu/zapwire"
)

// benchSink keeps encode results live so the compiler cannot elide the work.
var benchSink int

// benchEntry is the shared zap entry both encode paths start from, so the only
// difference measured is how each path turns (entry, fields) into msgpack.
var benchEntry = zapcore.Entry{
	Level:      zapcore.InfoLevel,
	Time:       time.Unix(1692959400, 123456789),
	LoggerName: "zapwire",
	Message:    "request completed",
	Caller:     zapcore.EntryCaller{Defined: true, File: "server/handler.go", Line: 142},
}

type benchProfile struct {
	name   string
	fields []zapcore.Field
}

// benchProfiles returns field sets of increasing size and type variety, so the
// benchmarks show how the encode gap scales with record size and field mix.
func benchProfiles() []benchProfile {
	return []benchProfile{
		{"small", []zapcore.Field{
			zap.String("service", "zapwire"),
			zap.Int("status", 200),
			zap.String("method", "GET"),
		}},
		{"medium", []zapcore.Field{
			zap.String("service", "zapwire"),
			zap.String("method", "GET"),
			zap.String("path", "/v1/resources/42"),
			zap.Int("status", 200),
			zap.Int64("bytes", 4096),
			zap.Bool("cached", true),
			zap.Float64("latency_ms", 12.5),
			zap.Duration("elapsed", 12500*time.Microsecond),
		}},
		{"large", []zapcore.Field{
			zap.String("service", "zapwire"),
			zap.String("env", "production"),
			zap.String("region", "us-east-1"),
			zap.String("method", "POST"),
			zap.String("path", "/v1/resources"),
			zap.String("trace_id", "abc123def456abc123def456abc12345"),
			zap.Int("status", 201),
			zap.Int64("bytes", 65536),
			zap.Uint64("seq", 9876543210),
			zap.Bool("cached", false),
			zap.Float64("latency_ms", 42.75),
			zap.Duration("elapsed", 42750*time.Microsecond),
			zap.Time("started_at", time.Unix(1692959400, 0)),
			zap.Strings("tags", []string{"api", "write", "v1"}),
			zap.Object("http", objMarshaler(func(enc zapcore.ObjectEncoder) error {
				enc.AddString("ua", "curl/8.0")
				enc.AddInt("retries", 2)

				return nil
			})),
		}},
	}
}

// benchEncoders builds the two encoders under test from one EncoderConfig and
// TimeCodec, so they share field keys and the "ts" timestamp field: the native
// msgpack encoder, the zapcore JSON encoder, and the fluent transcode encoder.
func benchEncoders() (native, jsonEnc zapcore.Encoder, transcode Encoder) {
	codec := AutoEpochCodec("ts")
	cfg := zap.NewProductionEncoderConfig()
	codec.ApplyTo(&cfg)

	return NewMsgpackEncoder(cfg), zapcore.NewJSONEncoder(cfg), NewEncoderWithCodec(codec)
}

// TestEncodePayload_Parity guards BenchmarkEncodePayload's fairness premise:
// the native and transcode paths must produce the SAME record for every
// benchmark profile, so the benchmark times equivalent work rather than the
// native path silently doing less (e.g. dropping a field). msgpack map key
// order is not significant, so records are compared decoded. Broader type-level
// equivalence is covered by the equiv / json_encoder-parity tests; this pins
// the exact entry + fields the benchmark measures, including Caller/LoggerName.
func TestEncodePayload_Parity(t *testing.T) {
	codec := AutoEpochCodec("ts")
	cfg := zap.NewProductionEncoderConfig()
	codec.ApplyTo(&cfg)

	for _, p := range benchProfiles() {
		t.Run(p.name, func(t *testing.T) {
			native := decodeEntryRecord(t, newMsgpackEncoder(cfg), benchEntry, p.fields)
			transcoded := transcodeRecord(t, cfg, benchEntry, p.fields)
			delete(transcoded, "ts") // native lifts time via the envelope extension

			// Canonicalize the one benign divergence: the transcode path folds
			// any integer that fits in int64 down to int64, while native keeps
			// the original uint64 msgpack subtype — same value either way. The
			// guard still catches real differences (dropped fields, wrong
			// values, wrong types).
			canonInts(native)
			canonInts(transcoded)
			require.Equal(t, transcoded, native)
		})
	}
}

// canonInts collapses uint64 values that fit in int64 down to int64, recursing
// into nested maps and slices. See TestEncodePayload_Parity for why.
func canonInts(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if u, ok := e.(uint64); ok && u <= math.MaxInt64 {
				x[k] = int64(u)
			} else {
				canonInts(e)
			}
		}
	case []any:
		for i, e := range x {
			if u, ok := e.(uint64); ok && u <= math.MaxInt64 {
				x[i] = int64(u)
			} else {
				canonInts(e)
			}
		}
	}
}

// BenchmarkEncodePayload compares producing the Fluent Forward
// [EventTime, record] msgpack payload two ways from the SAME zap entry + fields:
//
//   - native:    NewMsgpackEncoder.EncodeEntry writes zap fields straight to
//     msgpack.
//   - transcode: the zapcore JSON encoder emits a log line, then
//     fluent.Encoder.Encode parses that JSON back into the same msgpack payload
//     (the v1 json->msgpack path).
//
// Both start from (entry, fields) and end at the same msgpack payload, so this
// is a like-for-like encoder comparison — unlike the writer-level benches in
// bench_test.go, which feed the transcode path a prebuilt JSON []byte. Framing
// and the socket are excluded; only the encode step is measured.
func BenchmarkEncodePayload(b *testing.B) {
	native, jsonEnc, transcode := benchEncoders()

	for _, p := range benchProfiles() {
		b.Run(p.name+"/native", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf, err := native.EncodeEntry(benchEntry, p.fields)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += buf.Len()
				buf.Free()
			}
		})

		b.Run(p.name+"/transcode", func(b *testing.B) {
			// scratch mirrors the Writer's pooled sync destination, so the
			// transcode output buffer is reused just like native's pooled one.
			var scratch []byte
			b.ReportAllocs()
			for b.Loop() {
				jbuf, err := jsonEnc.EncodeEntry(benchEntry, p.fields)
				if err != nil {
					b.Fatal(err)
				}
				out, err := transcode.Encode(scratch[:0], jbuf.Bytes())
				jbuf.Free()
				if err != nil {
					b.Fatal(err)
				}
				scratch = out
				benchSink += len(out)
			}
		})
	}
}

// benchTranscodeLogger builds a real zap.Logger on the transcode NewCore path,
// the json->msgpack counterpart to benchNativeLogger.
func benchTranscodeLogger(b *testing.B, opts ...zapwire.Option) (*zap.Logger, func()) {
	b.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_transcode_bench_%d.sock", time.Now().UnixNano()))
	ln := drainingServer(b, path)
	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig(),
		WithZapwireOptions(opts...))
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return zap.New(core), func() { _ = w.Close(); _ = ln.Close(); _ = os.Remove(path) }
}

// BenchmarkLogger compares the two paths end to end through a real zap.Logger
// and the full Writer/Framer/socket pipeline, to a draining consumer. Both
// loggers start from identical zap fields, so this is the fair end-to-end
// counterpart to the encoder-only BenchmarkEncodePayload (and to the deliberately
// asymmetric writer benches in bench_test.go).
func BenchmarkLogger(b *testing.B) {
	for _, p := range benchProfiles() {
		b.Run(p.name+"/native", func(b *testing.B) {
			logger, cleanup := benchNativeLogger(b)
			defer cleanup()
			b.ReportAllocs()
			for b.Loop() {
				logger.Info(benchEntry.Message, p.fields...)
			}
		})

		b.Run(p.name+"/transcode", func(b *testing.B) {
			logger, cleanup := benchTranscodeLogger(b)
			defer cleanup()
			b.ReportAllocs()
			for b.Loop() {
				logger.Info(benchEntry.Message, p.fields...)
			}
		})
	}
}
