package conformance

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/arloliu/zapwire/otlp"
)

func spanCtx(t *testing.T) (trace.SpanContext, context.Context) {
	t.Helper()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:     trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
		TraceFlags: trace.FlagsSampled,
	})

	return sc, trace.ContextWithSpanContext(context.Background(), sc)
}

// roundTripRecord asserts byte-identity for one encoded entry.
func roundTripRecord(t *testing.T, fields ...zapcore.Field) *logspb.LogRecord {
	t.Helper()
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{
		Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "msg",
	}, fields)
	require.NoError(t, err)
	defer buf.Free()
	ours := append([]byte(nil), buf.Bytes()...)

	var rec logspb.LogRecord
	require.NoError(t, proto.Unmarshal(ours, &rec), "our bytes must decode")
	remarshaled, err := proto.Marshal(&rec)
	require.NoError(t, err)
	require.Equal(t, remarshaled, ours, "byte identity with official marshaling")

	return &rec
}

type objAll struct{}

func (objAll) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("s", "v")
	e.AddInt64("i", -7)
	e.OpenNamespace("deep")
	e.AddBool("b", true)

	return nil
}

type arrMixed struct{}

func (arrMixed) MarshalLogArray(e zapcore.ArrayEncoder) error {
	e.AppendString("x")
	e.AppendFloat64(2.5)
	_ = e.AppendObject(objAll{})

	return nil
}

func TestRecordRoundTripFieldTypes(t *testing.T) {
	long := make([]byte, 300) // multi-byte length varints
	for i := range long {
		long[i] = 'a'
	}
	rec := roundTripRecord(t,
		zap.String("str", "v"), zap.String("long", string(long)),
		zap.Bool("bt", true), zap.Bool("bf", false),
		zap.Int("i", 42), zap.Int64("neg", -1),
		zap.Uint64("umax", 1<<63+1), // > MaxInt64 → string
		zap.Float64("f", 3.5), zap.Float32("f32", 1.25),
		zap.Binary("bin", []byte{0xde, 0xad}),
		zap.ByteString("bs", []byte("text")),
		zap.Duration("d", 1500*time.Millisecond),
		zap.Time("tm", time.Unix(1, 2)),
		zap.Complex128("c", complex(1, 2)),
		zap.Uintptr("up", 7),
		zap.Object("obj", objAll{}),
		zap.Array("arr", arrMixed{}),
		zap.Namespace("ns"), zap.String("inner", "x"),
	)
	require.Equal(t, "msg", rec.Body.GetStringValue())
	require.EqualValues(t, 9, rec.SeverityNumber)
	require.Equal(t, "info", rec.SeverityText)
	require.EqualValues(t, time.Unix(7, 42).UnixNano(), rec.TimeUnixNano)
	require.Equal(t, rec.TimeUnixNano, rec.ObservedTimeUnixNano)
}

func TestRecordRoundTripTrace(t *testing.T) {
	sc, ctx := spanCtx(t)
	rec := roundTripRecord(t, otlp.SpanContext(ctx))
	wantT, wantS := sc.TraceID(), sc.SpanID()
	require.Equal(t, wantT[:], rec.TraceId)
	require.Equal(t, wantS[:], rec.SpanId)
	require.EqualValues(t, 1, rec.Flags)

	// Absent: fields omitted entirely (nil, not zeros).
	rec = roundTripRecord(t)
	require.Nil(t, rec.TraceId)
	require.Nil(t, rec.SpanId)
	require.Zero(t, rec.Flags)

	// Unsampled (valid IDs, flags 0): IDs present, flags omitted — the
	// byte-identity round-trip inside roundTripRecord proves we did not emit
	// a zero fixed32 that proto.Marshal would drop.
	scUnsampled := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})
	rec = roundTripRecord(t, zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scUnsampled})
	require.NotNil(t, rec.TraceId)
	require.NotNil(t, rec.SpanId)
	require.Zero(t, rec.Flags)
}

func TestRecordRoundTripDegradedField(t *testing.T) {
	rec := roundTripRecord(t, zap.Any("ch", make(chan int)))
	require.Len(t, rec.Attributes, 1)
	require.Equal(t, "chError", rec.Attributes[0].Key)
}

type failingObj struct{}

func (failingObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("partial", "bytes")
	e.OpenNamespace("opened")

	return errors.New("fail")
}

// failingArr writes elements then fails (nested-array rollback row).
type failingArr struct{}

func (failingArr) MarshalLogArray(e zapcore.ArrayEncoder) error {
	e.AppendString("partial")
	_ = e.AppendObject(objAll{})

	return errors.New("fail")
}

func TestRecordRoundTripRollback(t *testing.T) {
	// Round-trip THROUGH the stubs after each rollback shape — proves no
	// stray bytes survive (design §9 degradation matrix).

	// Failing ObjectMarshaler (writes attr + opens namespace).
	rec := roundTripRecord(t, zap.String("good", "1"), zap.Object("bad", failingObj{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "badError", rec.Attributes[1].Key)

	// Failing zap.Inline (writes into the CURRENT level — the pass-2 P0
	// seam): bare "Error" attribute, nothing else.
	rec = roundTripRecord(t, zap.String("good", "1"), zap.Inline(failingObj{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "Error", rec.Attributes[1].Key)

	// Failing nested ArrayMarshaler.
	rec = roundTripRecord(t, zap.String("good", "1"), zap.Array("badarr", failingArr{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "badarrError", rec.Attributes[1].Key)
}

func TestFullRequestRoundTrip(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, w, err := otlp.NewCore(srv.URL, zapcore.DebugLevel,
		otlp.WithServiceName("conf-svc"),
		otlp.WithResource(zap.String("deployment.environment.name", "test")),
		otlp.WithScopeName("conformance"), otlp.WithScopeVersion("v1"))
	require.NoError(t, err)
	logger := zap.New(core)
	_, ctx := spanCtx(t)
	logger.Info("one", otlp.SpanContext(ctx), zap.String("k", "v"))
	logger.Warn("two")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 1)
	var req collogspb.ExportLogsServiceRequest
	require.NoError(t, proto.Unmarshal(bodies[0], &req))
	remarshaled, err := proto.Marshal(&req)
	require.NoError(t, err)
	require.Equal(t, remarshaled, bodies[0], "request byte identity")

	require.Len(t, req.ResourceLogs, 1)
	rl := req.ResourceLogs[0]
	require.Equal(t, "service.name", rl.Resource.Attributes[0].Key)
	require.Equal(t, "conf-svc", rl.Resource.Attributes[0].Value.GetStringValue())
	require.Len(t, rl.ScopeLogs, 1)
	require.Equal(t, "conformance", rl.ScopeLogs[0].Scope.Name)
	require.Equal(t, "v1", rl.ScopeLogs[0].Scope.Version)
	require.Len(t, rl.ScopeLogs[0].LogRecords, 2)
	require.Equal(t, "one", rl.ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	require.NotNil(t, rl.ScopeLogs[0].LogRecords[0].TraceId)
	require.EqualValues(t, 13, rl.ScopeLogs[0].LogRecords[1].SeverityNumber) // warn
}

func TestRecordRoundTripZeroOmission(t *testing.T) {
	// Empty message: body is a SET oneof — emitted even when "" (byte-identity
	// guard: removing it diverges from proto.Marshal).
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42)}, nil)
	require.NoError(t, err)
	ours := append([]byte(nil), buf.Bytes()...)
	buf.Free()
	var rec logspb.LogRecord
	require.NoError(t, proto.Unmarshal(ours, &rec))
	remarshaled, err := proto.Marshal(&rec)
	require.NoError(t, err)
	require.Equal(t, remarshaled, ours, "empty-body byte identity")
	require.NotNil(t, rec.Body, "body oneof must be set even for an empty message")
	require.Equal(t, "", rec.Body.GetStringValue())

	// SeverityUnspecified (custom mapper → 0): severity_number AND
	// severity_text zero-omission. Level(42) also exercises the default
	// mapper's unspecified branch; use a custom mapper for determinism.
	enc = otlp.NewEncoder(otlp.WithSeverityMapper(func(zapcore.Level) otlp.SeverityNumber { return otlp.SeverityUnspecified }))
	buf, err = enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "m"}, nil)
	require.NoError(t, err)
	ours = append([]byte(nil), buf.Bytes()...)
	buf.Free()
	rec = logspb.LogRecord{}
	require.NoError(t, proto.Unmarshal(ours, &rec))
	remarshaled, err = proto.Marshal(&rec)
	require.NoError(t, err)
	require.Equal(t, remarshaled, ours, "zero-severity byte identity")
	require.Zero(t, rec.SeverityNumber)
	// severity_text is emitted from Level.String() regardless of mapped number;
	// byte-identity is the load-bearing assertion for omission correctness.
	require.Equal(t, "info", rec.SeverityText)
}

func TestRecordRoundTripWithNamespaceAndMeta(t *testing.T) {
	// With-fields live on the encoder; an OPEN namespace must wrap per-call
	// fields while entry-metadata attributes pin to the root (Task 5 order:
	// With → meta → per-call-inside-namespace). Byte identity through the
	// official stubs proves the framing.
	enc := otlp.NewEncoder()
	enc.AddString("with", "1")
	enc.OpenNamespace("wns") // left open — wraps the per-call fields

	pc, file, line, ok := runtime.Caller(0)
	require.True(t, ok)
	fn := runtime.FuncForPC(pc).Name()
	buf, err := enc.EncodeEntry(zapcore.Entry{
		Level:      zapcore.InfoLevel,
		Time:       time.Unix(7, 42),
		Message:    "msg",
		LoggerName: "lg",
		Caller: zapcore.EntryCaller{
			Defined:  true,
			PC:       pc,
			File:     file,
			Line:     line,
			Function: fn,
		},
	}, []zapcore.Field{zap.String("per", "call")})
	require.NoError(t, err)
	defer buf.Free()
	ours := append([]byte(nil), buf.Bytes()...)

	var rec logspb.LogRecord
	require.NoError(t, proto.Unmarshal(ours, &rec))
	remarshaled, err := proto.Marshal(&rec)
	require.NoError(t, err)
	require.Equal(t, remarshaled, ours, "byte identity with official marshaling")

	keys := make([]string, len(rec.Attributes))
	for i, kv := range rec.Attributes {
		keys[i] = kv.Key
	}
	require.Equal(t, []string{"with", "logger", "code.function.name", "code.file.path", "code.line.number", "wns"}, keys)
	wns := rec.Attributes[len(rec.Attributes)-1].Value.GetKvlistValue()
	require.NotNil(t, wns)
	require.Len(t, wns.Values, 1)
	require.Equal(t, "per", wns.Values[0].Key)
}
