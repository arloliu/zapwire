package conformance

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // registers gzip compressor for grpc-go server-side decompression
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"

	"github.com/arloliu/zapwire/otlp"
)

// logsSink is a real grpc-go LogsService capturing requests; respond is
// consulted per call (nil → empty success).
type logsSink struct {
	collogspb.UnimplementedLogsServiceServer
	mu      sync.Mutex
	reqs    []*collogspb.ExportLogsServiceRequest
	respond func(call int) (*collogspb.ExportLogsServiceResponse, error)
}

func (s *logsSink) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	call := len(s.reqs)
	respond := s.respond
	s.mu.Unlock()
	if respond != nil {
		return respond(call)
	}

	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s *logsSink) requests() []*collogspb.ExportLogsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*collogspb.ExportLogsServiceRequest(nil), s.reqs...)
}

func startGRPCSink(t *testing.T, sink *logsSink) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, sink)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String() // bare host:port → exercise WithInsecure
}

func TestGRPCInteropRoundTrip(t *testing.T) {
	sink := &logsSink{}
	addr := startGRPCSink(t, sink)
	core, w, err := otlp.NewGRPCCore(addr, zapcore.InfoLevel,
		otlp.WithInsecure(),
		otlp.WithServiceName("conformance"),
	)
	require.NoError(t, err)
	logger := zap.New(core)

	sc, ctx := spanCtx(t)
	_ = sc
	logger.Info("interop", zap.String("k", "v"), zap.Int64("n", -7), zap.Any("context", ctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())

	reqs := sink.requests()
	require.Len(t, reqs, 1)
	rl := reqs[0].GetResourceLogs()
	require.Len(t, rl, 1)
	attrs := rl[0].GetResource().GetAttributes()
	foundSvc := false
	for _, kv := range attrs {
		if kv.GetKey() == "service.name" {
			require.Equal(t, "conformance", kv.GetValue().GetStringValue())
			foundSvc = true
		}
	}
	require.True(t, foundSvc)
	recs := rl[0].GetScopeLogs()[0].GetLogRecords()
	require.Len(t, recs, 1)
	require.Equal(t, "interop", recs[0].GetBody().GetStringValue())
	require.NotZero(t, recs[0].GetTraceId(), "trace correlation must survive the gRPC transport")

	// Full-fidelity attribute check: every emitted field must survive the
	// hand-rolled gRPC transport and grpc-go deserialization (design §9).
	foundK, foundN := false, false
	for _, kv := range recs[0].GetAttributes() {
		switch kv.GetKey() {
		case "k":
			require.Equal(t, "v", kv.GetValue().GetStringValue(), `attribute "k" must be string "v"`)
			foundK = true
		case "n":
			require.Equal(t, int64(-7), kv.GetValue().GetIntValue(), `attribute "n" must be int64 -7`)
			foundN = true
		}
	}
	require.True(t, foundK, `attribute "k" not found in log record attributes`)
	require.True(t, foundN, `attribute "n" not found in log record attributes`)
}

func TestGRPCInteropGzip(t *testing.T) {
	sink := &logsSink{}
	addr := startGRPCSink(t, sink)
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithCompression(otlp.Gzip))
	require.NoError(t, err)
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "gz"}, nil)
	require.NoError(t, err)
	_, _ = w.Write(buf.Bytes())
	buf.Free()
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	reqs := sink.requests()
	require.Len(t, reqs, 1) // grpc-go transparently gunzipped our per-message compression
	require.Equal(t, "gz", reqs[0].GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue())
}

func TestGRPCInteropPartialSuccess(t *testing.T) {
	sink := &logsSink{respond: func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return &collogspb.ExportLogsServiceResponse{
			PartialSuccess: &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "one rejected"},
		}, nil
	}}
	addr := startGRPCSink(t, sink)
	var mu sync.Mutex
	var events []error
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithErrorHandler(func(e error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'}) // minimal LogRecord{body:"hi"}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 1)
	var ee *otlp.ExportError
	require.ErrorAs(t, events[0], &ee)
	require.EqualValues(t, 1, ee.Rejected)
	require.Equal(t, "one rejected", ee.Message)
}

func TestGRPCInteropRetryInfo(t *testing.T) {
	sink := &logsSink{respond: func(call int) (*collogspb.ExportLogsServiceResponse, error) {
		if call == 1 {
			st := status.New(codes.Unavailable, "throttled")
			st, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(50 * time.Millisecond)})
			if err != nil {
				return nil, err
			}

			return nil, st.Err()
		}

		return &collogspb.ExportLogsServiceResponse{}, nil
	}}
	addr := startGRPCSink(t, sink)
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(),
		otlp.WithRetry(otlp.RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	require.NoError(t, err)
	start := time.Now()
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'})
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	require.Less(t, time.Since(start), 10*time.Second, "RetryInfo (50ms) must beat the 1h backoff")
	require.Len(t, sink.requests(), 2)
}

func TestGRPCInteropInvalidArgumentDrops(t *testing.T) {
	sink := &logsSink{respond: func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad payload")
	}}
	addr := startGRPCSink(t, sink)
	var mu sync.Mutex
	var events []error
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithErrorHandler(func(e error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'})
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	var ee *otlp.ExportError
	require.ErrorAs(t, events[0], &ee)
	require.Equal(t, 3, ee.GRPCStatus)
	require.False(t, ee.Retryable)
	require.Contains(t, ee.Message, "bad payload")
}
