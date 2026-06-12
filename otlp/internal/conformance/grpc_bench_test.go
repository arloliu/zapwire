package conformance

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	respb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/arloliu/zapwire/otlp"
)

// benchSink is a minimal always-OK LogsService.
type benchSink struct {
	collogspb.UnimplementedLogsServiceServer
}

func (benchSink) Export(context.Context, *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func startBenchSink(b *testing.B) string {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, benchSink{})
	go func() { _ = srv.Serve(lis) }()
	b.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// encodedRecord produces one realistic encoded LogRecord via the zapwire encoder.
func encodedRecord(b *testing.B) []byte {
	b.Helper()
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "benchmark message"},
		[]zapcore.Field{{Key: "k", Type: zapcore.StringType, String: "v"}, {Key: "n", Type: zapcore.Int64Type, Integer: 12345}})
	if err != nil {
		b.Fatal(err)
	}
	defer buf.Free()

	return append([]byte(nil), buf.Bytes()...)
}

// BenchmarkGRPCExportZapwire: full zapwire pipeline per iteration —
// batchSize records written + Sync (one Export per iteration at steady state).
// NOTE: this side measures the FULL pipeline (queue/batch/encode/frame) while the
// grpc-go benchmark below measures only marshal+Export — the asymmetry structurally
// favors grpc-go, so a competitive zapwire number is conservative evidence.
func BenchmarkGRPCExportZapwire(b *testing.B) {
	for _, batch := range []int{1, 64, 512} {
		b.Run(fmtBatch(batch), func(b *testing.B) {
			addr := startBenchSink(b)
			w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(),
				otlp.WithBatchSize(batch), otlp.WithQueueSize(batch*2), otlp.WithFlushInterval(time.Hour))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			rec := encodedRecord(b)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for range batch {
					_, _ = w.Write(rec)
				}
				_ = w.Sync()
			}
			b.StopTimer()
			if d := w.DroppedLogs(); d != 0 {
				b.Fatalf("dropped %d records", d)
			}
		})
	}
}

// BenchmarkGRPCExportGRPCGo: conventional grpc-go client — proto marshal of
// an equivalent batch + unary Export per iteration.
func BenchmarkGRPCExportGRPCGo(b *testing.B) {
	for _, batch := range []int{1, 64, 512} {
		b.Run(fmtBatch(batch), func(b *testing.B) {
			addr := startBenchSink(b)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			client := collogspb.NewLogsServiceClient(conn)
			recs := make([]*logspb.LogRecord, batch)
			for i := range recs {
				recs[i] = &logspb.LogRecord{
					TimeUnixNano:   uint64(time.Unix(7, 42).UnixNano()), //nolint:gosec
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "benchmark message"}},
					Attributes: []*commonpb.KeyValue{
						{Key: "k", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "v"}}},
						{Key: "n", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 12345}}},
					},
				}
			}
			tmpl := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
				Resource:  &respb.Resource{Attributes: []*commonpb.KeyValue{{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "bench"}}}}},
				ScopeLogs: []*logspb.ScopeLogs{{LogRecords: recs}},
			}}}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := client.Export(ctx, tmpl); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func fmtBatch(n int) string {
	switch n {
	case 1:
		return "batch1"
	case 64:
		return "batch64"
	default:
		return "batch512"
	}
}
