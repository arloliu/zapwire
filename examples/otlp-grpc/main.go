// Command otlp-grpc ships zap logs to an OTLP/gRPC collector (port 4317)
// using zapwire/otlp's hand-rolled stdlib gRPC transport, with env-driven
// protocol dispatch as the fallback pattern.
//
// The example is safe to run without a collector: when nothing is listening on
// the endpoint every record is counted as a drop, and the final lines report
// that count. No panic, no hang.
//
// Run it from the examples directory with:
//
//	go run ./otlp-grpc
package main

import (
	"fmt"
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire/otlp"
)

func main() {
	endpoint := otlp.EndpointFromEnv()
	if endpoint == "" {
		endpoint = "127.0.0.1:4317"
	}

	var (
		core zapcore.Core
		w    *otlp.Writer
		err  error
	)
	switch otlp.ProtocolFromEnv() {
	case otlp.ProtocolHTTPProtobuf:
		core, w, err = otlp.NewHTTPCore(endpoint, zapcore.InfoLevel,
			otlp.WithServiceName("otlp-grpc-example"))
	default: // grpc is this example's default
		core, w, err = otlp.NewGRPCCore(endpoint, zapcore.InfoLevel,
			otlp.WithInsecure(), // local collector, plaintext h2c
			otlp.WithServiceName("otlp-grpc-example"),
		)
	}
	if err != nil {
		log.Fatalf("otlp: %v", err)
	}

	logger := zap.New(core)

	logger.Info("hello over gRPC",
		zap.String("transport", "grpc"),
		zap.Int("port", 4317),
	)

	// Graceful shutdown: Sync flushes everything enqueued before the call;
	// Close drains with a single attempt then tears down the exporter.
	// With no receiver running, records are counted as drops — the example is
	// safe to run without a collector.
	if err := w.Sync(); err != nil {
		log.Printf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Printf("close: %v", err)
	}

	fmt.Printf("\ndropped logs (expected > 0 when no collector is running): %d\n", w.DroppedLogs())
}
