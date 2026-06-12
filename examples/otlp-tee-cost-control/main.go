// Command otlp-tee-cost-control shows how to control OTel storage costs by
// gating the OTLP core at Warn+ while keeping a cheap console core at Info.
//
// The key lever is the LevelEnabler passed to otlp.NewCore: entries below the
// level are rejected in Check before any encoding, queueing, or network work
// occurs.  A zap.AtomicLevel makes the gate adjustable at runtime — useful
// during incidents when you temporarily want richer signal in OTel.
//
// The example is safe to run without a collector: without a receiver on :4318
// every OTel-bound record is counted as a drop.  The final lines report that
// count.  Expected drops: 3 (warn + error + the opened-gate info).
//
// Run it from the examples directory with:
//
//	go run ./otlp-tee-cost-control
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire/otlp"
)

func main() {
	// Endpoint: prefer the standard env vars, fall back to localhost.
	endpoint := otlp.EndpointFromEnv()
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4318"
	}

	// otelLvl is the cost gate.  Start at Warn so Info/Debug stay out of OTel.
	// Call otelLvl.SetLevel to widen or narrow the gate at runtime — no restart
	// required.  AtomicLevel also implements http.Handler for remote control.
	otelLvl := zap.NewAtomicLevelAt(zapcore.WarnLevel)

	otelCore, w, err := otlp.NewCore(
		endpoint,
		otelLvl,
		otlp.WithServiceName("checkout"),
		otlp.WithFlushInterval(200*time.Millisecond),
		otlp.WithErrorHandler(func(err error) {
			// Terminal ship-path events land here (exhausted retries, partial
			// success rejections, transport errors).  Print to stderr and keep
			// going — without a receiver these fire for every dropped record.
			fmt.Fprintf(os.Stderr, "otlp export: %v\n", err)
		}),
	)
	if err != nil {
		log.Fatalf("otlp.NewCore: %v", err)
	}

	// Console core: human-readable, Info and above — the "cheap" sink.
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(os.Stdout),
		zapcore.InfoLevel,
	)

	// Build logger from the console core, then tee in the OTLP core with
	// WrapCore.  The tee routes every entry to both cores; each core's own
	// LevelEnabler decides whether to accept it.
	logger := zap.New(consoleCore)
	logger = logger.WithOptions(zap.WrapCore(func(orig zapcore.Core) zapcore.Core {
		return zapcore.NewTee(orig, otelCore)
	}))

	fmt.Println("--- gate at WarnLevel: Info goes to console only ---")

	// Info: passes the console core's InfoLevel check, blocked by otelLvl WarnLevel.
	logger.Info("console only — below the OTel gate", zap.String("phase", "start"))

	// Warn and Error: pass both cores.
	logger.Warn("disk space low", zap.Int("pct", 92))  // both sinks
	logger.Error("write failed", zap.String("path", "/var/data/app.db")) // both sinks

	fmt.Println("\n--- runtime dial: open gate to InfoLevel ---")

	// Widen the gate: Info now reaches OTel too.
	otelLvl.SetLevel(zapcore.InfoLevel)
	logger.Info("now reaches OTel too", zap.String("phase", "incident"))

	// Close the gate again when the incident is resolved.
	otelLvl.SetLevel(zapcore.WarnLevel)

	// Sync flushes everything enqueued before the call; Close drains with a
	// single attempt then tears down the exporter.  Without a receiver the
	// OTel-bound records are counted as drops — safe to run standalone.
	if err := w.Sync(); err != nil {
		log.Printf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		log.Printf("close: %v", err)
	}

	// Expected: 3 drops — warn + error + the opened-gate info.
	// Without a collector all OTel-bound records are connection-refused and
	// counted immediately (transport errors are not retried).
	fmt.Printf("\ndropped logs (expected 3 when no collector is running): %d\n", w.DroppedLogs())
}
