// Command tee-console shows how to fan logs out to more than one destination
// with zapcore.NewTee: human-readable lines on the console AND structured logs
// shipped to a processor over the wire.
//
// Each core in the tee keeps its own level and encoder, so this example logs
// verbosely to the console (Debug+) while shipping only Info+ over the wire.
//
// Run it from the examples directory with:
//
//	go run ./tee-console
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
	"github.com/arloliu/zapwire/ndjson"
)

const wantLines = 2 // only Info+ is shipped to the sink

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	received := make(chan string, wantLines)
	go runSink(ln, received)

	// Core A: ship NDJSON over TCP, Info and above.
	wireCore, writer, err := ndjson.NewCore(
		zapwire.TCP(ln.Addr().String()),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
	)
	if err != nil {
		log.Fatalf("new core: %v", err)
	}
	defer writer.Close()

	// Core B: human-readable console, Debug and above.
	console := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(os.Stdout),
		zap.DebugLevel,
	)

	logger := zap.New(zapcore.NewTee(wireCore, console))

	fmt.Println("--- console output (Debug and above) ---")
	logger.Debug("verbose detail", zap.String("phase", "warmup"))  // console only
	logger.Info("service started", zap.String("version", "1.2.3")) // console + wire
	logger.Warn("disk space low", zap.Int("pct", 92))              // console + wire

	_ = logger.Sync()

	fmt.Println("\n--- logs shipped over the wire (Info and above) ---")
	for i := 0; i < wantLines; i++ {
		select {
		case line := <-received:
			fmt.Printf("sink received: %s\n", line)
		case <-time.After(2 * time.Second):
			log.Fatal("timed out waiting for logs to arrive")
		}
	}
}

func runSink(ln net.Listener, out chan<- string) {
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		out <- scanner.Text()
	}
}
