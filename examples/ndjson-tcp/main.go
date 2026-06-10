// Command ndjson-tcp is a self-contained zapwire example: it stands up a local
// TCP sink, ships a few logs to it as newline-delimited JSON (NDJSON) in the
// default synchronous mode, and prints exactly what the sink received.
//
// A real deployment would point the transport at Vector, Logstash, or an
// OpenTelemetry Collector instead of this in-process sink.
//
// Run it from the examples directory with:
//
//	go run ./ndjson-tcp
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
	"github.com/arloliu/zapwire/ndjson"
)

const wantLines = 3

func main() {
	// 1. Stand up a throwaway TCP sink on the loopback interface.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	received := make(chan string, wantLines)
	go runSink(ln, received)

	// 2. Build a zap logger whose core ships NDJSON over TCP to the sink. Sync
	//    mode (the default) writes each log inline with a bounded deadline.
	core, writer, err := ndjson.NewCore(
		zapwire.TCP(ln.Addr().String()),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
	)
	if err != nil {
		log.Fatalf("new core: %v", err)
	}
	defer writer.Close()

	logger := zap.New(core)

	// 3. Emit a few logs.
	logger.Info("service started", zap.String("version", "1.2.3"))
	logger.Warn("cache miss", zap.String("key", "user:42"))
	logger.Info("request handled", zap.Int("status", 200), zap.Duration("took", 12*time.Millisecond))

	// 4. Wait until the sink has received every line, then report. Waiting on a
	//    signal from the sink (not time.Sleep) keeps the example deterministic.
	for i := 0; i < wantLines; i++ {
		select {
		case line := <-received:
			fmt.Printf("sink received: %s\n", line)
		case <-time.After(2 * time.Second):
			log.Fatal("timed out waiting for logs to arrive")
		}
	}

	fmt.Printf("\nDelivered %d logs over TCP as NDJSON.\n", wantLines)
}

// runSink accepts one connection and forwards each received line to out.
func runSink(ln net.Listener, out chan<- string) {
	conn, err := ln.Accept()
	if err != nil {
		return // listener closed during shutdown
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		out <- scanner.Text()
	}
}
