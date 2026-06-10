// Command async-observability shows zapwire's asynchronous delivery mode and the
// runtime counters you use to monitor log-shipping health.
//
// Async mode never blocks the caller: Write enqueues into a bounded buffer and a
// background goroutine batches and flushes frames. This example ships to a live
// local sink, forces a flush with logger.Sync(), and prints the three health
// counters: DroppedLogs, ReconnectCount, and IsConnected.
//
// Run it from the examples directory with:
//
//	go run ./async-observability
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
	"github.com/arloliu/zapwire/ndjson"
)

const wantLines = 5

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var got atomic.Int64
	done := make(chan struct{})
	go runSink(ln, &got, done)

	core, writer, err := ndjson.NewCore(
		zapwire.TCP(ln.Addr().String()),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		zapwire.WithAsyncMode(),
		zapwire.WithBufferSize(1024),                   // queue up to 1024 logs (default 4096)
		zapwire.WithBatchSize(64),                      // frame up to 64 logs together (default 128)
		zapwire.WithFlushInterval(50*time.Millisecond), // flush at least this often (default 200ms)
		zapwire.WithDropPolicy(zapwire.DropNewest),     // drop incoming logs when the buffer is full
		zapwire.WithErrorHandler(func(err error) { // transport errors land here, off the caller's path
			log.Printf("zapwire transport error: %v", err)
		}),
	)
	if err != nil {
		log.Fatalf("new core: %v", err)
	}
	defer writer.Close()

	logger := zap.New(core)

	// Async Write never blocks on the socket — it just enqueues and returns.
	for i := 0; i < wantLines; i++ {
		logger.Info("event", zap.Int("seq", i))
	}

	// Force the background flush and wait for delivery (deterministic — no Sleep).
	_ = logger.Sync()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		log.Fatal("timed out waiting for logs to arrive")
	}

	fmt.Printf("\nlogs received by sink : %d\n", got.Load())
	fmt.Printf("dropped logs          : %d\n", writer.DroppedLogs())
	fmt.Printf("successful reconnects : %d\n", writer.ReconnectCount())
	fmt.Printf("connected             : %t\n", writer.IsConnected())
	fmt.Println("\nIf the consumer stalls or disconnects, async Write still returns immediately;")
	fmt.Println("overflowing logs are counted in DroppedLogs() and the writer reconnects in the background.")
}

func runSink(ln net.Listener, got *atomic.Int64, done chan<- struct{}) {
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		if got.Add(1) == wantLines {
			close(done)
		}
	}
}
