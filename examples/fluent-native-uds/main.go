// Command fluent-native-uds is a self-contained zapwire example for the Fluent
// Forward native path: it ships logs as msgpack PackedForward frames over a Unix
// domain socket and decodes each frame on the other end to prove end-to-end
// delivery.
//
// The native path (fluent.NewNativeCore) encodes straight from zap's fields to
// msgpack with no JSON round-trip: faster, fewer allocations, exact numeric
// types, and a structural (exact) event timestamp. A real deployment points the
// socket at Fluentd or Fluent-bit, which decodes the [time, record] entries.
//
// Run it from the examples directory with:
//
//	go run ./fluent-native-uds
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
	"github.com/arloliu/zapwire/fluent"
)

const wantFrames = 3

func main() {
	// 1. A throwaway Unix socket in a temp dir.
	dir, err := os.MkdirTemp("", "zapwire-example")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "fluent.sock")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go runSink(ln, done)

	// 2. Build the native Fluent core over the socket.
	core, writer, err := fluent.NewNativeCore(
		zapwire.UDS(sock),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		fluent.WithTag("app.logs"),
	)
	if err != nil {
		log.Fatalf("new core: %v", err)
	}
	defer writer.Close()

	logger := zap.New(core)

	// 3. Emit logs. On the native path a whole-number float stays a float and
	//    ints stay ints — no JSON coercion.
	logger.Info("service started", zap.String("version", "1.2.3"))
	logger.Info("metrics", zap.Float64("cpu", 3.0), zap.Int64("rss", 1<<31))
	logger.Warn("slow query", zap.Duration("took", 250*time.Millisecond))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		log.Fatal("timed out waiting for frames to arrive")
	}

	fmt.Printf("\nDelivered %d Fluent Forward frames over UDS (native msgpack).\n", wantFrames)
}

// runSink accepts one connection and decodes Fluent Forward PackedForward
// frames. The decoding stops at the outer frame — enough to prove delivery
// without re-implementing a Fluentd record decoder.
func runSink(ln net.Listener, done chan<- struct{}) {
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	r := msgp.NewReader(conn)
	for i := 0; i < wantFrames; i++ {
		tag, count, nbytes, err := readFrame(r)
		if err != nil {
			log.Printf("sink: decode frame: %v", err)
			return
		}
		fmt.Printf("sink received frame: tag=%s entries=%d (%d msgpack bytes)\n", tag, count, nbytes)
	}
	close(done)
}

// readFrame decodes one PackedForward message: [tag, <entries bin>, {"size": N}].
// It returns the tag, the entry count, and the byte length of the packed entries.
func readFrame(r *msgp.Reader) (tag string, count, nbytes int, err error) {
	if _, err = r.ReadArrayHeader(); err != nil {
		return "", 0, 0, err
	}
	if tag, err = r.ReadString(); err != nil {
		return "", 0, 0, err
	}
	entries, err := r.ReadBytes(nil) // the packed [time, record] entries
	if err != nil {
		return "", 0, 0, err
	}
	if count, err = readSize(r); err != nil {
		return "", 0, 0, err
	}

	return tag, count, len(entries), nil
}

// readSize reads the trailing options map and returns its "size" field (the
// number of entries the frame carries).
func readSize(r *msgp.Reader) (int, error) {
	fields, err := r.ReadMapHeader()
	if err != nil {
		return 0, err
	}
	size := 0
	for i := uint32(0); i < fields; i++ {
		key, err := r.ReadString()
		if err != nil {
			return 0, err
		}
		if key == "size" {
			if size, err = r.ReadInt(); err != nil {
				return 0, err
			}

			continue
		}
		if err := r.Skip(); err != nil {
			return 0, err
		}
	}

	return size, nil
}
