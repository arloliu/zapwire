package ndjson

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	zapwire "github.com/arloliu/zapwire"
)

func TestEncoder_TrimsTrailingNewline(t *testing.T) {
	out, err := NewEncoder().Encode(nil, []byte(`{"msg":"x"}`+"\n"))
	require.NoError(t, err)
	require.Equal(t, `{"msg":"x"}`, string(out))
}

func TestFramer_OneNewlinePerPayload(t *testing.T) {
	out, err := NewFramer().Frame(nil, [][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1}\n{\"b\":2}\n", string(out))
}

func TestNewWriter_EndToEnd(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_ndjson_%d.sock", time.Now().UnixNano()))
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer func() { _ = ln.Close(); _ = os.Remove(path) }()

	recv := make(chan []byte, 4)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig())
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("hello", zap.Int("n", 7))

	select {
	case line := <-recv:
		require.Equal(t, byte('\n'), line[len(line)-1], "must be newline-terminated")
		var m map[string]any
		require.NoError(t, json.Unmarshal(line[:len(line)-1], &m), "frame is one valid JSON line")
		require.Equal(t, "hello", m["msg"])
		require.EqualValues(t, 7, m["n"])
	case <-time.After(2 * time.Second):
		t.Fatal("no NDJSON line received")
	}
}
