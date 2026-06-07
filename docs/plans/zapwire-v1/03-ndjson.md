# Phase 03 — NDJSON subpackage (`ndjson/`)

Implements newline-delimited JSON: an `Encoder` that passes zap's JSON line through (trimming
any trailing newline) and a `Framer` that terminates each payload with `\n`. Dependency-free
beyond `zapcore`. Reaches Vector (`socket`), Logstash (`tcp`+`json_lines`), the OTel
Collector (`tcp_log`), and generic collectors.

---

### Task 3.1: Encoder, Framer and presets

**Files:**
- Create: `ndjson/ndjson.go`
- Test: `ndjson/ndjson_test.go`

- [ ] **Step 1: Write the failing tests**

`ndjson/ndjson_test.go`:
```go
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
	zapwire "github.com/arloliu/zapwire"
	"go.uber.org/zap"
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ndjson/... -run TestEncoder -run TestFramer -run TestNewWriter`
Expected: FAIL — `undefined: NewEncoder`.

- [ ] **Step 3: Write `ndjson/ndjson.go`**

```go
package ndjson

import (
	"bytes"

	"github.com/arloliu/zapwire"
	"go.uber.org/zap/zapcore"
)

// Encoder produces the per-line payload from zap's JSON output: the JSON object with any
// trailing newline trimmed (the Framer adds exactly one). It implements zapwire.Encoder.
type Encoder struct{}

// NewEncoder returns an NDJSON Encoder.
func NewEncoder() Encoder { return Encoder{} }

// Encode appends record (trailing newline trimmed) to dst.
func (Encoder) Encode(dst, record []byte) ([]byte, error) {
	return append(dst, bytes.TrimRight(record, "\n")...), nil
}

// Framer terminates each payload with a single newline. It implements zapwire.Framer.
type Framer struct{}

// NewFramer returns an NDJSON Framer.
func NewFramer() Framer { return Framer{} }

// Frame appends each payload followed by '\n' to dst.
func (Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		dst = append(dst, p...)
		dst = append(dst, '\n')
	}

	return dst, nil
}

// NewWriter builds a zapwire.Writer that ships newline-delimited JSON over t.
func NewWriter(t zapwire.Transport, opts ...zapwire.Option) (*zapwire.Writer, error) {
	return zapwire.New(t, NewEncoder(), NewFramer(), opts...)
}

// NewCore builds a zapcore.Core (JSON-encoding into the NDJSON writer) plus the underlying
// writer, which the caller must Close.
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...zapwire.Option,
) (zapcore.Core, *zapwire.Writer, error) {
	w, err := NewWriter(t, opts...)
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(zapcore.NewJSONEncoder(encCfg), w, level), w, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ndjson/... -race`
Expected: PASS.

- [ ] **Step 5: Assert dependency isolation (no msgp leak)**

Run: `go list -deps ./ndjson | grep -c tinylib/msgp`
Expected: `0` (NDJSON must not pull in the msgpack codec).

- [ ] **Step 6: Commit**

```bash
make lint && go test ./... -race
git add ndjson/ndjson.go ndjson/ndjson_test.go
git commit -m "feat(ndjson): add newline-delimited JSON encoder, framer and presets"
```

---

**Phase 03 done when:** `go test ./... -race` and `make lint` pass; a `zap.Logger` emits one
valid JSON object per `\n`-terminated frame; and `go list -deps ./ndjson` contains no
`tinylib/msgp`. Proceed to `04-integration.md`.
