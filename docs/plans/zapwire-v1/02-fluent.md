# Phase 02 — Fluent subpackage (`fluent/`)

Implements the Fluent Forward (msgpack) wire format: the `Entry` proto + `EventTime`
extension (msgp codegen), the v1 **transcode** `Encoder` (JSON → msgpack `[time, record]`),
the **PackedForward** `Framer`, and presets. This is the only package that imports
`github.com/tinylib/msgp`. Uses stdlib `encoding/json` (not goccy) to keep deps minimal —
the zero-round-trip native encoder is v2.

> **Porting note:** `EventTime`'s `MarshalBinaryTo`/`UnmarshalBinary` and the timestamp
> extraction logic come from `tmp/fluent/proto.go` and `tmp/fluent/writer.go`
> (`encodeMessage`). Copy them in.

---

### Task 2.1: msgpack proto (`Entry` + `EventTime`) and codegen

**Files:**
- Create: `fluent/proto.go`
- Generated: `fluent/proto_gen.go` (by `msgp` — do not hand-edit)
- Test: `fluent/proto_test.go`

- [ ] **Step 1: Write `fluent/proto.go`**

```go
//go:generate go tool msgp

package fluent

import (
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

// Entry is one Fluent Forward event encoded as a 2-element msgpack array [time, record].
// Entries are concatenated into the PackedForward entries stream by the Framer.
//
//msgp:tuple Entry
type Entry struct {
	Time   EventTime `msg:"time,extension"`
	Record any       `msg:"record"`
}

// EventTime is the Forward protocol's EventTime extension, carrying second + nanosecond
// components for sub-second precision.
//
// Spec: https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1
type EventTime time.Time

const (
	extensionType = 0
	length        = 8
)

func init() {
	msgp.RegisterExtension(extensionType, func() msgp.Extension { return new(EventTime) })
}

func (t *EventTime) ExtensionType() int8 { return extensionType }
func (t *EventTime) Len() int            { return length }

// MarshalBinaryTo writes 4 bytes of seconds + 4 bytes of nanoseconds (big-endian, UTC).
func (t *EventTime) MarshalBinaryTo(b []byte) error {
	utc := time.Time(*t).UTC()
	sec := uint32(utc.Unix()) //nolint:gosec // Forward EventTime is 32-bit seconds (Y2106)
	nsec := utc.Nanosecond()
	b[0], b[1], b[2], b[3] = byte(sec>>24), byte(sec>>16), byte(sec>>8), byte(sec)
	b[4], b[5], b[6], b[7] = byte(nsec>>24), byte(nsec>>16), byte(nsec>>8), byte(nsec)

	return nil
}

// UnmarshalBinary decodes the 8-byte EventTime payload (used by tests).
func (t *EventTime) UnmarshalBinary(b []byte) error {
	if len(b) != length {
		return fmt.Errorf("fluent: invalid EventTime length: %d", len(b))
	}
	sec := int64(b[0])<<24 | int64(b[1])<<16 | int64(b[2])<<8 | int64(b[3])
	nsec := int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
	*t = EventTime(time.Unix(sec, nsec))

	return nil
}
```

- [ ] **Step 2: Generate the msgpack methods**

Run: `go generate ./fluent/...`
Expected: creates `fluent/proto_gen.go` with `Entry.MarshalMsg`/`UnmarshalMsg`/etc. If
`msgp` is not found, confirm Task 0.3 added it (`go get -tool github.com/tinylib/msgp`).

- [ ] **Step 3: Write the failing round-trip test**

`fluent/proto_test.go`:
```go
package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEntry_RoundTrip(t *testing.T) {
	want := time.Unix(1692959400, 123456789).UTC()
	e := Entry{Time: EventTime(want), Record: map[string]any{"level": "info", "msg": "hi"}}

	b, err := e.MarshalMsg(nil)
	require.NoError(t, err)

	var got Entry
	_, err = got.UnmarshalMsg(b)
	require.NoError(t, err)

	gotTime := time.Time(got.Time)
	require.True(t, gotTime.Equal(want), "time round-trips: got %v want %v", gotTime, want)
	rec, ok := got.Record.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "info", rec["level"])
	require.Equal(t, "hi", rec["msg"])
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fluent/... -run TestEntry_RoundTrip -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
go mod tidy
git add fluent/proto.go fluent/proto_gen.go fluent/proto_test.go go.mod go.sum
git commit -m "feat(fluent): add Forward Entry proto and EventTime extension"
```

---

### Task 2.2: Transcode Encoder

**Files:**
- Create: `fluent/encoder.go`
- Test: `fluent/encoder_test.go`

- [ ] **Step 1: Write the failing tests**

`fluent/encoder_test.go`:
```go
package fluent

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeEntry(t *testing.T, b []byte) Entry {
	t.Helper()
	var e Entry
	_, err := e.UnmarshalMsg(b)
	require.NoError(t, err)

	return e
}

func TestEncoder_StripsTimestampAndKeepsFields(t *testing.T) {
	enc := NewEncoder()
	out, err := enc.Encode(nil, []byte(`{"level":"info","msg":"test","ts":"2023-08-26T10:30:00.000Z"}`))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	rec := e.Record.(map[string]any)
	assert.Equal(t, "info", rec["level"])
	assert.Equal(t, "test", rec["msg"])
	_, hasTS := rec["ts"]
	assert.False(t, hasTS, "ts must be lifted out of the record")

	want, _ := time.Parse(time.RFC3339Nano, "2023-08-26T10:30:00.000Z")
	assert.True(t, time.Time(e.Time).Equal(want))
}

func TestEncoder_EpochNanosTimestamp(t *testing.T) {
	enc := NewEncoder()
	want := time.Unix(1692959400, 123456789).UTC()
	out, err := enc.Encode(nil, []byte(fmt.Sprintf(`{"msg":"x","ts":%d}`, want.UnixNano())))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	diff := time.Time(e.Time).UnixNano() - want.UnixNano()
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, int64(1000), "epoch nanos preserved to sub-microsecond")
}

func TestEncoder_InvalidJSON(t *testing.T) {
	_, err := NewEncoder().Encode(nil, []byte(`{"bad":`))
	require.Error(t, err)
}

func TestEncoder_NoTimestampFallsBackToNow(t *testing.T) {
	before := time.Now().Add(-time.Second)
	out, err := NewEncoder().Encode(nil, []byte(`{"msg":"no ts"}`))
	require.NoError(t, err)
	e := decodeEntry(t, out)
	assert.False(t, time.Time(e.Time).Before(before), "zero ts falls back to ~now")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run TestEncoder`
Expected: FAIL — `undefined: NewEncoder`.

- [ ] **Step 3: Write `fluent/encoder.go`**

```go
package fluent

import (
	"encoding/json"
	"fmt"
	"time"
)

// Encoder is the v1 transcode encoder: it parses zap's JSON log line and emits a msgpack
// [time, record] Entry payload. It implements zapwire.Encoder.
type Encoder struct{}

// NewEncoder returns a transcode Encoder.
func NewEncoder() Encoder { return Encoder{} }

// Encode parses record (a JSON object) and appends its msgpack Entry payload to dst.
func (Encoder) Encode(dst, record []byte) ([]byte, error) {
	var rec map[string]any
	if err := json.Unmarshal(record, &rec); err != nil {
		return nil, fmt.Errorf("fluent: unmarshal log record: %w", err)
	}

	entry := Entry{Time: EventTime(extractTime(rec)), Record: rec}
	out, err := entry.MarshalMsg(dst)
	if err != nil {
		return nil, fmt.Errorf("fluent: marshal entry: %w", err)
	}

	return out, nil
}

// extractTime lifts a "ts" field out of rec and converts it to a time. It understands
// epoch-nanosecond numbers (zap's EpochNanosTimeEncoder, surfaced as float64 by JSON) and
// RFC3339Nano strings (zap's default). Unrecognized/zero timestamps fall back to now.
func extractTime(rec map[string]any) time.Time {
	var ts time.Time
	switch v := rec["ts"].(type) {
	case float64:
		ts = time.Unix(0, int64(v))
		delete(rec, "ts")
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			ts = parsed
		}
		delete(rec, "ts")
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	return ts
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fluent/... -run TestEncoder -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fluent/encoder.go fluent/encoder_test.go
git commit -m "feat(fluent): add transcode encoder (JSON to msgpack Entry)"
```

---

### Task 2.3: PackedForward Framer

**Files:**
- Create: `fluent/framer.go`
- Test: `fluent/framer_test.go`

The PackedForward message is `[tag, entries, option]` where `entries` is a msgpack **bin**
carrying the concatenation of per-entry `[time, record]` payloads, and `option` is
`{"size": N}`.

- [ ] **Step 1: Write the failing test**

`fluent/framer_test.go`:
```go
package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

// decodePackedForward parses a PackedForward frame into its tag, entries, and size option.
func decodePackedForward(t *testing.T, b []byte) (string, []Entry, int) {
	t.Helper()
	n, o, err := msgp.ReadArrayHeaderBytes(b)
	require.NoError(t, err)
	require.Equal(t, uint32(3), n)

	tag, o, err := msgp.ReadStringBytes(o)
	require.NoError(t, err)

	entriesBin, o, err := msgp.ReadBytesBytes(o, nil)
	require.NoError(t, err)

	var entries []Entry
	rest := entriesBin
	for len(rest) > 0 {
		var e Entry
		rest, err = e.UnmarshalMsg(rest)
		require.NoError(t, err)
		entries = append(entries, e)
	}

	mh, o, err := msgp.ReadMapHeaderBytes(o)
	require.NoError(t, err)
	require.Equal(t, uint32(1), mh)
	key, o, err := msgp.ReadStringBytes(o)
	require.NoError(t, err)
	require.Equal(t, "size", key)
	size, _, err := msgp.ReadIntBytes(o)
	require.NoError(t, err)

	return tag, entries, size
}

func encodeEntries(t *testing.T, recs ...map[string]any) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(recs))
	for _, r := range recs {
		e := Entry{Time: EventTime(time.Unix(1, 0)), Record: r}
		b, err := e.MarshalMsg(nil)
		require.NoError(t, err)
		out = append(out, b)
	}

	return out
}

func TestFramer_SingleEntry(t *testing.T) {
	f := NewFramer("app.logs")
	payloads := encodeEntries(t, map[string]any{"msg": "one"})

	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	require.Equal(t, "one", entries[0].Record.(map[string]any)["msg"])
}

func TestFramer_BatchPreservesOrder(t *testing.T) {
	f := NewFramer("t")
	payloads := encodeEntries(t,
		map[string]any{"i": int64(0)},
		map[string]any{"i": int64(1)},
		map[string]any{"i": int64(2)},
	)
	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "t", tag)
	require.Equal(t, 3, size)
	require.Len(t, entries, 3)
	for i, e := range entries {
		require.EqualValues(t, i, e.Record.(map[string]any)["i"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run TestFramer`
Expected: FAIL — `undefined: NewFramer`.

- [ ] **Step 3: Write `fluent/framer.go`**

```go
package fluent

import "github.com/tinylib/msgp/msgp"

// Framer wraps per-entry msgpack [time, record] payloads into a Fluent Forward
// PackedForward message: [tag, <entries bin>, {"size": N}]. It implements zapwire.Framer.
type Framer struct {
	tag string
}

// NewFramer returns a PackedForward Framer that stamps every frame with tag.
func NewFramer(tag string) Framer { return Framer{tag: tag} }

// Frame appends the PackedForward message for payloads to dst.
func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	total := 0
	for _, p := range payloads {
		total += len(p)
	}

	dst = msgp.AppendArrayHeader(dst, 3)
	dst = msgp.AppendString(dst, f.tag)
	dst = appendBinHeader(dst, total) // entries carried as a single msgpack bin
	for _, p := range payloads {
		dst = append(dst, p...)
	}
	dst = msgp.AppendMapHeader(dst, 1)
	dst = msgp.AppendString(dst, "size")
	dst = msgp.AppendInt(dst, len(payloads))

	return dst, nil
}

// appendBinHeader writes a msgpack bin8/bin16/bin32 header for a payload of n bytes.
func appendBinHeader(b []byte, n int) []byte {
	switch {
	case n < 1<<8:
		return append(b, 0xc4, byte(n))
	case n < 1<<16:
		return append(b, 0xc5, byte(n>>8), byte(n))
	default:
		return append(b, 0xc6, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fluent/... -run TestFramer -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fluent/framer.go fluent/framer_test.go
git commit -m "feat(fluent): add PackedForward framer"
```

---

### Task 2.4: Presets and end-to-end integration

**Files:**
- Create: `fluent/fluent.go`
- Test: `fluent/fluent_test.go`

- [ ] **Step 1: Write the failing test (full zap → PackedForward → mock server)**

`fluent/fluent_test.go`:
```go
package fluent

import (
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

func socketPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_fluent_%d.sock", time.Now().UnixNano()))
}

// mockFluentd accepts one UDS connection and surfaces decoded PackedForward frames.
type mockFluentd struct {
	ln   net.Listener
	recv chan []byte
	path string
}

func startMock(t *testing.T, path string) *mockFluentd {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	m := &mockFluentd{ln: ln, recv: make(chan []byte, 16), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				m.recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()

	return m
}

func (m *mockFluentd) stop() { _ = m.ln.Close(); _ = os.Remove(m.path) }

func TestNewWriter_EndToEnd(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	core, writer, err := NewCore(zapwire.UDS(path), zap.InfoLevel,
		zap.NewProductionEncoderConfig(), WithTag("svc.logs"))
	require.NoError(t, err)
	defer writer.Close()

	logger := zap.New(core)
	logger.Info("hello", zap.String("k", "v"))

	select {
	case frame := <-m.recv:
		tag, entries, size := decodePackedForward(t, frame)
		require.Equal(t, "svc.logs", tag)
		require.Equal(t, 1, size)
		require.Len(t, entries, 1)
		rec := entries[0].Record.(map[string]any)
		require.Equal(t, "hello", rec["msg"])
		require.Equal(t, "v", rec["k"])
		_, hasTS := rec["ts"]
		require.False(t, hasTS, "ts must be lifted into EventTime, not left in the record")
		// Regression guard for the ts encoder/decoder contract: a real zap log's decoded
		// EventTime must be ~now, not ~1970 (which is what a seconds/nanos mismatch yields).
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}
```
> Note: `zapwireUDS` is a tiny local alias in the test to avoid repeating the import path.
> Add at the top of the test file:
> ```go
> import zapwire "github.com/arloliu/zapwire"
> func zapwireUDS(p string) zapwire.Transport { return zapwire.UDS(p) }
> ```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run TestNewWriter_EndToEnd`
Expected: FAIL — `undefined: NewWriter` / `NewCore`.

> The test name covers both presets even though it calls `NewCore` (which calls `NewWriter`
> internally); add a focused `NewWriter`-only test if you want it isolated.

- [ ] **Step 3: Write `fluent/fluent.go`**

```go
package fluent

import (
	"github.com/arloliu/zapwire"
	"go.uber.org/zap/zapcore"
)

const defaultTag = "app.logs"

type options struct {
	tag      string
	wireOpts []zapwire.Option
}

// Option configures a fluent writer preset.
type Option func(*options)

// WithTag sets the Fluent Forward tag stamped on every frame (default "app.logs").
func WithTag(tag string) Option { return func(o *options) { o.tag = tag } }

// WithWriterOptions forwards core zapwire options (mode, buffer, timeouts, …).
func WithWriterOptions(opts ...zapwire.Option) Option {
	return func(o *options) { o.wireOpts = append(o.wireOpts, opts...) }
}

// NewWriter builds a zapwire.Writer that ships Fluent Forward PackedForward frames over t.
//
// Timestamp contract: the transcode Encoder reads the "ts" field and expects either an
// epoch-nanosecond NUMBER or an RFC3339Nano STRING. Callers wiring their own zapcore.Core
// (rather than using NewCore) MUST set encoder TimeKey="ts" and EncodeTime to
// zapcore.EpochNanosTimeEncoder or zapcore.RFC3339NanoTimeEncoder. zap's default
// EpochTimeEncoder emits float SECONDS, which the Encoder would misread as nanoseconds
// (decoding to ~1970). NewCore pins this for you.
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}
	if o.tag == "" {
		o.tag = defaultTag
	}

	return zapwire.New(t, NewEncoder(), NewFramer(o.tag), o.wireOpts...)
}

// NewCore builds a zapcore.Core (JSON-encoding into the fluent writer) plus the underlying
// writer, which the caller must Close.
//
// It pins the time wire-contract so the JSON encoder and the transcode decoder agree:
// TimeKey defaults to "ts" and EncodeTime is forced to EpochNanosTimeEncoder (an epoch-
// nanosecond number). Leaving zap's default EpochTimeEncoder (float seconds) would be
// misread as nanoseconds and decode to ~1970. (float64's mantissa loses sub-~100ns
// precision at current epoch-nanos magnitudes; that is acceptable for logs. Use
// RFC3339NanoTimeEncoder instead if exact nanoseconds matter.)
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	if encCfg.TimeKey == "" {
		encCfg.TimeKey = "ts"
	}
	encCfg.EncodeTime = zapcore.EpochNanosTimeEncoder

	w, err := NewWriter(t, opts...)
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(zapcore.NewJSONEncoder(encCfg), w, level), w, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fluent/... -run TestNewWriter_EndToEnd -race`
Expected: PASS.

- [ ] **Step 5: Full fluent pass, lint, commit**

```bash
go mod tidy
make lint && go test ./... -race
git add fluent/fluent.go fluent/fluent_test.go go.mod go.sum
git commit -m "feat(fluent): add NewWriter/NewCore presets"
```

---

**Phase 02 done when:** `go test ./... -race` and `make lint` pass; a real `zap.Logger`
produces decodable PackedForward frames over UDS with a correct (~now) timestamp, and
`tinylib/msgp` appears only in `fluent/`'s imports. Proceed to `03-ndjson.md`.

> **Caveat:** these unit tests are self-consistent (our Framer writes `bin`; our decoder
> reads `bin`) — they do not prove a real Fluentd/Fluent-bit/Vector accepts the exact
> framing. That confidence comes from the optional Fluent-bit integration test (design §8,
> deferred) — do not read green unit tests as "wire format confirmed against a real
> consumer."
