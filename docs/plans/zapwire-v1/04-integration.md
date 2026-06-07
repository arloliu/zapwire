# Phase 04 — Integration, benchmarks & polish

Locks down cross-cutting guarantees (no goroutine leaks, both modes end-to-end), measures
the hot path, and ships docs. After this phase the v1 library is complete.

---

### Task 4.1: Goroutine-leak guard on Close

**Files:**
- Create: `writer_lifecycle_test.go`

- [ ] **Step 1: Write the failing test**

`writer_lifecycle_test.go`:
```go
package zapwire

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriter_Close_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for _, async := range []bool{false, true} {
		opts := []Option{WithMaxRetries(1)}
		if async {
			opts = append(opts, WithAsyncMode())
		}
		w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, opts...)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}

	// Poll from THIS goroutine (require.Eventually's spawned goroutine would inflate the count).
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before,
		"reconnect and flush goroutines must exit after Close")
}
```

- [ ] **Step 2: Run test to verify it passes (or surfaces a real leak)**

Run: `go test ./... -run TestWriter_Close_NoGoroutineLeak -race`
Expected: PASS. If it fails, the async flush loop or reconnect loop is not honoring `done` —
fix the loop, do not weaken the test.

- [ ] **Step 3: Commit**

```bash
git add writer_lifecycle_test.go
git commit -m "test: guard against goroutine leaks on Close"
```

---

### Task 4.2: Hot-path benchmarks

**Files:**
- Create: `fluent/bench_test.go`

- [ ] **Step 1: Write the benchmarks**

`fluent/bench_test.go`:
```go
package fluent

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	zapwire "github.com/arloliu/zapwire"
)

const benchJSON = `{"level":"info","ts":1692959400123456789,"msg":"request completed","caller":"server/handler.go:142","service":"zapwire","status":200}`

// drainingServer accepts and discards everything (a fast, never-stalling consumer).
func drainingServer(b *testing.B, path string) net.Listener {
	b.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(b, err)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 65536)
				for {
					if _, rerr := c.Read(buf); rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return ln
}

func benchWriter(b *testing.B, opts ...zapwire.Option) (*zapwire.Writer, func()) {
	b.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_bench_%d.sock", time.Now().UnixNano()))
	ln := drainingServer(b, path)
	w, err := NewWriter(zapwire.UDS(path), WithWriterOptions(opts...))
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return w, func() { _ = w.Close(); _ = ln.Close(); _ = os.Remove(path) }
}

func BenchmarkFluentWriter_Sync(b *testing.B) {
	w, cleanup := benchWriter(b)
	defer cleanup()
	msg := []byte(benchJSON)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = w.Write(msg)
	}
}

func BenchmarkFluentWriter_Async(b *testing.B) {
	w, cleanup := benchWriter(b, zapwire.WithAsyncMode())
	defer cleanup()
	msg := []byte(benchJSON)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = w.Write(msg)
	}
}
```
> Add the missing imports `"fmt"` and `"path/filepath"` to the file's import block.

- [ ] **Step 2: Run the benchmarks**

Run: `make bench`
Expected: both benchmarks report `ns/op` and `allocs/op`. Record the numbers in the commit
body as the v1 baseline (the native encoder in v2 will be compared against them).

- [ ] **Step 3: Commit**

```bash
git add fluent/bench_test.go
git commit -m "test(fluent): add sync/async hot-path benchmarks"
```

---

### Task 4.3: Package docs and README

**Files:**
- Create: `doc.go`, `README.md`

- [ ] **Step 1: Write `doc.go` (package overview)**

```go
// Package zapwire provides a high-performance zap WriteSyncer that ships structured logs to
// log processors (Fluentd, Fluent-bit, Vector, Logstash, the OpenTelemetry Collector, …)
// over Unix domain sockets or TCP.
//
// The core is processor-agnostic: a connection manager with background reconnect and
// bounded, never-blocking writes (drop-on-stall), driven by two small interfaces — Encoder
// (zap bytes to a per-entry wire payload) and Framer (payloads to one wire frame). Per-
// processor wire formats live in subpackages: fluent (Fluent Forward, msgpack) and ndjson
// (newline-delimited JSON).
//
// Delivery is configurable: synchronous (write-per-log) or asynchronous (buffered, batched).
// Neither mode blocks the application; on a stalled or absent consumer, logs are dropped and
// counted (see Writer.DroppedLogs). Buffered logs are lost on a hard crash — zapwire is an
// at-most-once shipper, not a write-ahead log.
package zapwire
```

- [ ] **Step 2: Write `README.md`**

````markdown
# zapwire

High-performance [zap](https://github.com/uber-go/zap) `WriteSyncer` that ships structured
logs to log processors over UDS or TCP, with never-block drop-on-stall semantics.

## Install

```bash
go get github.com/arloliu/zapwire
```

## Processors & formats

| Format | Reaches | Subpackage |
|---|---|---|
| Fluent Forward (msgpack) | Fluentd, Fluent-bit, Vector | `fluent` |
| NDJSON | Vector, Logstash, OTel Collector, generic | `ndjson` |

Transports: `zapwire.UDS(path)` and `zapwire.TCP(addr)`.

## Quick start (Fluent Forward over UDS)

```go
core, writer, err := fluent.NewCore(
    zapwire.UDS("/var/run/fluent.sock"),
    zap.InfoLevel,
    zap.NewProductionEncoderConfig(),
    fluent.WithTag("app.logs"),
)
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

logger := zap.New(core)
logger.Info("started", zap.String("version", "1.0.0"))
```

## NDJSON over TCP

```go
core, writer, _ := ndjson.NewCore(zapwire.TCP("collector:9000"), zap.InfoLevel,
    zap.NewProductionEncoderConfig(), zapwire.WithAsyncMode())
defer writer.Close()
logger := zap.New(core)
```

## Delivery modes

- **Sync (default):** each log is written inline with a bounded deadline.
- **Async (`zapwire.WithAsyncMode()`):** logs are buffered and flushed in batches; call
  `logger.Sync()` to flush.

Both drop-on-stall and never block the app. Tune with `WithBufferSize`, `WithBatchSize`,
`WithFlushInterval`, `WithDropPolicy`, `WithWriteTimeout`, `WithReconnect`, `WithMaxRetries`,
`WithErrorHandler`. Introspect with `Writer.DroppedLogs()`, `ReconnectCount()`,
`IsConnected()`.

## Semantics

zapwire is **at-most-once**: buffered logs are lost on a hard crash, and a stalled or absent
consumer causes counted drops — never a blocked goroutine. See
[`docs/design/2026-06-07-zapwire-design.md`](docs/design/2026-06-07-zapwire-design.md).
````

- [ ] **Step 3: Commit**

```bash
git add doc.go README.md
git commit -m "docs: add package doc and README"
```

---

### Task 4.4: Final gate

- [ ] **Step 1: Tidy and run the full CI gate**

Run:
```bash
go mod tidy
make ci
```
Expected: `lint`, `vet`, `test` (`-race`), and `coverage` all pass. Confirm coverage on the
core and subpackages is reasonable (aim ≥ 80% on `zapwire`, `fluent`, `ndjson`).

- [ ] **Step 2: Verify dependency isolation end-to-end**

Run:
```bash
go list -deps . | grep -c tinylib/msgp        # expect 0 (root is msgp-free)
go list -deps ./ndjson | grep -c tinylib/msgp # expect 0
go list -deps ./fluent | grep -c tinylib/msgp # expect >=1 (fluent owns it)
```
Expected: `0`, `0`, and a non-zero count respectively.

- [ ] **Step 3: Commit any tidy changes**

```bash
git add -A
git commit -m "chore: tidy modules and finalize v1" || echo "nothing to commit"
```

---

**Phase 04 done when:** `make ci` is green, dependency isolation holds, and the README +
package docs describe the public API. **v1 is complete.**

## v2 follow-ups (out of scope here — see design §10/§11)

- Native msgpack `zapcore.Encoder` in `fluent/` (zero JSON round-trip); benchmark against
  the Task 4.2 baseline.
- `syslog/` (RFC5424) subpackage in the same module.
- `otlp/` subpackage with its own `go.mod` (grpc + protobuf).
- Async queue-slot pooling to cut the per-log allocation in async mode.
