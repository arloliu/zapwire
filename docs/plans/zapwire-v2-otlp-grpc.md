# OTLP/gRPC transport — Implementation Plan (zapwire/otlp v0.2)

**Status:** ✅ merge-clean (codex post-impl review v2: merge, zero P0/P1; reports tmp/zapwire-v2-otlp-grpc_post_implementation_review_v{1,2}.md) · **Date:** 2026-06-12 ·
**Branch:** `feature/otlp-grpc` · **Spec:** `docs/design/2026-06-12-otlp-grpc-design.md`

**Review history:** codex pass 1 (`tmp/zapwire-v2-otlp-grpc_pass1_review.md`) raised
1×P0 (gRPC `client.Do`/body-read errors were terminal, contradicting design §6.5's
connection-loss-is-retried promise — fixed: `transportError` maps them to retryable
UNAVAILABLE / DEADLINE_EXCEEDED, with transport-, deadline-, aborted-body-, and
writer-level retry tests; the HTTP path deliberately keeps v0.1.0's terminal transport
errors, now documented in design §6.3) + 1×P1 (`validateGRPCHeaders` checked keys only —
fixed: values must be printable ASCII %x20-%x7E at construction, with tests). Design
doc amended in the same round.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking. Execute tasks **in order** (0 → 12): each
> task leaves every module compiling with all prior tests green.

**Goal:** Add the OTLP/gRPC transport to `zapwire/otlp` as a hand-rolled, stdlib-only gRPC
unary client behind an internal transport seam — zero new dependencies, released API fully
preserved, shipped as `otlp/v0.2.0`.

**Architecture:** The existing `Writer` pipeline (queue, flush goroutine, byte-aware
batching, retry loop, Sync/Close lifecycle) is reused untouched. The HTTP-specific code
collapses behind an internal `transport` interface (`prepare` once per batch /
`attempt` once per try); a new `grpcTransport` speaks gRPC framing over stdlib HTTP/2
(h2c via `http.Protocols.SetUnencryptedHTTP2`, Go ≥1.24). Partial-success accounting is
centralized in a shared `resolveAccept` helper. Spike-proven reference client:
`tmp/spike-handrolled-grpc/client/main.go`.

**Tech stack:** Go stdlib only (`net/http` h2c, `compress/gzip`, `encoding/base64`,
`encoding/binary`). Heavy deps (grpc-go, official OTLP stubs) stay quarantined in
`otlp/internal/conformance` (own go.mod) for interop tests + the benchmark.

**Validation gate (every commit, per repo contract):**
`go fix ./otlp/...` → `make lint` → `make test`.

---

## File structure

| File | Responsibility |
|---|---|
| Create `otlp/transport.go` | `transport` interface, `prepared`, `acceptance`, `resolveAccept` |
| Create `otlp/http_transport.go` | `httpTransport` (relocated `attempt`, gzip block, `retryableStatus`, `parseRetryAfter`, `excerpt`) |
| Create `otlp/grpc_status.go` | gRPC status codes, retryability table, HTTP→gRPC mapping, percent-decode, `-bin` base64, `RetryInfo` decode, `grpc-timeout` formatting |
| Create `otlp/grpc_transport.go` | `grpcTransport`: endpoint resolution, header validation, h2c/TLS client, framing, attempt |
| Modify `otlp/writer.go` | seam rewire: `tr transport` field, `newWriterCore(tr, o)`, `export` uses prepare/attempt |
| Modify `otlp/options.go` | `insecure`/`tlsConfig` fields, `WithInsecure`, `WithTLSConfig`, `Protocol`, `ProtocolFromEnv`, `ExportError.GRPCStatus` |
| Modify `otlp/proto.go` | `forEachLenField` helper (repeated-field walk for `google.rpc.Status.details`) |
| Modify `otlp/core.go` | `NewHTTPCore`, `NewGRPCCore` |
| Modify `otlp/doc.go` | protocol-selection paragraph |
| Create `otlp/grpc_status_test.go`, `otlp/grpc_transport_test.go`, `otlp/transport_test.go` | unit tests (incl. stdlib-only fake gRPC server over h2c) |
| Modify `otlp/writer_test.go` | `newTestWriter` constructs via `newHTTPTransport` |
| Modify `otlp/internal/conformance/{go.mod,grpc_conformance_test.go,grpc_bench_test.go}` | grpc-go interop + benchmark |
| Modify `otlp/collector_integration_test.go` | `TestCollectorEndToEndGRPC` |
| Create `examples/otlp-grpc/main.go`; modify `docs/guide.md`, `README.md` | docs & runnable example |

Naming contract used throughout (type-consistency anchor):

```go
type transport interface {
    prepare(msg []byte) prepared
    attempt(p prepared) (*acceptance, *ExportError)
}
type prepared struct {
    body       []byte
    compressed bool
    warn       *ExportError
}
type acceptance struct {
    rejected int64
    event    *ExportError
}
func resolveAccept(respMsg []byte, base ExportError) *acceptance
func newHTTPTransport(endpoint string, o options) (*httpTransport, error)
func newGRPCTransport(endpoint string, o options) (*grpcTransport, error)
func newWriterCore(tr transport, o options) *Writer   // loses endpoint + error
```

---

### Task 0: Baseline check

No code changes. Confirm clean baseline before refactoring.

- [ ] **Step 0.1:** Run: `cd /home/arlo/projects/zapwire/.claude/worktrees/otel-otlp && git status --short`
  Expected: empty (clean tree on `feature/otlp-grpc`).
- [ ] **Step 0.2:** Run: `make lint && make test`
  Expected: `0 issues.` ×3 and all tests PASS. If not, STOP and report — do not start on a red baseline.

---

### Task 1: Transport seam refactor (behavior-preserving)

**Files:**
- Create: `otlp/transport.go`
- Create: `otlp/http_transport.go`
- Create: `otlp/transport_test.go`
- Modify: `otlp/writer.go`
- Modify: `otlp/writer_test.go:22-31` (`newTestWriter`)

The oracle for this task is the EXISTING test suite: behavior must not change. One new
unit test covers `resolveAccept` directly.

- [ ] **Step 1.1: Write the failing test** — create `otlp/transport_test.go`:

```go
package otlp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// psBody builds an ExportLogsServiceResponse with partial_success{rejected, msg}.
func psBody(rejected int64, msg string) []byte {
	var ps []byte
	if rejected != 0 {
		ps = appendTaggedVarint(ps, 0x08, rejected)
	}
	if msg != "" {
		ps = appendTaggedString(ps, 0x12, msg)
	}

	return appendTaggedBytes(nil, 0x0a, ps)
}

func TestResolveAccept(t *testing.T) {
	t.Run("clean accept", func(t *testing.T) {
		require.Nil(t, resolveAccept(nil, ExportError{StatusCode: 200}))
		require.Nil(t, resolveAccept(appendTaggedBytes(nil, 0x0a, nil), ExportError{}))
	})
	t.Run("rejected", func(t *testing.T) {
		a := resolveAccept(psBody(3, "boom"), ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.EqualValues(t, 3, a.rejected)
		require.NotNil(t, a.event)
		require.Equal(t, 200, a.event.StatusCode)
		require.EqualValues(t, 3, a.event.Rejected)
		require.Equal(t, "boom", a.event.Message)
		require.False(t, a.event.Warning)
	})
	t.Run("warning", func(t *testing.T) {
		a := resolveAccept(psBody(0, "heads-up"), ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.Zero(t, a.rejected)
		require.True(t, a.event.Warning)
		require.Equal(t, "heads-up", a.event.Message)
	})
	t.Run("malformed body is observability-only", func(t *testing.T) {
		a := resolveAccept([]byte{0xff}, ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.Zero(t, a.rejected)
		require.Error(t, a.event.Err)
	})
}
```

- [ ] **Step 1.2:** Run: `cd otlp && go test -run TestResolveAccept ./...`
  Expected: FAIL — `undefined: resolveAccept` (and `acceptance`).

- [ ] **Step 1.3: Create `otlp/transport.go`:**

```go
package otlp

// transport is the per-protocol ship layer (design 2026-06-12 §3). prepare
// runs ONCE per batch (compression + framing — a retrying batch must not
// re-gzip per attempt); attempt runs once per try. Transports never touch
// writer state: outcomes flow back through acceptance / *ExportError and
// the writer applies the shared OTLP semantics.
type transport interface {
	prepare(msg []byte) prepared
	attempt(p prepared) (*acceptance, *ExportError)
}

// prepared is the wire-ready body. warn carries non-fatal prepare
// diagnostics (gzip failure → shipped uncompressed).
type prepared struct {
	body       []byte
	compressed bool
	warn       *ExportError
}

// acceptance is a server-accepted outcome that still needs accounting:
// partial-success rejections (counted drops) or observability-only events.
// nil acceptance == clean accept.
type acceptance struct {
	rejected int64
	event    *ExportError
}

// resolveAccept decodes an ExportLogsServiceResponse and applies the OTLP
// partial-success classification (§5.3 of the v1 design): rejected>0 →
// counted drop; rejected==0 with message → warning; malformed body →
// observability-only (the server accepted the batch). base stamps transport
// identity (StatusCode 200 for HTTP, GRPCStatus 0 for gRPC).
func resolveAccept(respMsg []byte, base ExportError) *acceptance {
	rejected, msg, derr := decodePartialSuccess(respMsg)
	switch {
	case derr != nil:
		e := base
		e.Err = derr

		return &acceptance{event: &e}
	case rejected > 0:
		e := base
		e.Rejected = rejected
		e.Message = msg

		return &acceptance{rejected: rejected, event: &e}
	case msg != "":
		e := base
		e.Warning = true
		e.Message = msg

		return &acceptance{event: &e}
	}

	return nil
}
```

- [ ] **Step 1.4:** Run: `cd otlp && go test -run TestResolveAccept ./...`
  Expected: PASS.

- [ ] **Step 1.5: Create `otlp/http_transport.go`** — relocation of the HTTP code from
  `writer.go` (`attempt` writer.go:155-200, gzip block writer.go:106-115,
  `retryableStatus` writer.go:203-211, `parseRetryAfter` writer.go:214-228, `excerpt`
  writer.go:230-237) reshaped to the seam. The bodies of `retryableStatus`,
  `parseRetryAfter`, `excerpt` move VERBATIM (delete them from writer.go):

```go
package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"
)

// httpTransport is the OTLP/HTTP ship layer — the v0.1.0 behavior, verbatim,
// behind the transport seam.
type httpTransport struct {
	endpoint string
	client   *http.Client
	headers  map[string]string
	gzipOn   bool
	timeout  time.Duration
}

var _ transport = (*httpTransport)(nil)

func newHTTPTransport(endpoint string, o options) (*httpTransport, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	return &httpTransport{
		endpoint: ep,
		client:   o.client,
		headers:  o.headers,
		gzipOn:   o.compression == Gzip,
		timeout:  o.timeout,
	}, nil
}

// prepare applies whole-body gzip (Content-Encoding) once per batch. A gzip
// failure ships uncompressed rather than losing the batch (v0.1.0 behavior).
func (t *httpTransport) prepare(msg []byte) prepared {
	if !t.gzipOn {
		return prepared{body: msg}
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(msg); err == nil && zw.Close() == nil {
		return prepared{body: gz.Bytes(), compressed: true}
	}

	return prepared{body: msg, warn: &ExportError{Message: "gzip failed; sent uncompressed"}}
}

// attempt performs one POST. Its context is Background+timeout — NOT the
// lifecycle ctx — so Close never cancels a request the server may have
// already accepted (§5.4); Close interrupts the backoff sleeps instead.
func (t *httpTransport) attempt(p prepared) (*acceptance, *ExportError) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(p.body))
	if err != nil {
		return nil, &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if p.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, &ExportError{Err: err} // transport errors are non-retryable (§5.3)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		return resolveAccept(respBody, ExportError{StatusCode: http.StatusOK}), nil // never retried (§5.3)
	case retryableStatus(resp.StatusCode):
		return nil, &ExportError{
			StatusCode: resp.StatusCode,
			Retryable:  true,
			Message:    excerpt(respBody),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	default: // 400, 413, anything else: non-retryable
		return nil, &ExportError{StatusCode: resp.StatusCode, Message: excerpt(respBody)}
	}
}

// retryableStatus implements the OTLP/HTTP retryable class (§5.3).
func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}

	return false
}

// parseRetryAfter accepts delta-seconds or an HTTP-date (spec clarification).
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(h); err == nil {
		if d := time.Until(at); d > 0 {
			return d
		}
	}

	return 0
}

func excerpt(b []byte) string {
	const max = 256 //nolint:predeclared // local response-excerpt cap; shadowing the builtin is harmless here
	if len(b) > max {
		b = b[:max]
	}

	return string(b)
}
```

- [ ] **Step 1.6: Rewire `otlp/writer.go`.** Four edits:

(a) Struct — replace the five transport fields with `tr`:

```go
type Writer struct {
	tr         transport
	retry      RetryConfig
	maxBytes   int
	batchSize  int
	flushEvery time.Duration
	dropPolicy DropPolicy
	errFn      func(error)
	env        *envelope
	// ... queue, flushReq, done, ctx, cancel, admit, dropped, closed, closeOnce UNCHANGED
}
```

(b) `newWriterCore` — takes the transport, loses endpoint resolution and the error:

```go
// newWriterCore builds a Writer around a transport WITHOUT starting the
// flush goroutine (the constructors start it; tests drive export directly).
func newWriterCore(tr transport, o options) *Writer {
	w := &Writer{
		tr:         tr,
		retry:      o.retry,
		maxBytes:   o.maxRequestBytes,
		batchSize:  o.batchSize,
		flushEvery: o.flushInterval,
		dropPolicy: o.dropPolicy,
		errFn:      o.errFn,
		env:        newEnvelope(o),
		queue:      make(chan []byte, o.queueSize),
		flushReq:   make(chan chan struct{}),
		done:       make(chan struct{}),
	}
	//nolint:gosec // G118: w.cancel is invoked by Close and by test cleanup.
	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w
}
```

(c) `export` — prepare once, attempt per try, shared acceptance accounting. The retry
loop body is otherwise UNCHANGED (same jitter, deadline, ctx checks, drop calls):

```go
// export ships one batch with OTLP retry semantics (§5.3). allowRetry=false
// is the Close-drain single-attempt mode (§5.4). Counts the whole batch as
// dropped on failure.
func (w *Writer) export(records [][]byte, allowRetry bool) {
	if len(records) == 0 {
		return
	}
	raw := w.env.assemble(getFrameBuf(), records)
	defer putFrameBuf(raw)
	p := w.tr.prepare(raw)
	if p.warn != nil {
		w.errFn(p.warn)
	}

	deadline := time.Now().Add(w.retry.MaxElapsed)
	delay := w.retry.Initial
	for {
		accept, expErr := w.tr.attempt(p)
		if expErr == nil {
			switch {
			case accept == nil: // clean accept
			case accept.rejected > 0:
				w.drop(int(accept.rejected), accept.event)
			case accept.event != nil:
				w.errFn(accept.event)
			}

			return
		}
		if !allowRetry || !expErr.Retryable || w.ctx.Err() != nil {
			w.drop(len(records), expErr)

			return
		}
		d := delay/2 + rand.N(delay/2+1) //nolint:gosec // jitter in [delay/2, delay]; backoff jitter needs no crypto RNG
		if expErr.retryAfter > 0 {
			d = expErr.retryAfter
		}
		if time.Now().Add(d).After(deadline) {
			// Includes Retry-After beyond the remaining budget (§5.3).
			w.drop(len(records), expErr)

			return
		}
		select {
		case <-time.After(d):
		case <-w.ctx.Done():
			w.drop(len(records), expErr) // cancelled mid-retry: counted once (§5.4)

			return
		}
		if delay *= 2; delay > w.retry.MaxInterval {
			delay = w.retry.MaxInterval
		}
	}
}
```

(d) `NewWriter` — builds the HTTP transport; delete `attempt`, `retryableStatus`,
`parseRetryAfter`, `excerpt` from writer.go (now in http_transport.go); prune imports
(writer.go keeps: `context`, `math/rand/v2`, `sync`, `sync/atomic`, `time`, `zapcore`):

```go
// NewWriter builds the OTLP/HTTP exporter and starts its flush goroutine.
// Equivalent to NewHTTPWriter. The caller owns the Writer and must Close it.
// Only writer/envelope-end options take effect (design §6).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewWriter(endpoint string, opts ...Option) (*Writer, error) {
	o := applyOptions(opts)
	tr, err := newHTTPTransport(endpoint, o)
	if err != nil {
		return nil, err
	}
	w := newWriterCore(tr, o)
	go w.run()

	return w, nil
}
```

- [ ] **Step 1.7: Update `newTestWriter`** in `otlp/writer_test.go` (only the
  construction lines change):

```go
	w, err := func() (*Writer, error) {
		o := applyOptions(opts)
		tr, terr := newHTTPTransport(url, o)
		if terr != nil {
			return nil, terr
		}

		return newWriterCore(tr, o), nil
	}()
```

  Keep the helper's existing signature, error-collection wiring, and `t.Cleanup` exactly
  as they are. If other tests call `newWriterCore` directly, apply the same two-line
  substitution (`grep -n "newWriterCore" otlp/*_test.go` and fix each).

- [ ] **Step 1.8:** Run: `cd otlp && go test -race ./... && cd .. && make lint`
  Expected: ALL tests PASS (the v0.1.0 suite is the refactor oracle), `0 issues.`.
  Behavior-change tripwires to watch: partial-success ExportErrors still carry
  `StatusCode: 200`; gzip-failure warning still fires once per batch.

- [ ] **Step 1.9: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/transport.go otlp/http_transport.go otlp/transport_test.go otlp/writer.go otlp/writer_test.go
git commit -m "refactor(otlp): transport seam — prepare/attempt interface, httpTransport extraction"
```

---

### Task 2: `ExportError.GRPCStatus`

**Files:**
- Modify: `otlp/options.go:96-110` (ExportError struct + Error())
- Modify: `otlp/options_test.go` (or wherever `TestExportError` lives — `grep -rn "func (e \*ExportError) Error" otlp/*_test.go` to find the existing Error() test)

- [ ] **Step 2.1: Write the failing test** (append to the file containing the existing
  ExportError test; if none exists, add to `otlp/options_test.go`):

```go
func TestExportErrorGRPCStatus(t *testing.T) {
	e := &ExportError{GRPCStatus: 14, Retryable: true, Message: "unavailable"}
	require.Contains(t, e.Error(), "grpc=14")
	// HTTP-only errors must not mention grpc.
	require.NotContains(t, (&ExportError{StatusCode: 503}).Error(), "grpc=")
}
```

- [ ] **Step 2.2:** Run: `cd otlp && go test -run TestExportErrorGRPCStatus ./...`
  Expected: FAIL — `unknown field GRPCStatus`.

- [ ] **Step 2.3: Implement.** In `otlp/options.go`, add the field after `StatusCode`
  and extend `Error()`:

```go
type ExportError struct {
	StatusCode int   // HTTP status; 0 for transport/encode errors and for gRPC
	                 // responses (HTTP 200 by construction — set only when a
	                 // non-gRPC intermediary answered without a grpc-status)
	GRPCStatus int   // gRPC status code (0 = OK); meaningful only on writers
	                 // built by NewGRPCWriter/NewGRPCCore
	Retryable  bool  // whether the failure was in the retryable class
	Rejected   int64 // partial-success rejected_log_records
	Warning    bool  // partial success with Rejected == 0 and a message
	Message    string // partial-success error_message, grpc-message, or short response excerpt
	Err        error // wrapped underlying error, may be nil

	retryAfter time.Duration // parsed Retry-After / RetryInfo delay; internal to the retry loop
}

func (e *ExportError) Error() string {
	g := ""
	if e.GRPCStatus != 0 {
		g = fmt.Sprintf(" grpc=%d", e.GRPCStatus)
	}

	return fmt.Sprintf("otlp export: status=%d%s retryable=%v rejected=%d warning=%v msg=%q err=%v",
		e.StatusCode, g, e.Retryable, e.Rejected, e.Warning, e.Message, e.Err)
}
```

- [ ] **Step 2.4:** Run: `cd otlp && go test -race ./...`
  Expected: PASS (including any pre-existing Error()-format assertions — if one asserts
  the exact v0.1.0 string, it still matches because `g` is empty for HTTP errors).

- [ ] **Step 2.5: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/options.go otlp/options_test.go
git commit -m "feat(otlp): ExportError.GRPCStatus field"
```

---

### Task 3: gRPC wire helpers (`grpc_status.go` + `forEachLenField`)

**Files:**
- Create: `otlp/grpc_status.go`
- Create: `otlp/grpc_status_test.go`
- Modify: `otlp/proto.go` (append `forEachLenField`)

All pure functions — straight table tests, no server needed.

- [ ] **Step 3.1: Write the failing tests** — create `otlp/grpc_status_test.go`:

```go
package otlp

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryableGRPCStatus(t *testing.T) {
	// OTLP spec table (design §6.3).
	for _, code := range []int{grpcCancelled, grpcDeadlineExceeded, grpcAborted, grpcOutOfRange, grpcUnavailable, grpcDataLoss} {
		require.True(t, retryableGRPCStatus(code, false), "code %d", code)
	}
	require.True(t, retryableGRPCStatus(grpcResourceExhausted, true), "RESOURCE_EXHAUSTED with RetryInfo")
	require.False(t, retryableGRPCStatus(grpcResourceExhausted, false), "RESOURCE_EXHAUSTED without RetryInfo")
	for _, code := range []int{grpcOK, grpcUnknown, grpcInvalidArgument, grpcNotFound, grpcAlreadyExists,
		grpcPermissionDenied, grpcFailedPrecondition, grpcUnimplemented, grpcInternal, grpcUnauthenticated} {
		require.False(t, retryableGRPCStatus(code, false), "code %d", code)
	}
}

func TestHTTPStatusToGRPCStatus(t *testing.T) {
	cases := map[int]int{
		400: grpcInternal, 401: grpcUnauthenticated, 403: grpcPermissionDenied,
		404: grpcUnimplemented, 429: grpcUnavailable, 502: grpcUnavailable,
		503: grpcUnavailable, 504: grpcUnavailable, 418: grpcUnknown,
	}
	for in, want := range cases {
		require.Equal(t, want, httpStatusToGRPCStatus(in), "http %d", in)
	}
}

func TestPercentDecode(t *testing.T) {
	require.Equal(t, "plain", percentDecode("plain"))
	require.Equal(t, "a b", percentDecode("a%20b"))
	require.Equal(t, "ümlaut", percentDecode("%C3%BCmlaut"))
	// Invalid sequences pass through literally (gRPC spec tolerance).
	require.Equal(t, "100%", percentDecode("100%"))
	require.Equal(t, "%zz", percentDecode("%zz"))
}

func TestDecodeBinHeader(t *testing.T) {
	raw := []byte{0x08, 0x0e}
	for name, enc := range map[string]string{
		"unpadded": base64.RawStdEncoding.EncodeToString(raw),
		"padded":   base64.StdEncoding.EncodeToString(raw),
	} {
		got, err := decodeBinHeader(enc)
		require.NoError(t, err, name)
		require.Equal(t, raw, got, name)
	}
	_, err := decodeBinHeader("!!not-base64!!")
	require.Error(t, err)
}

// statusBin builds a serialized google.rpc.Status carrying a RetryInfo detail.
// Field numbers: Status{code=1,message=2,details=3}; Any{type_url=1,value=2};
// RetryInfo{retry_delay=1}; Duration{seconds=1,nanos=2}.
func statusBin(code int64, msg string, delay time.Duration, withRetry bool) []byte {
	st := appendTaggedVarint(nil, 0x08, code)
	st = appendTaggedString(st, 0x12, msg)
	if withRetry {
		var dur []byte
		if s := int64(delay / time.Second); s != 0 {
			dur = appendTaggedVarint(dur, 0x08, s)
		}
		if n := int64(delay % time.Second); n != 0 {
			dur = appendTaggedVarint(dur, 0x10, n)
		}
		ri := appendTaggedBytes(nil, 0x0a, dur)
		anyMsg := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.RetryInfo")
		anyMsg = appendTaggedBytes(anyMsg, 0x12, ri)
		st = appendTaggedBytes(st, 0x1a, anyMsg)
	}

	return st
}

func TestRetryDelayFromStatus(t *testing.T) {
	d, ok := retryDelayFromStatus(statusBin(14, "throttled", 7*time.Second+500*time.Millisecond, true))
	require.True(t, ok)
	require.Equal(t, 7*time.Second+500*time.Millisecond, d)

	_, ok = retryDelayFromStatus(statusBin(14, "no details", 0, false))
	require.False(t, ok)

	// A foreign detail BEFORE RetryInfo must be skipped, not aborted: build
	// Status{code=14, details=[DebugInfo, RetryInfo(3s)]} by hand.
	foreignAny := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.DebugInfo")
	foreignAny = appendTaggedBytes(foreignAny, 0x12, []byte("x"))
	dur := appendTaggedVarint(nil, 0x08, 3) // Duration{seconds: 3}
	ri := appendTaggedBytes(nil, 0x0a, dur)
	retryAny := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.RetryInfo")
	retryAny = appendTaggedBytes(retryAny, 0x12, ri)
	st := appendTaggedVarint(nil, 0x08, 14)
	st = appendTaggedBytes(st, 0x1a, foreignAny)
	st = appendTaggedBytes(st, 0x1a, retryAny)
	d2, ok2 := retryDelayFromStatus(st)
	require.True(t, ok2)
	require.Equal(t, 3*time.Second, d2)

	// Malformed bytes → (0, false), never panic.
	_, ok = retryDelayFromStatus([]byte{0xff, 0xff})
	require.False(t, ok)
}

func TestGRPCTimeoutValue(t *testing.T) {
	require.Equal(t, "", grpcTimeoutValue(0))
	require.Equal(t, "10000m", grpcTimeoutValue(10*time.Second))
	require.Equal(t, "1m", grpcTimeoutValue(500*time.Microsecond)) // sub-ms clamps up to 1m
	// Beyond 8 digits of ms (~27.7h) falls back to seconds.
	require.Equal(t, "108000S", grpcTimeoutValue(30*time.Hour))
}
```

- [ ] **Step 3.2:** Run: `cd otlp && go test -run 'TestRetryable|TestHTTPStatusTo|TestPercentDecode|TestDecodeBin|TestRetryDelay|TestGRPCTimeout' ./...`
  Expected: FAIL — undefined identifiers.

- [ ] **Step 3.3: Implement `otlp/grpc_status.go`:**

```go
package otlp

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// gRPC status codes (the subset the OTLP spec classifies; google.golang.org/
// grpc/codes values, hand-pinned — no dependency).
const (
	grpcOK                 = 0
	grpcCancelled          = 1
	grpcUnknown            = 2
	grpcInvalidArgument    = 3
	grpcDeadlineExceeded   = 4
	grpcNotFound           = 5
	grpcAlreadyExists      = 6
	grpcPermissionDenied   = 7
	grpcResourceExhausted  = 8
	grpcFailedPrecondition = 9
	grpcAborted            = 10
	grpcOutOfRange         = 11
	grpcUnimplemented      = 12
	grpcInternal           = 13
	grpcUnavailable        = 14
	grpcDataLoss           = 15
	grpcUnauthenticated    = 16
)

// retryableGRPCStatus implements the OTLP/gRPC retryable class (design §6.3):
// RESOURCE_EXHAUSTED is retryable ONLY when the server attached RetryInfo.
func retryableGRPCStatus(code int, hasRetryDelay bool) bool {
	switch code {
	case grpcCancelled, grpcDeadlineExceeded, grpcAborted, grpcOutOfRange,
		grpcUnavailable, grpcDataLoss:
		return true
	case grpcResourceExhausted:
		return hasRetryDelay
	}

	return false
}

// httpStatusToGRPCStatus is the canonical gRPC HTTP→status mapping, used only
// when a response carries no grpc-status (non-gRPC intermediary, §6.3 case 3).
func httpStatusToGRPCStatus(code int) int {
	switch code {
	case http.StatusBadRequest:
		return grpcInternal
	case http.StatusUnauthorized:
		return grpcUnauthenticated
	case http.StatusForbidden:
		return grpcPermissionDenied
	case http.StatusNotFound:
		return grpcUnimplemented
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return grpcUnavailable
	}

	return grpcUnknown
}

// percentDecode reverses gRPC's grpc-message percent-encoding (RFC 3986 %XX);
// invalid sequences pass through literally per the gRPC spec's tolerance rule.
func percentDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if hi, ok1 := unhex(s[i+1]); ok1 {
				if lo, ok2 := unhex(s[i+2]); ok2 {
					b.WriteByte(hi<<4 | lo)
					i += 2

					continue
				}
			}
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}

	return 0, false
}

// decodeBinHeader decodes gRPC -bin metadata: canonical emission is unpadded
// base64, but the spec requires accepting both (design §6.3).
func decodeBinHeader(s string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return b, nil
	}

	return base64.StdEncoding.DecodeString(s)
}

const retryInfoTypeURL = "type.googleapis.com/google.rpc.RetryInfo"

// retryDelayFromStatus extracts RetryInfo.retry_delay from a serialized
// google.rpc.Status (grpc-status-details-bin payload). Malformed input
// degrades to (0, false) — plain backoff, never an error (design §6.3).
func retryDelayFromStatus(raw []byte) (time.Duration, bool) {
	var delay time.Duration
	found := false
	_ = forEachLenField(raw, 3, func(anyMsg []byte) bool { // Status.details (repeated Any)
		tu, err := findField(anyMsg, 1) // Any.type_url
		if err != nil || string(tu) != retryInfoTypeURL {
			return true // keep scanning
		}
		val, err := findField(anyMsg, 2) // Any.value
		if err != nil || val == nil {
			return true
		}
		dur, err := findField(val, 1) // RetryInfo.retry_delay
		if err != nil || dur == nil {
			return true
		}
		secs, err := findVarint(dur, 1)
		if err != nil {
			return true
		}
		nanos, err := findVarint(dur, 2)
		if err != nil {
			return true
		}
		if d := time.Duration(secs)*time.Second + time.Duration(nanos); d > 0 { //nolint:gosec
			delay, found = d, true
		}

		return false // RetryInfo found — stop
	})

	return delay, found
}

// grpcTimeoutValue renders WithTimeout as a grpc-timeout header value:
// milliseconds up to the spec's 8-digit cap, then seconds (design §6.2).
func grpcTimeoutValue(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	const maxDigits = 99999999
	if ms <= maxDigits {
		return strconv.FormatInt(ms, 10) + "m"
	}
	s := int64(d / time.Second)
	if s > maxDigits {
		s = maxDigits
	}

	return strconv.FormatInt(s, 10) + "S"
}
```

- [ ] **Step 3.4: Append `forEachLenField` to `otlp/proto.go`** (same skip discipline
  as `findField`, but visits EVERY occurrence — `findField` keeps only the last, which
  cannot walk `repeated Any details`):

```go
// forEachLenField invokes fn for every len-delimited occurrence of field in
// b, in order. fn returns false to stop early. Unknown fields are skipped
// with the same wire-type discipline as findField.
func forEachLenField(b []byte, field int, fn func(payload []byte) bool) error {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0: // varint
			_, vn := binary.Uvarint(b)
			if vn <= 0 {
				return errTruncatedResponse
			}
			adv = vn
		case 1: // fixed64
			adv = 8
		case 2: // len-delimited
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l { //nolint:gosec
				return errTruncatedResponse
			}
			if num == field && !fn(b[ln:ln+int(l)]) { //nolint:gosec
				return nil
			}
			adv = ln + int(l) //nolint:gosec
		case 5: // fixed32
			adv = 4
		default:
			return errTruncatedResponse
		}
		if len(b) < adv {
			return errTruncatedResponse
		}
		b = b[adv:]
	}

	return nil
}
```

  Also remove the now-stale `//nolint:unused` from `appendTaggedVarint`
  (proto.go:37) — the new tests use it.

- [ ] **Step 3.5:** Run: `cd otlp && go test -race ./...`
  Expected: PASS.

- [ ] **Step 3.6: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/grpc_status.go otlp/grpc_status_test.go otlp/proto.go
git commit -m "feat(otlp): gRPC status classification, RetryInfo decode, wire helpers"
```

---

### Task 4: gRPC endpoint resolution + new options

**Files:**
- Modify: `otlp/options.go` (fields `insecure`, `tlsConfig`; `WithInsecure`, `WithTLSConfig`)
- Create: `otlp/grpc_transport.go` (starts with `resolveGRPCEndpoint`, `validateGRPCHeaders`, `grpcMethodPath`)
- Create: `otlp/grpc_transport_test.go`

- [ ] **Step 4.1: Write the failing tests** — create `otlp/grpc_transport_test.go`:

```go
package otlp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveGRPCEndpoint(t *testing.T) {
	cases := []struct {
		name      string
		endpoint  string
		insecure  bool
		hasTLSCfg bool
		wantBase  string
		wantTLS   bool
		wantErr   bool
	}{
		{name: "bare defaults to TLS and 4317", endpoint: "collector.example.com", wantBase: "https://collector.example.com:4317", wantTLS: true},
		{name: "bare with port keeps port", endpoint: "localhost:14317", wantBase: "https://localhost:14317", wantTLS: true},
		{name: "bare insecure is h2c", endpoint: "localhost:4317", insecure: true, wantBase: "http://localhost:4317"},
		{name: "bare with tls config", endpoint: "localhost:4317", hasTLSCfg: true, wantBase: "https://localhost:4317", wantTLS: true},
		{name: "bare insecure + tls config conflict", endpoint: "localhost:4317", insecure: true, hasTLSCfg: true, wantErr: true},
		{name: "http scheme is h2c", endpoint: "http://localhost:4317", wantBase: "http://localhost:4317"},
		{name: "http scheme wins over default-secure", endpoint: "http://localhost:4317", insecure: false, wantBase: "http://localhost:4317"},
		{name: "http scheme + WithInsecure redundant ok", endpoint: "http://localhost:4317", insecure: true, wantBase: "http://localhost:4317"},
		{name: "http scheme + tls config conflict", endpoint: "http://localhost:4317", hasTLSCfg: true, wantErr: true},
		{name: "https scheme", endpoint: "https://collector:4317", wantBase: "https://collector:4317", wantTLS: true},
		{name: "https scheme wins over WithInsecure", endpoint: "https://collector:4317", insecure: true, wantBase: "https://collector:4317", wantTLS: true},
		{name: "empty", endpoint: "", wantErr: true},
		{name: "path rejected", endpoint: "http://localhost:4317/v1/logs", wantErr: true},
		{name: "bare path rejected", endpoint: "localhost:4317/v1/logs", wantErr: true},
		{name: "query rejected", endpoint: "http://localhost:4317?x=1", wantErr: true},
		{name: "unsupported scheme", endpoint: "grpc://localhost:4317", wantErr: true},
		{name: "trailing slash tolerated", endpoint: "http://localhost:4317/", wantBase: "http://localhost:4317"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, useTLS, err := resolveGRPCEndpoint(tc.endpoint, tc.insecure, tc.hasTLSCfg)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantBase, base)
			require.Equal(t, tc.wantTLS, useTLS)
		})
	}
}

func TestValidateGRPCHeaders(t *testing.T) {
	require.NoError(t, validateGRPCHeaders(map[string]string{"x-api-key": "k", "Authorization": "Bearer abc123 =/+~"}))
	for _, bad := range []string{"grpc-timeout", "Grpc-Encoding", "content-type", "TE", "x-data-bin", ":authority"} {
		require.Error(t, validateGRPCHeaders(map[string]string{bad: "v"}), bad)
	}
	// Values must be printable ASCII (gRPC ASCII-metadata rule) — a
	// deterministic config error must fail at construction, not send time
	// (plan-review pass-1 P1).
	for name, val := range map[string]string{
		"high byte":      "\x80",
		"utf8":           "ümlaut",
		"newline":        "a\nb",
		"control":        "a\x01b",
		"del":            "a\x7fb",
		"leading space":  " v",
		"trailing space": "v ",
	} {
		require.Error(t, validateGRPCHeaders(map[string]string{"x-ok-key": val}), name)
	}
}
```

- [ ] **Step 4.2:** Run: `cd otlp && go test -run 'TestResolveGRPCEndpoint|TestValidateGRPCHeaders' ./...`
  Expected: FAIL — undefined identifiers.

- [ ] **Step 4.3: Implement.** Add to `otlp/options.go` — two fields on `options`
  (after `compression`) and two options (after `WithCompression`):

```go
	// (options struct additions)
	insecure  bool
	tlsConfig *tls.Config
```

```go
// WithInsecure selects plaintext h2c for SCHEME-LESS gRPC endpoints
// ("host:4317"). An explicit http/https scheme always takes precedence (OTel
// exporter-spec precedence, design 2026-06-12 §5). No-op on the HTTP path.
func WithInsecure() Option { return func(o *options) { o.insecure = true } }

// WithTLSConfig supplies the gRPC TLS configuration (custom CA, mTLS) and
// implies TLS for scheme-less endpoints. Combining it with a plaintext
// endpoint ("http://" scheme, or bare + WithInsecure) is a construction
// error. No-op on the HTTP path — use WithHTTPClient there. nil is ignored.
func WithTLSConfig(c *tls.Config) Option {
	return func(o *options) {
		if c != nil {
			o.tlsConfig = c
		}
	}
}
```

  Add `"crypto/tls"` to options.go imports.

- [ ] **Step 4.4: Create `otlp/grpc_transport.go`** (endpoint + header validation only;
  the transport itself is Task 5):

```go
package otlp

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// grpcMethodPath is the only :path OTLP/gRPC ever uses; user-supplied URL
// paths are a misconfiguration and are rejected (design 2026-06-12 §5).
const grpcMethodPath = "/opentelemetry.proto.collector.logs.v1.LogsService/Export"

const grpcDefaultPort = "4317"

// resolveGRPCEndpoint maps the accepted endpoint forms to (base URL, TLS):
//
//	host[:port]          TLS by default (spec insecure=false); WithInsecure → h2c
//	http://host[:port]   plaintext h2c; scheme wins; +WithTLSConfig errors
//	https://host[:port]  TLS; scheme wins
//
// Scheme-less endpoints default to port 4317; http/https leave the URL's
// own port semantics (80/443) to the HTTP client.
func resolveGRPCEndpoint(endpoint string, insecure, hasTLSCfg bool) (base string, useTLS bool, err error) {
	if endpoint == "" {
		return "", false, ErrNoEndpoint
	}
	if !strings.Contains(endpoint, "://") {
		if strings.ContainsAny(endpoint, "/?#") {
			return "", false, fmt.Errorf("otlp: grpc endpoint %q must be host[:port] — gRPC uses the fixed method path", endpoint)
		}
		host := endpoint
		if _, _, sperr := net.SplitHostPort(host); sperr != nil {
			host = net.JoinHostPort(host, grpcDefaultPort)
		}
		if insecure {
			if hasTLSCfg {
				return "", false, fmt.Errorf("otlp: WithInsecure and WithTLSConfig conflict for endpoint %q", endpoint)
			}

			return "http://" + host, false, nil
		}

		return "https://" + host, true, nil
	}

	u, perr := url.Parse(endpoint)
	if perr != nil {
		return "", false, fmt.Errorf("otlp: invalid grpc endpoint %q: %w", endpoint, perr)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("otlp: grpc endpoint %q has no host", endpoint)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false, fmt.Errorf("otlp: grpc endpoint %q must not carry a path/query/fragment — gRPC uses the fixed method path", endpoint)
	}
	switch u.Scheme {
	case "http":
		if hasTLSCfg {
			return "", false, fmt.Errorf("otlp: WithTLSConfig conflicts with plaintext scheme in %q", endpoint)
		}

		return "http://" + u.Host, false, nil
	case "https":
		return "https://" + u.Host, true, nil
	default:
		return "", false, fmt.Errorf("otlp: grpc endpoint %q must be host:port or an http(s) URL", endpoint)
	}
}

// validateGRPCHeaders rejects WithHeaders entries that would corrupt the
// gRPC request (design §4.4): reserved keys (grpc-* metadata, -bin suffix,
// framing headers, pseudo-headers — binary metadata is unsupported) and
// non-printable-ASCII values (the gRPC ASCII-metadata value rule,
// %x20-%x7E). Both fail at construction: a deterministic configuration
// error must not surface as a send-time export event.
func validateGRPCHeaders(h map[string]string) error {
	for k, v := range h {
		lk := strings.ToLower(k)
		switch {
		case strings.HasPrefix(lk, "grpc-"),
			strings.HasSuffix(lk, "-bin"),
			strings.HasPrefix(lk, ":"),
			lk == "content-type", lk == "te":
			return fmt.Errorf("otlp: header %q is reserved on the gRPC transport", k)
		}
		for i := 0; i < len(v); i++ {
			if v[i] < 0x20 || v[i] > 0x7e {
				return fmt.Errorf("otlp: header %q value contains non-printable-ASCII byte 0x%02x", k, v[i])
			}
		}
		if v != strings.TrimSpace(v) {
			return fmt.Errorf("otlp: header %q value has leading/trailing whitespace", k)
		}
	}

	return nil
}
```

- [ ] **Step 4.5:** Run: `cd otlp && go test -race ./...`
  Expected: PASS.

- [ ] **Step 4.6: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/options.go otlp/grpc_transport.go otlp/grpc_transport_test.go
git commit -m "feat(otlp): gRPC endpoint resolution, WithInsecure/WithTLSConfig, header validation"
```

---

### Task 5: `grpcTransport` — framing, attempt, fake-server tests

**Files:**
- Modify: `otlp/grpc_transport.go` (add the transport)
- Modify: `otlp/grpc_transport_test.go` (fake stdlib h2c gRPC server + scenario tests)

The fake server is pure stdlib: Go ≥1.24 `http.Server.Protocols` accepts unencrypted
HTTP/2, and `http.TrailerPrefix` emits undeclared trailers — everything a unary gRPC
exchange needs. Spike-proven gotchas under test: trailers-only errors surface
`grpc-status` in HEADERS; body must be read to EOF before `resp.Trailer` populates;
HTTP `:status` is 200 even for gRPC errors.

- [ ] **Step 5.1: Write the failing tests** — append to `otlp/grpc_transport_test.go`:

```go
// --- fake gRPC server (pure stdlib, h2c) ---

// fakeGRPCServer runs an h2c HTTP/2 server that de-frames Export requests and
// delegates the response to handler. It records every received request message.
type fakeGRPCServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	msgs [][]byte
}

func (f *fakeGRPCServer) received() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]byte(nil), f.msgs...)
}

func newFakeGRPCServer(t *testing.T, handler http.HandlerFunc) *fakeGRPCServer {
	t.Helper()
	f := &fakeGRPCServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+grpcMethodPath, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) >= 5 {
			msg := body[5:]
			if body[0] == 1 && r.Header.Get("Grpc-Encoding") == "gzip" {
				if zr, err := gzip.NewReader(bytes.NewReader(msg)); err == nil {
					msg, _ = io.ReadAll(zr)
				}
			}
			f.mu.Lock()
			f.msgs = append(f.msgs, append([]byte(nil), msg...))
			f.mu.Unlock()
		}
		handler(w, r)
	})
	f.srv = httptest.NewUnstartedServer(mux)
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	f.srv.Config.Protocols = protos
	f.srv.Start()
	t.Cleanup(f.srv.Close)

	return f
}

// okResponse writes a framed ExportLogsServiceResponse with trailers.
func okResponse(w http.ResponseWriter, respMsg []byte) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
	frame := make([]byte, 5, 5+len(respMsg))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(respMsg)))
	_, _ = w.Write(append(frame, respMsg...))
}

// trailersOnlyError writes the status in the response HEADERS (no body) —
// exactly how grpc-go reports immediate errors.
func trailersOnlyError(w http.ResponseWriter, code int, msg, detailsBin string) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", strconv.Itoa(code))
	if msg != "" {
		w.Header().Set("Grpc-Message", msg)
	}
	if detailsBin != "" {
		w.Header().Set("Grpc-Status-Details-Bin", detailsBin)
	}
	w.WriteHeader(http.StatusOK)
}

func newTestGRPCTransport(t *testing.T, url string, opts ...Option) *grpcTransport {
	t.Helper()
	tr, err := newGRPCTransport(url, applyOptions(opts))
	require.NoError(t, err)

	return tr
}

// --- scenarios ---

func TestGRPCAttemptSuccessEmpty(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	p := tr.prepare([]byte("payload"))
	accept, expErr := tr.attempt(p)
	require.Nil(t, expErr)
	require.Nil(t, accept)
	require.Equal(t, [][]byte{[]byte("payload")}, f.received())
}

func TestGRPCAttemptRequestHeaders(t *testing.T) {
	var got http.Header
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		okResponse(w, nil)
	})
	tr := newTestGRPCTransport(t, f.srv.URL, WithHeaders(map[string]string{"x-api-key": "k"}))
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.Equal(t, "application/grpc", got.Get("Content-Type"))
	require.Equal(t, "trailers", got.Get("Te"))
	require.Equal(t, "identity,gzip", got.Get("Grpc-Accept-Encoding"))
	require.Equal(t, "10000m", got.Get("Grpc-Timeout")) // default WithTimeout 10s
	require.Equal(t, "k", got.Get("X-Api-Key"))
	require.Contains(t, got.Get("User-Agent"), "zapwire-otlp")
}

func TestGRPCAttemptPartialSuccess(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		okResponse(w, psBody(3, "spike-partial"))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.NotNil(t, accept)
	require.EqualValues(t, 3, accept.rejected)
	require.Equal(t, "spike-partial", accept.event.Message)
	require.Zero(t, accept.event.GRPCStatus)
}

func TestGRPCAttemptTrailersOnlyUnavailableWithRetryInfo(t *testing.T) {
	bin := base64.RawStdEncoding.EncodeToString(statusBin(14, "throttled", 7*time.Second, true))
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		trailersOnlyError(w, 14, "try%20later", bin)
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, accept)
	require.NotNil(t, expErr)
	require.Equal(t, 14, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
	require.Equal(t, "try later", expErr.Message) // percent-decoded
	require.Equal(t, 7*time.Second, expErr.retryAfter)
	require.Zero(t, expErr.StatusCode) // gRPC errors ride HTTP 200
}

func TestGRPCAttemptResourceExhausted(t *testing.T) {
	t.Run("without RetryInfo is terminal", func(t *testing.T) {
		f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
			trailersOnlyError(w, 8, "quota", "")
		})
		tr := newTestGRPCTransport(t, f.srv.URL)
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.NotNil(t, expErr)
		require.False(t, expErr.Retryable)
	})
	t.Run("with RetryInfo is retryable", func(t *testing.T) {
		bin := base64.StdEncoding.EncodeToString(statusBin(8, "quota", 3*time.Second, true)) // padded variant on purpose
		f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
			trailersOnlyError(w, 8, "quota", bin)
		})
		tr := newTestGRPCTransport(t, f.srv.URL)
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.NotNil(t, expErr)
		require.True(t, expErr.Retryable)
		require.Equal(t, 3*time.Second, expErr.retryAfter)
	})
}

func TestGRPCAttemptInvalidArgument(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		trailersOnlyError(w, 3, "bad", "")
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, 3, expErr.GRPCStatus)
	require.False(t, expErr.Retryable)
}

func TestGRPCAttemptStatusInRealTrailers(t *testing.T) {
	// Error status delivered via genuine trailers AFTER a body write — the
	// trailer-first resolution path (not trailers-only).
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "14")
		w.Header().Set(http.TrailerPrefix+"Grpc-Message", "drain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{}) // headers flushed, no message frame
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, 14, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
}

func TestGRPCAttemptNoGRPCStatusFallsBackToHTTPMapping(t *testing.T) {
	// A non-gRPC intermediary (plain 503, no grpc-status anywhere).
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream down"))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, http.StatusServiceUnavailable, expErr.StatusCode)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
	require.Contains(t, expErr.Message, "upstream down")
}

func TestGRPCPrepareGzip(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL, WithCompression(Gzip))
	p := tr.prepare([]byte("compress me"))
	require.True(t, p.compressed)
	require.Nil(t, p.warn)
	require.Equal(t, byte(1), p.body[0]) // frame compressed-flag
	accept, expErr := tr.attempt(p)
	require.Nil(t, expErr)
	require.Nil(t, accept)
	require.Equal(t, [][]byte{[]byte("compress me")}, f.received()) // server-side gunzip round-trip
}

func TestGRPCGzippedResponseFrame(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		_, _ = zw.Write(psBody(2, "gz-partial"))
		_ = zw.Close()
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Encoding", "gzip")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		frame := make([]byte, 5, 5+gz.Len())
		frame[0] = 1
		binary.BigEndian.PutUint32(frame[1:5], uint32(gz.Len()))
		_, _ = w.Write(append(frame, gz.Bytes()...))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.NotNil(t, accept)
	require.EqualValues(t, 2, accept.rejected)
	require.Equal(t, "gz-partial", accept.event.Message)
}

func TestGRPCMalformedResponseFrameIsObservabilityOnly(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		_, _ = w.Write([]byte{0x00, 0xff}) // truncated frame on an OK status
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr) // server said OK: batch is accepted
	require.NotNil(t, accept)
	require.Zero(t, accept.rejected)
	require.Error(t, accept.event.Err)
}

func TestGRPCAttemptTLS(t *testing.T) {
	// https + WithTLSConfig path over stdlib ALPN h2.
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+grpcMethodPath, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		okResponse(w, nil)
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr := newTestGRPCTransport(t, srv.URL, WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.Nil(t, accept)
}

func TestGRPCConnectionReuse(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	for i := range 3 {
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.Nil(t, expErr, "attempt %d", i)
	}
	// One h2c connection serves all requests (stdlib pools per-host).
	require.Len(t, f.received(), 3)
}

// flakyRT fails the first n round trips, then delegates.
type flakyRT struct {
	mu    sync.Mutex
	fails int
	inner http.RoundTripper
}

func (f *flakyRT) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	fail := f.fails > 0
	if fail {
		f.fails--
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("connection reset by peer (injected)")
	}

	return f.inner.RoundTrip(r)
}

func TestGRPCTransportErrorRetryable(t *testing.T) {
	// P0 (plan-review pass 1): dial/reset failures must be RETRYABLE
	// (UNAVAILABLE), unlike the HTTP path's terminal transport errors.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	tr.client.Transport = &flakyRT{fails: 1, inner: tr.client.Transport}
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
	require.Error(t, expErr.Err)
}

func TestGRPCDeadlineExceededRetryable(t *testing.T) {
	blocked := make(chan struct{})
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		<-blocked // hold the request past the client timeout
		okResponse(w, nil)
	})
	t.Cleanup(func() { close(blocked) })
	tr := newTestGRPCTransport(t, f.srv.URL, WithTimeout(50*time.Millisecond))
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcDeadlineExceeded, expErr.GRPCStatus)
}

func TestGRPCBodyReadFailureRetryable(t *testing.T) {
	// Partial body then an aborted stream: trailers are unreliable, so the
	// attempt must come back retryable, NOT fall through to the no-status
	// HTTP-mapping path.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0x00, 0x00, 0x00, 0x00, 0xff}) // claims more bytes than sent
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		panic(http.ErrAbortHandler) // RST_STREAM mid-body
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
}

func TestGRPCWriterRetriesTransportError(t *testing.T) {
	// Writer-level proof of the §6.5 promise: first-attempt connection
	// failure → retry → success, zero drops.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	o := applyOptions([]Option{WithRetry(RetryConfig{Initial: 10 * time.Millisecond, MaxInterval: 50 * time.Millisecond, MaxElapsed: 30 * time.Second})})
	tr, err := newGRPCTransport(f.srv.URL, o)
	require.NoError(t, err)
	tr.client.Transport = &flakyRT{fails: 1, inner: tr.client.Transport}
	w := newWriterCore(tr, o)
	go w.run()
	t.Cleanup(func() { _ = w.Close() })

	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.Len(t, f.received(), 1)
}
```

  New imports for the test file:
  `bytes`, `compress/gzip`, `crypto/tls`, `crypto/x509`, `encoding/base64`,
  `encoding/binary`, `errors`, `io`, `net/http`, `net/http/httptest`, `strconv`,
  `sync`, `time`.

- [ ] **Step 5.2:** Run: `cd otlp && go test -run TestGRPC ./...`
  Expected: FAIL — `undefined: newGRPCTransport` (and `grpcTransport`).

- [ ] **Step 5.3: Implement the transport** — append to `otlp/grpc_transport.go`
  (imports grow to: `bytes`, `compress/gzip`, `context`, `encoding/binary`, `fmt`,
  `io`, `net`, `net/http`, `net/url`, `strconv`, `strings`, `time` — NOT `crypto/tls`;
  the transport only forwards `o.tlsConfig`, it names no tls symbols):

```go
// grpcTransport is the hand-rolled OTLP/gRPC ship layer (design 2026-06-12
// §6): a unary gRPC client over stdlib HTTP/2 — h2c for plaintext (Go ≥1.24
// Protocols), ALPN h2 for TLS. It owns its http.Client: a user-supplied
// client with HTTP/1 enabled would break gRPC, hence WithHTTPClient is a
// documented no-op here.
type grpcTransport struct {
	url        string // base + grpcMethodPath
	client     *http.Client
	headers    map[string]string
	gzipOn     bool
	timeout    time.Duration
	timeoutHdr string // precomputed grpc-timeout value ("" = omit)
	userAgent  string
}

var _ transport = (*grpcTransport)(nil)

func newGRPCTransport(endpoint string, o options) (*grpcTransport, error) {
	base, useTLS, err := resolveGRPCEndpoint(endpoint, o.insecure, o.tlsConfig != nil)
	if err != nil {
		return nil, err
	}
	if err := validateGRPCHeaders(o.headers); err != nil {
		return nil, err
	}

	protos := new(http.Protocols)
	htr := &http.Transport{Protocols: protos}
	if useTLS {
		// ALPN h2 only — no HTTP/1 fallback on this dedicated transport.
		protos.SetHTTP2(true)
		htr.TLSClientConfig = o.tlsConfig
		htr.ForceAttemptHTTP2 = true
	} else {
		// h2c (prior knowledge). HTTP/1 must stay DISABLED: with both
		// enabled, stdlib picks HTTP/1 for http:// URLs (spike gotcha #1).
		protos.SetUnencryptedHTTP2(true)
	}

	ua := "zapwire-otlp"
	if v := moduleVersion(); v != "" {
		ua += "/" + v
	}

	return &grpcTransport{
		url:        base + grpcMethodPath,
		client:     &http.Client{Transport: htr},
		headers:    o.headers,
		gzipOn:     o.compression == Gzip,
		timeout:    o.timeout,
		timeoutHdr: grpcTimeoutValue(o.timeout),
		userAgent:  ua,
	}, nil
}

// grpcFrame wraps msg in the gRPC Length-Prefixed-Message framing:
// 1-byte compressed flag + 4-byte big-endian length + message.
func grpcFrame(msg []byte, compressed bool) []byte {
	body := make([]byte, 5, 5+len(msg))
	if compressed {
		body[0] = 1
	}
	binary.BigEndian.PutUint32(body[1:5], uint32(len(msg))) //nolint:gosec

	return append(body, msg...)
}

// prepare frames (and per-message gzips) once per batch. A gzip failure
// ships uncompressed rather than losing the batch (httpTransport parity).
func (t *grpcTransport) prepare(msg []byte) prepared {
	if t.gzipOn {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		if _, err := zw.Write(msg); err == nil && zw.Close() == nil {
			return prepared{body: grpcFrame(gz.Bytes(), true), compressed: true}
		}

		return prepared{body: grpcFrame(msg, false), warn: &ExportError{Message: "gzip failed; sent uncompressed"}}
	}

	return prepared{body: grpcFrame(msg, false)}
}

// attempt performs one unary Export call. Context is Background+timeout, not
// the lifecycle ctx — Close never cancels an accepted request (§5.4 parity).
func (t *grpcTransport) attempt(p prepared) (*acceptance, *ExportError) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(p.body))
	if err != nil {
		return nil, &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	req.Header.Set("Grpc-Accept-Encoding", "identity,gzip")
	req.Header.Set("User-Agent", t.userAgent)
	if t.timeoutHdr != "" {
		req.Header.Set("Grpc-Timeout", t.timeoutHdr)
	}
	if p.compressed {
		req.Header.Set("Grpc-Encoding", "gzip")
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		// Transport failures (dial, reset, GOAWAY, local deadline) are
		// RETRYABLE on gRPC — grpc-go's own classification, and the §6.5
		// promise that connection loss is absorbed by retry + re-dial.
		// (Deliberate asymmetry: the HTTP path keeps v0.1.0's terminal
		// transport errors.)
		return nil, t.transportError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Body MUST be read to EOF before resp.Trailer is populated (spike
	// gotcha #3). A failed read also means trailers are unreliable —
	// return retryable rather than resolving status from incomplete metadata.
	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if rerr != nil {
		return nil, t.transportError(ctx, rerr)
	}

	// Trailer-first status resolution; trailers-only errors surface the
	// status (and details) in resp.Header instead (spike gotcha #2).
	statusStr := resp.Trailer.Get("Grpc-Status")
	meta := resp.Trailer
	if statusStr == "" {
		statusStr = resp.Header.Get("Grpc-Status")
		meta = resp.Header
	}
	if statusStr == "" {
		// Not a gRPC response (proxy interference): canonical HTTP mapping.
		code := httpStatusToGRPCStatus(resp.StatusCode)

		return nil, &ExportError{
			StatusCode: resp.StatusCode,
			GRPCStatus: code,
			Retryable:  retryableGRPCStatus(code, false),
			Message:    excerpt(respBody),
		}
	}

	code, cerr := strconv.Atoi(statusStr)
	if cerr != nil {
		return nil, &ExportError{Message: "malformed grpc-status " + excerpt([]byte(statusStr)), Err: cerr}
	}
	if code == grpcOK {
		msg, derr := deframeGRPCResponse(respBody, resp.Header.Get("Grpc-Encoding"))
		if derr != nil {
			// Accepted by the server; the malformed frame is observability-only.
			return &acceptance{event: &ExportError{Err: derr}}, nil
		}
		if msg == nil {
			return nil, nil
		}

		return resolveAccept(msg, ExportError{}), nil
	}

	var retryAfter time.Duration
	hasRetryInfo := false
	if det := meta.Get("Grpc-Status-Details-Bin"); det != "" {
		if raw, berr := decodeBinHeader(det); berr == nil {
			if d, ok := retryDelayFromStatus(raw); ok {
				retryAfter, hasRetryInfo = d, true
			}
		}
	}

	return nil, &ExportError{
		GRPCStatus: code,
		Retryable:  retryableGRPCStatus(code, hasRetryInfo),
		Message:    percentDecode(meta.Get("Grpc-Message")),
		retryAfter: retryAfter,
	}
}

// transportError classifies a client.Do / body-read failure: UNAVAILABLE
// (connection-level), or DEADLINE_EXCEEDED when the attempt's local timeout
// elapsed. Both are in the OTLP retryable class (design §6.3).
func (t *grpcTransport) transportError(ctx context.Context, err error) *ExportError {
	code := grpcUnavailable
	if ctx.Err() != nil {
		code = grpcDeadlineExceeded
	}

	return &ExportError{GRPCStatus: code, Retryable: true, Err: err}
}

// deframeGRPCResponse strips the 5-byte prefix and gunzips a compressed
// message frame. A zero-length body (no response frame) returns (nil, nil).
func deframeGRPCResponse(body []byte, encoding string) ([]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}
	if len(body) < 5 {
		return nil, errTruncatedResponse
	}
	mlen := binary.BigEndian.Uint32(body[1:5])
	if uint64(len(body)-5) < uint64(mlen) {
		return nil, errTruncatedResponse
	}
	msg := body[5 : 5+mlen]
	if body[0]&1 == 0 {
		return msg, nil
	}
	if encoding != "gzip" {
		return nil, fmt.Errorf("otlp: compressed response frame with grpc-encoding %q", encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()

	return io.ReadAll(io.LimitReader(zr, 1<<20))
}
```

- [ ] **Step 5.4:** Run: `cd otlp && go test -race -run TestGRPC ./...`
  Expected: PASS — all scenarios.

- [ ] **Step 5.5:** Run the full suite + lint: `cd otlp && go test -race ./... && cd .. && make lint`
  Expected: PASS, `0 issues.`.

- [ ] **Step 5.6: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/grpc_transport.go otlp/grpc_transport_test.go
git commit -m "feat(otlp): hand-rolled gRPC transport — h2c/TLS, framing, status resolution, RetryInfo"
```

---

### Task 6: Public constructors + `Protocol` env helper + doc.go

**Files:**
- Modify: `otlp/writer.go` (`NewHTTPWriter`, `NewGRPCWriter`)
- Modify: `otlp/core.go` (`NewHTTPCore`, `NewGRPCCore`)
- Modify: `otlp/options.go` (`Protocol`, `ProtocolFromEnv`)
- Modify: `otlp/doc.go` (protocol-selection paragraph)
- Modify: `otlp/writer_test.go`, `otlp/options_test.go` (tests)

- [ ] **Step 6.1: Write the failing tests.** Append to `otlp/writer_test.go`:

```go
func TestNewGRPCWriterValidation(t *testing.T) {
	_, err := NewGRPCWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)

	_, err = NewGRPCWriter("http://localhost:4317/v1/logs")
	require.Error(t, err) // path rejected

	_, err = NewGRPCWriter("localhost:4317", WithInsecure(), WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	require.Error(t, err) // insecure × TLS conflict

	_, err = NewGRPCWriter("localhost:4317", WithInsecure(), WithHeaders(map[string]string{"grpc-timeout": "1S"}))
	require.Error(t, err) // reserved header
}

func TestNewGRPCWriterEndToEnd(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	w, err := NewGRPCWriter(f.srv.URL) // http:// scheme → h2c
	require.NoError(t, err)
	_, _ = w.Write([]byte("rec"))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	require.Len(t, f.received(), 1)
}

func TestNewHTTPWriterIsNewWriter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	w, err := NewHTTPWriter(srv.URL)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = NewHTTPWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)
}

func TestNewGRPCCore(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	core, w, err := NewGRPCCore(f.srv.URL, zapcore.InfoLevel)
	require.NoError(t, err)
	logger := zap.New(core)
	logger.Info("hello grpc")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Len(t, f.received(), 1)

	_, _, err = NewGRPCCore("", zapcore.InfoLevel)
	require.ErrorIs(t, err, ErrNoEndpoint)
}
```

  Append to `otlp/options_test.go`:

```go
func TestProtocolFromEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	require.Equal(t, Protocol(""), ProtocolFromEnv())

	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	require.Equal(t, ProtocolGRPC, ProtocolFromEnv())

	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "http/protobuf")
	require.Equal(t, ProtocolHTTPProtobuf, ProtocolFromEnv(), "signal-specific wins")
}
```

- [ ] **Step 6.2:** Run: `cd otlp && go test -run 'TestNewGRPC|TestNewHTTP|TestProtocolFromEnv' ./...`
  Expected: FAIL — undefined identifiers.

- [ ] **Step 6.3: Implement constructors.** Append to `otlp/writer.go` (after
  `NewWriter`; also add the "Equivalent to NewHTTPWriter." line to NewWriter's godoc
  if Task 1 didn't already):

```go
// NewHTTPWriter is the explicit symmetric counterpart of NewGRPCWriter; it
// is exactly NewWriter (OTLP/HTTP, the spec's default protocol).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewHTTPWriter(endpoint string, opts ...Option) (*Writer, error) {
	return NewWriter(endpoint, opts...)
}

// NewGRPCWriter builds the OTLP/gRPC exporter and starts its flush
// goroutine. Endpoint forms (design 2026-06-12 §5):
//
//	"host:4317"          TLS (spec default); WithInsecure() selects h2c
//	"http://host:4317"   plaintext h2c (scheme wins)
//	"https://host:4317"  TLS (scheme wins); WithTLSConfig for custom CA/mTLS
//
// URL paths are rejected — gRPC always posts to the fixed method path. The
// transport owns its HTTP/2-only client (WithHTTPClient is a no-op here)
// and does not traverse proxies. The caller owns the Writer and must Close
// it.
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint, endpoint validation, option conflicts, or
//     reserved WithHeaders keys
func NewGRPCWriter(endpoint string, opts ...Option) (*Writer, error) {
	o := applyOptions(opts)
	tr, err := newGRPCTransport(endpoint, o)
	if err != nil {
		return nil, err
	}
	w := newWriterCore(tr, o)
	go w.run()

	return w, nil
}
```

- [ ] **Step 6.4: Implement cores.** Append to `otlp/core.go` (and add "Equivalent to
  NewHTTPCore." to NewCore's godoc):

```go
// NewHTTPCore is the explicit symmetric counterpart of NewGRPCCore; it is
// exactly NewCore (OTLP/HTTP).
func NewHTTPCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error) {
	return NewCore(endpoint, level, opts...)
}

// NewGRPCCore wires NewEncoder + NewGRPCWriter into the custom trace-aware
// core (design §2.2) plus its Writer, which the caller must Close. One opts
// list feeds all three ends (each option sets only the fields it owns).
//
// Returns:
//   - zapcore.Core: the OTLP core (sticky zap.Any("context", ctx) works here)
//   - *Writer: the underlying exporter; the caller owns it and must Close it
//   - error: a non-nil error from NewGRPCWriter
func NewGRPCCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error) {
	w, err := NewGRPCWriter(endpoint, opts...)
	if err != nil {
		return nil, nil, err
	}

	return newOTLPCore(newEncoder(applyOptions(opts)), w, level), w, nil
}
```

- [ ] **Step 6.5: Implement the env helper.** Append to `otlp/options.go` next to
  `EndpointFromEnv`:

```go
// Protocol is an OTLP transport protocol name as used by
// OTEL_EXPORTER_OTLP_PROTOCOL.
type Protocol string

const (
	ProtocolGRPC         Protocol = "grpc"
	ProtocolHTTPProtobuf Protocol = "http/protobuf"
)

// ProtocolFromEnv resolves OTEL_EXPORTER_OTLP_LOGS_PROTOCOL then
// OTEL_EXPORTER_OTLP_PROTOCOL. Returns "" when neither is set. Explicit and
// opt-in like EndpointFromEnv — zapwire never reads env behind the caller's
// back (design §5.5). Callers dispatch themselves:
//
//	switch otlp.ProtocolFromEnv() {
//	case otlp.ProtocolGRPC:
//	    w, err = otlp.NewGRPCWriter(otlp.EndpointFromEnv())
//	default: // "", http/protobuf, http/json (json is not implemented here)
//	    w, err = otlp.NewHTTPWriter(otlp.EndpointFromEnv())
//	}
func ProtocolFromEnv() Protocol {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"); v != "" {
		return Protocol(v)
	}

	return Protocol(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))
}
```

- [ ] **Step 6.6: Update `otlp/doc.go`.** Add a protocol-selection paragraph to the
  package comment (adapt to the existing prose style, near the existing
  NewWriter/NewCore mentions):

```text
Two OTLP transports are provided. NewWriter/NewHTTPWriter (and
NewCore/NewHTTPCore) speak OTLP/HTTP — binary protobuf POSTed to /v1/logs,
default port 4318, the OTel spec's default protocol. NewGRPCWriter (and
NewGRPCCore) speak OTLP/gRPC — a unary LogsService/Export call, default
port 4317, implemented with a hand-rolled stdlib HTTP/2 client (no grpc-go
dependency). ProtocolFromEnv reads OTEL_EXPORTER_OTLP_[LOGS_]PROTOCOL for
env-driven dispatch between them.
```

- [ ] **Step 6.7:** Run: `cd otlp && go test -race ./...`
  Expected: PASS. Test-file imports to add where needed: `crypto/tls`, `go.uber.org/zap`.

- [ ] **Step 6.8: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/writer.go otlp/core.go otlp/options.go otlp/doc.go otlp/writer_test.go otlp/options_test.go
git commit -m "feat(otlp): NewGRPCWriter/NewGRPCCore, NewHTTPWriter/NewHTTPCore symmetry, ProtocolFromEnv"
```

---

### Task 7: gRPC lifecycle parity tests

**Files:**
- Modify: `otlp/writer_lifecycle_test.go`

The lifecycle machinery (admit lock, Sync barrier, Close drain) is transport-agnostic
and already covered over HTTP. These tests pin the gRPC writer to the same contract —
especially that the retry loop honors RetryInfo delays and Close interrupts them.

- [ ] **Step 7.1: Write the failing tests** — append to `otlp/writer_lifecycle_test.go`:

```go
// grpcLifecycleWriter builds a started gRPC writer against a fake server.
func grpcLifecycleWriter(t *testing.T, handler http.HandlerFunc, opts ...Option) (*Writer, *fakeGRPCServer) {
	t.Helper()
	f := newFakeGRPCServer(t, handler)
	w, err := NewGRPCWriter(f.srv.URL, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w, f
}

func TestGRPCSyncBarrierFlushesAll(t *testing.T) {
	w, f := grpcLifecycleWriter(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) },
		WithBatchSize(2), WithFlushInterval(time.Hour)) // ticker out of the picture
	for range 5 {
		_, _ = w.Write([]byte("r"))
	}
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.GreaterOrEqual(t, len(f.received()), 3) // 5 records at batch size 2 → ≥3 exports
}

func TestGRPCRetryInfoDelayHonored(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	w, f := grpcLifecycleWriter(t, func(rw http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			det := base64.RawStdEncoding.EncodeToString(statusBin(14, "throttled", 50*time.Millisecond, true))
			trailersOnlyError(rw, 14, "throttled", det)

			return
		}
		okResponse(rw, nil)
	}, WithRetry(RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	// Initial backoff is 1h — only the 50ms RetryInfo delay can make the
	// retry happen within the test budget.
	start := time.Now()
	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.Less(t, time.Since(start), 10*time.Second)
	require.Len(t, f.received(), 2) // first attempt + post-RetryInfo retry
}

func TestGRPCCloseDrainsSingleAttempt(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	w, _ := grpcLifecycleWriter(t, func(rw http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		trailersOnlyError(rw, 14, "down", "") // retryable — but Close drain is single-attempt
	}, WithRetry(RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.LessOrEqual(t, calls, 2) // flush-tick attempt at most once + drain attempt
}
```

  (Adjust imports as needed: `bytes`, `encoding/base64`, `net/http`, `sync`, `time`.)

- [ ] **Step 7.2:** Run: `cd otlp && go test -race -run 'TestGRPCSync|TestGRPCRetryInfo|TestGRPCClose' ./...`
  Expected: PASS directly if Tasks 5–6 are correct — these tests exist to pin behavior,
  so a failure here means a REAL bug in the transport/retry integration; debug it, do
  not weaken the test.

- [ ] **Step 7.3: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/writer_lifecycle_test.go
git commit -m "test(otlp): gRPC writer lifecycle parity — Sync barrier, RetryInfo delay, Close drain"
```

---

### Task 8: Conformance — interop against real grpc-go

**Files:**
- Modify: `otlp/internal/conformance/go.mod` (add direct requires)
- Create: `otlp/internal/conformance/grpc_conformance_test.go`

grpc-go and the official stubs are ALREADY in this module's graph (indirect via
`go.opentelemetry.io/proto/otlp`); they become direct requires. This module never
touches `otlp/go.mod` — the dependency quarantine holds.

- [ ] **Step 8.1:** In `otlp/internal/conformance/`, run:
  `GOWORK=off go get google.golang.org/grpc@latest && GOWORK=off go mod tidy`
  Expected: `google.golang.org/grpc` moves to the direct require block.

- [ ] **Step 8.2: Write the tests** — create
  `otlp/internal/conformance/grpc_conformance_test.go`:

```go
package conformance

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"

	"github.com/arloliu/zapwire/otlp"
)

// logsSink is a real grpc-go LogsService capturing requests; respond is
// consulted per call (nil → empty success).
type logsSink struct {
	collogspb.UnimplementedLogsServiceServer
	mu      sync.Mutex
	reqs    []*collogspb.ExportLogsServiceRequest
	respond func(call int) (*collogspb.ExportLogsServiceResponse, error)
}

func (s *logsSink) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	call := len(s.reqs)
	respond := s.respond
	s.mu.Unlock()
	if respond != nil {
		return respond(call)
	}

	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s *logsSink) requests() []*collogspb.ExportLogsServiceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*collogspb.ExportLogsServiceRequest(nil), s.reqs...)
}

func startGRPCSink(t *testing.T, sink *logsSink) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, sink)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String() // bare host:port → exercise WithInsecure
}

func TestGRPCInteropRoundTrip(t *testing.T) {
	sink := &logsSink{}
	addr := startGRPCSink(t, sink)
	core, w, err := otlp.NewGRPCCore(addr, zapcore.InfoLevel,
		otlp.WithInsecure(),
		otlp.WithServiceName("conformance"),
	)
	require.NoError(t, err)
	logger := zap.New(core)

	sc, ctx := spanCtx(t)
	_ = sc
	logger.Info("interop", zap.String("k", "v"), zap.Int64("n", -7), zap.Any("context", ctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())

	reqs := sink.requests()
	require.Len(t, reqs, 1)
	rl := reqs[0].GetResourceLogs()
	require.Len(t, rl, 1)
	attrs := rl[0].GetResource().GetAttributes()
	foundSvc := false
	for _, kv := range attrs {
		if kv.GetKey() == "service.name" {
			require.Equal(t, "conformance", kv.GetValue().GetStringValue())
			foundSvc = true
		}
	}
	require.True(t, foundSvc)
	recs := rl[0].GetScopeLogs()[0].GetLogRecords()
	require.Len(t, recs, 1)
	require.Equal(t, "interop", recs[0].GetBody().GetStringValue())
	require.NotZero(t, recs[0].GetTraceId(), "trace correlation must survive the gRPC transport")
}

func TestGRPCInteropGzip(t *testing.T) {
	sink := &logsSink{}
	addr := startGRPCSink(t, sink)
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithCompression(otlp.Gzip))
	require.NoError(t, err)
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "gz"}, nil)
	require.NoError(t, err)
	_, _ = w.Write(buf.Bytes())
	buf.Free()
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	reqs := sink.requests()
	require.Len(t, reqs, 1) // grpc-go transparently gunzipped our per-message compression
	require.Equal(t, "gz", reqs[0].GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue())
}

func TestGRPCInteropPartialSuccess(t *testing.T) {
	sink := &logsSink{respond: func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return &collogspb.ExportLogsServiceResponse{
			PartialSuccess: &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "one rejected"},
		}, nil
	}}
	addr := startGRPCSink(t, sink)
	var mu sync.Mutex
	var events []error
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithErrorHandler(func(e error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'}) // minimal LogRecord{body:"hi"}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, events, 1)
	var ee *otlp.ExportError
	require.ErrorAs(t, events[0], &ee)
	require.EqualValues(t, 1, ee.Rejected)
	require.Equal(t, "one rejected", ee.Message)
}

func TestGRPCInteropRetryInfo(t *testing.T) {
	sink := &logsSink{respond: func(call int) (*collogspb.ExportLogsServiceResponse, error) {
		if call == 1 {
			st := status.New(codes.Unavailable, "throttled")
			st, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(50 * time.Millisecond)})
			if err != nil {
				return nil, err
			}

			return nil, st.Err()
		}

		return &collogspb.ExportLogsServiceResponse{}, nil
	}}
	addr := startGRPCSink(t, sink)
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(),
		otlp.WithRetry(otlp.RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	require.NoError(t, err)
	start := time.Now()
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'})
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	require.Less(t, time.Since(start), 10*time.Second, "RetryInfo (50ms) must beat the 1h backoff")
	require.Len(t, sink.requests(), 2)
}

func TestGRPCInteropInvalidArgumentDrops(t *testing.T) {
	sink := &logsSink{respond: func(int) (*collogspb.ExportLogsServiceResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad payload")
	}}
	addr := startGRPCSink(t, sink)
	var mu sync.Mutex
	var events []error
	w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(), otlp.WithErrorHandler(func(e error) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x2a, 0x04, 0x0a, 0x02, 'h', 'i'})
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	var ee *otlp.ExportError
	require.ErrorAs(t, events[0], &ee)
	require.Equal(t, 3, ee.GRPCStatus)
	require.False(t, ee.Retryable)
	require.Contains(t, ee.Message, "bad payload")
}
```

- [ ] **Step 8.3:** Run: `cd otlp/internal/conformance && GOWORK=off go test -race -run TestGRPCInterop ./...`
  Expected: PASS — hand-rolled client ↔ real grpc-go server, full fidelity.
  (`errdetails`/`durationpb` come from `google.golang.org/genproto` /
  `google.golang.org/protobuf`, both already in the graph; `go mod tidy` again if the
  direct-require set changed.)

- [ ] **Step 8.4: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/internal/conformance/go.mod otlp/internal/conformance/go.sum otlp/internal/conformance/grpc_conformance_test.go
git commit -m "test(otlp): gRPC conformance — hand-rolled client interop against real grpc-go server"
```

---

### Task 9: Benchmark — hand-rolled vs grpc-go

**Files:**
- Create: `otlp/internal/conformance/grpc_bench_test.go`

Both sides export to the same in-process grpc-go sink. Layer caveat (state it in the
bench comments AND the PR): the zapwire side measures the full pipeline
(Write→queue→batch→assemble→transport); the grpc-go side measures
proto-marshal+Export. That asymmetry favors grpc-go, so a competitive zapwire number
is conservative evidence. Acceptance (design §9.3): within ~2× of grpc-go per-export
CPU at batch 512; if badly worse, STOP and re-open design §2 before release.

- [ ] **Step 9.1: Write the benchmarks** — create
  `otlp/internal/conformance/grpc_bench_test.go`:

```go
package conformance

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	respb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/arloliu/zapwire/otlp"
)

// benchSink is a minimal always-OK LogsService.
type benchSink struct{ collogspb.UnimplementedLogsServiceServer }

func (benchSink) Export(context.Context, *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func startBenchSink(b *testing.B) string {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, benchSink{})
	go func() { _ = srv.Serve(lis) }()
	b.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// encodedRecord produces one realistic encoded LogRecord via the zapwire encoder.
func encodedRecord(b *testing.B) []byte {
	b.Helper()
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "benchmark message"},
		[]zapcore.Field{{Key: "k", Type: zapcore.StringType, String: "v"}, {Key: "n", Type: zapcore.Int64Type, Integer: 12345}})
	if err != nil {
		b.Fatal(err)
	}
	defer buf.Free()

	return append([]byte(nil), buf.Bytes()...)
}

// BenchmarkGRPCExportZapwire: full zapwire pipeline per iteration —
// batchSize records written + Sync (one Export per iteration at steady state).
func BenchmarkGRPCExportZapwire(b *testing.B) {
	for _, batch := range []int{1, 64, 512} {
		b.Run(fmtBatch(batch), func(b *testing.B) {
			addr := startBenchSink(b)
			w, err := otlp.NewGRPCWriter(addr, otlp.WithInsecure(),
				otlp.WithBatchSize(batch), otlp.WithQueueSize(batch*2), otlp.WithFlushInterval(time.Hour))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			rec := encodedRecord(b)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for range batch {
					_, _ = w.Write(rec)
				}
				_ = w.Sync()
			}
			b.StopTimer()
			if d := w.DroppedLogs(); d != 0 {
				b.Fatalf("dropped %d records", d)
			}
		})
	}
}

// BenchmarkGRPCExportGRPCGo: conventional grpc-go client — proto marshal of
// an equivalent batch + unary Export per iteration.
func BenchmarkGRPCExportGRPCGo(b *testing.B) {
	for _, batch := range []int{1, 64, 512} {
		b.Run(fmtBatch(batch), func(b *testing.B) {
			addr := startBenchSink(b)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			client := collogspb.NewLogsServiceClient(conn)
			recs := make([]*logspb.LogRecord, batch)
			for i := range recs {
				recs[i] = &logspb.LogRecord{
					TimeUnixNano:   uint64(time.Unix(7, 42).UnixNano()),
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "benchmark message"}},
					Attributes: []*commonpb.KeyValue{
						{Key: "k", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "v"}}},
						{Key: "n", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 12345}}},
					},
				}
			}
			tmpl := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
				Resource:  &respb.Resource{Attributes: []*commonpb.KeyValue{{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "bench"}}}}},
				ScopeLogs: []*logspb.ScopeLogs{{LogRecords: recs}},
			}}}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := client.Export(ctx, tmpl); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func fmtBatch(n int) string {
	switch n {
	case 1:
		return "batch1"
	case 64:
		return "batch64"
	default:
		return "batch512"
	}
}
```

- [ ] **Step 9.2:** Run:
  `cd otlp/internal/conformance && GOWORK=off go test -run '^$' -bench BenchmarkGRPCExport -benchmem -benchtime=2s ./... | tee /tmp/grpc-bench.txt`
  Expected: both benchmarks complete; capture the table.

- [ ] **Step 9.3: Record results.** Paste the numbers into this plan file under a new
  `## Benchmark results` section at the bottom, with the layer caveat. Check
  acceptance: zapwire ns/op within ~2× of grpc-go at batch512. If it fails
  acceptance, STOP — report to the user before any further task.

- [ ] **Step 9.4: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/internal/conformance/grpc_bench_test.go docs/plans/zapwire-v2-otlp-grpc.md
git commit -m "bench(otlp): hand-rolled gRPC transport vs grpc-go exporter"
```

---

### Task 10: otelcol gRPC integration test

**Files:**
- Modify: `otlp/collector_integration_test.go`

Mirrors `TestCollectorEndToEnd` with a gRPC receiver. The existing Makefile target
already matches (`-run TestCollectorEndToEnd` prefix-matches the new name) — no
Makefile change.

- [ ] **Step 10.1: Write the test** — append to `otlp/collector_integration_test.go`,
  reusing its existing helpers (`freePort`, the wait/assert utilities — read the
  HTTP test first and mirror its structure exactly, only the config and constructor
  differ):

```go
// TestCollectorEndToEndGRPC ships through NewGRPCWriter to a real otel
// collector OTLP/gRPC receiver (h2c) and asserts the file-exporter output —
// the same oracle as the HTTP variant.
func TestCollectorEndToEndGRPC(t *testing.T) {
	bin := os.Getenv("OTELCOL_BIN")
	if bin == "" {
		bin = "/usr/local/bin/otelcol"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("otel-collector binary not found at %s", bin)
	}

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.json")
	port := freePort(t)
	cfg := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:%d
exporters:
  file:
    path: %s
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
`, port, outFile)
	// ... start the collector exactly as TestCollectorEndToEnd does ...

	core, w, err := NewGRPCCore(fmt.Sprintf("127.0.0.1:%d", port), zapcore.InfoLevel,
		WithInsecure(),
		WithServiceName("itest-grpc"),
	)
	require.NoError(t, err)
	logger := zap.New(core)
	logger.Info("grpc end to end", zap.String("transport", "grpc"))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// ... poll outFile and assert body/attributes exactly as the HTTP test does,
	// expecting body "grpc end to end" and attribute transport="grpc" ...
}
```

  The elided sections MUST be copied from `TestCollectorEndToEnd` (collector launch,
  readiness wait, JSON polling/assertions) — same timeouts, same teardown. Keep both
  tests structurally parallel so future edits touch them together.

- [ ] **Step 10.2:** Run: `make integration-otel` (requires otelcol; if absent the test
  Skips — note that in the task report).
  Expected: PASS (or SKIP with the binary missing).

- [ ] **Step 10.3:** Run: `cd otlp && go vet -tags otelcollector ./...`
  Expected: clean — the tagged file must compile even where the binary is absent.

- [ ] **Step 10.4: Commit:**

```bash
go fix ./otlp/... && make lint && make test
git add otlp/collector_integration_test.go
git commit -m "test(otlp): opt-in otel-collector OTLP/gRPC end-to-end test"
```

---

### Task 11: Docs & example

**Files:**
- Modify: `docs/guide.md` (OTLP section + options reference)
- Modify: `README.md` (feature/transport matrix row)
- Create: `examples/otlp-grpc/main.go`

- [ ] **Step 11.1: Guide.** In `docs/guide.md`, locate the OTLP section (`grep -n
  "OTLP" docs/guide.md`). Add a "Choosing a protocol" subsection containing:
  (a) the table — `http/protobuf` (4318, default, spec-recommended) vs `grpc` (4317,
  for collectors/agents standardized on gRPC); (b) a `NewGRPCWriter` snippet showing
  the three endpoint forms and `WithInsecure`/`WithTLSConfig`; (c) the
  `ProtocolFromEnv` dispatch snippet from the design §4.3; (d) the benchmark table
  from Task 9 with one sentence of context; (e) a note that `WithHTTPClient` is a
  no-op on gRPC and proxies are unsupported there. Add `WithInsecure`/`WithTLSConfig`
  rows to the options-reference table (guide.md §"Options reference").

- [ ] **Step 11.2: README.** Update the transport/feature matrix row for OTLP from
  HTTP-only to `gRPC + HTTP` (find it via `grep -n -i "otlp" README.md`).

- [ ] **Step 11.3: Example.** Create `examples/otlp-grpc/main.go` modeled on the
  existing `examples/otlp-trace-correlation/main.go` (same module; the examples
  go.mod already replaces zapwire locally — verify with `grep -n replace
  examples/go.mod`, and add a replace for `./otlp` if only the root is mapped):

```go
// Command otlp-grpc ships zap logs to an OTLP/gRPC collector (port 4317)
// using zapwire/otlp's hand-rolled stdlib gRPC transport, with env-driven
// protocol dispatch as the fallback pattern.
package main

import (
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
	defer func() { _ = w.Close() }()

	logger := zap.New(core)
	defer func() { _ = logger.Sync() }()

	logger.Info("hello over gRPC",
		zap.String("transport", "grpc"),
		zap.Int("port", 4317),
	)
}
```

- [ ] **Step 11.4:** Run: `make examples`
  Expected: builds and vets clean.

- [ ] **Step 11.5: Commit:**

```bash
go fix ./otlp/... && make lint && make test && make examples
git add docs/guide.md README.md examples/otlp-grpc/main.go examples/go.mod examples/go.sum
git commit -m "docs(otlp): gRPC protocol guide section, README matrix, runnable otlp-grpc example"
```

---

### Task 12: Final gate & release prep

- [ ] **Step 12.1:** Full local gate: `make ci`
  Expected: lint `0 issues.`, vet clean, all tests PASS, coverage prints, examples build.
- [ ] **Step 12.2:** Conformance + bench rerun:
  `cd otlp/internal/conformance && GOWORK=off go test -race ./... && GOWORK=off go test -run '^$' -bench . -benchmem ./...`
  Expected: PASS.
- [ ] **Step 12.3:** Integration (binaries permitting): `make integration-otel`
  Expected: both `TestCollectorEndToEnd` and `TestCollectorEndToEndGRPC` PASS.
- [ ] **Step 12.4:** Module hygiene: `cd otlp && GOWORK=off go mod tidy && git diff --exit-code go.mod go.sum`
  Expected: NO diff — the otlp module gained zero dependencies (the load-bearing
  promise of this whole design). If there IS a diff, something violated the
  dependency policy: STOP and report.
- [ ] **Step 12.5:** Update the design doc status line (`draft for review` →
  `implemented (plan: docs/plans/zapwire-v2-otlp-grpc.md)`) and this plan's Status
  header; commit:

```bash
git add docs/design/2026-06-12-otlp-grpc-design.md docs/plans/zapwire-v2-otlp-grpc.md
git commit -m "docs(otlp): mark gRPC design implemented"
```

- [ ] **Step 12.6:** Report to the user: summary of commits, benchmark table,
  remaining release steps (PR off `feature/otlp-grpc`, tag `otlp/v0.2.0` after merge,
  then the otx zaplog grpc-rejection lift as a separate otx PR).

---

## Benchmark results

Measured on Linux/amd64 (AMD Ryzen 9 9950X3D, 32 threads), `go test -bench BenchmarkGRPCExport -benchmem -benchtime=2s`.

**Layer caveat:** the zapwire side measures the full pipeline (Write→queue→batch→assemble→transport); the grpc-go side measures proto-marshal+Export only. That asymmetry structurally favors grpc-go, so a competitive (or better) zapwire number is conservative evidence.

| Benchmark | batch | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| BenchmarkGRPCExportZapwire | 1 | 35,299 | 17,313 | 192 |
| BenchmarkGRPCExportZapwire | 64 | 101,914 | 80,050 | 1,080 |
| BenchmarkGRPCExportZapwire | 512 | 409,710 | 823,723 | 7,371 |
| BenchmarkGRPCExportGRPCGo | 1 | 25,940 | 11,148 | 168 |
| BenchmarkGRPCExportGRPCGo | 64 | 95,007 | 58,297 | 930 |
| BenchmarkGRPCExportGRPCGo | 512 | 515,686 | 386,021 | 6,321 |

**Acceptance verdict (batch512):** zapwire 409,710 ns/op vs grpc-go 515,686 ns/op — zapwire is ~0.79× grpc-go (i.e., faster), well within the ~2× budget. ✓ Accepted.



