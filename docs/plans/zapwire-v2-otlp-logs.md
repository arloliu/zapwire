# OTLP logs subpackage — Implementation Plan (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 3) · **Date:** 2026-06-11 ·
**Branch:** `worktree-otel-otlp` · **Spec:** `docs/design/2026-06-11-otlp-logs-design.md`

**Review history:** design spec reached codex plan-review consensus (pass 3 + scoped pass 4
on the injector addendum, no open P0/P1). This plan: codex pass 1
(`tmp/zapwire-v2-otlp-logs_pass1_review.md`) raised 4×P0 (inline-marshaler rollback not
realized — `Field.AddTo` dispatches `InlineMarshalerType` straight to `MarshalLogObject`,
so the snapshot must wrap the dispatch via `applyField`; a Close/Write admission race that
could strand an enqueue after the final drain — fixed with the root writer's
admission-barrier discipline; unused `fmt` import; broken Task 5 gate command) + 3×P1
(missing lifecycle test rows; `newTestWriter` option-order bug; commit gates drifting from
the repo `go fix → make lint → make test` contract) + 1×P2 (stale `assemble` signature in
the contract table) — all resolved (see "Plan-review pass-1 resolutions"). Pass 2
(`tmp/zapwire-v2-otlp-logs_pass2_review.md`) audited pass-1 fixes and raised 2×P0
(stock-core `With(zap.Inline(failing))` is uninterceptable — `ioCore.With` dispatches inline
marshalers straight into the encoder, so the limitation is now documented/pinned, matching
zap's own encoders; the oversized-record error handler ran under `admit.RLock` and could
self-deadlock against `Close` — `errFn` now fires after release) + 1×P1 (two lifecycle test
oracles didn't prove their design rows — strengthened with per-request record-count
assertions and a deterministic blocked-first-request choreography) — all resolved (see
"Plan-review pass-2 resolutions"). Pass 3 (`tmp/zapwire-v2-otlp-logs_pass3_review.md`)
audited all pass-2 fixes as genuinely resolved (verified against zap source), walked the
`Sync`/`Close` byte-cut choreography step by step finding **no remaining scheduling hole**,
and raised exactly one P0 — a missing `"io"` import in the Task 9 test block — fixed
in-place per the prescribed one-line change. **Converged: no open findings.**

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Execute tasks **in order** (0 → 12): each task
> leaves every module compiling with all prior tests green.

**Goal:** Add the `otlp/` module (own go.mod) that ships zap logs as native OTLP/HTTP binary
protobuf to any OTLP receiver, with logs↔traces correlation populated from `context.Context`
as LogRecord proto fields.

**Architecture:** A hand-rolled proto3 wire encoder (no grpc/protobuf/OTel-SDK deps) behind
three layers: a frame-stack `zapcore.Encoder` emitting bare `LogRecord` bytes per entry; a
thin custom `zapcore.Core` whose `With` pre-scans raw fields for trace context (the only way
to intercept `zap.Any("context", ctx)`, which zap classifies as `StringerType`); and an
async-only HTTP exporter (`otlp.Writer`) with bounded queue, count/byte/interval batching,
OTLP retry semantics, and partial-success accounting. Conformance against the official
`go.opentelemetry.io/proto/otlp` stubs lives in a quarantined test-only module.

**Tech Stack:** Go 1.25, `go.uber.org/zap` (zapcore + buffer), `go.opentelemetry.io/otel/trace`
(stable v1.x — the **only** new runtime dep, quarantined in `otlp/`'s own go.mod),
`net/http`, `github.com/stretchr/testify` (tests). Conformance test module additionally uses
`go.opentelemetry.io/proto/otlp` + `google.golang.org/protobuf` (never visible to `otlp/`
consumers).

**Design source:** `docs/design/2026-06-11-otlp-logs-design.md` (consensus). Read it first —
section refs below (e.g. §3.4) point into it.

**Before EVERY commit (repo contract, `.agents/rules/600-go-after-write.md`):**

```bash
(cd otlp && go fix ./...)   # touched packages
make lint
make test                    # race-enabled — keep -race when adding module loops
```

The per-task `go test` commands are fast red/green iteration checks only; the trio above is
the commit gate.

---

## Dependency policy

| Module | Allowed imports | This plan adds |
|---|---|---|
| root `zapwire` | unchanged (stdlib + zap) | **nothing** (only `go.work`, docs, Makefile/CI) |
| `otlp/` (new module) | stdlib + `go.uber.org/zap{,core,buffer}` + `go.opentelemetry.io/otel/trace` | everything in this plan |
| `otlp/internal/conformance/` (new test-only module) | the above + `go.opentelemetry.io/proto/otlp` + `google.golang.org/protobuf` | golden round-trip tests |

`otlp/` must **not** import `github.com/arloliu/zapwire` (design §2.1). Gate (Task 12):
`go list -deps ./...` inside `otlp/` contains no `grpc`, no `google.golang.org/protobuf`, no
`github.com/arloliu/zapwire`.

## File structure (created by this plan)

| File | Status | Responsibility |
|---|---|---|
| `go.work` | **create** (repo root) | workspace: `.`, `./otlp`, `./otlp/internal/conformance` |
| `otlp/go.mod` | **create** | module `github.com/arloliu/zapwire/otlp` |
| `otlp/proto.go` | **create** | proto3 wire primitives: varint/tag/fixed/len-delimited appenders, size helpers, partial-success response decoder |
| `otlp/options.go` | **create** | `SeverityNumber`, `DropPolicy`, `Compression`, `RetryConfig`, `ExportError`, `options`/`Option`/`With*`, defaults + `normalize`, `EndpointFromEnv`, `resolveEndpoint`, `ErrNoEndpoint` |
| `otlp/tracectx.go` | **create** | `SpanContext`, `InjectTraceFields`, `InjectTraceKVs`, `TraceCorrelationFields`, `spanContextFromField`, nil guards |
| `otlp/attr.go` | **create** | frame-stack proto `ObjectEncoder` (`encState`): KeyValue/AnyValue encoding, namespaces, arrays, objects, snapshot/rollback |
| `otlp/encoder.go` | **create** | `encoder` (`zapcore.Encoder`): `Clone`, `EncodeEntry`, severity mapping, entry-metadata attributes, per-call trace scan, With-stash |
| `otlp/core.go` | **create** | custom `zapcore.Core` (With pre-scan), `NewCore`, `NewEncoder` |
| `otlp/envelope.go` | **create** | precomputed Resource/Scope blobs, `ExportLogsServiceRequest` batch assembly, exact size arithmetic |
| `otlp/writer.go` | **create** | `Writer`: queue, flush loop, count/byte/interval batching, HTTP attempt, retry/backoff/Retry-After, partial success, `Sync`/`Close`, `NewWriter` |
| `otlp/doc.go` | **create** | package docs: compat matrix (§2.2), tee caveat, foundation/app-layer boundary |
| `otlp/*_test.go` | **create** | unit tests per area (named per task) |
| `otlp/collector_integration_test.go` | **create** | opt-in real-collector test behind `//go:build otelcollector` |
| `otlp/internal/conformance/go.mod`, `conformance_test.go` | **create** | byte-identical round-trip vs official stubs |
| `Makefile` | **modify** | multi-module `test`/`lint`/`bench` loops + `integration-otel` target |
| `.github/workflows/ci.yml` | **modify** | matrix entries for the two new modules |
| `README.md`, `docs/guide.md` | **modify** | add `otlp` row to the processors table |

## Shared type contract (used consistently by every task)

```go
// options.go
type SeverityNumber int32
const (
    SeverityUnspecified SeverityNumber = 0
    SeverityTrace       SeverityNumber = 1
    SeverityDebug       SeverityNumber = 5
    SeverityInfo        SeverityNumber = 9
    SeverityWarn        SeverityNumber = 13
    SeverityError       SeverityNumber = 17
    SeverityFatal       SeverityNumber = 21
    SeverityFatal2      SeverityNumber = 22
    SeverityFatal3      SeverityNumber = 23
    maxSeverity         SeverityNumber = 24
)
type DropPolicy uint8
const ( DropNewest DropPolicy = iota; DropOldest )
type Compression uint8
const ( NoCompression Compression = iota; Gzip )
type RetryConfig struct{ Initial, MaxInterval, MaxElapsed time.Duration }
type ExportError struct {
    StatusCode int    // 0 for transport errors
    Retryable  bool   // classification at the time it was surfaced
    Rejected   int64  // partial-success rejected_log_records
    Warning    bool   // partial-success warning (Rejected == 0, message != "")
    Message    string // partial-success error_message or response excerpt
    Err        error  // wrapped transport/encode error, may be nil
}
func (e *ExportError) Error() string
func (e *ExportError) Unwrap() error
var ErrNoEndpoint = errors.New("otlp: no endpoint")

// tracectx.go
func SpanContext(ctx context.Context) zap.Field            // key "span_context", ReflectType
func InjectTraceFields(ctx context.Context, fields ...zap.Field) []zap.Field
func InjectTraceKVs(ctx context.Context, kvs ...any) []any
func TraceCorrelationFields(ctx context.Context) []zap.Field
func spanContextFromField(f zapcore.Field) (trace.SpanContext, bool) // (sc, isTraceField)
func spanContextFromValue(val any) (trace.SpanContext, bool)         // value-level form

// attr.go
type frameKind uint8
const ( frameRoot frameKind = iota; frameKVList; frameArray )
type frame struct{ kind frameKind; nsKey string; buf []byte } // buf: tagged entries, pooled
type encState struct {
    stack   []frame
    scratch []byte
    // trace sink — AddReflected stores resolved span contexts here instead of
    // encoding them (nil sink → encode normally). Points at the owning
    // encoder's stash during With (fresh clone) and at EncodeEntry locals
    // during entry encoding.
    scSink    *trace.SpanContext
    scSinkSet *bool
} // implements zapcore.ObjectEncoder; arrays via the arrayEnc wrapper
type arrayEnc struct{ s *encState } // implements zapcore.ArrayEncoder
type snapshot struct{ depth, bufLen int }

// encoder.go
type encConfig struct {
    severityOf    func(zapcore.Level) SeverityNumber
    callerAttrs   bool
    loggerNameKey string
}
type encoder struct {
    encState               // embedded: persistent With-field state + ObjectEncoder surface
    cfg   encConfig
    sc    trace.SpanContext // With-stash (§3.4); written only on fresh clones (via scSink)
    scSet bool
}
func NewEncoder(opts ...Option) zapcore.Encoder // returns *encoder

// core.go
type core struct {
    zapcore.LevelEnabler
    enc *encoder
    out zapcore.WriteSyncer
}
func NewCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error)

// envelope.go
type envelope struct {
    resourceBlob []byte // Resource message bytes (no outer tag)
    scopeBlob    []byte // InstrumentationScope message bytes (no outer tag)
}
func newEnvelope(o options) *envelope
func (e *envelope) sizeFor(totalTaggedRecordBytes int) int  // exact request size
func (e *envelope) recordCost(recLen int) int               // tag+varint+len cost of one record
func (e *envelope) assemble(dst []byte, records [][]byte) []byte // appends full request bytes

// encoder.go (also used by core.go With and envelope.go resource fields)
func applyField(work *encState, f zapcore.Field) // transactional dispatch incl. InlineMarshalerType

// writer.go
type Writer struct{ /* §Task 9 */ }
func NewWriter(endpoint string, opts ...Option) (*Writer, error)
func (w *Writer) Write(p []byte) (int, error)
func (w *Writer) Sync() error
func (w *Writer) Close() error
func (w *Writer) DroppedLogs() uint64
```

**Naming discipline:** these exact identifiers are referenced across tasks. If you rename
anything, grep the plan and update every occurrence.

## Proto wire cheat sheet (used by Tasks 1, 4, 5, 7)

Wire types: varint=0, fixed64=1 (little-endian), len-delimited=2, fixed32=5 (LE).
`tag = fieldNum<<3 | wireType`. All tags below fit one byte.

| Message | Field | Tag byte |
|---|---|---|
| LogRecord | time_unix_nano=1 fixed64 | `0x09` |
| LogRecord | severity_number=2 varint | `0x10` |
| LogRecord | severity_text=3 len | `0x1a` |
| LogRecord | body=5 len(AnyValue) | `0x2a` |
| LogRecord | attributes=6 len(KeyValue), repeated | `0x32` |
| LogRecord | flags=8 fixed32 | `0x45` |
| LogRecord | trace_id=9 len(16) | `0x4a` |
| LogRecord | span_id=10 len(8) | `0x52` |
| LogRecord | observed_time_unix_nano=11 fixed64 | `0x59` |
| AnyValue | string_value=1 | `0x0a` |
| AnyValue | bool_value=2 varint | `0x10` |
| AnyValue | int_value=3 varint | `0x18` |
| AnyValue | double_value=4 fixed64 | `0x21` |
| AnyValue | array_value=5 len(ArrayValue) | `0x2a` |
| AnyValue | kvlist_value=6 len(KeyValueList) | `0x32` |
| AnyValue | bytes_value=7 len | `0x3a` |
| KeyValue | key=1 len | `0x0a` |
| KeyValue | value=2 len(AnyValue) | `0x12` |
| ArrayValue / KeyValueList | values=1 len, repeated | `0x0a` |
| Resource | attributes=1 len(KeyValue), repeated | `0x0a` |
| InstrumentationScope | name=1 / version=2 | `0x0a` / `0x12` |
| ResourceLogs | resource=1 / scope_logs=2 | `0x0a` / `0x12` |
| ScopeLogs | scope=1 / log_records=2 | `0x0a` / `0x12` |
| ExportLogsServiceRequest | resource_logs=1 | `0x0a` |
| ExportLogsPartialSuccess | rejected_log_records=1 varint / error_message=2 len | `0x08` / `0x12` |
| ExportLogsServiceResponse | partial_success=1 len | `0x0a` |

**Byte-identity rules** (so `proto.Marshal(proto.Unmarshal(ours)) == ours`, the conformance
gate): emit fields in **ascending field-number order**; use **minimal varints**; **omit**
zero-valued scalar fields (`severity_number == 0`, empty `severity_text`,
`time_unix_nano == 0` — guard the epoch-exact edge, and `flags == 0` — an **unsampled**
span emits `trace_id`/`span_id` but NO flags field, because `proto.Marshal` omits a zero
fixed32); **always emit `body`** (a set oneof marshals even when the string is empty);
negative `int_value` encodes as 10-byte two's-complement varint.

**Attribute order within a record (pinned, §Task 5):** persistent `With` fields (already in
the root frame, in `With` order) → entry-metadata attributes (appended to the **root** frame
so an open `With` namespace cannot capture them: logger name, `code.function.name`,
`code.file.path`, `code.line.number`, `code.stacktrace`) → per-call fields (into the current
frame, namespaces respected).

---

### Task 0: Module scaffolding (go.mod, go.work, CI/Makefile wiring)

**Files:**
- Create: `otlp/go.mod`, `otlp/doc.go` (stub), `go.work`
- Modify: `Makefile`, `.github/workflows/ci.yml`

- [ ] **Step 1: Create the module and workspace**

```bash
mkdir -p otlp
cat > otlp/go.mod <<'EOF'
module github.com/arloliu/zapwire/otlp

go 1.25.0

require (
	go.opentelemetry.io/otel/trace v1.44.0
	go.uber.org/zap v1.28.0
)
EOF
cat > otlp/doc.go <<'EOF'
// Package otlp ships zap logs as native OTLP/HTTP binary protobuf
// (ExportLogsServiceRequest) with logs-to-traces correlation populated from
// context.Context as LogRecord proto fields. See the package README and
// docs/design/2026-06-11-otlp-logs-design.md in the repository for the full
// design. The package is its own Go module so importing it never adds
// OpenTelemetry dependencies to plain zapwire users.
package otlp
EOF
cat > go.work <<'EOF'
go 1.25.0

use (
	.
	./otlp
)
EOF
cd otlp && go mod tidy && cd ..
```

Note: `./otlp/internal/conformance` joins `go.work` in Task 11 (it does not exist yet — a
`use` entry for a missing directory breaks every `go` command in the workspace).

- [ ] **Step 2: Verify both modules build**

Run: `go build ./... && (cd otlp && go build ./... && go vet ./...)`
Expected: success, no output. `go.sum` created under `otlp/` containing only
`go.opentelemetry.io/otel*` + zap entries (no grpc, no protobuf).

- [ ] **Step 3: Wire Makefile multi-module loops**

Read `Makefile` first. Extend each relevant target to iterate modules — keep existing
recipes intact and add the `otlp` module. The shape (adapt to the existing Makefile style):

```makefile
MODULES := . ./otlp

test: ## Run unit tests (race-enabled) for all modules
	@for m in $(MODULES); do (cd $$m && go test -race ./...) || exit 1; done

lint: ## Lint all modules
	@for m in $(MODULES); do (cd $$m && golangci-lint run ./...) || exit 1; done
```

**Preserve the existing target's flags** — the current `make test` is the race-enabled unit
gate (`Makefile:17-19`); the module loop must not silently drop `-race` or any other flag
the existing recipe carries.

- [ ] **Step 4: Wire CI**

Read `.github/workflows/ci.yml` and mirror whatever job runs root `go test` / lint for the
`otlp` directory (a `strategy.matrix.module: [".", "otlp"]` with
`working-directory: ${{ matrix.module }}` is the smallest change if the workflow shape
allows; otherwise duplicate the steps with `working-directory: otlp`).

- [ ] **Step 5: Run gates and commit**

Run: `make lint && make test`
Expected: both pass (otlp module has no tests yet — `go test ./...` reports `ok` with no
test files warning, which is fine).

```bash
git add otlp/go.mod otlp/go.sum otlp/doc.go go.work Makefile .github/workflows/ci.yml
git commit -m "feat(otlp): scaffold otlp module, workspace, CI wiring"
```

---

### Task 1: Proto wire primitives (`otlp/proto.go`)

Pure append-style helpers — the vocabulary every later task speaks. No zap imports.

**Files:**
- Create: `otlp/proto.go`
- Test: `otlp/proto_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendUvarint(t *testing.T) {
	cases := []struct {
		v    uint64
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xac, 0x02}}, // protobuf docs example
		{1<<64 - 1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}},
	}
	for _, c := range cases {
		require.Equal(t, c.want, appendUvarint(nil, c.v), "value %d", c.v)
		require.Equal(t, len(c.want), uvarintLen(c.v), "len %d", c.v)
	}
}

func TestAppendVarintNegative(t *testing.T) {
	// int64(-1) as two's-complement uint64 → ten 0xff-leading bytes.
	got := appendVarint(nil, -1)
	require.Equal(t, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, got)
}

func TestAppendTaggedHelpers(t *testing.T) {
	// severity_text=3 ("info"): tag 0x1a, len 4, bytes.
	require.Equal(t, []byte{0x1a, 0x04, 'i', 'n', 'f', 'o'},
		appendTaggedString(nil, 0x1a, "info"))
	// time_unix_nano=1 fixed64 LE.
	require.Equal(t, []byte{0x09, 0x01, 0, 0, 0, 0, 0, 0, 0},
		appendTaggedFixed64(nil, 0x09, 1))
	// flags=8 fixed32 LE.
	require.Equal(t, []byte{0x45, 0x01, 0, 0, 0},
		appendTaggedFixed32(nil, 0x45, 1))
	// bytes payload.
	require.Equal(t, []byte{0x4a, 0x02, 0xde, 0xad},
		appendTaggedBytes(nil, 0x4a, []byte{0xde, 0xad}))
	// varint payload (severity_number=2, value 9).
	require.Equal(t, []byte{0x10, 0x09}, appendTaggedUvarint(nil, 0x10, 9))
}

func TestDecodePartialSuccess(t *testing.T) {
	// ExportLogsServiceResponse{partial_success:{rejected_log_records:3, error_message:"bad"}}
	body := []byte{
		0x0a, 0x07, // partial_success, len 7
		0x08, 0x03, // rejected_log_records = 3
		0x12, 0x03, 'b', 'a', 'd', // error_message = "bad"
	}
	rejected, msg, err := decodePartialSuccess(body)
	require.NoError(t, err)
	require.Equal(t, int64(3), rejected)
	require.Equal(t, "bad", msg)

	// Empty body → success, nothing rejected.
	rejected, msg, err = decodePartialSuccess(nil)
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Empty(t, msg)

	// Truncated message → error, never panic.
	_, _, err = decodePartialSuccess([]byte{0x0a, 0x07, 0x08})
	require.Error(t, err)

	// Unknown extra field in response → ignored (forward compat).
	withUnknown := append([]byte{0x1a, 0x01, 0x00}, body...) // fake field 3, then real
	rejected, msg, err = decodePartialSuccess(withUnknown)
	require.NoError(t, err)
	require.Equal(t, int64(3), rejected)
	require.Equal(t, "bad", msg)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestAppend|TestDecodePartial' -v`
Expected: FAIL — `undefined: appendUvarint` etc.

- [ ] **Step 3: Implement `otlp/proto.go`**

```go
package otlp

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

// Proto3 wire-format append helpers. All functions append to dst and return
// the extended slice (the core zapwire dst-append contract). Tags are
// precomputed single bytes — see the wire cheat sheet in the plan / design §3.1.

func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// uvarintLen returns the encoded size of v without encoding it.
func uvarintLen(v uint64) int {
	return (bits.Len64(v|1) + 6) / 7
}

// appendVarint encodes a signed int64 as proto "int64" (two's-complement,
// NOT zigzag — AnyValue.int_value is int64, not sint64).
func appendVarint(dst []byte, v int64) []byte {
	return appendUvarint(dst, uint64(v))
}

func appendTaggedUvarint(dst []byte, tag byte, v uint64) []byte {
	return appendUvarint(append(dst, tag), v)
}

func appendTaggedVarint(dst []byte, tag byte, v int64) []byte {
	return appendVarint(append(dst, tag), v)
}

func appendTaggedFixed64(dst []byte, tag byte, v uint64) []byte {
	dst = append(dst, tag)
	return binary.LittleEndian.AppendUint64(dst, v)
}

func appendTaggedFixed32(dst []byte, tag byte, v uint32) []byte {
	dst = append(dst, tag)
	return binary.LittleEndian.AppendUint32(dst, v)
}

func appendTaggedBytes(dst []byte, tag byte, b []byte) []byte {
	dst = appendUvarint(append(dst, tag), uint64(len(b)))
	return append(dst, b...)
}

func appendTaggedString(dst []byte, tag byte, s string) []byte {
	dst = appendUvarint(append(dst, tag), uint64(len(s)))
	return append(dst, s...)
}

var errTruncatedResponse = errors.New("otlp: truncated export response")

// decodePartialSuccess parses an ExportLogsServiceResponse body. Unknown
// fields are skipped (receivers may add fields within 1.x). The only
// len-delimited submessage we descend into is partial_success (field 1).
func decodePartialSuccess(body []byte) (rejected int64, msg string, err error) {
	ps, err := findField(body, 1)
	if err != nil || ps == nil {
		return 0, "", err
	}
	rejRaw, err := findVarint(ps, 1)
	if err != nil {
		return 0, "", err
	}
	msgRaw, err := findField(ps, 2)
	if err != nil {
		return 0, "", err
	}
	return int64(rejRaw), string(msgRaw), nil
}

// findField scans a proto message for the last occurrence of a len-delimited
// field with the given number, returning its payload (nil if absent).
func findField(b []byte, field int) (payload []byte, err error) {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0: // varint
			_, vn := binary.Uvarint(b)
			if vn <= 0 {
				return nil, errTruncatedResponse
			}
			adv = vn
		case 1: // fixed64
			adv = 8
		case 2: // len-delimited
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l {
				return nil, errTruncatedResponse
			}
			if num == field {
				payload = b[ln : ln+int(l)]
			}
			adv = ln + int(l)
		case 5: // fixed32
			adv = 4
		default:
			return nil, errTruncatedResponse
		}
		if len(b) < adv {
			return nil, errTruncatedResponse
		}
		b = b[adv:]
	}
	return payload, nil
}

// findVarint scans for the last varint field with the given number.
func findVarint(b []byte, field int) (uint64, error) {
	var out uint64
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0:
			v, vn := binary.Uvarint(b)
			if vn <= 0 {
				return 0, errTruncatedResponse
			}
			if num == field {
				out = v
			}
			adv = vn
		case 1:
			adv = 8
		case 2:
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l {
				return 0, errTruncatedResponse
			}
			adv = ln + int(l)
		case 5:
			adv = 4
		default:
			return 0, errTruncatedResponse
		}
		if len(b) < adv {
			return 0, errTruncatedResponse
		}
		b = b[adv:]
	}
	return out, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -run 'TestAppend|TestDecodePartial' -v`
Expected: PASS (all cases).

- [ ] **Step 5: Commit**

```bash
git add otlp/proto.go otlp/proto_test.go
git commit -m "feat(otlp): proto3 wire primitives and partial-success decoder"
```

---

### Task 2: Options, severity, endpoint resolution (`otlp/options.go`)

**Files:**
- Create: `otlp/options.go`
- Test: `otlp/options_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestDefaultSeverityMapper(t *testing.T) {
	cases := []struct {
		lvl  zapcore.Level
		want SeverityNumber
	}{
		{zapcore.DebugLevel, SeverityDebug},   // 5
		{zapcore.InfoLevel, SeverityInfo},     // 9
		{zapcore.WarnLevel, SeverityWarn},     // 13
		{zapcore.ErrorLevel, SeverityError},   // 17
		{zapcore.DPanicLevel, SeverityFatal},  // 21
		{zapcore.PanicLevel, SeverityFatal2},  // 22
		{zapcore.FatalLevel, SeverityFatal3},  // 23
		{zapcore.Level(42), SeverityUnspecified},
	}
	for _, c := range cases {
		require.Equal(t, c.want, defaultSeverityMapper(c.lvl), "level %v", c.lvl)
	}
}

func TestClampSeverity(t *testing.T) {
	require.Equal(t, SeverityNumber(0), clampSeverity(-3))
	require.Equal(t, SeverityNumber(24), clampSeverity(99))
	require.Equal(t, SeverityInfo, clampSeverity(SeverityInfo))
}

func TestDefaultsAndNormalize(t *testing.T) {
	o := applyOptions(nil)
	require.Equal(t, 2048, o.queueSize)
	require.Equal(t, 512, o.batchSize)
	require.Equal(t, 4<<20, o.maxRequestBytes)
	require.Equal(t, time.Second, o.flushInterval)
	require.Equal(t, 10*time.Second, o.timeout)
	require.Equal(t, DropNewest, o.dropPolicy)
	require.Equal(t, RetryConfig{Initial: 5 * time.Second, MaxInterval: 30 * time.Second, MaxElapsed: time.Minute}, o.retry)
	require.Equal(t, NoCompression, o.compression)
	require.True(t, o.callerAttrs)
	require.Equal(t, "logger", o.loggerNameKey)
	require.Contains(t, o.serviceName, "unknown_service:")
	require.Equal(t, "github.com/arloliu/zapwire/otlp", o.scopeName)
	require.NotNil(t, o.severityOf)
	require.NotNil(t, o.errFn) // no-op, never nil
	require.NotNil(t, o.client)

	// Non-positive values clamp back to defaults (core normalizeConfig discipline).
	o = applyOptions([]Option{
		WithQueueSize(-1), WithBatchSize(0), WithMaxRequestBytes(-5),
		WithFlushInterval(0), WithTimeout(-time.Second),
		WithRetry(RetryConfig{}), // zero fields → defaults
	})
	require.Equal(t, 2048, o.queueSize)
	require.Equal(t, 512, o.batchSize)
	require.Equal(t, 4<<20, o.maxRequestBytes)
	require.Equal(t, time.Second, o.flushInterval)
	require.Equal(t, 10*time.Second, o.timeout)
	require.Equal(t, 5*time.Second, o.retry.Initial)
}

func TestOptionSetters(t *testing.T) {
	hc := &http.Client{}
	called := false
	o := applyOptions([]Option{
		WithServiceName("svc"),
		WithScopeName("scope"), WithScopeVersion("v9"),
		WithSeverityMapper(func(zapcore.Level) SeverityNumber { return 7 }),
		WithSeverityMapper(nil), // nil mapper ignored, previous kept
		WithCallerAttributes(false),
		WithLoggerNameKey(""), // empty disables
		WithDropPolicy(DropOldest),
		WithHeaders(map[string]string{"x-api-key": "k"}),
		WithHTTPClient(hc),
		WithCompression(Gzip),
		WithErrorHandler(func(error) { called = true }),
	})
	require.Equal(t, "svc", o.serviceName)
	require.Equal(t, "scope", o.scopeName)
	require.Equal(t, "v9", o.scopeVersion)
	require.Equal(t, SeverityNumber(7), o.severityOf(zapcore.InfoLevel))
	require.False(t, o.callerAttrs)
	require.Empty(t, o.loggerNameKey)
	require.Equal(t, DropOldest, o.dropPolicy)
	require.Equal(t, "k", o.headers["x-api-key"])
	require.Same(t, hc, o.client)
	require.Equal(t, Gzip, o.compression)
	o.errFn(nil)
	require.True(t, called)
}

func TestResolveEndpoint(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{"http://collector:4318", "http://collector:4318/v1/logs", false},
		{"http://collector:4318/", "http://collector:4318/v1/logs", false},
		{"https://collector:4318/custom/path", "https://collector:4318/custom/path", false},
		{"", "", true},                 // ErrNoEndpoint
		{"://bad", "", true},           // parse error
		{"collector:4318", "", true},   // no http(s) scheme
	}
	for _, c := range cases {
		got, err := resolveEndpoint(c.in)
		if c.wantErr {
			require.Error(t, err, "input %q", c.in)
			continue
		}
		require.NoError(t, err, "input %q", c.in)
		require.Equal(t, c.want, got)
	}
}

func TestEndpointFromEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	require.Empty(t, EndpointFromEnv())

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://base:4318")
	require.Equal(t, "http://base:4318", EndpointFromEnv())

	// Signal-specific endpoint wins.
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://logs:4318/v1/logs")
	require.Equal(t, "http://logs:4318/v1/logs", EndpointFromEnv())
}

func TestExportError(t *testing.T) {
	inner := errTruncatedResponse
	e := &ExportError{StatusCode: 503, Retryable: true, Message: "busy", Err: inner}
	require.ErrorIs(t, e, errTruncatedResponse)
	require.Contains(t, e.Error(), "503")
	require.Contains(t, e.Error(), "busy")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestDefault|TestClamp|TestOption|TestResolve|TestEndpoint|TestExportError' -v`
Expected: FAIL — `undefined: applyOptions` etc.

- [ ] **Step 3: Implement `otlp/options.go`**

```go
package otlp

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SeverityNumber is the OTLP severity (data model: 1-24, 0 = unspecified).
type SeverityNumber int32

const (
	SeverityUnspecified SeverityNumber = 0
	SeverityTrace       SeverityNumber = 1
	SeverityDebug       SeverityNumber = 5
	SeverityInfo        SeverityNumber = 9
	SeverityWarn        SeverityNumber = 13
	SeverityError       SeverityNumber = 17
	SeverityFatal       SeverityNumber = 21
	SeverityFatal2      SeverityNumber = 22
	SeverityFatal3      SeverityNumber = 23

	maxSeverity SeverityNumber = 24
)

// defaultSeverityMapper is the contrib-bridge canonical mapping (design §3.2).
func defaultSeverityMapper(l zapcore.Level) SeverityNumber {
	switch l {
	case zapcore.DebugLevel:
		return SeverityDebug
	case zapcore.InfoLevel:
		return SeverityInfo
	case zapcore.WarnLevel:
		return SeverityWarn
	case zapcore.ErrorLevel:
		return SeverityError
	case zapcore.DPanicLevel:
		return SeverityFatal
	case zapcore.PanicLevel:
		return SeverityFatal2
	case zapcore.FatalLevel:
		return SeverityFatal3
	default:
		return SeverityUnspecified
	}
}

// clampSeverity bounds mapper output per entry (design §3.2).
func clampSeverity(s SeverityNumber) SeverityNumber {
	if s < 0 {
		return 0
	}
	if s > maxSeverity {
		return maxSeverity
	}
	return s
}

// DropPolicy selects which record is dropped when the queue is full.
type DropPolicy uint8

const (
	DropNewest DropPolicy = iota
	DropOldest
)

// Compression selects the request body encoding.
type Compression uint8

const (
	NoCompression Compression = iota
	Gzip
)

// RetryConfig bounds the §5.3 retry loop. Zero fields fall back to defaults.
type RetryConfig struct {
	Initial     time.Duration // first backoff delay (default 5s)
	MaxInterval time.Duration // backoff cap (default 30s)
	MaxElapsed  time.Duration // total retry budget per batch (default 60s)
}

// ExportError is delivered to WithErrorHandler for every ship-path event.
type ExportError struct {
	StatusCode int    // HTTP status; 0 for transport/encode errors
	Retryable  bool   // whether the failure was in the retryable class
	Rejected   int64  // partial-success rejected_log_records
	Warning    bool   // partial success with Rejected == 0 and a message
	Message    string // partial-success error_message or short response excerpt
	Err        error  // wrapped underlying error, may be nil
}

func (e *ExportError) Error() string {
	return fmt.Sprintf("otlp export: status=%d retryable=%v rejected=%d warning=%v msg=%q err=%v",
		e.StatusCode, e.Retryable, e.Rejected, e.Warning, e.Message, e.Err)
}

func (e *ExportError) Unwrap() error { return e.Err }

// ErrNoEndpoint is returned by NewWriter/NewCore for an empty endpoint.
var ErrNoEndpoint = errors.New("otlp: no endpoint")

type options struct {
	// envelope end
	serviceName    string
	resourceFields []zap.Field
	scopeName      string
	scopeVersion   string
	// encoder end
	severityOf    func(zapcore.Level) SeverityNumber
	callerAttrs   bool
	loggerNameKey string
	// writer end
	queueSize       int
	batchSize       int
	maxRequestBytes int
	flushInterval   time.Duration
	timeout         time.Duration
	dropPolicy      DropPolicy
	retry           RetryConfig
	headers         map[string]string
	client          *http.Client
	compression     Compression
	errFn           func(error)
}

// Option configures the otlp preset. Envelope/encoder options are documented
// no-ops on NewWriter-only paths and writer options are no-ops on NewEncoder,
// per the subpackage convention (design §6).
type Option func(*options)

func defaultOptions() options {
	return options{
		serviceName:   "unknown_service:" + filepath.Base(os.Args[0]),
		scopeName:     "github.com/arloliu/zapwire/otlp",
		scopeVersion:  moduleVersion(),
		severityOf:    defaultSeverityMapper,
		callerAttrs:   true,
		loggerNameKey: "logger",
		queueSize:     2048,
		batchSize:     512,
		maxRequestBytes: 4 << 20,
		flushInterval: time.Second,
		timeout:       10 * time.Second,
		dropPolicy:    DropNewest,
		retry:         RetryConfig{Initial: 5 * time.Second, MaxInterval: 30 * time.Second, MaxElapsed: time.Minute},
		client:        &http.Client{},
		errFn:         func(error) {},
	}
}

func applyOptions(opts []Option) options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return normalize(o)
}

// normalize clamps invalid values back to defaults (core normalizeConfig
// discipline, options.go:171 in the root module).
func normalize(o options) options {
	d := defaultOptions()
	if o.queueSize <= 0 {
		o.queueSize = d.queueSize
	}
	if o.batchSize <= 0 {
		o.batchSize = d.batchSize
	}
	if o.maxRequestBytes <= 0 {
		o.maxRequestBytes = d.maxRequestBytes
	}
	if o.flushInterval <= 0 {
		o.flushInterval = d.flushInterval
	}
	if o.timeout <= 0 {
		o.timeout = d.timeout
	}
	if o.retry.Initial <= 0 {
		o.retry.Initial = d.retry.Initial
	}
	if o.retry.MaxInterval <= 0 {
		o.retry.MaxInterval = d.retry.MaxInterval
	}
	if o.retry.MaxElapsed <= 0 {
		o.retry.MaxElapsed = d.retry.MaxElapsed
	}
	if o.severityOf == nil {
		o.severityOf = d.severityOf
	}
	if o.client == nil {
		o.client = d.client
	}
	if o.errFn == nil {
		o.errFn = d.errFn
	}
	return o
}

// moduleVersion best-efforts this module's version from build info ("" in
// dev/test builds).
func moduleVersion() string {
	// runtime/debug.ReadBuildInfo walks deps of the *main* module; when otlp
	// is a dependency its version appears there. Inside this module's own
	// tests it is "(devel)" or absent — both render as "".
	return readModuleVersion()
}

// WithServiceName sets the Resource service.name (default "unknown_service:<exe>").
func WithServiceName(s string) Option { return func(o *options) { o.serviceName = s } }

// WithResource appends extra Resource attributes, encoded through the same
// proto ObjectEncoder as record attributes (design §4).
func WithResource(fields ...zap.Field) Option {
	return func(o *options) { o.resourceFields = append(o.resourceFields, fields...) }
}

// WithScopeName overrides the InstrumentationScope name.
func WithScopeName(s string) Option { return func(o *options) { o.scopeName = s } }

// WithScopeVersion overrides the InstrumentationScope version.
func WithScopeVersion(s string) Option { return func(o *options) { o.scopeVersion = s } }

// WithSeverityMapper overrides the zap-level → SeverityNumber mapping; results
// are clamped to 0..24 per entry. A nil mapper is ignored.
func WithSeverityMapper(fn func(zapcore.Level) SeverityNumber) Option {
	return func(o *options) {
		if fn != nil {
			o.severityOf = fn
		}
	}
}

// WithCallerAttributes toggles code.* attributes from Entry.Caller (default on).
func WithCallerAttributes(on bool) Option { return func(o *options) { o.callerAttrs = on } }

// WithLoggerNameKey sets the attribute key carrying Entry.LoggerName (default
// "logger"; empty disables the attribute).
func WithLoggerNameKey(k string) Option { return func(o *options) { o.loggerNameKey = k } }

// WithQueueSize bounds the ingest queue (records; default 2048).
func WithQueueSize(n int) Option { return func(o *options) { o.queueSize = n } }

// WithBatchSize caps records per request (default 512).
func WithBatchSize(n int) Option { return func(o *options) { o.batchSize = n } }

// WithMaxRequestBytes caps the uncompressed request body (default 4 MiB);
// batches are cut early and oversized single records are dropped at Write.
func WithMaxRequestBytes(n int) Option { return func(o *options) { o.maxRequestBytes = n } }

// WithFlushInterval caps batch latency (default 1s).
func WithFlushInterval(d time.Duration) Option { return func(o *options) { o.flushInterval = d } }

// WithDropPolicy selects the queue-full policy (default DropNewest).
func WithDropPolicy(p DropPolicy) Option { return func(o *options) { o.dropPolicy = p } }

// WithTimeout bounds each HTTP attempt (default 10s).
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithRetry overrides retry/backoff bounds; zero fields keep defaults.
func WithRetry(rc RetryConfig) Option { return func(o *options) { o.retry = rc } }

// WithHeaders adds headers to every export request (auth, api keys).
func WithHeaders(h map[string]string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(map[string]string, len(h))
		}
		for k, v := range h {
			o.headers[k] = v
		}
	}
}

// WithHTTPClient supplies the http.Client (TLS, proxies); nil keeps the default.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) {
		if c != nil {
			o.client = c
		}
	}
}

// WithCompression selects request compression (default NoCompression).
func WithCompression(c Compression) Option { return func(o *options) { o.compression = c } }

// WithErrorHandler observes ship-path events (*ExportError); nil keeps the
// no-op. The handler is invoked synchronously from Write (oversized records)
// and from the flush goroutine (export failures) — it must be fast and
// non-blocking, but it MAY call the Writer's own methods (Close/Sync): no
// internal lock is held across handler invocations.
func WithErrorHandler(fn func(error)) Option {
	return func(o *options) {
		if fn != nil {
			o.errFn = fn
		}
	}
}

// resolveEndpoint validates the endpoint URL and appends /v1/logs when the
// path is empty (design §5.5).
func resolveEndpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", ErrNoEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("otlp: invalid endpoint %q: %w", endpoint, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("otlp: endpoint %q must be an http(s) URL", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/logs"
	}
	return u.String(), nil
}

// EndpointFromEnv resolves OTEL_EXPORTER_OTLP_LOGS_ENDPOINT (used as-is) then
// OTEL_EXPORTER_OTLP_ENDPOINT (base URL; NewWriter appends /v1/logs when the
// path is empty). Returns "" when neither is set. Env handling is explicit
// and opt-in — zapwire never reads env behind the caller's back (design §5.5).
func EndpointFromEnv() string {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"); v != "" {
		return v
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}
```

Also create the tiny build-info shim in the same file (kept separate for testability):

```go
// readModuleVersion is split out so tests can exercise the "" path without
// faking build info.
func readModuleVersion() string {
	bi, ok := debugReadBuildInfo()
	if !ok {
		return ""
	}
	const path = "github.com/arloliu/zapwire/otlp"
	if bi.Main.Path == path && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	for _, d := range bi.Deps {
		if d.Path == path {
			return d.Version
		}
	}
	return ""
}

// debugReadBuildInfo is a seam for tests.
var debugReadBuildInfo = debug.ReadBuildInfo
```

(Add `"runtime/debug"` to the import block.)

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v`
Expected: PASS (all Task 1 + Task 2 tests).

- [ ] **Step 5: Commit**

```bash
git add otlp/options.go otlp/options_test.go
git commit -m "feat(otlp): options, severity mapping, endpoint resolution"
```

---

### Task 3: Trace-context helpers (`otlp/tracectx.go`)

The §3.4 helper surface: eager capture, injectors (clone-before-append — pass-4 P1), flat
correlation fields, and the shared nil-guarded extraction used by encoder and core.

**Files:**
- Create: `otlp/tracectx.go`
- Test: `otlp/tracectx_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// testSpanContext returns a valid, sampled span context and a ctx carrying it.
func testSpanContext(t *testing.T) (trace.SpanContext, context.Context) {
	t.Helper()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:     trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
		TraceFlags: trace.FlagsSampled,
	})
	require.True(t, sc.IsValid())
	return sc, trace.ContextWithSpanContext(context.Background(), sc)
}

// typedNilCtx implements context.Context on a pointer receiver, so a nil
// *typedNilCtx stored in an interface is a typed-nil context (pass-3 P2).
type typedNilCtx struct{}

func (*typedNilCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*typedNilCtx) Done() <-chan struct{}       { return nil }
func (*typedNilCtx) Err() error                  { return nil }
func (*typedNilCtx) Value(any) any               { return nil }

func TestSpanContextField(t *testing.T) {
	sc, ctx := testSpanContext(t)
	f := SpanContext(ctx)
	require.Equal(t, "span_context", f.Key)
	require.Equal(t, zapcore.ReflectType, f.Type)
	require.Equal(t, sc, f.Interface)

	// No span / nil ctx → field still returned, zero (invalid) span context.
	f = SpanContext(context.Background())
	require.False(t, f.Interface.(trace.SpanContext).IsValid())
	f = SpanContext(nil) //nolint:staticcheck // deliberate nil-safety check
	require.False(t, f.Interface.(trace.SpanContext).IsValid())
}

func TestSpanContextFromField(t *testing.T) {
	sc, ctx := testSpanContext(t)

	// Eager helper payload.
	got, ok := spanContextFromField(SpanContext(ctx))
	require.True(t, ok)
	require.Equal(t, sc, got)

	// Raw ctx via zap.Any — zap classifies stdlib ctx as StringerType, but
	// Interface preserves the value (design §3.4).
	f := zap.Any("context", ctx)
	got, ok = spanContextFromField(f)
	require.True(t, ok)
	require.Equal(t, sc, got)

	// Ordinary fields never match.
	_, ok = spanContextFromField(zap.String("trace_id", "deadbeef"))
	require.False(t, ok)
	_, ok = spanContextFromField(zap.Int("n", 1))
	require.False(t, ok)

	// Typed-nil ctx: matched (consumed) but resolves to zero span context —
	// must NOT panic (otel guards only interface-nil).
	var tn *typedNilCtx
	got, ok = spanContextFromField(zap.Field{Key: "context", Type: zapcore.ReflectType, Interface: tn})
	require.True(t, ok)
	require.False(t, got.IsValid())
}

func TestInjectTraceFieldsCloneSemantics(t *testing.T) {
	_, ctx := testSpanContext(t)

	out := InjectTraceFields(ctx, zap.String("k", "v"))
	require.Len(t, out, 2)
	require.Equal(t, "k", out[0].Key)
	require.Equal(t, "span_context", out[1].Key)

	// Pass-4 P1 pin: a caller slice with spare capacity must NOT be mutated.
	base := make([]zap.Field, 1, 2)
	base[0] = zap.String("k", "v")
	probe := base[:2]
	probe[1] = zap.String("sentinel", "untouched")
	out = InjectTraceFields(ctx, base...)
	require.Len(t, out, 2)
	require.Equal(t, "sentinel", probe[1].Key, "caller backing array was mutated")

	// Zero fields: still returns the span context field.
	out = InjectTraceFields(ctx)
	require.Len(t, out, 1)
}

func TestInjectTraceKVs(t *testing.T) {
	_, ctx := testSpanContext(t)

	out := InjectTraceKVs(ctx, "url", "https://x", "attempt", 3)
	require.Len(t, out, 5)
	f, isField := out[0].(zap.Field)
	require.True(t, isField)
	require.Equal(t, "span_context", f.Key)
	require.Equal(t, "url", out[1])

	// Zero kvs: lone typed field, no dangling key (sugared contract pinned in
	// the encoder integration test, Task 6).
	out = InjectTraceKVs(ctx)
	require.Len(t, out, 1)
}

func TestTraceCorrelationFields(t *testing.T) {
	_, ctx := testSpanContext(t)

	fields := TraceCorrelationFields(ctx)
	require.Len(t, fields, 2)
	require.Equal(t, "trace_id", fields[0].Key)
	require.Equal(t, "5b8efff798038103d269b633813fc60c", fields[0].String)
	require.Equal(t, "span_id", fields[1].Key)
	require.Equal(t, "eee19b7ec3c1b174", fields[1].String)

	// No span → nil (no empty-string pollution).
	require.Nil(t, TraceCorrelationFields(context.Background()))
	require.Nil(t, TraceCorrelationFields(nil)) //nolint:staticcheck
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestSpanContext|TestInject|TestTraceCorrelation' -v`
Expected: FAIL — `undefined: SpanContext` etc.

- [ ] **Step 3: Implement `otlp/tracectx.go`**

```go
package otlp

import (
	"context"
	"reflect"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// spanContextKey is the eager helper's field key. It is part of the API
// contract: non-OTLP cores in a tee render the field under this key.
const spanContextKey = "span_context"

// SpanContext eagerly captures the active span context from ctx as a zap
// field. It always returns a field — with no active span the encoder omits
// the (invalid) context — so call sites stay branch-free. The payload type
// trace.SpanContext implements json.Marshaler, so tee'd JSON/console cores
// render it legibly (design §3.4).
func SpanContext(ctx context.Context) zap.Field {
	return zap.Field{Key: spanContextKey, Type: zapcore.ReflectType, Interface: spanContextOrZero(ctx)}
}

// InjectTraceFields returns fields plus the eager span-context field.
// It ALWAYS allocates a fresh slice (clone-before-append): with the
// `existingSlice...` call form Go passes the slice unchanged, so a bare
// append could mutate the caller's backing array (plan-review pass-4 P1).
func InjectTraceFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	out := make([]zap.Field, 0, len(fields)+1)
	out = append(out, fields...)
	return append(out, SpanContext(ctx))
}

// InjectTraceKVs prepends the eager span-context field to a sugared
// keysAndValues list. zap's SugaredLogger consumes strongly-typed Fields
// mixed into keysAndValues before pair processing, so the prepended field
// never splits a key/value pair.
func InjectTraceKVs(ctx context.Context, kvs ...any) []any {
	out := make([]any, 0, len(kvs)+1)
	out = append(out, SpanContext(ctx))
	return append(out, kvs...)
}

// TraceCorrelationFields returns flat lowercase-hex "trace_id"/"span_id"
// string fields for NON-OTLP sinks (ndjson/fluent/syslog → Loki derived
// fields, Datadog log parsing). With the OTLP core these land in attributes,
// NOT the LogRecord trace fields — use SpanContext / the Inject helpers for
// OTLP correlation. Returns nil when ctx carries no valid span.
func TraceCorrelationFields(ctx context.Context) []zap.Field {
	sc := spanContextOrZero(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []zap.Field{
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	}
}

// spanContextOrZero extracts the span context with full nil-safety:
// trace.SpanContextFromContext guards only interface-nil before calling
// ctx.Value, so typed-nil contexts must be rejected here (pass-3 P2).
func spanContextOrZero(ctx context.Context) trace.SpanContext {
	if ctx == nil || isNilValue(ctx) {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(ctx)
}

// spanContextFromField reports whether f carries trace context (the eager
// trace.SpanContext payload or any context.Context value, regardless of how
// zap classified the field) and resolves it. A matched field is consumed by
// callers — never encoded as an attribute — even when the resolved span
// context is invalid (design §3.4).
func spanContextFromField(f zapcore.Field) (trace.SpanContext, bool) {
	return spanContextFromValue(f.Interface)
}

// spanContextFromValue is the value-level form, shared with the encoder's
// AddReflected hook (Task 4/5), which receives bare values rather than Fields.
func spanContextFromValue(val any) (trace.SpanContext, bool) {
	switch v := val.(type) {
	case nil:
		return trace.SpanContext{}, false
	case trace.SpanContext:
		return v, true
	case context.Context:
		return spanContextOrZero(v), true
	default:
		return trace.SpanContext{}, false
	}
}

// isNilValue reports whether v is a typed-nil pointer/map/etc. boxed in a
// non-nil interface.
func isNilValue(v any) bool {
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add otlp/tracectx.go otlp/tracectx_test.go
git commit -m "feat(otlp): trace-context helpers (SpanContext, injectors, correlation fields)"
```

---

### Task 4: Frame-stack proto ObjectEncoder (`otlp/attr.go`)

Zap fields → `KeyValue{key, AnyValue}` proto bytes. Per-frame `[]byte` buffers (pooled) hold
**already-tagged entries** for their container, so sealing is `outer-tag + varint(len) +
contents` with no backpatching (proto repeated fields have no count headers — simpler than
the msgpack frame stack). Snapshot/rollback implements the §3.3 transactionality.

Frame entry tagging:
- `frameRoot`: KeyValue entries tagged `0x32` (LogRecord.attributes) — also reused verbatim
  for Resource.attributes? **No** — Resource uses tag `0x0a`; the envelope re-tags (Task 7).
  Root frames here always mean LogRecord attributes.
- `frameKVList`: KeyValue entries tagged `0x0a` (KeyValueList.values)
- `frameArray`: AnyValue entries tagged `0x0a` (ArrayValue.values)

**Files:**
- Create: `otlp/attr.go`
- Test: `otlp/attr_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// rootBytes seals all frames and returns the root attribute bytes.
func rootBytes(t *testing.T, s *encState) []byte {
	t.Helper()
	s.sealAll()
	return append([]byte(nil), s.stack[0].buf...)
}

func TestAddStringGolden(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("k", "v")
	// KeyValue{key:"k", value:AnyValue{string_value:"v"}}
	// kv body: 0x0a 0x01 'k'  0x12 0x03 (0x0a 0x01 'v')  → len 8
	want := []byte{0x32, 0x08, 0x0a, 0x01, 'k', 0x12, 0x03, 0x0a, 0x01, 'v'}
	require.Equal(t, want, rootBytes(t, s))
}

func TestScalarShapes(t *testing.T) {
	cases := []struct {
		name string
		add  func(s *encState)
		av   []byte // expected AnyValue bytes
	}{
		{"bool_true", func(s *encState) { s.AddBool("k", true) }, []byte{0x10, 0x01}},
		{"bool_false", func(s *encState) { s.AddBool("k", false) }, []byte{0x10, 0x00}},
		{"int", func(s *encState) { s.AddInt64("k", 3) }, []byte{0x18, 0x03}},
		{"int_negative", func(s *encState) { s.AddInt64("k", -1) },
			append([]byte{0x18}, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)},
		{"double", func(s *encState) { s.AddFloat64("k", 1.0) },
			[]byte{0x21, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f}},
		{"binary", func(s *encState) { s.AddBinary("k", []byte{0xde}) }, []byte{0x3a, 0x01, 0xde}},
		{"bytestring_is_string", func(s *encState) { s.AddByteString("k", []byte("txt")) },
			[]byte{0x0a, 0x03, 't', 'x', 't'}},
		{"duration_nanos", func(s *encState) { s.AddDuration("k", 2 * time.Nanosecond) }, []byte{0x18, 0x02}},
		{"time_unixnanos", func(s *encState) { s.AddTime("k", time.Unix(0, 5)) }, []byte{0x18, 0x05}},
		{"uint64_overflow_string", func(s *encState) { s.AddUint64("k", 1<<63 + 1) },
			append([]byte{0x0a, 0x13}, []byte("9223372036854775809")...)},
		{"complex", func(s *encState) { s.AddComplex128("k", complex(1, 2)) },
			append([]byte{0x0a, 0x04}, []byte("1+2i")...)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newEncState()
			defer s.free()
			c.add(s)
			kvLen := 3 + 2 + len(c.av) // key part (0x0a 0x01 'k') + value header (0x12 len)
			want := append([]byte{0x32, byte(kvLen), 0x0a, 0x01, 'k', 0x12, byte(len(c.av))}, c.av...)
			require.Equal(t, want, rootBytes(t, s))
		})
	}
}

func TestNamespaceNesting(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("a", "1")
	s.OpenNamespace("ns")
	s.AddString("b", "2")
	got := rootBytes(t, s)

	// inner kv: KeyValue{"b", "2"} tagged for KeyValueList → 0x0a 0x08 ...
	innerKV := []byte{0x0a, 0x08, 0x0a, 0x01, 'b', 0x12, 0x03, 0x0a, 0x01, '2'}
	// ns AnyValue: kvlist_value → 0x32 0x0a innerKV
	nsAV := append([]byte{0x32, byte(len(innerKV))}, innerKV...)
	// ns KeyValue at root: 0x32 len 0x0a 0x02 'n' 's' 0x12 len(nsAV) nsAV
	nsKV := append([]byte{0x0a, 0x02, 'n', 's', 0x12, byte(len(nsAV))}, nsAV...)
	first := []byte{0x32, 0x08, 0x0a, 0x01, 'a', 0x12, 0x03, 0x0a, 0x01, '1'}
	want := append(first, append([]byte{0x32, byte(len(nsKV))}, nsKV...)...)
	require.Equal(t, want, got)
}

type okObj struct{}

func (okObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("x", "y")
	return nil
}

type failObj struct{ partial bool }

func (f failObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if f.partial {
		e.AddString("written", "before-failing")
		e.OpenNamespace("opened")
	}
	return errors.New("marshal failed")
}

type intArr []int64

func (a intArr) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, v := range a {
		e.AppendInt64(v)
	}
	return nil
}

func TestAddObjectAndArray(t *testing.T) {
	s := newEncState()
	defer s.free()
	require.NoError(t, s.AddObject("o", okObj{}))
	require.NoError(t, s.AddArray("arr", intArr{1, 2}))
	got := rootBytes(t, s)

	// o → kvlist with KeyValue{"x","y"}
	xy := []byte{0x0a, 0x08, 0x0a, 0x01, 'x', 0x12, 0x03, 0x0a, 0x01, 'y'}
	oAV := append([]byte{0x32, byte(len(xy))}, xy...)
	oKV := append([]byte{0x0a, 0x01, 'o', 0x12, byte(len(oAV))}, oAV...)
	want := append([]byte{0x32, byte(len(oKV))}, oKV...)
	// arr → ArrayValue{AnyValue{1}, AnyValue{2}} → elements 0x0a 0x02 0x18 v
	elems := []byte{0x0a, 0x02, 0x18, 0x01, 0x0a, 0x02, 0x18, 0x02}
	aAV := append([]byte{0x2a, byte(len(elems))}, elems...)
	aKV := append([]byte{0x0a, 0x03, 'a', 'r', 'r', 0x12, byte(len(aAV))}, aAV...)
	want = append(want, append([]byte{0x32, byte(len(aKV))}, aKV...)...)
	require.Equal(t, want, got)
}

func TestRollbackNoPartialBytes(t *testing.T) {
	// Failing object marshaler that wrote an attr and opened a namespace
	// before erroring: rollback must leave the state byte-identical to never
	// having added the field (design §3.3 / pass-2 P0).
	clean := newEncState()
	defer clean.free()
	clean.AddString("a", "1")
	want := rootBytes(t, clean)

	dirty := newEncState()
	defer dirty.free()
	dirty.AddString("a", "1")
	require.Error(t, dirty.AddObject("bad", failObj{partial: true}))
	require.Equal(t, want, rootBytes(t, dirty))
}

func TestAddReflectedJSONAndSink(t *testing.T) {
	s := newEncState()
	defer s.free()
	// JSON fallback (loose shape assertion; the conformance module decodes
	// reflected values through the official stubs).
	require.NoError(t, s.AddReflected("r", map[string]int{"n": 1}))
	got := rootBytes(t, s)
	require.Contains(t, string(got), `{"n":1}`)

	// Unmarshalable value → error, nothing written (transactional).
	s2 := newEncState()
	defer s2.free()
	require.Error(t, s2.AddReflected("bad", make(chan int)))
	require.Empty(t, rootBytes(t, s2))
}

func TestSnapshotRollbackDiscardsFrames(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("keep", "1")
	sn := s.snap()
	s.OpenNamespace("n1")
	s.OpenNamespace("n2")
	s.AddString("drop", "2")
	s.rollback(sn)
	require.Len(t, s.stack, 1)

	clean := newEncState()
	defer clean.free()
	clean.AddString("keep", "1")
	require.Equal(t, rootBytes(t, clean), rootBytes(t, s))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestAddString|TestScalar|TestNamespace|TestAddObject|TestRollback|TestAddReflected|TestSnapshot' -v`
Expected: FAIL — `undefined: newEncState`.

- [ ] **Step 3: Implement `otlp/attr.go`**

```go
package otlp

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

type frameKind uint8

const (
	frameRoot frameKind = iota // KeyValue entries tagged 0x32 (LogRecord.attributes)
	frameKVList                // KeyValue entries tagged 0x0a (KeyValueList.values)
	frameArray                 // AnyValue entries tagged 0x0a (ArrayValue.values)
)

type frame struct {
	kind  frameKind
	nsKey string // KeyValue key this frame seals into (unused for frameRoot)
	buf   []byte // already-tagged entries for this container (pooled)
}

var frameBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

func getFrameBuf() []byte  { return (*frameBufPool.Get().(*[]byte))[:0] }
func putFrameBuf(b []byte) { frameBufPool.Put(&b) }

// encState is the proto-writing zapcore.ObjectEncoder. It is the working
// state of EncodeEntry, the persistent With-state of the encoder (embedded),
// and the Resource attribute builder (Task 7).
type encState struct {
	stack   []frame
	scratch []byte // reused AnyValue assembly buffer
	// trace sink: AddReflected stores resolved span contexts here instead of
	// encoding them (design §3.4). Nil sink → trace values are still consumed
	// (never encoded) but not stored.
	scSink    *trace.SpanContext
	scSinkSet *bool
}

func newEncState() *encState {
	return &encState{stack: []frame{{kind: frameRoot, buf: getFrameBuf()}}}
}

// free returns all frame buffers to the pool. The state is unusable after.
func (s *encState) free() {
	for i := range s.stack {
		putFrameBuf(s.stack[i].buf)
	}
	s.stack = nil
}

// clone deep-copies the state for per-call work (design §3.7): every frame
// buffer is copied into a pooled slice so EncodeEntry never mutates the
// receiver-owned persistent state.
func (s *encState) clone() *encState {
	c := &encState{stack: make([]frame, len(s.stack))}
	for i := range s.stack {
		buf := getFrameBuf()
		buf = append(buf, s.stack[i].buf...)
		c.stack[i] = frame{kind: s.stack[i].kind, nsKey: s.stack[i].nsKey, buf: buf}
	}
	return c
}

func (s *encState) cur() *frame { return &s.stack[len(s.stack)-1] }

type snapshot struct {
	depth  int
	bufLen int
}

func (s *encState) snap() snapshot {
	return snapshot{depth: len(s.stack), bufLen: len(s.cur().buf)}
}

// rollback restores the state captured by snap: frames opened above the
// snapshot depth are discarded and the then-current frame is truncated, so
// no partial bytes survive a failed marshaler (design §3.3, pass-2 P0).
func (s *encState) rollback(sn snapshot) {
	for len(s.stack) > sn.depth {
		top := s.stack[len(s.stack)-1]
		s.stack = s.stack[:len(s.stack)-1]
		putFrameBuf(top.buf)
	}
	f := s.cur()
	f.buf = f.buf[:sn.bufLen]
}

// entryTag returns the tag byte for entries of this container.
func (f *frame) entryTag() byte {
	if f.kind == frameRoot {
		return 0x32 // LogRecord.attributes
	}
	return 0x0a // KeyValueList.values / ArrayValue.values
}

// addKV appends KeyValue{key, av} as a tagged entry of the current frame.
func (s *encState) addKV(key string, av []byte) {
	f := s.cur()
	kvLen := 1 + uvarintLen(uint64(len(key))) + len(key) +
		1 + uvarintLen(uint64(len(av))) + len(av)
	f.buf = append(f.buf, f.entryTag())
	f.buf = appendUvarint(f.buf, uint64(kvLen))
	f.buf = appendTaggedString(f.buf, 0x0a, key)
	f.buf = appendTaggedBytes(f.buf, 0x12, av)
}

// addAV appends a bare AnyValue entry (array containers only).
func (s *encState) addAV(av []byte) {
	f := s.cur()
	f.buf = appendTaggedBytes(f.buf, f.entryTag(), av)
}

// addValue routes an AnyValue to the current container: arrays take bare
// AnyValues, root/kvlist take KeyValue-wrapped ones.
func (s *encState) addValue(key string, av []byte) {
	if s.cur().kind == frameArray {
		s.addAV(av)
		return
	}
	s.addKV(key, av)
}

// AnyValue builders (into s.scratch; valid until the next any* call).
func (s *encState) anyString(v string) []byte {
	s.scratch = appendTaggedString(s.scratch[:0], 0x0a, v)
	return s.scratch
}
func (s *encState) anyBool(v bool) []byte {
	b := byte(0)
	if v {
		b = 1
	}
	s.scratch = append(s.scratch[:0], 0x10, b)
	return s.scratch
}
func (s *encState) anyInt(v int64) []byte {
	s.scratch = appendTaggedVarint(s.scratch[:0], 0x18, v)
	return s.scratch
}
func (s *encState) anyDouble(v float64) []byte {
	s.scratch = appendTaggedFixed64(s.scratch[:0], 0x21, math.Float64bits(v))
	return s.scratch
}
func (s *encState) anyBytes(v []byte) []byte {
	s.scratch = appendTaggedBytes(s.scratch[:0], 0x3a, v)
	return s.scratch
}
```

(Add `"math"` to the import block.)

```go
// --- container open/seal ---

func (s *encState) openFrame(kind frameKind, key string) {
	s.stack = append(s.stack, frame{kind: kind, nsKey: key, buf: getFrameBuf()})
}

// sealTop closes the top frame into its parent as kvlist/array AnyValue.
func (s *encState) sealTop() {
	top := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	avTag := byte(0x32) // AnyValue.kvlist_value
	if top.kind == frameArray {
		avTag = 0x2a // AnyValue.array_value
	}
	avLen := 1 + uvarintLen(uint64(len(top.buf))) + len(top.buf)
	parent := s.cur()
	if parent.kind == frameArray {
		// bare AnyValue element
		parent.buf = append(parent.buf, parent.entryTag())
		parent.buf = appendUvarint(parent.buf, uint64(avLen))
	} else {
		kvLen := 1 + uvarintLen(uint64(len(top.nsKey))) + len(top.nsKey) +
			1 + uvarintLen(uint64(avLen)) + avLen
		parent.buf = append(parent.buf, parent.entryTag())
		parent.buf = appendUvarint(parent.buf, uint64(kvLen))
		parent.buf = appendTaggedString(parent.buf, 0x0a, top.nsKey)
		parent.buf = append(parent.buf, 0x12)
		parent.buf = appendUvarint(parent.buf, uint64(avLen))
	}
	parent.buf = append(parent.buf, avTag)
	parent.buf = appendUvarint(parent.buf, uint64(len(top.buf)))
	parent.buf = append(parent.buf, top.buf...)
	putFrameBuf(top.buf)
}

// sealAll closes every open namespace down to the root frame (entry end).
func (s *encState) sealAll() {
	for len(s.stack) > 1 {
		s.sealTop()
	}
}

// --- zapcore.ObjectEncoder ---

func (s *encState) AddString(k, v string)     { s.addValue(k, s.anyString(v)) }
func (s *encState) AddBool(k string, v bool)  { s.addValue(k, s.anyBool(v)) }
func (s *encState) AddInt(k string, v int)    { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt64(k string, v int64) { s.addValue(k, s.anyInt(v)) }
func (s *encState) AddInt32(k string, v int32) { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt16(k string, v int16) { s.AddInt64(k, int64(v)) }
func (s *encState) AddInt8(k string, v int8)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint(k string, v uint)   { s.AddUint64(k, uint64(v)) }
func (s *encState) AddUint64(k string, v uint64) {
	if v > math.MaxInt64 {
		// OTel attribute model has no uint64 (design §3.3): decimal string.
		s.addValue(k, s.anyString(strconv.FormatUint(v, 10)))
		return
	}
	s.AddInt64(k, int64(v))
}
func (s *encState) AddUint32(k string, v uint32)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint16(k string, v uint16)   { s.AddInt64(k, int64(v)) }
func (s *encState) AddUint8(k string, v uint8)     { s.AddInt64(k, int64(v)) }
func (s *encState) AddUintptr(k string, v uintptr) { s.AddUint64(k, uint64(v)) }
func (s *encState) AddFloat64(k string, v float64) { s.addValue(k, s.anyDouble(v)) }
func (s *encState) AddFloat32(k string, v float32) { s.AddFloat64(k, float64(v)) }
func (s *encState) AddBinary(k string, v []byte)   { s.addValue(k, s.anyBytes(v)) }
func (s *encState) AddByteString(k string, v []byte) {
	// zap semantic: UTF-8 text bytes → string_value (design §3.3).
	s.addValue(k, s.anyString(string(v)))
}
func (s *encState) AddComplex128(k string, v complex128) {
	s.addValue(k, s.anyString(formatComplex(v)))
}
func (s *encState) AddComplex64(k string, v complex64) { s.AddComplex128(k, complex128(v)) }
func (s *encState) AddDuration(k string, v time.Duration) { s.AddInt64(k, v.Nanoseconds()) }
func (s *encState) AddTime(k string, v time.Time)          { s.AddInt64(k, v.UnixNano()) }

func (s *encState) OpenNamespace(k string) { s.openFrame(frameKVList, k) }

func (s *encState) AddObject(k string, m zapcore.ObjectMarshaler) error {
	sn := s.snap()
	s.openFrame(frameKVList, k)
	if err := m.MarshalLogObject(s); err != nil {
		s.rollback(sn)
		return err
	}
	s.sealDownTo(sn.depth)
	return nil
}

func (s *encState) AddArray(k string, m zapcore.ArrayMarshaler) error {
	sn := s.snap()
	s.openFrame(frameArray, k)
	if err := m.MarshalLogArray(arrayEnc{s}); err != nil {
		s.rollback(sn)
		return err
	}
	s.sealDownTo(sn.depth)
	return nil
}

// sealDownTo seals frames (incl. namespaces the marshaler opened and never
// closed — zap permits that) until the stack is back at depth.
func (s *encState) sealDownTo(depth int) {
	for len(s.stack) > depth {
		s.sealTop()
	}
}

func (s *encState) AddReflected(k string, v any) error {
	if sc, ok := spanContextFromValue(v); ok {
		// Trace-context value: consume, never encode (design §3.4). Stored
		// only when a sink is armed (With on a fresh clone / EncodeEntry
		// locals) — receiver-safe by construction (§3.7).
		if s.scSink != nil {
			*s.scSink, *s.scSinkSet = sc, true
		}
		return nil
	}
	sn := s.snap()
	js, err := json.Marshal(v)
	if err != nil {
		s.rollback(sn) // nothing was written; keeps the invariant explicit
		return err
	}
	s.addValue(k, s.anyString(string(js)))
	return nil
}

func formatComplex(v complex128) string {
	// zap JSON convention "a+bi" (strips the parentheses of strconv).
	str := strconv.FormatComplex(v, 'f', -1, 128)
	return str[1 : len(str)-1]
}

// --- zapcore.ArrayEncoder (elements of the current frameArray) ---

type arrayEnc struct{ s *encState }

func (a arrayEnc) AppendBool(v bool)       { a.s.addAV(a.s.anyBool(v)) }
func (a arrayEnc) AppendByteString(v []byte) { a.s.addAV(a.s.anyString(string(v))) }
func (a arrayEnc) AppendComplex128(v complex128) { a.s.addAV(a.s.anyString(formatComplex(v))) }
func (a arrayEnc) AppendComplex64(v complex64)   { a.AppendComplex128(complex128(v)) }
func (a arrayEnc) AppendFloat64(v float64) { a.s.addAV(a.s.anyDouble(v)) }
func (a arrayEnc) AppendFloat32(v float32) { a.AppendFloat64(float64(v)) }
func (a arrayEnc) AppendInt(v int)         { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt64(v int64)     { a.s.addAV(a.s.anyInt(v)) }
func (a arrayEnc) AppendInt32(v int32)     { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt16(v int16)     { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendInt8(v int8)       { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendString(v string)   { a.s.addAV(a.s.anyString(v)) }
func (a arrayEnc) AppendUint(v uint)       { a.AppendUint64(uint64(v)) }
func (a arrayEnc) AppendUint64(v uint64) {
	if v > math.MaxInt64 {
		a.s.addAV(a.s.anyString(strconv.FormatUint(v, 10)))
		return
	}
	a.AppendInt64(int64(v))
}
func (a arrayEnc) AppendUint32(v uint32)   { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUint16(v uint16)   { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUint8(v uint8)     { a.AppendInt64(int64(v)) }
func (a arrayEnc) AppendUintptr(v uintptr) { a.AppendUint64(uint64(v)) }
func (a arrayEnc) AppendDuration(v time.Duration) { a.AppendInt64(v.Nanoseconds()) }
func (a arrayEnc) AppendTime(v time.Time)         { a.AppendInt64(v.UnixNano()) }

func (a arrayEnc) AppendArray(m zapcore.ArrayMarshaler) error {
	sn := a.s.snap()
	a.s.openFrame(frameArray, "")
	if err := m.MarshalLogArray(a); err != nil {
		a.s.rollback(sn)
		return err
	}
	a.s.sealDownTo(sn.depth)
	return nil
}

func (a arrayEnc) AppendObject(m zapcore.ObjectMarshaler) error {
	sn := a.s.snap()
	a.s.openFrame(frameKVList, "")
	if err := m.MarshalLogObject(a.s); err != nil {
		a.s.rollback(sn)
		return err
	}
	a.s.sealDownTo(sn.depth)
	return nil
}

func (a arrayEnc) AppendReflected(v any) error {
	sn := a.s.snap()
	js, err := json.Marshal(v)
	if err != nil {
		a.s.rollback(sn)
		return err
	}
	a.s.addAV(a.s.anyString(string(js)))
	return nil
}

var _ zapcore.ObjectEncoder = (*encState)(nil)
var _ zapcore.ArrayEncoder = arrayEnc{}
```

**Implementation notes for the engineer:**
- `addValue` routes through the array check so a misuse (zap never calls keyed `Add*` while
  an array frame is current) cannot corrupt framing.
- `snap()` captures `{depth, len(cur().buf)}` — counts are unnecessary because proto
  repeated fields carry no count headers (simpler than the msgpack frame stack; design §3.3
  mentions count for generality, this is the concrete form).
- The `anyX` builders share `s.scratch` — every `addValue/addAV` call must consume the
  scratch before the next builder call (they all do: builders are only invoked as arguments).

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v`
Expected: PASS, including the rollback byte-identity tests.

- [ ] **Step 5: Commit**

```bash
git add otlp/attr.go otlp/attr_test.go
git commit -m "feat(otlp): frame-stack proto ObjectEncoder with snapshot/rollback"
```

---

### Task 5: The LogRecord encoder (`otlp/encoder.go`)

**Files:**
- Create: `otlp/encoder.go`
- Modify: `otlp/attr.go` (add `addKVRoot` — meta attributes target the root frame)
- Test: `otlp/encoder_test.go`

- [ ] **Step 1: Add the root-targeting helper to `otlp/attr.go`**

```go
// addKVRoot appends KeyValue{key, av} to the ROOT frame regardless of open
// namespaces — entry-metadata attributes are never namespaced (design §3.5,
// pinned attribute order).
func (s *encState) addKVRoot(key string, av []byte) {
	f := &s.stack[0]
	kvLen := 1 + uvarintLen(uint64(len(key))) + len(key) +
		1 + uvarintLen(uint64(len(av))) + len(av)
	f.buf = append(f.buf, f.entryTag())
	f.buf = appendUvarint(f.buf, uint64(kvLen))
	f.buf = appendTaggedString(f.buf, 0x0a, key)
	f.buf = appendTaggedBytes(f.buf, 0x12, av)
}
```

- [ ] **Step 2: Write the failing tests**

Test helpers reuse Task 1's `findField`/`findVarint` to decode our own records — full
independent validation happens in the conformance module (Task 11).

```go
package otlp

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func testEntry() zapcore.Entry {
	return zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Unix(0, 1),
		Message: "m",
	}
}

func encodeRecord(t *testing.T, e zapcore.Encoder, ent zapcore.Entry, fields ...zapcore.Field) []byte {
	t.Helper()
	buf, err := e.EncodeEntry(ent, fields)
	require.NoError(t, err)
	defer buf.Free()
	return append([]byte(nil), buf.Bytes()...)
}

// attrKeys decodes the top-level attribute keys of a LogRecord, in order.
func attrKeys(t *testing.T, rec []byte) []string {
	t.Helper()
	var keys []string
	b := rec
	for len(b) > 0 {
		tag, n := uvarint(b) // see test helper below
		require.Positive(t, n)
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		switch wt {
		case 0:
			_, vn := uvarint(b)
			b = b[vn:]
		case 1:
			b = b[8:]
		case 2:
			l, ln := uvarint(b)
			payload := b[ln : ln+int(l)]
			if num == 6 {
				key, err := findField(payload, 1)
				require.NoError(t, err)
				keys = append(keys, string(key))
			}
			b = b[ln+int(l):]
		case 5:
			b = b[4:]
		}
	}
	return keys
}

func uvarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b); i++ {
		v |= uint64(b[i]&0x7f) << (7 * i)
		if b[i] < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

func TestEncodeEntryMinimalGolden(t *testing.T) {
	e := NewEncoder()
	rec := encodeRecord(t, e, testEntry())
	want := []byte{
		0x09, 1, 0, 0, 0, 0, 0, 0, 0, // time_unix_nano = 1
		0x10, 0x09, // severity_number = 9 (INFO)
		0x1a, 0x04, 'i', 'n', 'f', 'o', // severity_text
		0x2a, 0x03, 0x0a, 0x01, 'm', // body = AnyValue{"m"}
		0x59, 1, 0, 0, 0, 0, 0, 0, 0, // observed_time_unix_nano = 1
	}
	require.Equal(t, want, rec)
}

// topLevelFieldNums walks a record's top-level proto fields in order.
func topLevelFieldNums(t *testing.T, rec []byte) []int {
	t.Helper()
	var nums []int
	b := rec
	for len(b) > 0 {
		tag, n := uvarint(b)
		require.Positive(t, n)
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		nums = append(nums, num)
		switch wt {
		case 0:
			_, vn := uvarint(b)
			b = b[vn:]
		case 1:
			b = b[8:]
		case 2:
			l, ln := uvarint(b)
			b = b[ln+int(l):]
		case 5:
			b = b[4:]
		default:
			t.Fatalf("unexpected wire type %d", wt)
		}
	}
	return nums
}

func TestEncodeEntryEpochZeroOmitsTimes(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()
	ent.Time = time.Unix(0, 0)
	rec := encodeRecord(t, e, ent)
	nums := topLevelFieldNums(t, rec)
	require.NotContains(t, nums, 1, "time_unix_nano must be omitted at epoch zero")
	require.NotContains(t, nums, 11, "observed_time_unix_nano must be omitted at epoch zero")
}

func TestUnsampledSpanOmitsFlags(t *testing.T) {
	// Valid IDs, TraceFlags 0: trace_id/span_id emitted, flags OMITTED
	// (proto.Marshal drops a zero fixed32 — byte-identity rule).
	scUnsampled := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})
	e := NewEncoder()
	rec := encodeRecord(t, e, testEntry(),
		zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scUnsampled})
	nums := topLevelFieldNums(t, rec)
	require.Contains(t, nums, 9)
	require.Contains(t, nums, 10)
	require.NotContains(t, nums, 8, "zero flags must be omitted")
}

func TestEncodeEntryTraceFields(t *testing.T) {
	sc, ctx := testSpanContext(t)
	e := NewEncoder()

	for _, f := range []zapcore.Field{SpanContext(ctx), zap.Any("context", ctx)} {
		rec := encodeRecord(t, e, testEntry(), f, zap.String("k", "v"))
		tid, err := findField(rec, 9)
		require.NoError(t, err)
		wantTID := sc.TraceID()
		require.Equal(t, wantTID[:], tid)
		sid, err := findField(rec, 10)
		require.NoError(t, err)
		wantSID := sc.SpanID()
		require.Equal(t, wantSID[:], sid)
		// flags: fixed32 tag 0x45 followed by 01 00 00 00 (sampled).
		require.Contains(t, string(rec), string([]byte{0x45, 0x01, 0x00, 0x00, 0x00}))
		// consumed: not an attribute.
		require.Equal(t, []string{"k"}, attrKeys(t, rec))
	}

	// No span → all three omitted.
	rec := encodeRecord(t, e, testEntry(), SpanContext(nil)) //nolint:staticcheck
	tid, err := findField(rec, 9)
	require.NoError(t, err)
	require.Nil(t, tid)
}

func TestTracePrecedenceAndLastWins(t *testing.T) {
	scA, ctxA := testSpanContext(t)
	scB := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})

	// With-stash seeds; per-call overrides (Clone + AddTo simulates the
	// stock-core With path with the EAGER field, which is ReflectType).
	e := NewEncoder().Clone()
	SpanContext(ctxA).AddTo(e.(zapcore.ObjectEncoder))
	rec := encodeRecord(t, e, testEntry())
	tid, _ := findField(rec, 9)
	wantA := scA.TraceID()
	require.Equal(t, wantA[:], tid)

	// Per-call beats the stash; two per-call trace fields → last wins, both consumed.
	fB := zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scB}
	rec = encodeRecord(t, e, testEntry(), SpanContext(ctxA), fB)
	tid, _ = findField(rec, 9)
	wantB := scB.TraceID()
	require.Equal(t, wantB[:], tid)
	require.Empty(t, attrKeys(t, rec))
}

func TestMetaAttributes(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()
	ent.LoggerName = "svc.sub"
	ent.Caller = zapcore.EntryCaller{Defined: true, File: "/a/b.go", Line: 7, Function: "pkg.F"}
	ent.Stack = "stack..."
	rec := encodeRecord(t, e, ent, zap.String("k", "v"))
	require.Equal(t,
		[]string{"logger", "code.function.name", "code.file.path", "code.line.number", "code.stacktrace", "k"},
		attrKeys(t, rec))

	// Disabled variants.
	e = NewEncoder(WithCallerAttributes(false), WithLoggerNameKey(""))
	rec = encodeRecord(t, e, ent, zap.String("k", "v"))
	require.Equal(t, []string{"code.stacktrace", "k"}, attrKeys(t, rec))
}

func TestMetaStaysAtRootUnderWithNamespace(t *testing.T) {
	// With opens a namespace; meta attrs must still land at the root.
	e := NewEncoder().Clone()
	zap.Namespace("req").AddTo(e.(zapcore.ObjectEncoder))
	ent := testEntry()
	ent.LoggerName = "L"
	rec := encodeRecord(t, e, ent, zap.String("in", "ns"))
	require.Equal(t, []string{"logger", "req"}, attrKeys(t, rec))
}

func TestFieldDegradation(t *testing.T) {
	e := NewEncoder()
	ent := testEntry()

	// Failing object marshaler — entry ships, <key>Error attr, no partial bytes.
	recBad := encodeRecord(t, e, ent, zap.String("good", "1"), zap.Object("bad", failObj{partial: true}))
	require.Equal(t, []string{"good", "badError"}, attrKeys(t, recBad))

	// Failing zap.Inline (empty key → "Error").
	recInline := encodeRecord(t, e, ent, zap.String("good", "1"), zap.Inline(failObj{partial: true}))
	require.Equal(t, []string{"good", "Error"}, attrKeys(t, recInline))

	// zap.Any with an unmarshalable channel.
	recChan := encodeRecord(t, e, ent, zap.Any("ch", make(chan int)))
	require.Equal(t, []string{"chError"}, attrKeys(t, recChan))
}

// verboseErr implements fmt.Formatter so zap's encodeError emits <key>Verbose.
type verboseErr struct{ msg string }

func (e verboseErr) Error() string { return e.msg }
func (e verboseErr) Format(s fmt.State, verb rune) {
	if verb == 'v' && s.Flag('+') {
		fmt.Fprintf(s, "%s\nwith stack", e.msg)
		return
	}
	fmt.Fprint(s, e.msg)
}

func TestErrorFieldExpansion(t *testing.T) {
	e := NewEncoder()

	// Plain error → single string attribute under the field key.
	rec := encodeRecord(t, e, testEntry(), zap.Error(errors.New("boom")))
	require.Equal(t, []string{"error"}, attrKeys(t, rec))

	// fmt.Formatter error → zap's encodeError adds <key>Verbose (design §3.3:
	// errors delegate to zap's standard expansion through the ObjectEncoder).
	rec = encodeRecord(t, e, testEntry(), zap.Error(verboseErr{msg: "boom"}))
	require.Equal(t, []string{"error", "errorVerbose"}, attrKeys(t, rec))

	// Grouped errors (multierr) → <key>Causes array attribute.
	rec = encodeRecord(t, e, testEntry(),
		zap.Error(multierr.Combine(errors.New("a"), errors.New("b"))))
	require.Equal(t, []string{"error", "errorCauses"}, attrKeys(t, rec))
}

func TestEncoderConcurrency(t *testing.T) {
	e := NewEncoder()
	core := zapcore.NewCore(e, zapcore.AddSync(io.Discard), zapcore.DebugLevel)
	logger := zap.New(core)
	_, ctx := testSpanContext(t)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := logger.With(SpanContext(ctx), zap.Namespace("ns"), zap.String("w", "1"))
			for j := 0; j < 200; j++ {
				logger.Info("shared", zap.Int("j", j))
				l.Info("cloned", zap.Int("j", j))
			}
		}()
	}
	wg.Wait()
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestEncode|TestTrace|TestMeta|TestField|TestError|TestEncoderConcurrency' -v`
Expected: FAIL — `undefined: NewEncoder`.

- [ ] **Step 4: Implement `otlp/encoder.go`** (plus a temporary `NewEncoder` — promoted to
`core.go` in Task 6; keep it here for now and MOVE it in Task 6 Step 3)

```go
package otlp

import (
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var recordPool = buffer.NewPool()

type encConfig struct {
	severityOf    func(zapcore.Level) SeverityNumber
	callerAttrs   bool
	loggerNameKey string
}

// encoder is the OTLP zapcore.Encoder: EncodeEntry emits the bare LogRecord
// message bytes for one entry (design §3). The embedded encState is the
// persistent With-field state AND the ObjectEncoder surface that stock
// ioCore.With dispatches into.
type encoder struct {
	encState
	cfg   encConfig
	sc    trace.SpanContext // With-stash; written only on fresh clones (§3.7)
	scSet bool
}

var _ zapcore.Encoder = (*encoder)(nil)

func newEncoder(o options) *encoder {
	e := &encoder{
		encState: *newEncState(),
		cfg: encConfig{
			severityOf:    o.severityOf,
			callerAttrs:   o.callerAttrs,
			loggerNameKey: o.loggerNameKey,
		},
	}
	e.armSink()
	return e
}

// armSink points the embedded state's trace sink at THIS encoder's stash so
// the AddReflected hook (eager-helper fields through stock ioCore.With)
// stashes on the right struct. Must run after every clone.
func (e *encoder) armSink() {
	e.encState.scSink = &e.sc
	e.encState.scSinkSet = &e.scSet
}

func (e *encoder) Clone() zapcore.Encoder { return e.cloneTyped() }

func (e *encoder) cloneTyped() *encoder {
	c := &encoder{encState: *e.encState.clone(), cfg: e.cfg, sc: e.sc, scSet: e.scSet}
	c.armSink()
	return c
}

func (e *encoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	// Per-call working copy: the receiver is never mutated (§3.7).
	work := e.encState.clone()
	defer work.free()

	// Trace resolution: With-stash seeds; per-call fields override (last
	// wins). Trace values surfacing through nested marshalers reach the
	// per-call locals via the sink.
	sc, scSet := e.sc, e.scSet
	work.scSink, work.scSinkSet = &sc, &scSet

	// Entry-metadata attributes → ROOT frame (pinned order: With, meta, call).
	e.addMeta(work, ent)

	for i := range fields {
		if fsc, ok := spanContextFromField(fields[i]); ok {
			sc, scSet = fsc, true
			continue
		}
		applyField(work, fields[i])
	}
	work.sealAll()

	rec := getFrameBuf()
	defer putFrameBuf(rec)
	if t := ent.Time.UnixNano(); t != 0 {
		rec = appendTaggedFixed64(rec, 0x09, uint64(t))
	}
	if sev := clampSeverity(e.cfg.severityOf(ent.Level)); sev != SeverityUnspecified {
		rec = appendTaggedUvarint(rec, 0x10, uint64(sev))
	}
	if txt := ent.Level.String(); txt != "" {
		rec = appendTaggedString(rec, 0x1a, txt)
	}
	// body: AnyValue{string_value: Message}; a set oneof is always emitted.
	bodyLen := 1 + uvarintLen(uint64(len(ent.Message))) + len(ent.Message)
	rec = append(rec, 0x2a)
	rec = appendUvarint(rec, uint64(bodyLen))
	rec = appendTaggedString(rec, 0x0a, ent.Message)
	// attributes: root frame holds already-tagged 0x32 entries.
	rec = append(rec, work.stack[0].buf...)
	if scSet && sc.IsValid() {
		// flags == 0 (unsampled) is OMITTED: proto.Marshal drops a zero
		// fixed32, and conformance demands byte identity (plan cheat sheet).
		if f := uint32(sc.TraceFlags()); f != 0 {
			rec = appendTaggedFixed32(rec, 0x45, f)
		}
		tid, sid := sc.TraceID(), sc.SpanID()
		rec = appendTaggedBytes(rec, 0x4a, tid[:])
		rec = appendTaggedBytes(rec, 0x52, sid[:])
	}
	if t := ent.Time.UnixNano(); t != 0 {
		rec = appendTaggedFixed64(rec, 0x59, uint64(t)) // observed == time (§3.1)
	}

	out := recordPool.Get()
	_, _ = out.Write(rec)
	return out, nil
}

// applyField dispatches one zap field into the state, transactionally.
// zap's Field.AddTo calls MarshalLogObject(enc) DIRECTLY for
// InlineMarshalerType — no child frame, no error-to-Error conversion until
// after partial bytes are written (zapcore/field.go:122-124,183-185) — so the
// snapshot/rollback must wrap the dispatch here (design §3.3, pass-2 P0).
// Every other fallible type is transactional inside encState's own methods.
// Shared by EncodeEntry, the custom core's With, and resource-field encoding.
func applyField(work *encState, f zapcore.Field) {
	if f.Type == zapcore.InlineMarshalerType {
		sn := work.snap()
		if err := f.Interface.(zapcore.ObjectMarshaler).MarshalLogObject(work); err != nil {
			work.rollback(sn)
			// Mirror zap's convention exactly: <key>Error (bare "Error" for
			// zap.Inline's empty key), added AFTER rollback.
			work.AddString(f.Key+"Error", err.Error())
		}
		return
	}
	f.AddTo(work)
}

// addMeta appends entry-metadata attributes to the ROOT frame (design §3.5).
func (e *encoder) addMeta(work *encState, ent zapcore.Entry) {
	if k := e.cfg.loggerNameKey; k != "" && ent.LoggerName != "" {
		work.addKVRoot(k, work.anyString(ent.LoggerName))
	}
	if e.cfg.callerAttrs && ent.Caller.Defined {
		if fn := ent.Caller.Function; fn != "" {
			work.addKVRoot("code.function.name", work.anyString(fn))
		}
		work.addKVRoot("code.file.path", work.anyString(ent.Caller.File))
		work.addKVRoot("code.line.number", work.anyInt(int64(ent.Caller.Line)))
	}
	if ent.Stack != "" {
		work.addKVRoot("code.stacktrace", work.anyString(ent.Stack))
	}
}

// NewEncoder builds the OTLP zapcore.Encoder. Only encoder-end options take
// effect (design §3.6: no zapcore.EncoderConfig — every slot it would
// configure is structurally defined by the OTLP data model).
func NewEncoder(opts ...Option) zapcore.Encoder {
	return newEncoder(applyOptions(opts))
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cd otlp && go test ./... -v && go test -race -run TestEncoderConcurrency ./...`
Expected: PASS, race detector clean.

- [ ] **Step 6: Commit**

```bash
git add otlp/encoder.go otlp/encoder_test.go otlp/attr.go otlp/attr_test.go
git commit -m "feat(otlp): LogRecord zapcore.Encoder with trace-context interception"
```

---

### Task 6: The custom core (`otlp/core.go`)

The §2.2 pre-scanning core — the only way sticky `zap.Any("context", ctx)` can work.
`NewCore` itself lands in Task 9 (it needs the Writer); this task builds and fully tests the
core type against an in-memory sink, and moves `NewEncoder` here from Task 5.

**Files:**
- Create: `otlp/core.go`
- Modify: `otlp/encoder.go` (remove `NewEncoder` — it moves to `core.go`)
- Test: `otlp/core_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// memSink captures every record written through the core.
type memSink struct {
	mu   sync.Mutex
	recs [][]byte
}

func (m *memSink) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, append([]byte(nil), p...))
	return len(p), nil
}
func (m *memSink) Sync() error { return nil }

func (m *memSink) last(t *testing.T) []byte {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.recs)
	return m.recs[len(m.recs)-1]
}

func newTestCore(t *testing.T, opts ...Option) (zapcore.Core, *memSink) {
	t.Helper()
	sink := &memSink{}
	enc := newEncoder(applyOptions(opts))
	return newOTLPCore(enc, sink, zapcore.DebugLevel), sink
}

func traceIDOf(t *testing.T, rec []byte) []byte {
	t.Helper()
	tid, err := findField(rec, 9)
	require.NoError(t, err)
	return tid
}

func TestStickyCtxThroughOTLPCore(t *testing.T) {
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core)

	// THE headline behavior: sticky zap.Any("context", ctx) works because the
	// custom core's With pre-scans raw fields (design §2.2).
	logger.With(zap.Any("context", ctx)).Info("sticky")
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
	require.Empty(t, attrKeys(t, sink.last(t))) // consumed, not an attribute

	// Eager helper, sticky.
	logger.With(SpanContext(ctx)).Info("eager-sticky")
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))

	// Per-call on the plain logger.
	logger.Info("per-call", zap.Any("context", ctx))
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
}

func TestStickyCtxDegradesOnStockCore(t *testing.T) {
	sc, ctx := testSpanContext(t)
	sink := &memSink{}
	stock := zapcore.NewCore(NewEncoder(), sink, zapcore.DebugLevel)
	logger := zap.New(stock)

	// Documented §2.2 degradation: ioCore.With stringifies the ctx
	// (StringerType) — junk string attribute, NO trace fields.
	logger.With(zap.Any("context", ctx)).Info("degraded")
	rec := sink.last(t)
	require.Nil(t, traceIDOf(t, rec))
	require.Equal(t, []string{"context"}, attrKeys(t, rec))

	// But the eager helper IS sticky on a stock core (ReflectType →
	// AddReflected hook), and per-call ctx works everywhere.
	logger.With(SpanContext(ctx)).Info("eager")
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
	logger.Info("per-call", zap.Any("context", ctx))
	require.Equal(t, wantTID[:], traceIDOf(t, sink.last(t)))
}

func TestWithRemainingFieldsStillApply(t *testing.T) {
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core).With(zap.Any("context", ctx), zap.String("req", "42"))
	logger.Info("m", zap.String("k", "v"))
	require.Equal(t, []string{"req", "k"}, attrKeys(t, sink.last(t)))
}

func TestInjectTraceFieldsThroughLogger(t *testing.T) {
	// Design §9: the structural injector through the plain Logger path.
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	logger := zap.New(core)

	logger.Info("m", InjectTraceFields(ctx, zap.String("k", "v"))...)
	rec := sink.last(t)
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, rec))
	require.Equal(t, []string{"k"}, attrKeys(t, rec)) // helper field consumed

	// No-span variant stays branch-free: field appended, encoder omits.
	logger.Info("m", InjectTraceFields(context.Background())...)
	require.Nil(t, traceIDOf(t, sink.last(t)))
}

func TestSugaredInjectKVs(t *testing.T) {
	sc, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	sugar := zap.New(core).Sugar()

	sugar.Infow("m", InjectTraceKVs(ctx, "url", "https://x")...)
	rec := sink.last(t)
	wantTID := sc.TraceID()
	require.Equal(t, wantTID[:], traceIDOf(t, rec))
	require.Equal(t, []string{"url"}, attrKeys(t, rec))

	// Lone injected field, zero kvs: no dangling-key noise.
	sugar.Infow("m", InjectTraceKVs(ctx)...)
	require.Empty(t, attrKeys(t, sink.last(t)))
}

func TestTeeRendersEagerHelperLegibly(t *testing.T) {
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)

	var jsonOut bytes.Buffer
	jsonCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&jsonOut), zapcore.DebugLevel)

	logger := zap.New(zapcore.NewTee(core, jsonCore))
	logger.Info("teed", SpanContext(ctx))

	// OTLP side: structural, consumed.
	require.NotNil(t, traceIDOf(t, sink.last(t)))
	require.Empty(t, attrKeys(t, sink.last(t)))
	// JSON side: rendered under the documented key (trace.SpanContext
	// implements json.Marshaler).
	var m map[string]any
	require.NoError(t, json.Unmarshal(jsonOut.Bytes(), &m))
	require.Contains(t, m, "span_context")
}

func TestStockCoreWithInlineFailurePinsZapBehavior(t *testing.T) {
	// Documented caveat (doc.go / design §3.3 scope note): behind a STOCK
	// zapcore.NewCore, a failing zap.Inline applied via With follows zap's
	// standard partial-write behavior (ioCore.With dispatches inline
	// marshalers directly into the encoder — no interception point; zap's
	// own encoders behave identically). This test PINS that documented
	// degradation; on otlp.NewCore the same shape rolls back cleanly.
	stockSink := &memSink{}
	stock := zapcore.NewCore(NewEncoder(), stockSink, zapcore.DebugLevel)
	zap.New(stock).With(zap.Inline(failObj{partial: true})).Info("m")
	stockKeys := attrKeys(t, stockSink.last(t))
	require.Contains(t, stockKeys, "written", "zap-standard partial write persists on stock core")

	core, sink := newTestCore(t)
	zap.New(core).With(zap.Inline(failObj{partial: true})).Info("m")
	require.Equal(t, []string{"Error"}, attrKeys(t, sink.last(t)),
		"otlp core rolls back and keeps only the Error attribute")
}

func TestCorrelationFieldsAreAttributesOnOTLPCore(t *testing.T) {
	// Pins the documented §3.4 caveat.
	_, ctx := testSpanContext(t)
	core, sink := newTestCore(t)
	zap.New(core).Info("flat", TraceCorrelationFields(ctx)...)
	rec := sink.last(t)
	require.Nil(t, traceIDOf(t, rec)) // NOT proto-field correlation
	require.Equal(t, []string{"trace_id", "span_id"}, attrKeys(t, rec))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestSticky|TestWithRemaining|TestSugared|TestTee|TestCorrelationFields' -v`
Expected: FAIL — `undefined: newOTLPCore`.

- [ ] **Step 3: Implement `otlp/core.go`** (and delete `NewEncoder` from `encoder.go`)

```go
package otlp

import "go.uber.org/zap/zapcore"

// core is ioCore plus one behavior: With pre-scans the RAW field slice for
// trace context before zap's Field.AddTo dispatch can erase it — zap.Any
// classifies stdlib contexts as StringerType, so an encoder behind the stock
// zapcore.NewCore only ever sees the stringified context (design §2.2). The
// official contrib bridge intercepts in its own Core for the same reason.
type core struct {
	zapcore.LevelEnabler
	enc *encoder
	out zapcore.WriteSyncer
}

func newOTLPCore(enc *encoder, ws zapcore.WriteSyncer, level zapcore.LevelEnabler) zapcore.Core {
	return &core{LevelEnabler: level, enc: enc, out: ws}
}

func (c *core) With(fields []zapcore.Field) zapcore.Core {
	clone := &core{LevelEnabler: c.LevelEnabler, enc: c.enc.cloneTyped(), out: c.out}
	for i := range fields {
		if sc, ok := spanContextFromField(fields[i]); ok {
			// Stash on the fresh clone — never mutated after this loop (§3.7).
			clone.enc.sc, clone.enc.scSet = sc, true
			continue
		}
		applyField(&clone.enc.encState, fields[i]) // transactional incl. zap.Inline
	}
	return clone
}

func (c *core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *core) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	buf, err := c.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	_, werr := c.out.Write(buf.Bytes())
	buf.Free()
	if werr != nil {
		return werr
	}
	if ent.Level > zapcore.ErrorLevel {
		// Mirror ioCore: best-effort sync on Panic/Fatal so the batch flushes
		// before the process dies.
		_ = c.Sync()
	}
	return nil
}

func (c *core) Sync() error { return c.out.Sync() }

// NewEncoder builds the OTLP zapcore.Encoder. Only encoder-end options take
// effect (design §3.6). Pair with NewWriter for a BYO-core setup — but note
// the §2.2 compatibility matrix: behind a stock zapcore.NewCore, sticky
// zap.Any("context", ctx) degrades to a stringified attribute; use NewCore
// or the eager SpanContext helper.
func NewEncoder(opts ...Option) zapcore.Encoder {
	return newEncoder(applyOptions(opts))
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v && go test -race ./...`
Expected: PASS, race-clean.

- [ ] **Step 5: Commit**

```bash
git add otlp/core.go otlp/core_test.go otlp/encoder.go
git commit -m "feat(otlp): custom core with raw-field With pre-scan for trace context"
```

---

### Task 7: Envelope assembly (`otlp/envelope.go`)

Resource/Scope proto bytes precomputed once; per-batch wrapping is pure size arithmetic +
appends (design §4). Key trick: a `frameKVList` frame's entries are tagged `0x0a` — exactly
the `Resource.attributes` (field 1) encoding — so the Resource message bytes ARE the kvlist
frame contents.

**Files:**
- Create: `otlp/envelope.go`
- Test: `otlp/envelope_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEnvelopeGolden(t *testing.T) {
	o := applyOptions([]Option{WithServiceName("s"), WithScopeName("n"), WithScopeVersion("")})
	env := newEnvelope(o)

	// Resource = repeated KeyValue{service.name:"s"} tagged 0x0a:
	// kv body: 0x0a 0x0c "service.name" 0x12 0x03 0x0a 0x01 's' → len 19
	wantRes := append([]byte{0x0a, 0x13, 0x0a, 0x0c}, []byte("service.name")...)
	wantRes = append(wantRes, 0x12, 0x03, 0x0a, 0x01, 's')
	require.Equal(t, wantRes, env.resourceBlob)
	// Scope = name only: 0x0a 0x01 'n'
	require.Equal(t, []byte{0x0a, 0x01, 'n'}, env.scopeBlob)

	rec := []byte{0x10, 0x09} // minimal LogRecord{severity_number:9}
	got := env.assemble(nil, [][]byte{rec})

	// Inside-out: scopeLogs = scope(0x0a len blob) + records(0x12 len rec)
	scopeLogs := append([]byte{0x0a, 0x03, 0x0a, 0x01, 'n'}, 0x12, 0x02, 0x10, 0x09)
	// resourceLogs = resource(0x0a len res) + scope_logs(0x12 len scopeLogs)
	resourceLogs := append([]byte{0x0a, byte(len(wantRes))}, wantRes...)
	resourceLogs = append(resourceLogs, 0x12, byte(len(scopeLogs)))
	resourceLogs = append(resourceLogs, scopeLogs...)
	// request = resource_logs(0x0a len resourceLogs)
	want := append([]byte{0x0a, byte(len(resourceLogs))}, resourceLogs...)
	require.Equal(t, want, got)

	// Exact size arithmetic.
	require.Equal(t, len(got), env.sizeFor(env.recordCost(len(rec))))
}

func TestEnvelopeSizeForMultiByteVarints(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{WithServiceName("svc")}))
	// Cross the 127/128 and 16383/16384 varint length boundaries.
	for _, recLen := range []int{1, 100, 127, 128, 1000, 16383, 16384, 100000} {
		rec := bytes.Repeat([]byte{0x00}, recLen) // content irrelevant for sizing
		records := [][]byte{rec, rec[:recLen/2+1]}
		tagged := 0
		for _, r := range records {
			tagged += env.recordCost(len(r))
		}
		got := env.assemble(nil, records)
		require.Equal(t, len(got), env.sizeFor(tagged), "recLen=%d", recLen)
	}
}

func TestEnvelopeResourceFields(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{
		WithServiceName("s"),
		WithResource(zap.String("env", "prod"), zap.Int("shard", 3)),
	}))
	// service.name first, then WithResource fields in order: assert keys via
	// the kvlist structure (each entry 0x0a len KeyValue).
	keys := []string{}
	b := env.resourceBlob
	for len(b) > 0 {
		l := int(b[1]) // single-byte lens in this test
		kv := b[2 : 2+l]
		k, err := findField(kv, 1)
		require.NoError(t, err)
		keys = append(keys, string(k))
		b = b[2+l:]
	}
	require.Equal(t, []string{"service.name", "env", "shard"}, keys)
}

func TestEnvelopeEmptyScopeOmitted(t *testing.T) {
	env := newEnvelope(applyOptions([]Option{WithServiceName("s"), WithScopeName(""), WithScopeVersion("")}))
	require.Empty(t, env.scopeBlob)
	got := env.assemble(nil, [][]byte{{0x10, 0x09}})
	// ScopeLogs must contain ONLY log_records (no 0x0a scope part).
	resourceLogs, err := findField(got, 1)
	require.NoError(t, err)
	scopeLogs, err := findField(resourceLogs, 2)
	require.NoError(t, err)
	require.Equal(t, byte(0x12), scopeLogs[0])
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run TestEnvelope -v`
Expected: FAIL — `undefined: newEnvelope`.

- [ ] **Step 3: Implement `otlp/envelope.go`**

```go
package otlp

// envelope wraps batches of bare LogRecord payloads into an
// ExportLogsServiceRequest. Resource and InstrumentationScope bytes are
// immutable per writer, built once at construction (design §4).
type envelope struct {
	resourceBlob []byte // Resource message bytes (attributes only)
	scopeBlob    []byte // InstrumentationScope message bytes; empty = omit scope
}

func newEnvelope(o options) *envelope {
	// Resource.attributes (field 1) entries are tagged 0x0a — identical to
	// KeyValueList.values — so a kvlist frame builds them directly.
	s := newEncState()
	s.openFrame(frameKVList, "")
	depth := len(s.stack)
	s.AddString("service.name", o.serviceName)
	for i := range o.resourceFields {
		applyField(s, o.resourceFields[i]) // transactional incl. zap.Inline
	}
	s.sealDownTo(depth) // close namespaces a WithResource field may have opened
	res := append([]byte(nil), s.cur().buf...)
	s.free()

	var scope []byte
	if o.scopeName != "" {
		scope = appendTaggedString(scope, 0x0a, o.scopeName)
	}
	if o.scopeVersion != "" {
		scope = appendTaggedString(scope, 0x12, o.scopeVersion)
	}
	return &envelope{resourceBlob: res, scopeBlob: scope}
}

// recordCost is the tagged size of one record inside ScopeLogs.log_records.
func (e *envelope) recordCost(recLen int) int {
	return 1 + uvarintLen(uint64(recLen)) + recLen
}

// scopePartLen is the tagged size of the scope field inside ScopeLogs (0 when omitted).
func (e *envelope) scopePartLen() int {
	if len(e.scopeBlob) == 0 {
		return 0
	}
	return 1 + uvarintLen(uint64(len(e.scopeBlob))) + len(e.scopeBlob)
}

// sizeFor returns the exact request size for records whose recordCost sum is
// taggedRecords. Used for byte-aware batch cutting and the Write-time
// oversized-record guard (design §5.1/§5.2).
func (e *envelope) sizeFor(taggedRecords int) int {
	scopeLogs := e.scopePartLen() + taggedRecords
	resourceLogs := 1 + uvarintLen(uint64(len(e.resourceBlob))) + len(e.resourceBlob) +
		1 + uvarintLen(uint64(scopeLogs)) + scopeLogs
	return 1 + uvarintLen(uint64(resourceLogs)) + resourceLogs
}

// assemble appends the full ExportLogsServiceRequest to dst.
func (e *envelope) assemble(dst []byte, records [][]byte) []byte {
	tagged := 0
	for _, r := range records {
		tagged += e.recordCost(len(r))
	}
	scopeLogsLen := e.scopePartLen() + tagged
	resourceLogsLen := 1 + uvarintLen(uint64(len(e.resourceBlob))) + len(e.resourceBlob) +
		1 + uvarintLen(uint64(scopeLogsLen)) + scopeLogsLen

	dst = append(dst, 0x0a) // ExportLogsServiceRequest.resource_logs
	dst = appendUvarint(dst, uint64(resourceLogsLen))
	dst = appendTaggedBytes(dst, 0x0a, e.resourceBlob) // ResourceLogs.resource
	dst = append(dst, 0x12)                            // ResourceLogs.scope_logs
	dst = appendUvarint(dst, uint64(scopeLogsLen))
	if len(e.scopeBlob) != 0 {
		dst = appendTaggedBytes(dst, 0x0a, e.scopeBlob) // ScopeLogs.scope
	}
	for _, r := range records {
		dst = appendTaggedBytes(dst, 0x12, r) // ScopeLogs.log_records
	}
	return dst
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add otlp/envelope.go otlp/envelope_test.go
git commit -m "feat(otlp): ExportLogsServiceRequest envelope assembly"
```

---

### Task 8: HTTP shipping core — attempt, classification, retry loop (`otlp/writer.go`)

The Writer struct and its export path, tested directly against `httptest` — the flush
goroutine and lifecycle land in Task 9.

**Files:**
- Create: `otlp/writer.go`
- Test: `otlp/writer_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fastRetry keeps tests quick while exercising the real loop.
var fastRetry = RetryConfig{Initial: time.Millisecond, MaxInterval: 4 * time.Millisecond, MaxElapsed: 100 * time.Millisecond}

// newTestWriter builds an un-started Writer (no flush goroutine) pointed at
// url. Defaults are PREPENDED so caller-supplied options (e.g. a custom
// WithRetry) win — options apply in slice order.
func newTestWriter(t *testing.T, url string, opts ...Option) (*Writer, *[]error) {
	t.Helper()
	var errs []error
	opts = append([]Option{
		WithRetry(fastRetry),
		WithErrorHandler(func(e error) { errs = append(errs, e) }),
	}, opts...)
	w, err := newWriterCore(url, applyOptions(opts))
	require.NoError(t, err)
	t.Cleanup(w.cancel)
	return w, &errs
}

func TestExportSuccess(t *testing.T) {
	var gotBody []byte
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, errs := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Empty(t, *errs)
	require.Zero(t, w.DroppedLogs())
	require.Equal(t, "application/x-protobuf", gotCT)
	require.Equal(t, w.env.sizeFor(w.env.recordCost(2)), len(gotBody))
}

func TestExportRetriesThenSucceeds(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Equal(t, int32(3), hits.Load())
	require.Zero(t, w.DroppedLogs())
}

func TestExport400NeverRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	w, errs := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}, {0x10, 0x05}}, true)
	require.Equal(t, int32(1), hits.Load())
	require.Equal(t, uint64(2), w.DroppedLogs()) // whole batch counted
	require.Len(t, *errs, 1)
	var ee *ExportError
	require.ErrorAs(t, (*errs)[0], &ee)
	require.Equal(t, 400, ee.StatusCode)
	require.False(t, ee.Retryable)
}

func TestExportRetryBudgetExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	w, errs := newTestWriter(t, srv.URL)
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
	require.NotEmpty(t, *errs)
}

func TestExportRetryAfterBeyondBudgetGivesUp(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rw.Header().Set("Retry-After", "3600")
		rw.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Equal(t, int32(1), hits.Load(), "must give up immediately")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestExportNoRetryWhenDisallowed(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, false) // Close-drain mode (§5.4)
	require.Equal(t, int32(1), hits.Load())
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestExportCancelledDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Retry-After", "30") // would sleep 30s
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL, WithRetry(RetryConfig{Initial: time.Millisecond, MaxInterval: time.Second, MaxElapsed: time.Hour}))
	go func() {
		time.Sleep(20 * time.Millisecond)
		w.cancel()
	}()
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Less(t, time.Since(start), 5*time.Second, "Close must abort the backoff sleep")
	require.Equal(t, uint64(1), w.DroppedLogs()) // counted exactly once (§5.4)
}

func TestPartialSuccessHandling(t *testing.T) {
	// rejected=2 + message → drops counted, handler notified, NO retry.
	respRejected := []byte{0x0a, 0x07, 0x08, 0x02, 0x12, 0x03, 'b', 'a', 'd'}
	// rejected=0 + message → warning only.
	respWarning := []byte{0x0a, 0x05, 0x12, 0x03, 'h', 'm', 'm'}

	for _, tc := range []struct {
		name     string
		resp     []byte
		dropped  uint64
		warning  bool
	}{
		{"rejected", respRejected, 2, false},
		{"warning", respWarning, 0, true},
		{"clean", nil, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(tc.resp)
			}))
			defer srv.Close()

			w, errs := newTestWriter(t, srv.URL)
			w.export([][]byte{{0x10, 0x09}, {0x10, 0x05}, {0x10, 0x0d}}, true)
			require.Equal(t, int32(1), hits.Load(), "partial success must NOT retry")
			require.Equal(t, tc.dropped, w.DroppedLogs())
			if tc.dropped > 0 || tc.warning {
				require.Len(t, *errs, 1)
				var ee *ExportError
				require.ErrorAs(t, (*errs)[0], &ee)
				require.Equal(t, tc.warning, ee.Warning)
			} else {
				require.Empty(t, *errs)
			}
		})
	}
}

func TestGzipAndHeaders(t *testing.T) {
	var gotEnc, gotKey string
	var plain []byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotEnc = r.Header.Get("Content-Encoding")
		gotKey = r.Header.Get("x-api-key")
		zr, err := gzip.NewReader(r.Body)
		if err == nil {
			plain, _ = io.ReadAll(zr)
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL, WithCompression(Gzip), WithHeaders(map[string]string{"x-api-key": "k"}))
	w.export([][]byte{{0x10, 0x09}}, true)
	require.Equal(t, "gzip", gotEnc)
	require.Equal(t, "k", gotKey)
	require.Equal(t, w.env.sizeFor(w.env.recordCost(2)), len(plain))
}

func TestParseRetryAfter(t *testing.T) {
	require.Equal(t, 7*time.Second, parseRetryAfter("7"))
	require.Zero(t, parseRetryAfter(""))
	require.Zero(t, parseRetryAfter("garbage"))
	// HTTP-date in the future (~1 minute): allow generous slack.
	d := parseRetryAfter(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
	require.Greater(t, d, 30*time.Second)
	// HTTP-date in the past → 0.
	require.Zero(t, parseRetryAfter(time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)))
}
```

(The `context` import in the test file is unnecessary — only `writer.go` itself needs it.)

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestExport|TestPartial|TestGzip|TestParseRetryAfter' -v`
Expected: FAIL — `undefined: newWriterCore`.

- [ ] **Step 3: Implement the shipping core in `otlp/writer.go`**

```go
package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/zapcore"
)

// Writer is the OTLP/HTTP exporter: a zapcore.WriteSyncer with a bounded
// queue, a single flush goroutine, and OTLP retry semantics (design §5).
// Async-only — Sync() is the flush barrier.
type Writer struct {
	endpoint    string
	client      *http.Client
	headers     map[string]string
	compression Compression
	timeout     time.Duration
	retry       RetryConfig
	maxBytes    int
	batchSize   int
	flushEvery  time.Duration
	dropPolicy  DropPolicy
	errFn       func(error)
	env         *envelope

	queue    chan []byte
	flushReq chan chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc

	// admit closes the closed-check→enqueue window: Write holds it shared
	// across check+send; Close takes it exclusively after setting closed, so
	// no enqueue can land after the final drain starts (the root writer's
	// lifecycle-barrier discipline, writer.go:175-188,373-384).
	admit sync.RWMutex

	dropped   atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
}

var _ zapcore.WriteSyncer = (*Writer)(nil)

// newWriterCore builds a Writer WITHOUT starting the flush goroutine
// (NewWriter starts it; tests drive export directly).
func newWriterCore(endpoint string, o options) (*Writer, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		endpoint:    ep,
		client:      o.client,
		headers:     o.headers,
		compression: o.compression,
		timeout:     o.timeout,
		retry:       o.retry,
		maxBytes:    o.maxRequestBytes,
		batchSize:   o.batchSize,
		flushEvery:  o.flushInterval,
		dropPolicy:  o.dropPolicy,
		errFn:       o.errFn,
		env:         newEnvelope(o),
		queue:       make(chan []byte, o.queueSize),
		flushReq:    make(chan chan struct{}),
		done:        make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w, nil
}

// DroppedLogs reports records not confirmed delivered (queue overflow,
// retry exhaustion, partial-success rejections, cancelled batches).
func (w *Writer) DroppedLogs() uint64 { return w.dropped.Load() }

func (w *Writer) drop(n int, err *ExportError) {
	w.dropped.Add(uint64(n))
	if err != nil {
		w.errFn(err)
	}
}

// export ships one batch with OTLP retry semantics (§5.3). allowRetry=false
// is the Close-drain single-attempt mode (§5.4). Counts the whole batch as
// dropped on failure.
func (w *Writer) export(records [][]byte, allowRetry bool) {
	if len(records) == 0 {
		return
	}
	raw := w.env.assemble(getFrameBuf(), records)
	defer putFrameBuf(raw)
	body := raw
	compressed := false
	if w.compression == Gzip {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		if _, err := zw.Write(raw); err == nil && zw.Close() == nil {
			body, compressed = gz.Bytes(), true
		} else {
			// gzip failure: ship uncompressed rather than lose the batch.
			w.errFn(&ExportError{Message: "gzip failed; sent uncompressed"})
		}
	}

	deadline := time.Now().Add(w.retry.MaxElapsed)
	delay := w.retry.Initial
	for {
		expErr := w.attempt(body, compressed)
		if expErr == nil {
			return
		}
		if !allowRetry || !expErr.Retryable || w.ctx.Err() != nil {
			w.drop(len(records), expErr)
			return
		}
		d := delay/2 + rand.N(delay/2+1) // jitter in [delay/2, delay]
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

// attempt performs one POST. Its context is Background+timeout — NOT the
// lifecycle ctx — so Close never cancels a request the server may have
// already accepted (§5.4); Close interrupts the backoff sleeps instead.
func (w *Writer) attempt(body []byte, compressed bool) *ExportError {
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint, bytes.NewReader(body))
	if err != nil {
		return &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range w.headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return &ExportError{Err: err} // transport errors are non-retryable (§5.3)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		rejected, msg, derr := decodePartialSuccess(respBody)
		switch {
		case derr != nil:
			// Accepted; the malformed response body is observability-only.
			w.errFn(&ExportError{StatusCode: 200, Err: derr})
		case rejected > 0:
			w.drop(int(rejected), &ExportError{StatusCode: 200, Rejected: rejected, Message: msg})
		case msg != "":
			w.errFn(&ExportError{StatusCode: 200, Warning: true, Message: msg})
		}
		return nil // never retried (§5.3)
	case retryableStatus(resp.StatusCode):
		return &ExportError{
			StatusCode: resp.StatusCode,
			Retryable:  true,
			Message:    excerpt(respBody),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	default: // 400, 413, anything else: non-retryable
		return &ExportError{StatusCode: resp.StatusCode, Message: excerpt(respBody)}
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
	const max = 256
	if len(b) > max {
		b = b[:max]
	}
	return string(b)
}
```

Also add the `retryAfter` carrier to `ExportError` in `otlp/options.go`:

```go
type ExportError struct {
	StatusCode int
	Retryable  bool
	Rejected   int64
	Warning    bool
	Message    string
	Err        error

	retryAfter time.Duration // parsed Retry-After; internal to the retry loop
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd otlp && go test ./... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add otlp/writer.go otlp/writer_test.go otlp/options.go
git commit -m "feat(otlp): HTTP export with OTLP retry and partial-success semantics"
```

---

### Task 9: Flush loop, lifecycle, public constructors (`otlp/writer.go`, `otlp/core.go`)

**Files:**
- Modify: `otlp/writer.go` (add `Write`, `run`, `Sync`, `Close`, `NewWriter`)
- Modify: `otlp/core.go` (add `NewCore`)
- Test: `otlp/writer_lifecycle_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package otlp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// reqCounter is an httptest handler counting requests and their record counts.
type reqCounter struct {
	mu       sync.Mutex
	requests int
	records  []int // records per request, via counting 0x12 entries in ScopeLogs
	status   atomic.Int32
	retryAfter atomic.Value // string
}

func (rc *reqCounter) handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rc.mu.Lock()
		rc.requests++
		rc.records = append(rc.records, countRecords(body))
		rc.mu.Unlock()
		if ra, _ := rc.retryAfter.Load().(string); ra != "" {
			rw.Header().Set("Retry-After", ra)
		}
		st := int(rc.status.Load())
		if st == 0 {
			st = http.StatusOK
		}
		rw.WriteHeader(st)
	}
}

func (rc *reqCounter) snapshot() (int, []int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.requests, append([]int(nil), rc.records...)
}

// countRecords walks request → resource_logs(1) → scope_logs(2) → counts
// log_records(2) entries. Implement with the Task 5 top-level field walker.
func countRecords(req []byte) int {
	rl, _ := findField(req, 1)
	sl, _ := findField(rl, 2)
	n := 0
	b := sl
	for len(b) > 0 {
		tag, tn := uvarint(b)
		b = b[tn:]
		l, ln := uvarint(b)
		if int(tag>>3) == 2 {
			n++
		}
		b = b[ln+int(l):]
	}
	return n
}

func newStartedWriter(t *testing.T, url string, opts ...Option) *Writer {
	t.Helper()
	opts = append([]Option{WithRetry(fastRetry)}, opts...)
	w, err := NewWriter(url, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestNewWriterValidation(t *testing.T) {
	_, err := NewWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)
	_, err = NewWriter("not-a-url")
	require.Error(t, err)
}

func TestBatchByCount(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithBatchSize(2), WithFlushInterval(time.Hour))
	for i := 0; i < 4; i++ {
		_, _ = w.Write([]byte{0x10, 0x09})
	}
	require.NoError(t, w.Sync())
	reqs, recs := rc.snapshot()
	require.Equal(t, 2, reqs)
	require.Equal(t, []int{2, 2}, recs)
}

func TestBatchByBytes(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	// Envelope overhead + one ~200B record exceeds 300 bytes → one record per
	// request despite batchSize 512. Records are opaque to the writer; any
	// byte content works for batching tests.
	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(300), WithFlushInterval(time.Hour))
	rec := make([]byte, 200)
	for i := 0; i < 3; i++ {
		_, _ = w.Write(rec)
	}
	require.NoError(t, w.Sync())
	reqs, recs := rc.snapshot()
	require.Equal(t, 3, reqs)
	require.Equal(t, []int{1, 1, 1}, recs)
}

func TestBatchByInterval(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithFlushInterval(20*time.Millisecond))
	_, _ = w.Write([]byte{0x10, 0x09})
	require.Eventually(t, func() bool { n, _ := rc.snapshot(); return n == 1 },
		2*time.Second, 5*time.Millisecond)
	_ = w // no Sync — the ticker must have flushed
}

func TestOversizedRecordDroppedAtWrite(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	var errs atomic.Int32
	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(128),
		WithErrorHandler(func(error) { errs.Add(1) }))
	n, err := w.Write(make([]byte, 4096))
	require.NoError(t, err) // zap multi-writer contract
	require.Equal(t, 4096, n)
	require.Equal(t, uint64(1), w.DroppedLogs())
	require.Equal(t, int32(1), errs.Load())
	require.NoError(t, w.Sync())
	reqs, _ := rc.snapshot()
	require.Zero(t, reqs)
}

func TestQueueFullDropPolicies(t *testing.T) {
	// Stalled server: the flush goroutine blocks in export while the queue
	// fills. release unblocks it at test end. Both policies covered.
	for _, policy := range []DropPolicy{DropNewest, DropOldest} {
		t.Run(map[DropPolicy]string{DropNewest: "newest", DropOldest: "oldest"}[policy], func(t *testing.T) {
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				<-release
				rw.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			defer close(release)

			w := newStartedWriter(t, srv.URL, WithQueueSize(2), WithBatchSize(1),
				WithFlushInterval(time.Millisecond), WithDropPolicy(policy))
			for i := 0; i < 10; i++ {
				_, _ = w.Write([]byte{0x10, byte(i)})
			}
			require.Eventually(t, func() bool { return w.DroppedLogs() > 0 },
				2*time.Second, time.Millisecond, "queue overflow must surface as counted drops")
		})
	}
}

func TestSyncDuringRetryBackoffReturns(t *testing.T) {
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithBatchSize(1), WithFlushInterval(time.Millisecond))
	_, _ = w.Write([]byte{0x10, 0x09})
	start := time.Now()
	require.NoError(t, w.Sync()) // waits ≤ retry budget (fastRetry: 100ms) + slack
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestSyncMultiBatchByteCutAgainstRetryingBackend(t *testing.T) {
	// Design §9: multiple pending batches under a tiny WithMaxRequestBytes
	// (one record per request) against a retrying backend — Sync resolves
	// EVERY batch within the per-batch documented bound, and accounting is
	// exact (all records dropped after each batch's budget).
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(300), WithFlushInterval(time.Hour))
	rec := make([]byte, 200) // one record per batch via byte cutting
	for i := 0; i < 3; i++ {
		_, _ = w.Write(rec)
	}
	start := time.Now()
	require.NoError(t, w.Sync())
	// 3 batches × (fastRetry budget 100ms + attempt) — generous CI slack.
	require.Less(t, time.Since(start), 10*time.Second)
	require.Equal(t, uint64(3), w.DroppedLogs(), "every byte-cut batch resolved and counted")

	// The oracle that proves byte cutting (pass-2 P1): the retrying backend
	// saw ≥ 3 requests (each batch retried within its own budget) and EVERY
	// request carried exactly ONE record — a broken implementation shipping
	// all three records as a single retrying batch fails here.
	reqs, recs := rc.snapshot()
	require.GreaterOrEqual(t, reqs, 3)
	for i, n := range recs {
		require.Equal(t, 1, n, "request %d must carry exactly one byte-cut record", i)
	}
}

func TestCloseDuringRetryAfterSleep(t *testing.T) {
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	rc.retryAfter.Store("30") // 30s sleep without cancellation
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w, err := NewWriter(srv.URL,
		WithRetry(RetryConfig{Initial: time.Millisecond, MaxInterval: time.Second, MaxElapsed: time.Hour}),
		WithBatchSize(1), WithFlushInterval(time.Millisecond))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x10, 0x09})
	time.Sleep(50 * time.Millisecond) // let the batch enter retry

	start := time.Now()
	require.NoError(t, w.Close())
	require.Less(t, time.Since(start), 5*time.Second, "Close must abort the Retry-After sleep")
	require.Equal(t, uint64(1), w.DroppedLogs(), "cancelled batch counted exactly once")
}

func TestCloseDrainsSingleAttempt(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w, err := NewWriter(srv.URL, WithRetry(fastRetry), WithBatchSize(2), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, _ = w.Write([]byte{0x10, 0x09})
	}
	require.NoError(t, w.Close())
	reqs, recs := rc.snapshot()
	require.GreaterOrEqual(t, reqs, 3) // 2+2+1
	total := 0
	for _, n := range recs {
		total += n
	}
	require.Equal(t, 5, total, "Close drain must export everything")
}

func TestCloseDrainByteCutAgainstFailingBackend(t *testing.T) {
	// Design §9, deterministic shape (pass-2 P1): the FIRST request blocks
	// in-flight while four more records queue behind the busy flush
	// goroutine; Close must (a) let the in-flight attempt finish within its
	// timeout, then (b) drain the queue as byte-cut single-record batches
	// with exactly ONE attempt each (no retries during close), every record
	// counted dropped exactly once.
	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	first.Store(true)
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	base := rc.handler()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		base(rw, r)
	}))
	defer srv.Close()

	w, err := NewWriter(srv.URL, WithRetry(fastRetry), WithQueueSize(8),
		WithMaxRequestBytes(300), WithFlushInterval(time.Millisecond),
		WithTimeout(5*time.Second))
	require.NoError(t, err)
	rec := make([]byte, 200)
	_, _ = w.Write(rec)
	<-entered // batch 1 is in-flight; the flush goroutine is busy in export
	for i := 0; i < 4; i++ {
		_, _ = w.Write(rec) // queue behind the blocked export
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release) // server answers 503 while Close waits
	}()
	start := time.Now()
	require.NoError(t, w.Close())
	require.Less(t, time.Since(start), 10*time.Second)

	reqs, recs := rc.snapshot()
	require.Equal(t, 5, reqs, "1 in-flight attempt + 4 single-attempt drain batches")
	for i, n := range recs {
		require.Equal(t, 1, n, "request %d must carry exactly one byte-cut record", i)
	}
	require.Equal(t, uint64(5), w.DroppedLogs(), "every record counted dropped exactly once")
}

func TestErrorHandlerMayCallClose(t *testing.T) {
	// Pass-2 P0 pin: the error handler runs outside the admission lock, so a
	// handler that calls Close must not self-deadlock.
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var w *Writer
	var err error
	w, err = NewWriter(srv.URL, WithRetry(fastRetry), WithMaxRequestBytes(128),
		WithErrorHandler(func(error) { _ = w.Close() }))
	require.NoError(t, err)
	donec := make(chan struct{})
	go func() {
		_, _ = w.Write(make([]byte, 4096)) // oversized → handler → Close
		close(donec)
	}()
	select {
	case <-donec:
	case <-time.After(10 * time.Second):
		t.Fatal("Write deadlocked against Close inside the error handler")
	}
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestCloseWhileRequestInFlight(t *testing.T) {
	// Design §9/§5.4: the in-flight HTTP attempt is allowed to finish within
	// its per-attempt timeout — Close must not cancel a request the server
	// may have already accepted.
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewWriter(srv.URL, WithRetry(fastRetry),
		WithBatchSize(1), WithFlushInterval(time.Millisecond), WithTimeout(5*time.Second))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x10, 0x09})
	<-entered // request is in flight
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release) // server responds 200 while Close is waiting
	}()
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs(), "in-flight attempt completed; record delivered, not dropped")
}

func TestPostCloseWritesUncounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewWriter(srv.URL, WithRetry(fastRetry))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close()) // idempotent
	n, werr := w.Write([]byte{0x10, 0x09})
	require.NoError(t, werr)
	require.Equal(t, 2, n)
	require.Zero(t, w.DroppedLogs()) // §5.1: silently discarded, uncounted
	require.NoError(t, w.Sync())     // post-close Sync is a nil no-op
}

func TestConcurrentWriteSyncClose(t *testing.T) {
	// Run against BOTH a healthy and a sustained-failure backend (design §9)
	// under -race: exercises the admit gate, retry cancellation, and drain.
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.WriteHeader(status)
			}))
			defer srv.Close()

			w, err := NewWriter(srv.URL, WithRetry(fastRetry), WithFlushInterval(time.Millisecond))
			require.NoError(t, err)
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 100; j++ {
						_, _ = w.Write([]byte{0x10, 0x09})
						if j%10 == 0 {
							_ = w.Sync()
						}
					}
				}()
			}
			time.Sleep(10 * time.Millisecond)
			_ = w.Close()
			wg.Wait()
		})
	}
}

func TestNewCoreEndToEnd(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	core, w, err := NewCore(srv.URL, zapcore.InfoLevel, WithServiceName("e2e"), WithRetry(fastRetry))
	require.NoError(t, err)
	logger := zap.New(core)
	_, ctx := testSpanContext(t)
	logger.Info("hello", zap.String("k", "v"), SpanContext(ctx))
	logger.Debug("filtered-out")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	reqs, recs := rc.snapshot()
	require.Equal(t, 1, reqs)
	require.Equal(t, []int{1}, recs)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd otlp && go test ./... -run 'TestNewWriter|TestBatch|TestOversized|TestQueueFull|TestSyncDuring|TestCloseD|TestPostClose|TestConcurrentWrite|TestNewCoreEndToEnd' -v`
Expected: FAIL — `undefined: NewWriter`.

- [ ] **Step 3: Implement the lifecycle in `otlp/writer.go`**

```go
// NewWriter builds the OTLP/HTTP exporter and starts its flush goroutine.
// The caller owns the Writer and must Close it. Only writer/envelope-end
// options take effect (design §6).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewWriter(endpoint string, opts ...Option) (*Writer, error) {
	w, err := newWriterCore(endpoint, applyOptions(opts))
	if err != nil {
		return nil, err
	}
	go w.run()
	return w, nil
}

// Write enqueues one encoded LogRecord. It never blocks beyond the
// non-blocking enqueue: full queue → drop per policy; post-Close → silently
// discarded, uncounted (§5.1). Always returns (len(p), nil) to honor zap's
// multi-writer contract. The shared admit lock spans check+enqueue so a
// concurrent Close cannot strand a record after the final drain — and the
// user error handler is invoked AFTER releasing it, so a handler that calls
// Close cannot self-deadlock (plan-review pass-2 P0).
func (w *Writer) Write(p []byte) (int, error) {
	var notify *ExportError
	w.admit.RLock()
	switch {
	case w.closed.Load():
		// fallthrough to unlock; silently discarded, uncounted
	case w.env.sizeFor(w.env.recordCost(len(p))) > w.maxBytes:
		w.dropped.Add(1)
		notify = &ExportError{Message: "record exceeds WithMaxRequestBytes"}
	default:
		rec := make([]byte, len(p))
		copy(rec, p) // zap frees the encoder buffer after Write returns (§5.1)
		select {
		case w.queue <- rec:
		default:
			if w.dropPolicy == DropOldest {
				select {
				case <-w.queue:
					w.dropped.Add(1)
				default:
				}
				select {
				case w.queue <- rec:
				default:
					w.dropped.Add(1)
				}
			} else {
				w.dropped.Add(1)
			}
		}
	}
	w.admit.RUnlock()
	if notify != nil {
		w.errFn(notify) // outside admit: handler may call Close/Sync safely
	}
	return len(p), nil
}

// run is the single flush goroutine (§5.2): one select over lifecycle, Sync
// requests, the ticker, and the queue. Exports run inline — a retrying batch
// head-of-line blocks later batches; the bounded queue absorbs and overflows
// to counted drops.
func (w *Writer) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	var batch [][]byte
	var tagged int

	flush := func(allowRetry bool) {
		if len(batch) > 0 {
			w.export(batch, allowRetry)
			batch, tagged = batch[:0], 0
		}
	}
	add := func(rec []byte, allowRetry bool) {
		cost := w.env.recordCost(len(rec))
		// Byte-aware cut BEFORE adding (§5.2): never assemble past maxBytes.
		if len(batch) > 0 && w.env.sizeFor(tagged+cost) > w.maxBytes {
			flush(allowRetry)
		}
		batch = append(batch, rec)
		tagged += cost
		if len(batch) >= w.batchSize || w.env.sizeFor(tagged) >= w.maxBytes {
			flush(allowRetry)
		}
	}
	drainQueued := func(allowRetry bool) {
		for {
			select {
			case rec := <-w.queue:
				add(rec, allowRetry)
			default:
				flush(allowRetry)
				return
			}
		}
	}

	for {
		select {
		case <-w.ctx.Done():
			drainQueued(false) // final drain: one attempt per batch (§5.4)
			return
		case ack := <-w.flushReq:
			drainQueued(true) // Sync barrier: everything enqueued before the call
			close(ack)
		case <-ticker.C:
			flush(true)
		case rec := <-w.queue:
			add(rec, true)
		}
	}
}

// Sync flushes every record enqueued before the call and waits for
// resolution (exported or dropped). Worst case is pending_batches ×
// (retry budget + attempt timeout) — a barrier, not a hard deadline (§5.4).
// Post-Close, Sync is a nil no-op.
func (w *Writer) Sync() error {
	if w.closed.Load() {
		return nil
	}
	ack := make(chan struct{})
	select {
	case w.flushReq <- ack:
	case <-w.ctx.Done():
		return nil
	}
	select {
	case <-ack:
	case <-w.done:
	}
	return nil
}

// Close stops intake, aborts any backoff sleep (the in-flight HTTP attempt
// finishes within its own timeout), drains the queue with one attempt per
// batch, and releases resources. Idempotent; always nil (§5.4).
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		// Wait out in-flight Writes (they hold admit shared); once acquired,
		// every future Write observes closed and discards — so the cancel +
		// final drain below cannot be raced by a late enqueue.
		w.admit.Lock()
		w.admit.Unlock() //nolint:staticcheck // barrier, not a critical section
		w.cancel()
		<-w.done
	})
	return nil
}
```

And `NewCore` in `otlp/core.go`:

```go
// NewCore wires NewEncoder + NewWriter into the custom trace-aware core
// (design §2.2) plus its Writer, which the caller must Close. One opts list
// feeds all three ends (each option sets only the fields it owns).
//
// Returns:
//   - zapcore.Core: the OTLP core (sticky zap.Any("context", ctx) works here)
//   - *Writer: the underlying exporter; the caller owns it and must Close it
//   - error: a non-nil error from NewWriter (e.g. ErrNoEndpoint)
func NewCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error) {
	w, err := NewWriter(endpoint, opts...)
	if err != nil {
		return nil, nil, err
	}
	return newOTLPCore(newEncoder(applyOptions(opts)), w, level), w, nil
}
```

- [ ] **Step 4: Run to verify pass (including race)**

Run: `cd otlp && go test ./... -v && go test -race ./...`
Expected: PASS. Pay attention to `TestCloseDuringRetryAfterSleep` and
`TestConcurrentWriteSyncClose` under `-race`.

- [ ] **Step 5: Commit**

```bash
git add otlp/writer.go otlp/writer_lifecycle_test.go otlp/core.go
git commit -m "feat(otlp): flush loop, Sync/Close lifecycle, NewWriter/NewCore"
```

---

### Task 10: Package documentation (`otlp/doc.go`)

**Files:**
- Modify: `otlp/doc.go` (replace the Task 0 stub)

- [ ] **Step 1: Write the full package doc**

```go
// Package otlp ships zap logs as native OTLP/HTTP binary protobuf
// (ExportLogsServiceRequest, POST /v1/logs) to any OTLP receiver — the OTel
// Collector, Grafana Loki ≥ 3.0, Elastic, Datadog Agent — with logs↔traces
// correlation populated from context.Context as LogRecord proto fields
// (trace_id, span_id, flags), not string attributes.
//
// # Quick start
//
//	core, w, err := otlp.NewCore("http://collector:4318", zapcore.InfoLevel,
//	    otlp.WithServiceName("checkout"))
//	if err != nil { ... }
//	defer w.Close()
//	logger := zap.New(core)
//
//	logger.Info("payment ok", otlp.SpanContext(ctx))            // eager
//	logger.Info("payment ok", zap.Any("context", ctx))          // bridge-compatible
//	reqLog := logger.With(zap.Any("context", ctx))               // sticky
//	sugar.Infow("payment ok", otlp.InjectTraceKVs(ctx, "k", 1)...)
//
// # Trace-context compatibility matrix
//
// The encoder consumes any field whose value is a context.Context (the
// official contrib/bridges/otelzap convention) or the eager SpanContext
// payload. Sticky With behavior depends on the core:
//
//	form                                  | otlp.NewCore | stock zapcore.NewCore
//	per-call zap.Any("context", ctx)      | yes          | yes
//	per-call otlp.SpanContext(ctx)        | yes          | yes
//	With(otlp.SpanContext(ctx))           | yes          | yes
//	With(zap.Any("context", ctx))         | yes          | NO — stringified attribute
//
// The stock-core limitation is structural: zap classifies contexts as
// fmt.Stringer, so ioCore.With erases the value before any encoder hook.
// Use otlp.NewCore (recommended) or the eager helper.
//
// A second stock-core caveat: transactional rollback for FAILING zap.Inline
// marshalers applied via With. ioCore.With dispatches InlineMarshalerType
// straight into the encoder with no interception point, so a failing inline
// marshaler's partial writes persist in the With-state — zap's own JSON and
// console encoders behave identically there. otlp.NewCore rolls such
// failures back cleanly; per-call fields are transactional on every core.
//
// In zapcore.NewTee setups, the OTHER cores receive trace-context fields as
// ordinary fields; the eager helper renders legibly (span_context JSON) and
// is the recommended form there. TraceCorrelationFields produces flat
// trace_id/span_id hex strings for non-OTLP sinks; on THIS core they are
// plain attributes, not correlation.
//
// # Delivery semantics
//
// Async-only, at-most-once: bounded queue, count/byte/interval batching,
// OTLP retry (429/502/503/504 with backoff and Retry-After), partial-success
// accounting, counted drops, never blocks the application goroutine, no WAL.
// Sync flushes; Close drains with single attempts. See DroppedLogs.
//
// # Boundary
//
// zapwire provides the foundation (encoder, core, injector helpers); the
// application layer decides whether to build wrapper methods such as
// InfoCtx(ctx, msg, ...) — they are trivial over InjectTraceFields and
// InjectTraceKVs.
package otlp
```

- [ ] **Step 2: Verify and commit**

Run: `cd otlp && go vet ./... && go doc .`
Expected: doc renders, vet clean.

```bash
git add otlp/doc.go
git commit -m "docs(otlp): package documentation with compat matrix"
```

---

### Task 11: Conformance module (`otlp/internal/conformance/`)

Byte-identical round-trip against the official stubs — the binding correctness gate (design
§9). Own go.mod so grpc/protobuf never enter `otlp/`'s graph.

**Files:**
- Create: `otlp/internal/conformance/go.mod`, `otlp/internal/conformance/conformance_test.go`
- Modify: `go.work` (add the module)

- [ ] **Step 1: Scaffold the module**

```bash
mkdir -p otlp/internal/conformance
cat > otlp/internal/conformance/go.mod <<'EOF'
module github.com/arloliu/zapwire/otlp/internal/conformance

go 1.25.0

require (
	github.com/arloliu/zapwire/otlp v0.0.0
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel/trace v1.44.0
	go.opentelemetry.io/proto/otlp v1.10.0
	go.uber.org/zap v1.28.0
	google.golang.org/protobuf v1.36.6
)

replace github.com/arloliu/zapwire/otlp => ../..
EOF
```

Update `go.work`:

```
go 1.25.0

use (
	.
	./otlp
	./otlp/internal/conformance
)
```

Then: `cd otlp/internal/conformance && go mod tidy`

- [ ] **Step 2: Write the conformance tests**

Two capture paths: (a) bare `LogRecord` bytes straight from the encoder — `proto.Unmarshal`
into `logspb.LogRecord`; (b) full requests captured via `httptest` through the REAL pipeline
(`NewCore` → log → `Sync`) — `proto.Unmarshal` into `collogspb.ExportLogsServiceRequest`.
Both must satisfy: unmarshal cleanly AND `proto.Marshal(decoded)` == our bytes (byte
identity ⇒ ascending field order, minimal varints, correct zero-omission).

```go
package conformance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/arloliu/zapwire/otlp"
)

func spanCtx(t *testing.T) (trace.SpanContext, context.Context) {
	t.Helper()
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
		SpanID:     trace.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74},
		TraceFlags: trace.FlagsSampled,
	})
	return sc, trace.ContextWithSpanContext(context.Background(), sc)
}

// roundTripRecord asserts byte-identity for one encoded entry.
func roundTripRecord(t *testing.T, fields ...zapcore.Field) *logspb.LogRecord {
	t.Helper()
	enc := otlp.NewEncoder()
	buf, err := enc.EncodeEntry(zapcore.Entry{
		Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "msg",
	}, fields)
	require.NoError(t, err)
	defer buf.Free()
	ours := append([]byte(nil), buf.Bytes()...)

	var rec logspb.LogRecord
	require.NoError(t, proto.Unmarshal(ours, &rec), "our bytes must decode")
	remarshaled, err := proto.Marshal(&rec)
	require.NoError(t, err)
	require.Equal(t, remarshaled, ours, "byte identity with official marshaling")
	return &rec
}

type objAll struct{}

func (objAll) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("s", "v")
	e.AddInt64("i", -7)
	e.OpenNamespace("deep")
	e.AddBool("b", true)
	return nil
}

type arrMixed struct{}

func (arrMixed) MarshalLogArray(e zapcore.ArrayEncoder) error {
	e.AppendString("x")
	e.AppendFloat64(2.5)
	_ = e.AppendObject(objAll{})
	return nil
}

func TestRecordRoundTripFieldTypes(t *testing.T) {
	long := make([]byte, 300) // multi-byte length varints
	for i := range long {
		long[i] = 'a'
	}
	rec := roundTripRecord(t,
		zap.String("str", "v"), zap.String("long", string(long)),
		zap.Bool("bt", true), zap.Bool("bf", false),
		zap.Int("i", 42), zap.Int64("neg", -1),
		zap.Uint64("umax", 1<<63+1), // > MaxInt64 → string
		zap.Float64("f", 3.5), zap.Float32("f32", 1.25),
		zap.Binary("bin", []byte{0xde, 0xad}),
		zap.ByteString("bs", []byte("text")),
		zap.Duration("d", 1500*time.Millisecond),
		zap.Time("tm", time.Unix(1, 2)),
		zap.Complex128("c", complex(1, 2)),
		zap.Uintptr("up", 7),
		zap.Object("obj", objAll{}),
		zap.Array("arr", arrMixed{}),
		zap.Namespace("ns"), zap.String("inner", "x"),
	)
	require.Equal(t, "msg", rec.Body.GetStringValue())
	require.EqualValues(t, 9, rec.SeverityNumber)
	require.Equal(t, "info", rec.SeverityText)
	require.EqualValues(t, time.Unix(7, 42).UnixNano(), rec.TimeUnixNano)
	require.Equal(t, rec.TimeUnixNano, rec.ObservedTimeUnixNano)
}

func TestRecordRoundTripTrace(t *testing.T) {
	sc, ctx := spanCtx(t)
	rec := roundTripRecord(t, otlp.SpanContext(ctx))
	wantT, wantS := sc.TraceID(), sc.SpanID()
	require.Equal(t, wantT[:], rec.TraceId)
	require.Equal(t, wantS[:], rec.SpanId)
	require.EqualValues(t, 1, rec.Flags)

	// Absent: fields omitted entirely (nil, not zeros).
	rec = roundTripRecord(t)
	require.Nil(t, rec.TraceId)
	require.Nil(t, rec.SpanId)
	require.Zero(t, rec.Flags)

	// Unsampled (valid IDs, flags 0): IDs present, flags omitted — the
	// byte-identity round-trip inside roundTripRecord proves we did not emit
	// a zero fixed32 that proto.Marshal would drop.
	scUnsampled := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		SpanID:  trace.SpanID{8, 8, 8, 8, 8, 8, 8, 8},
	})
	rec = roundTripRecord(t, zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: scUnsampled})
	require.NotNil(t, rec.TraceId)
	require.NotNil(t, rec.SpanId)
	require.Zero(t, rec.Flags)
}

func TestRecordRoundTripDegradedField(t *testing.T) {
	rec := roundTripRecord(t, zap.Any("ch", make(chan int)))
	require.Len(t, rec.Attributes, 1)
	require.Equal(t, "chError", rec.Attributes[0].Key)
}

type failingObj struct{}

func (failingObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("partial", "bytes")
	e.OpenNamespace("opened")
	return errors.New("fail")
}

// failingArr writes elements then fails (nested-array rollback row).
type failingArr struct{}

func (failingArr) MarshalLogArray(e zapcore.ArrayEncoder) error {
	e.AppendString("partial")
	_ = e.AppendObject(objAll{})
	return errors.New("fail")
}

func TestRecordRoundTripRollback(t *testing.T) {
	// Round-trip THROUGH the stubs after each rollback shape — proves no
	// stray bytes survive (design §9 degradation matrix).

	// Failing ObjectMarshaler (writes attr + opens namespace).
	rec := roundTripRecord(t, zap.String("good", "1"), zap.Object("bad", failingObj{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "badError", rec.Attributes[1].Key)

	// Failing zap.Inline (writes into the CURRENT level — the pass-2 P0
	// seam): bare "Error" attribute, nothing else.
	rec = roundTripRecord(t, zap.String("good", "1"), zap.Inline(failingObj{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "Error", rec.Attributes[1].Key)

	// Failing nested ArrayMarshaler.
	rec = roundTripRecord(t, zap.String("good", "1"), zap.Array("badarr", failingArr{}))
	require.Len(t, rec.Attributes, 2)
	require.Equal(t, "good", rec.Attributes[0].Key)
	require.Equal(t, "badarrError", rec.Attributes[1].Key)
}

func TestFullRequestRoundTrip(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, w, err := otlp.NewCore(srv.URL, zapcore.DebugLevel,
		otlp.WithServiceName("conf-svc"),
		otlp.WithResource(zap.String("deployment.environment.name", "test")),
		otlp.WithScopeName("conformance"), otlp.WithScopeVersion("v1"))
	require.NoError(t, err)
	logger := zap.New(core)
	_, ctx := spanCtx(t)
	logger.Info("one", otlp.SpanContext(ctx), zap.String("k", "v"))
	logger.Warn("two")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 1)
	var req collogspb.ExportLogsServiceRequest
	require.NoError(t, proto.Unmarshal(bodies[0], &req))
	remarshaled, err := proto.Marshal(&req)
	require.NoError(t, err)
	require.Equal(t, remarshaled, bodies[0], "request byte identity")

	require.Len(t, req.ResourceLogs, 1)
	rl := req.ResourceLogs[0]
	require.Equal(t, "service.name", rl.Resource.Attributes[0].Key)
	require.Equal(t, "conf-svc", rl.Resource.Attributes[0].Value.GetStringValue())
	require.Len(t, rl.ScopeLogs, 1)
	require.Equal(t, "conformance", rl.ScopeLogs[0].Scope.Name)
	require.Equal(t, "v1", rl.ScopeLogs[0].Scope.Version)
	require.Len(t, rl.ScopeLogs[0].LogRecords, 2)
	require.Equal(t, "one", rl.ScopeLogs[0].LogRecords[0].Body.GetStringValue())
	require.NotNil(t, rl.ScopeLogs[0].LogRecords[0].TraceId)
	require.EqualValues(t, 13, rl.ScopeLogs[0].LogRecords[1].SeverityNumber) // warn
}
```

(Add `"io"` to the imports.)

- [ ] **Step 3: Run**

Run: `cd otlp/internal/conformance && go mod tidy && go test ./... -v`
Expected: PASS — every round-trip byte-identical. **Any mismatch here is a bug in Tasks
1/4/5/7; do not weaken the assertion.**

- [ ] **Step 4: Confirm quarantine and commit**

Run: `cd otlp && go list -deps ./... | grep -E 'grpc|google.golang.org/protobuf|arloliu/zapwire$' ; echo "exit=$?"`
Expected: no matches, `exit=1`.

```bash
git add otlp/internal/conformance go.work
git commit -m "test(otlp): conformance suite — byte-identical round-trip vs official stubs"
```

---

### Task 12: Integration test, benchmarks, docs, final gate

**Files:**
- Create: `otlp/collector_integration_test.go`, `otlp/bench_test.go`
- Modify: `Makefile`, `README.md`, `docs/guide.md`

- [ ] **Step 1: Opt-in collector integration test** (`//go:build otelcollector`; mirrors the
fluent-bit pattern: real binary, file exporter, poll output)

```go
//go:build otelcollector

package otlp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Requires a real otel-collector binary; set OTELCOL_BIN (default
// /usr/local/bin/otelcol). Run via `make integration-otel`.
func TestCollectorEndToEnd(t *testing.T) {
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
      http:
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
	cfgFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPort(t, port)

	core, w, err := NewCore(fmt.Sprintf("http://127.0.0.1:%d", port), zapcore.InfoLevel,
		WithServiceName("itest"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Poll the file exporter output for our record with intact trace IDs.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line == "" {
				continue
			}
			var doc map[string]any
			if json.Unmarshal([]byte(line), &doc) != nil {
				continue
			}
			s := string(data)
			_ = doc
			if strings.Contains(s, "integration") &&
				strings.Contains(strings.ToLower(s), strings.ToLower(sc.TraceID().String())) &&
				strings.Contains(strings.ToLower(s), strings.ToLower(sc.SpanID().String())) {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "record with trace IDs must reach the collector")
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitPort(t *testing.T, port int) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			c.Close()
		}
		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
}
```

Makefile target (mirror the existing fluent-bit `integration` target style):

```makefile
integration-otel: ## Run the opt-in otel-collector integration test (needs OTELCOL_BIN)
	cd otlp && go test -tags otelcollector -run TestCollectorEndToEnd -v ./...
```

- [ ] **Step 2: Benchmarks** (`otlp/bench_test.go`)

```go
package otlp

import (
	"io"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func benchFields() []zapcore.Field {
	return []zapcore.Field{
		zap.String("method", "GET"), zap.Int("status", 200),
		zap.Duration("latency", 1500*time.Microsecond),
		zap.String("path", "/api/v1/things"), zap.Bool("cache", true),
	}
}

func BenchmarkEncodeEntry(b *testing.B) {
	enc := NewEncoder()
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "request handled"}
	fields := benchFields()
	b.ReportAllocs()
	for b.Loop() {
		buf, err := enc.EncodeEntry(ent, fields)
		if err != nil {
			b.Fatal(err)
		}
		buf.Free()
	}
}

func BenchmarkEncodeEntryWithTrace(b *testing.B) {
	enc := NewEncoder()
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Time: time.Unix(7, 42), Message: "request handled"}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	})
	fields := append(benchFields(), zap.Field{Key: "span_context", Type: zapcore.ReflectType, Interface: sc})
	b.ReportAllocs()
	for b.Loop() {
		buf, err := enc.EncodeEntry(ent, fields)
		if err != nil {
			b.Fatal(err)
		}
		buf.Free()
	}
}

func BenchmarkEndToEndLogger(b *testing.B) {
	// Encoder + custom core into a discard sink (no HTTP): the hot path zap sees.
	enc := newEncoder(applyOptions(nil))
	core := newOTLPCore(enc, zapcore.AddSync(io.Discard), zapcore.InfoLevel)
	logger := zap.New(core)
	fields := benchFields()
	b.ReportAllocs()
	for b.Loop() {
		logger.Info("request handled", fields...)
	}
}
```

(Add the `trace` import.) Record the first numbers in the PR description; budgets follow the
v1 design §8 discipline (track `ns/op`/`allocs/op` regressions in review, no hard CI gate
in this plan).

- [ ] **Step 3: README + guide rows**

Add to the processors/subpackage tables in `README.md` and `docs/guide.md` (match the
existing table format exactly — read them first):

```
| `otlp` | OTLP/HTTP protobuf logs | OTel Collector, Loki ≥3.0, Elastic, Datadog | own module; trace correlation from context.Context |
```

- [ ] **Step 4: Final gate**

```bash
make lint && make test
cd otlp && go test -race ./... && cd ..
cd otlp/internal/conformance && go test ./... && cd ../../..
# dependency quarantine
cd otlp && go list -deps ./... | grep -cE 'grpc|google.golang.org/protobuf' # must print 0
go list -deps ./... | grep -c 'arloliu/zapwire$' # must print 0 (no root import)
cd ..
# root module untouched by otel
go list -m all | grep -c opentelemetry # must print 0
```

Expected: lint clean, all tests green in all three modules, all greps print 0.

- [ ] **Step 5: Commit**

```bash
git add otlp/collector_integration_test.go otlp/bench_test.go Makefile README.md docs/guide.md
git commit -m "test(otlp): collector integration test, benchmarks, docs rows"
```

---

## Execution notes

- **Task order is load-bearing:** 0 → 12. Each task ends with every module compiling and all
  prior tests green.
- **When a conformance test fails (Task 11):** the bug is in Tasks 1/4/5/7 byte emission —
  fix there; never weaken byte-identity assertions.
- **Timing-sensitive tests** (Task 9) use `fastRetry` + `require.Eventually`; if flaky in CI,
  widen the `Eventually` windows, not the semantics.
- The design doc (`docs/design/2026-06-11-otlp-logs-design.md`) is the authority for any
  ambiguity; its §12/§13 tables explain WHY the sharp edges (rollback, With pre-scan,
  lifecycle bounds) look the way they do.

## Plan-review pass-1 resolutions

| Finding | Resolution |
|---|---|
| **P0** Inline-marshaler rollback missing — `Field.AddTo` calls `MarshalLogObject(enc)` directly for `InlineMarshalerType` (`zapcore/field.go:122-124,183-185`), bypassing `AddObject`'s snapshot | `applyField` helper added (Task 5): snapshots around the inline dispatch, rolls back on error, emits zap-convention `<key>Error` itself. Used by `EncodeEntry`, `core.With` (Task 6), and resource-field encoding (Task 7). Conformance inline/nested-array rollback rows added (Task 11). |
| **P0** Close/Write admission race — a Write past the closed check could enqueue after the final drain, stranding the record | `admit sync.RWMutex` added to the Writer (Tasks 8/9): Write holds it shared across check+enqueue; Close stores `closed`, takes it exclusively as a barrier, then cancels and drains — the root writer's discipline (`writer.go:175-188,373-384`). |
| **P0** `otlp/attr.go` imported unused `fmt` (compile failure) | Import removed (Task 4); `fmt` is needed only in `encoder_test.go`. |
| **P0** Task 5 gate command did `cd otlp && … && cd otlp` (enters `otlp/otlp`) | Command fixed to a single `cd otlp`. |
| **P1** Lifecycle tests missed design §9 rows (both drop policies, multi-batch byte-cut `Sync` vs retrying backend, byte-cut `Close` drain vs failing backend, `Close` while a request is in flight, sustained-failure concurrency) | All five added to Task 9 (`TestQueueFullDropPolicies` subtests, `TestSyncMultiBatchByteCutAgainstRetryingBackend`, `TestCloseDrainByteCutAgainstFailingBackend`, `TestCloseWhileRequestInFlight`, `TestConcurrentWriteSyncClose` healthy+failing subtests). |
| **P1** `newTestWriter` appended `fastRetry` AFTER caller options, silently overriding `TestExportCancelledDuringBackoff`'s hour-long budget | Defaults now PREPENDED so caller options win (Task 8). |
| **P1** Commit gates drifted from the repo contract (`go fix → make lint → make test`, race-enabled `make test`) | "Before EVERY commit" block added to the header; Task 0 Makefile sketch keeps `-race` with an explicit preserve-flags instruction. |
| **P2** Contract table listed a stale `assemble(dst *buffer.Buffer, …)` signature | Corrected to the implemented `assemble(dst []byte, records [][]byte) []byte`; `applyField` added to the contract. |

## Plan-review pass-2 resolutions

| Finding | Resolution |
|---|---|
| **P0** Stock-core `With(zap.Inline(failing))` still bypasses rollback — `ioCore.With` → `Field.AddTo` dispatches `InlineMarshalerType` directly into the encoder; no interception point exists | Documented as a precise stock-core caveat (it is zap's universal `With`-inline behavior — zap's own JSON/console encoders leave the same partial writes): doc.go compat section extended (Task 10), design §3.3 gained a scope note, and `TestStockCoreWithInlineFailurePinsZapBehavior` (Task 6) pins the degradation on the stock core AND the clean rollback on `otlp.NewCore`. `AddObject`/`AddArray`/`AddReflected` remain transactional on every core (encoder methods). |
| **P0** Oversized-record handler invoked under `admit.RLock` — a handler calling `Close` self-deadlocks | `Write` restructured (Task 9): counting/enqueue under the lock, `errFn` invoked after `RUnlock`. `WithErrorHandler` doc states handlers may call Writer methods. `TestErrorHandlerMayCallClose` added. |
| **P1** Two lifecycle tests didn't prove their rows: multi-batch byte-cut `Sync` only asserted drops (a single 3-record batch also drops 3); byte-cut `Close` drain claimed "full queue" without forcing one and its request count was scheduling-sensitive | `TestSyncMultiBatchByteCutAgainstRetryingBackend` now asserts ≥3 requests with exactly ONE record per request. `TestCloseDrainByteCutAgainstFailingBackend` rebuilt deterministically: blocked first request holds the flush goroutine while 4 records queue; asserts exactly 5 requests, 1 record each, 5 drops. |
