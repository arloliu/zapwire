# Syslog RFC5424 subpackage — Implementation Plan (zapwire v2)

**Status:** ✅ implementation-ready (codex plan-review consensus, pass 2) · **Date:** 2026-06-10 ·
**Branch:** `feat/zapwire-syslog` · **Spec:** `docs/design/2026-06-10-syslog-rfc5424-design.md`

**Review history:** design spec reached codex plan-review consensus (pass 3, no P0/P1). This
plan: codex plan-review pass 1 (`tmp/zapwire-v2-syslog-rfc5424_pass1_review.md`) raised 1×P0
(missing `strings` import) / 1×P1 (dropped §3.5 `bodyFormat` seam) / 1×P2 (commit gate) — all
resolved (see "Plan-review pass-1 resolutions"). Pass 2
(`tmp/zapwire-v2-syslog-rfc5424_pass2_review.md`) confirmed P0+P1 resolved with **no new
P0/P1**, leaving one P2 (Task 6 final gate should call `make test`) — now fixed.
**Implementation-ready.**

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Execute tasks **in order** (1 → 6): each task leaves
> the tree compiling with all prior tests green.

**Goal:** Add a stdlib-only `syslog/` subpackage that ships zap logs as RFC5424 syslog
messages (JSON message body) to rsyslog/syslog-ng/Vector/Logstash over the existing
reconnecting `Writer` (UDS/TCP), reusing the core via the `Encoder`/`Framer` seam.

**Architecture:** A custom `zapcore.Encoder` **embeds** an inner `zapcore.NewJSONEncoder`
(built with `SkipLineEnding=true`) so all field encoding is inherited; it overrides only
`EncodeEntry` (prepend the RFC5424 header built from `Entry.Level`/`Entry.Time`) and `Clone`.
The encoder output is the complete `SYSLOG-MSG`, shipped through `zapwire.Passthrough()` + a
new `syslog.Framer` (octet-counting default / LF), exactly like the `fluent` native path.

**Tech Stack:** Go 1.25, `go.uber.org/zap/zapcore` + `go.uber.org/zap/buffer` (already
present), `github.com/stretchr/testify` (tests). **No new runtime dependencies; the root
`zapwire` package and `syslog/` stay free of `tinylib/msgp`** (same footprint as `ndjson`).

**Design source:** `docs/design/2026-06-10-syslog-rfc5424-design.md` (consensus, pass 3). Read
it first — section refs below (e.g. §3.4) point into it.

---

## Dependency policy

| Package | Allowed imports | This plan adds |
|---|---|---|
| `syslog/` | stdlib + `go.uber.org/zap/zapcore` + `go.uber.org/zap/buffer` + `github.com/arloliu/zapwire` | the encoder, framer, presets |

Identical footprint to `ndjson/`. `go list -deps ./syslog | grep -c tinylib/msgp` must be `0`
(verified in Task 6).

## File structure (created by this plan)

| File | Status | Responsibility |
|---|---|---|
| `syslog/framer.go` | **create** | `Framing` type + constants, `Framer`, `NewFramer`, `Frame` (octet-counting / LF) |
| `syslog/encoder.go` | **create** | `Facility`/`Severity` types + constants, `defaultSeverityMapper`, `clampSeverity`, `validateFacility`, `sanitizeField`, `appendRFC3339Micros`, `header` struct, the `bodyFormat` seam + `jsonBody` (§3.5), the `encoder` type, `NewEncoder`, `Clone`, `EncodeEntry` |
| `syslog/syslog.go` | **create** | `options`, `Option`, all `With*`, `defaultOptions`, `apply`, `resolveHeader`, `NewWriter`, `NewCore`, `buildWriter` |
| `syslog/doc.go` | **create** | package doc comment |
| `syslog/framer_test.go`, `syslog/encoder_test.go`, `syslog/syslog_test.go` | **create** | tests per the design §8 matrix |
| `README.md`, `docs/guide.md` | **modify** | add a `syslog` row to the processors table |

**Decomposition:** framer (Task 1) and the pure primitives (Task 2) are independent and ship
first; options (Task 3) feed the encoder (Task 4); the writer wiring + end-to-end + concurrency
(Task 5) need the encoder; docs/bench/gate close it out (Task 6).

---

## Shared invariants & contracts (referenced by tasks below)

1. **Header from the Entry (§2, §3.1).** Severity comes from `Entry.Level` via the mapper;
   `TIMESTAMP` from `Entry.Time`. These are only on the `zapcore.Entry`, so the header is built
   in `EncodeEntry`, never in the core `Encoder`/`Framer`.
2. **`SkipLineEnding=true`, no trim (§3.1, pass-2 P0).** `NewEncoder` copies `encCfg` (value
   param → caller untouched) and sets `encCfg.SkipLineEnding = true`. Setting `LineEnding=""`
   does **not** work (zap renormalizes it to `"\n"`); only `SkipLineEnding` suppresses the
   terminator. The MSG body is then the bare JSON object and is appended with **no** trimming.
3. **PRI is bounded before arithmetic (§3.4, pass-1 P0).** Facility validated to `0..23` once
   in `NewEncoder` (else `LOCAL0`); severity clamped to `0..7` per entry (`>7→7`, `<0→0`). So
   `PRI = facility*8 + severity ∈ 0..191` always — header assembly cannot fail.
4. **Header fields pre-sanitized (§3.3).** HOSTNAME(255)/APP-NAME(48)/PROCID(128)/MSGID(32)
   are stripped to printable US-ASCII (33..126), truncated, and empty→NILVALUE `-`, once at
   construction. `EncodeEntry` only appends pre-cleaned strings.
5. **Concurrency (§3.6).** zap may call `Clone`/`EncodeEntry` concurrently on the **same**
   encoder and they must not mutate the receiver. Safe here because: field state lives only in
   the **embedded** JSON encoder (whose `EncodeEntry` clones internally), `cfg` is immutable,
   and each call takes a **fresh** buffer from `pool`. `Clone` deep-clones the inner encoder.
6. **Buffer lifecycle.** `EncodeEntry` returns a `pool.Get()` buffer (zap's `ioCore` frees it
   after `Write`); the inner body buffer is `defer body.Free()`. The async path copies bytes
   (`enc.Encode(nil,p)` in `writer.go`) before zap frees the entry buffer — no use-after-free.

### Conventions for every task

- **TDD:** write the failing test → run it red → implement minimally → run it green → commit.
- **Test command:** `go test ./syslog/ -race` while iterating; `go test ./... -race` before
  finishing a task.
- **Before every commit (repo gate — AGENTS.md / rule 600):** `go fix ./syslog/...`, then
  `make lint` (fix all issues; `make fmt` if formatting is flagged), then `make test`
  (the module's `-race` unit tests). The per-task `go test ./syslog/ -race` runs are
  faster red/green iteration; the gate's `make test` is the authoritative pre-commit check.
- **Commits:** Conventional Commits, present tense, **no attribution trailers** (global CLAUDE.md).

---

## Task 1: `Framer` (octet-counting / LF)

Independent of the encoder; ship it first. Mirrors `ndjson.Framer` but with RFC 6587 framing.

**Files:**
- Create: `syslog/framer.go`
- Test: `syslog/framer_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// syslog/framer_test.go
package syslog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFramer_OctetCounting(t *testing.T) {
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{[]byte("<13>1 msg")})
	require.NoError(t, err)
	require.Equal(t, "9 <13>1 msg", string(out)) // len("<13>1 msg") == 9
}

func TestFramer_OctetCounting_IsByteLengthNotRuneCount(t *testing.T) {
	msg := []byte("<13>1 \xEF\xBB\xBF{}") // 6 ASCII + 3-byte BOM + 2 = 11 bytes, 9 runes
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{msg})
	require.NoError(t, err)
	require.Equal(t, "11 "+string(msg), string(out))
}

func TestFramer_LFTerminated(t *testing.T) {
	out, err := NewFramer(LFTerminated).Frame(nil, [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, "a\nb\n", string(out))
}

func TestFramer_OctetCounting_MultiPayloadBatch(t *testing.T) {
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{[]byte("aa"), []byte("bbb")})
	require.NoError(t, err)
	require.Equal(t, "2 aa3 bbb", string(out))
}

func TestFramer_DefaultIsOctetCounting(t *testing.T) {
	// The zero Framing value is OctetCounting.
	out, err := NewFramer(Framing(0)).Frame(nil, [][]byte{[]byte("xy")})
	require.NoError(t, err)
	require.Equal(t, "2 xy", string(out))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./syslog/ -run TestFramer -race`
Expected: build failure — package `syslog` does not exist yet / `undefined: NewFramer`.

- [ ] **Step 3: Implement the framer**

```go
// syslog/framer.go
package syslog

import "strconv"

// Framing selects the RFC 6587 stream-framing method for syslog over a byte stream.
type Framing int

const (
	// OctetCounting prefixes each message with its byte length: "MSG-LEN SP SYSLOG-MSG"
	// (RFC 6587 §3.4.1). The default — unambiguous and robust to any payload byte.
	OctetCounting Framing = iota
	// LFTerminated terminates each message with a single '\n' (RFC 6587 §3.4.2,
	// non-transparent framing). Simpler, but safe only when the message contains no LF.
	LFTerminated
)

// Framer wraps each per-entry SYSLOG-MSG payload for an octet stream. It implements
// zapwire.Framer.
type Framer struct{ octetCounting bool }

// NewFramer returns a Framer using f (the zero Framing value, OctetCounting, is the default).
//
// Parameters:
//   - f: the framing method (OctetCounting or LFTerminated)
//
// Returns:
//   - Framer: a framer for the chosen method
func NewFramer(f Framing) Framer { return Framer{octetCounting: f == OctetCounting} }

// Frame appends each payload to dst, framed per the configured method. MSG-LEN in
// octet-counting mode is the byte length of the payload. It implements zapwire.Framer.
//
// Parameters:
//   - dst: buffer to append to; may be nil or a pooled slice to reuse
//   - payloads: per-entry SYSLOG-MSG payloads (one in sync mode, N when batched)
//
// Returns:
//   - []byte: dst extended with the framed payloads
//   - error: always nil (kept for the zapwire.Framer contract)
func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		if f.octetCounting {
			dst = strconv.AppendInt(dst, int64(len(p)), 10)
			dst = append(dst, ' ')
			dst = append(dst, p...)
		} else {
			dst = append(dst, p...)
			dst = append(dst, '\n')
		}
	}

	return dst, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./syslog/ -run TestFramer -race`
Expected: PASS (5 tests).

- [ ] **Step 5: Lint and commit**

```bash
go fix ./syslog/...
make lint
make test
git add syslog/framer.go syslog/framer_test.go
git commit -m "feat(syslog): add RFC6587 octet-counting/LF framer"
```

---

## Task 2: Header primitives (types, mapper, clamp, sanitize, timestamp)

Pure functions + types, no `options` yet. After this task the package still compiles and the
primitives are independently tested.

**Files:**
- Create: `syslog/encoder.go`
- Test: `syslog/encoder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// syslog/encoder_test.go
package syslog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestValidateFacility(t *testing.T) {
	require.Equal(t, KERN, validateFacility(KERN))
	require.Equal(t, LOCAL7, validateFacility(LOCAL7)) // 23, the max
	require.Equal(t, LOCAL0, validateFacility(Facility(24)))
	require.Equal(t, LOCAL0, validateFacility(Facility(-1)))
}

func TestClampSeverity(t *testing.T) {
	require.Equal(t, Emergency, clampSeverity(Emergency)) // 0
	require.Equal(t, Debug, clampSeverity(Debug))         // 7
	require.Equal(t, Debug, clampSeverity(Severity(9)))
	require.Equal(t, Emergency, clampSeverity(Severity(-1)))
}

func TestDefaultSeverityMapper(t *testing.T) {
	cases := map[zapcore.Level]Severity{
		zapcore.DebugLevel:  Debug,
		zapcore.InfoLevel:   Informational,
		zapcore.WarnLevel:   Warning,
		zapcore.ErrorLevel:  Error,
		zapcore.DPanicLevel: Critical,
		zapcore.PanicLevel:  Critical,
		zapcore.FatalLevel:  Critical,
	}
	for lvl, want := range cases {
		require.Equalf(t, want, defaultSeverityMapper(lvl), "level %v", lvl)
	}
}

func TestSanitizeField(t *testing.T) {
	require.Equal(t, "host", sanitizeField("host", 255))
	require.Equal(t, "-", sanitizeField("", 48))                 // empty → NILVALUE
	require.Equal(t, "-", sanitizeField("   ", 48))              // spaces (0x20) are not 33..126
	require.Equal(t, "abc", sanitizeField("a b\tc", 48))         // drop space + tab
	require.Equal(t, "ab", sanitizeField("abcdef", 2))           // truncate to cap
	require.Equal(t, "ok", sanitizeField("o\x00k\x7f", 48))      // drop NUL + DEL(127)
}

func TestAppendRFC3339Micros(t *testing.T) {
	b := buffer.NewPool().Get()
	appendRFC3339Micros(b, time.Date(2026, 6, 10, 22, 14, 15, 3456000, time.UTC))
	require.Equal(t, "2026-06-10T22:14:15.003456Z", b.String())

	z := buffer.NewPool().Get()
	appendRFC3339Micros(z, time.Time{})
	require.Equal(t, "-", z.String()) // zero time → NILVALUE
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./syslog/ -run 'TestValidateFacility|TestClampSeverity|TestDefaultSeverityMapper|TestSanitizeField|TestAppendRFC3339Micros' -race`
Expected: build failure — `undefined: validateFacility`, etc.

- [ ] **Step 3: Implement the primitives**

```go
// syslog/encoder.go
package syslog

import (
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Facility is a syslog facility (RFC5424 §6.2.1), valid range 0..23.
type Facility int

// Syslog facilities. PRI = int(facility)*8 + int(severity).
const (
	KERN Facility = iota
	USER
	MAIL
	DAEMON
	AUTH
	SYSLOG
	LPR
	NEWS
	UUCP
	CRON
	AUTHPRIV
	FTP
	NTP
	LOGAUDIT
	LOGALERT
	CLOCK
	LOCAL0
	LOCAL1
	LOCAL2
	LOCAL3
	LOCAL4
	LOCAL5
	LOCAL6
	LOCAL7
)

// Severity is a syslog severity (RFC5424 §6.2.1), valid range 0..7.
type Severity int

// Syslog severities, most-to-least severe.
const (
	Emergency Severity = iota
	Alert
	Critical
	Error
	Warning
	Notice
	Informational
	Debug
)

const nilValue = "-"

// validateFacility returns f when in 0..23, else the default LOCAL0 (resolves spec §3.4 /
// pass-1 P0: a typed but out-of-range facility must not produce an invalid PRI).
func validateFacility(f Facility) Facility {
	if f < KERN || f > LOCAL7 {
		return LOCAL0
	}

	return f
}

// clampSeverity coerces s into the valid 0..7 range (>7→Debug, <0→Emergency), so any custom
// mapper still yields a syntactically valid PRI (spec §3.4 / pass-1 P0).
func clampSeverity(s Severity) Severity {
	switch {
	case s < Emergency:
		return Emergency
	case s > Debug:
		return Debug
	default:
		return s
	}
}

// defaultSeverityMapper is the conservative zap-level → syslog-severity mapping (spec §3.4):
// it never auto-emits Emergency/Alert (0..1).
func defaultSeverityMapper(l zapcore.Level) Severity {
	switch {
	case l <= zapcore.DebugLevel:
		return Debug
	case l == zapcore.InfoLevel:
		return Informational
	case l == zapcore.WarnLevel:
		return Warning
	case l == zapcore.ErrorLevel:
		return Error
	default: // DPanic, Panic, Fatal
		return Critical
	}
}

// sanitizeField normalizes one RFC5424 header field: drop bytes outside printable US-ASCII
// (33..126), truncate to maxLen, and map an empty result to the NILVALUE "-" (spec §3.3).
// (Param is maxLen, not max, to avoid shadowing the Go 1.21 builtin — the predeclared linter.)
func sanitizeField(s string, maxLen int) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(out) < maxLen; i++ {
		if c := s[i]; c >= 33 && c <= 126 {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nilValue
	}

	return string(out)
}

// appendRFC3339Micros appends t in UTC as RFC3339 with 6 fractional digits — the RFC5424
// TIMESTAMP profile (TIME-SECFRAC ≤ 6). A zero time appends the NILVALUE "-" (spec §3.2).
func appendRFC3339Micros(b *buffer.Buffer, t time.Time) {
	if t.IsZero() {
		b.AppendString(nilValue)

		return
	}
	b.AppendTime(t.UTC(), "2006-01-02T15:04:05.000000Z07:00")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./syslog/ -run 'TestValidateFacility|TestClampSeverity|TestDefaultSeverityMapper|TestSanitizeField|TestAppendRFC3339Micros' -race`
Expected: PASS (5 tests).

- [ ] **Step 5: Lint and commit**

```bash
go fix ./syslog/...
make lint
make test
git add syslog/encoder.go syslog/encoder_test.go
git commit -m "feat(syslog): add RFC5424 header primitives (facility/severity/sanitize/timestamp)"
```

---

## Task 3: Options & config resolution

`options`, all `With*`, computed defaults, and resolution to the immutable `header`. Tested
in-package via the unexported `apply`/`resolveHeader`.

**Files:**
- Create: `syslog/syslog.go`
- Test: `syslog/syslog_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// syslog/syslog_test.go
package syslog

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	require.Equal(t, LOCAL0, o.facility)
	require.Equal(t, OctetCounting, o.framing)
	require.False(t, o.bom)
	require.Equal(t, strconv.Itoa(os.Getpid()), o.procID)
	require.Equal(t, filepath.Base(os.Args[0]), o.appName)
	require.NotNil(t, o.severityOf)
}

func TestApply_OverridesAndResolveHeader(t *testing.T) {
	o := apply([]Option{
		WithFacility(LOCAL3),
		WithHostname("h\x00ost"), // illegal byte dropped on resolve
		WithAppName("my-app"),
		WithProcID("4123"),
		WithMsgID("ID7"),
		WithBOM(true),
		WithFraming(LFTerminated),
	})
	require.Equal(t, LFTerminated, o.framing)

	h := o.resolveHeader()
	require.Equal(t, LOCAL3, h.facility)
	require.Equal(t, "host", h.hostname) // NUL dropped
	require.Equal(t, "my-app", h.appName)
	require.Equal(t, "4123", h.procID)
	require.Equal(t, "ID7", h.msgID)
	require.True(t, h.bom)
}

func TestResolveHeader_EmptyFieldsBecomeNilValue(t *testing.T) {
	h := apply([]Option{WithHostname(""), WithAppName(""), WithProcID(""), WithMsgID("")}).resolveHeader()
	require.Equal(t, "-", h.hostname)
	require.Equal(t, "-", h.appName)
	require.Equal(t, "-", h.procID)
	require.Equal(t, "-", h.msgID)
}

func TestWithSeverityMapper_NilKeepsDefault(t *testing.T) {
	o := apply([]Option{WithSeverityMapper(nil)})
	require.Equal(t, Informational, o.severityOf(zapcore.InfoLevel)) // still the default
}

func TestResolveHeader_OutOfRangeFacilityFallsBack(t *testing.T) {
	h := apply([]Option{WithFacility(Facility(99))}).resolveHeader()
	require.Equal(t, LOCAL0, h.facility)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./syslog/ -run 'TestDefaultOptions|TestApply|TestResolveHeader|TestWithSeverityMapper' -race`
Expected: build failure — `undefined: defaultOptions`, etc.

- [ ] **Step 3: Implement options + resolution**

```go
// syslog/syslog.go
package syslog

import (
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// header is the per-encoder, sanitized, immutable RFC5424 header config.
type header struct {
	facility   Facility
	hostname   string
	appName    string
	procID     string
	msgID      string
	bom        bool
	severityOf func(zapcore.Level) Severity
}

type options struct {
	facility   Facility
	severityOf func(zapcore.Level) Severity
	hostname   string
	appName    string
	procID     string
	msgID      string
	bom        bool
	framing    Framing
	wireOpts   []zapwire.Option
}

// defaultOptions pre-populates the computed defaults; With* options overwrite the raw field,
// and resolveHeader sanitizes — so WithHostname("") yields "-" while an unset hostname keeps
// os.Hostname().
func defaultOptions() options {
	host, _ := os.Hostname()

	return options{
		facility:   LOCAL0,
		severityOf: defaultSeverityMapper,
		hostname:   host,
		appName:    filepath.Base(os.Args[0]),
		procID:     strconv.Itoa(os.Getpid()),
		msgID:      nilValue,
		framing:    OctetCounting,
	}
}

func apply(opts []Option) options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

func (o options) resolveHeader() header {
	return header{
		facility:   validateFacility(o.facility),
		hostname:   sanitizeField(o.hostname, 255),
		appName:    sanitizeField(o.appName, 48),
		procID:     sanitizeField(o.procID, 128),
		msgID:      sanitizeField(o.msgID, 32),
		bom:        o.bom,
		severityOf: o.severityOf,
	}
}

// Option configures a syslog preset. Header/severity/BOM options affect NewEncoder and
// NewCore; WithFraming and WithZapwireOptions affect NewWriter and NewCore.
type Option func(*options)

// WithFacility sets the syslog facility (default LOCAL0). An out-of-range value falls back to
// LOCAL0 at construction.
func WithFacility(f Facility) Option { return func(o *options) { o.facility = f } }

// WithSeverityMapper overrides the zap-level → syslog-severity mapping. The result is clamped
// to 0..7 per entry. A nil mapper is ignored (the default is kept).
func WithSeverityMapper(fn func(zapcore.Level) Severity) Option {
	return func(o *options) {
		if fn != nil {
			o.severityOf = fn
		}
	}
}

// WithHostname sets the HOSTNAME field (default os.Hostname()); empty → "-".
func WithHostname(h string) Option { return func(o *options) { o.hostname = h } }

// WithAppName sets the APP-NAME field (default the process name); empty → "-".
func WithAppName(a string) Option { return func(o *options) { o.appName = a } }

// WithProcID sets the PROCID field (default the OS pid); empty → "-".
func WithProcID(p string) Option { return func(o *options) { o.procID = p } }

// WithMsgID sets the MSGID field (default "-").
func WithMsgID(m string) Option { return func(o *options) { o.msgID = m } }

// WithBOM prepends a UTF-8 BOM to the MSG (strict RFC5424 MSG-UTF8). Default off — a leading
// BOM trips naive JSON consumers.
func WithBOM(on bool) Option { return func(o *options) { o.bom = on } }

// WithFraming selects the stream framing (default OctetCounting).
func WithFraming(f Framing) Option { return func(o *options) { o.framing = f } }

// WithZapwireOptions forwards core zapwire options (mode, buffer, timeouts, …).
func WithZapwireOptions(opts ...zapwire.Option) Option {
	return func(o *options) { o.wireOpts = append(o.wireOpts, opts...) }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./syslog/ -run 'TestDefaultOptions|TestApply|TestResolveHeader|TestWithSeverityMapper' -race`
Expected: PASS (5 tests).

- [ ] **Step 5: Lint and commit**

```bash
go fix ./syslog/...
make lint
make test
git add syslog/syslog.go syslog/syslog_test.go
git commit -m "feat(syslog): add options, defaults, and header resolution"
```

---

## Task 4: The `zapcore.Encoder` (`NewEncoder` / `Clone` / `EncodeEntry`)

Add the structural encoder to `syslog/encoder.go`. After this task the full `SYSLOG-MSG` is
produced and golden-tested.

**Files:**
- Modify: `syslog/encoder.go` (append the encoder type + methods)
- Test: `syslog/encoder_test.go` (append)

- [ ] **Step 1: Write the failing tests**

```go
// syslog/encoder_test.go  (append to the existing file)
//
// add imports: "strings", "github.com/stretchr/testify/assert", "go.uber.org/zap"

// goldenEncCfg yields a minimal JSON body: only the message key, so the body is
// {"msg":...} plus the call-site fields — keeping golden assertions focused on the header.
func goldenEncCfg() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{MessageKey: "msg"}
}

func TestEncoder_Golden_InfoLine(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithHostname("h"), WithAppName("app"), WithProcID("123"))
	ent := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2026, 6, 10, 22, 14, 15, 3456000, time.UTC),
		Message: "hi",
	}
	buf, err := enc.EncodeEntry(ent, []zapcore.Field{zap.String("k", "v")})
	require.NoError(t, err)
	want := `<134>1 2026-06-10T22:14:15.003456Z h app 123 - - {"msg":"hi","k":"v"}`
	require.Equal(t, want, buf.String())
}

func TestEncoder_PRI_Boundaries(t *testing.T) {
	emerg := func(zapcore.Level) Severity { return Emergency }
	encMin := NewEncoder(goldenEncCfg(), WithFacility(KERN), WithSeverityMapper(emerg),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := encMin.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<0>1 "), buf.String()) // 0*8+0

	dbg := func(zapcore.Level) Severity { return Debug }
	encMax := NewEncoder(goldenEncCfg(), WithFacility(LOCAL7), WithSeverityMapper(dbg),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ = encMax.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<191>1 "), buf.String()) // 23*8+7
}

func TestEncoder_PRI_OutOfRangeInputsAreBounded(t *testing.T) {
	bad := func(zapcore.Level) Severity { return Severity(9) } // clamped to 7
	enc := NewEncoder(goldenEncCfg(), WithFacility(Facility(99)), WithSeverityMapper(bad),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<135>1 "), buf.String()) // LOCAL0(16)*8+7
}

func TestEncoder_LineEndingIndependence(t *testing.T) {
	for _, tc := range []struct {
		ending string
		skip   bool
	}{{"\n", false}, {"\r\n", false}, {"", false}, {"END\n", false}, {"\n", true}, {"\r\n", true}} {
		cfg := goldenEncCfg()
		cfg.LineEnding = tc.ending
		cfg.SkipLineEnding = tc.skip
		enc := NewEncoder(cfg, WithHostname("h"), WithAppName("a"), WithProcID("1"))
		buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
		require.NoError(t, err)
		line := buf.String()
		assert.Falsef(t, strings.HasSuffix(line, "\n"), "no trailing LF for %+v: %q", tc, line)
		assert.NotContainsf(t, line, "\r", "no stray CR for %+v: %q", tc, line)
		assert.Truef(t, strings.HasSuffix(line, "}"), "ends with JSON body for %+v: %q", tc, line)
	}
}

func TestEncoder_BOM(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithBOM(true), WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.Contains(t, buf.String(), " - \uFEFF{") // SD, SP, BOM, then JSON
}

// The \u00A73.5 body-format seam: jsonBody emits SP + SD "-" + SP + optional BOM + the JSON MSG.
func TestJSONBody_AppendBody(t *testing.T) {
	b := buffer.NewPool().Get()
	jsonBody{}.appendBody(b, false, []byte(`{"a":1}`))
	require.Equal(t, ` - {"a":1}`, b.String())

	b2 := buffer.NewPool().Get()
	jsonBody{}.appendBody(b2, true, []byte(`{"a":1}`))
	require.Equal(t, " - \uFEFF{\"a\":1}", b2.String())
}

func TestEncoder_ZeroTimeIsNilValue(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<134>1 - h a 1 - - {"), buf.String())
}

// JSON-body parity (spec §8): the MSG equals a plain JSON encoder body built from the SAME
// SkipLineEnding=true config, including With fields and a namespace.
func TestEncoder_JSONBodyParity_WithAndNamespace(t *testing.T) {
	cfg := goldenEncCfg()
	syslogEnc := NewEncoder(cfg, WithHostname("h"), WithAppName("a"), WithProcID("1"))

	oracleCfg := cfg
	oracleCfg.SkipLineEnding = true // NewEncoder forces this internally; match it here
	oracle := zapcore.NewJSONEncoder(oracleCfg)

	sysClone := syslogEnc.Clone()
	oraClone := oracle.Clone()
	for _, f := range []zapcore.Field{zap.String("svc", "x"), zap.Namespace("ns")} {
		f.AddTo(sysClone)
		f.AddTo(oraClone)
	}
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hi"}
	call := []zapcore.Field{zap.Int("n", 7)}

	sysBuf, err := sysClone.EncodeEntry(ent, call)
	require.NoError(t, err)
	oraBuf, err := oraClone.EncodeEntry(ent, call)
	require.NoError(t, err)

	i := strings.IndexByte(sysBuf.String(), '{') // header fields contain no '{' here
	require.GreaterOrEqual(t, i, 0)
	require.Equal(t, oraBuf.String(), sysBuf.String()[i:])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./syslog/ -run TestEncoder_ -race`
Expected: build failure — `undefined: NewEncoder` (the type/methods aren't implemented yet).

- [ ] **Step 3: Implement the encoder**

```go
// syslog/encoder.go  (append; add imports "go.uber.org/zap/buffer" is already present)

// encoder is a zapcore.Encoder that prepends an RFC5424 header to a JSON message body. It
// embeds an inner JSON encoder so every ObjectEncoder method is inherited; only EncodeEntry
// and Clone are overridden.
type encoder struct {
	zapcore.Encoder // embedded inner JSON encoder (field handling inherited)
	cfg             header
	body            bodyFormat // renders the SD + MSG section (design §3.5 seam)
	pool            buffer.Pool
}

// bodyFormat renders the STRUCTURED-DATA + MSG section of a SYSLOG-MSG (the leading SP, the SD
// value, optional BOM, and the message). Only jsonBody is implemented; a future structuredData
// mode is a second implementation, leaving the header assembly and framer untouched (§3.5).
type bodyFormat interface {
	appendBody(dst *buffer.Buffer, bom bool, jsonMsg []byte)
}

// jsonBody emits STRUCTURED-DATA "-" and carries the structured fields as the JSON MSG body.
type jsonBody struct{}

func (jsonBody) appendBody(dst *buffer.Buffer, bom bool, msg []byte) {
	dst.AppendString(" - ") // SP + STRUCTURED-DATA "-" + SP
	if bom {
		dst.AppendString("\uFEFF")
	}
	dst.AppendBytes(msg) // MSG = bare JSON body (no trailing terminator)
}

var _ zapcore.Encoder = (*encoder)(nil)

// NewEncoder builds the RFC5424 zapcore.Encoder. It copies encCfg (a value parameter, so the
// caller's config is untouched) and sets SkipLineEnding=true so the inner JSON body carries no
// trailing terminator (spec §3.1). Pair it with NewWriter (Passthrough + the syslog Framer) to
// build a core yourself, or use NewCore.
//
// Parameters:
//   - encCfg: zap JSON encoder config for the message body (its LineEnding is overridden)
//   - opts: syslog options (WithFacility, WithHostname, WithSeverityMapper, WithBOM, …)
//
// Returns:
//   - zapcore.Encoder: the RFC5424 encoder
func NewEncoder(encCfg zapcore.EncoderConfig, opts ...Option) zapcore.Encoder {
	encCfg.SkipLineEnding = true

	return &encoder{
		Encoder: zapcore.NewJSONEncoder(encCfg),
		cfg:     apply(opts).resolveHeader(),
		body:    jsonBody{},
		pool:    buffer.NewPool(),
	}
}

// Clone deep-clones the inner encoder and copies the immutable header config.
func (e *encoder) Clone() zapcore.Encoder {
	return &encoder{Encoder: e.Encoder.Clone(), cfg: e.cfg, body: e.body, pool: e.pool}
}

// EncodeEntry assembles one SYSLOG-MSG: the RFC5424 header (from ent) + SP + optional BOM +
// the bare JSON body. It returns a pooled buffer that zap's ioCore frees after Write.
func (e *encoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	body, err := e.Encoder.EncodeEntry(ent, fields)
	if err != nil {
		return nil, err // surface the inner encoder error verbatim
	}
	defer body.Free()

	pri := int(e.cfg.facility)*8 + int(clampSeverity(e.cfg.severityOf(ent.Level)))

	line := e.pool.Get()
	line.AppendByte('<')
	line.AppendInt(int64(pri))
	line.AppendString(">1 ") // PRI + VERSION(1) + SP
	appendRFC3339Micros(line, ent.Time)
	line.AppendByte(' ')
	line.AppendString(e.cfg.hostname)
	line.AppendByte(' ')
	line.AppendString(e.cfg.appName)
	line.AppendByte(' ')
	line.AppendString(e.cfg.procID)
	line.AppendByte(' ')
	line.AppendString(e.cfg.msgID)
	e.body.appendBody(line, e.cfg.bom, body.Bytes()) // " - " [BOM] MSG (design \u00A73.5 seam)

	return line, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./syslog/ -run TestEncoder_ -race`
Expected: PASS (7 tests).

- [ ] **Step 5: Lint and commit**

```bash
go fix ./syslog/...
make lint
make test
git add syslog/encoder.go syslog/encoder_test.go
git commit -m "feat(syslog): add the RFC5424 zapcore.Encoder (header + JSON body)"
```

---

## Task 5: Writer wiring + end-to-end + concurrency

`NewWriter`/`NewCore`/`buildWriter`, then end-to-end over a loopback UDS with an RFC5424
validity oracle, and a `-race` concurrency test through a shared core and `With`/`Namespace`
clones.

**Files:**
- Modify: `syslog/syslog.go` (append `buildWriter`, `NewWriter`, `NewCore`)
- Test: `syslog/syslog_test.go` (append)

- [ ] **Step 1: Write the failing tests**

```go
// syslog/syslog_test.go  (append; add imports "bufio", "encoding/json", "io", "net",
// "regexp", "strings", "sync", "time", "github.com/arloliu/zapwire", "go.uber.org/zap"
// — "os", "path/filepath", "strconv", "testing", testify/require, zapcore are already
// imported from Task 3)

// rfc5424Re matches HEADER SP STRUCTURED-DATA SP MSG (SD is "-" in JSON-body mode).
var rfc5424Re = regexp.MustCompile(`^<(\d{1,3})>1 (\S+) (\S+) (\S+) (\S+) (\S+) - (.*)$`)

func assertValidRFC5424(t *testing.T, msg string) {
	t.Helper()
	m := rfc5424Re.FindStringSubmatch(msg)
	require.NotNilf(t, m, "not a valid RFC5424 line: %q", msg)
	pri, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	require.LessOrEqual(t, pri, 191)
	require.True(t, json.Valid([]byte(m[7])), "MSG must be valid JSON: %q", m[7])
}

func dialableUDS(t *testing.T) (string, net.Listener) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "syslog.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)

	return path, ln
}

func TestNewCore_EndToEnd_OctetCounting(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()

	recv := make(chan []byte, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		// Octet-counted frame: "<len> <SYSLOG-MSG>". Read the count, then that many bytes.
		r := bufio.NewReader(conn)
		countField, _ := r.ReadString(' ')
		n, _ := strconv.Atoi(strings.TrimSpace(countField))
		msg := make([]byte, n)
		_, _ = io.ReadFull(r, msg)
		recv <- msg
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg(),
		WithHostname("h"), WithAppName("app"), WithProcID("123"))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("hello", zap.Int("n", 7))

	select {
	case msg := <-recv:
		assertValidRFC5424(t, string(msg))
		require.Contains(t, string(msg), `"msg":"hello"`)
		require.Contains(t, string(msg), `"n":7`)
	case <-time.After(2 * time.Second):
		t.Fatal("no syslog message received")
	}
}

func TestNewCore_EndToEnd_LFFraming(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()

	recv := make(chan string, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		line, _ := bufio.NewReader(conn).ReadString('\n')
		recv <- strings.TrimRight(line, "\n")
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg(),
		WithFraming(LFTerminated), WithHostname("h"), WithAppName("app"), WithProcID("1"))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("lf")

	select {
	case line := <-recv:
		assertValidRFC5424(t, line)
	case <-time.After(2 * time.Second):
		t.Fatal("no LF-terminated message received")
	}
}

func TestNewWriter_HeaderOptionsAreNoOps(t *testing.T) {
	// NewWriter builds only Passthrough + Framer; header options must be accepted and ignored
	// without error (a BYO-core caller supplies them to NewEncoder).
	path, ln := dialableUDS(t)
	defer ln.Close()
	w, err := NewWriter(zapwire.UDS(path), WithHostname("ignored"), WithFacility(KERN))
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func TestConcurrent_Logging_Race(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()
	go func() { // drain
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c); _ = c.Close() }(conn)
		}
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg())
	require.NoError(t, err)
	defer w.Close()

	base := zap.New(core)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			base.Info("shared", zap.Int("g", i))
			base.With(zap.String("ctx", "c")).Info("with")
			base.With(zap.Namespace("ns")).Info("nested", zap.Int("k", i))
		}(i)
	}
	wg.Wait()
}
```

> The readers/drains above use `io`, `strings`, `bufio`, and `net` — all listed in the import
> note at the top of this test block.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./syslog/ -run 'TestNewCore|TestNewWriter|TestConcurrent' -race`
Expected: build failure — `undefined: NewWriter` / `undefined: NewCore`.

- [ ] **Step 3: Implement the writer wiring**

```go
// syslog/syslog.go  (append)

func buildWriter(t zapwire.Transport, o options) (*zapwire.Writer, error) {
	return zapwire.New(t, zapwire.Passthrough(), NewFramer(o.framing), o.wireOpts...)
}

// NewWriter builds a zapwire.Writer for the syslog path: Passthrough + the syslog Framer over
// t. Only WithFraming and WithZapwireOptions take effect here; header/severity/BOM options are
// no-ops (supply those to NewEncoder for a BYO-core path). Pair with NewEncoder to build a core
// yourself (e.g. inside zapcore.NewTee).
//
// Parameters:
//   - t: transport to ship messages over; must be non-nil
//   - opts: syslog options (WithFraming, WithZapwireOptions take effect)
//
// Returns:
//   - *zapwire.Writer: the writer; the caller owns it and must Close it
//   - error: a non-nil error from the underlying zapwire.New
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	return buildWriter(t, apply(opts))
}

// NewCore wires NewEncoder + NewWriter into a ready core plus its writer, which the caller must
// Close. A single opts list feeds both ends (each option sets only the fields it owns).
//
// Parameters:
//   - t: transport to ship messages over; must be non-nil
//   - level: minimum level an entry must meet to be encoded
//   - encCfg: zap JSON encoder config for the message body
//   - opts: syslog options
//
// Returns:
//   - zapcore.Core: an RFC5424-encoding core writing into the syslog writer
//   - *zapwire.Writer: the underlying writer; the caller must Close it
//   - error: a non-nil error if the writer cannot be built
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	w, err := buildWriter(t, apply(opts))
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(NewEncoder(encCfg, opts...), w, level), w, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./syslog/ -run 'TestNewCore|TestNewWriter|TestConcurrent' -race`
Expected: PASS (4 tests, including `-race` clean).

- [ ] **Step 5: Full package run, lint, commit**

```bash
go test ./syslog/ -race
go fix ./syslog/...
make lint
make test
git add syslog/syslog.go syslog/syslog_test.go
git commit -m "feat(syslog): wire NewWriter/NewCore and add end-to-end + race tests"
```

---

## Task 6: Benchmarks, package doc, README/guide, final gate

**Files:**
- Create: `syslog/doc.go`
- Create: `syslog/bench_test.go`
- Modify: `README.md`, `docs/guide.md`

- [ ] **Step 1: Write the package doc**

```go
// syslog/doc.go
//
// Package syslog provides a zapwire preset that ships zap logs as RFC5424 syslog
// messages (with a JSON message body) to rsyslog, syslog-ng, Vector, Logstash, and other
// syslog receivers over Unix domain sockets or TCP.
//
// A custom zapcore.Encoder prepends the RFC5424 header — PRI (facility+severity), version,
// RFC3339 timestamp, hostname, app-name, procid, msgid — to a JSON body encoded by an inner
// zap JSON encoder, and ships the result through the core Writer via zapwire.Passthrough and
// a Framer (RFC6587 octet-counting by default, or LF-terminated). The header severity is
// mapped from the zap level (configurable); all header fields are configurable and sanitized
// to RFC5424 limits.
package syslog
```

- [ ] **Step 2: Write the benchmarks**

```go
// syslog/bench_test.go
package syslog

import (
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
)

const benchMsg = "request completed"

func benchCore(b *testing.B, opts ...Option) (*zap.Logger, func()) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "syslog_bench.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(b, err)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c); _ = c.Close() }(conn)
		}
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig(), opts...)
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return zap.New(core), func() { _ = w.Close(); _ = ln.Close() }
}

func BenchmarkSyslogWriter_Sync(b *testing.B) {
	logger, cleanup := benchCore(b)
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info(benchMsg, zap.String("svc", "zapwire"), zap.Int("status", 200))
	}
}

func BenchmarkSyslogWriter_Async(b *testing.B) {
	logger, cleanup := benchCore(b, WithZapwireOptions(zapwire.WithAsyncMode()))
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info(benchMsg, zap.String("svc", "zapwire"), zap.Int("status", 200))
	}
}
```

- [ ] **Step 3: Run the benchmarks**

Run: `go test ./syslog/ -run='^$' -bench=. -benchmem -benchtime=0.3s`
Expected: both report `ns/op` and `allocs/op`. Record the numbers in the commit body; they
should be in the ballpark of `ndjson` (syslog adds only the header on top of the JSON body).

- [ ] **Step 4: Update README and guide**

In `README.md` and `docs/guide.md`, add a row to the processors table mirroring the existing
`fluent`/`ndjson` rows:

```
| Syslog (RFC5424) | rsyslog, syslog-ng, Vector, Logstash | `syslog` |
```

(Match the exact column layout of the table already in each file.)

- [ ] **Step 5: Final gate**

```bash
go fix ./syslog/...                                   # repo gate (AGENTS.md / rule 600)
make lint
make test                                             # whole module, -race (== go test ./... -timeout=5m -race)
make coverage                                         # confirm ≥80% on syslog/
go list -deps ./syslog | grep -c tinylib/msgp         # MUST be 0 (stdlib-only)
make examples                                          # examples module still builds
```
Expected: tests + lint pass; coverage on `syslog/` ≥ 80%; the msgp count is `0`.

- [ ] **Step 6: Commit**

```bash
git add syslog/doc.go syslog/bench_test.go README.md docs/guide.md
git commit -m "docs(syslog): add package doc, benchmarks, and README/guide rows"
```

---

## Spec coverage check

| Spec section | Task(s) |
|---|---|
| §2 architecture (encoder + Passthrough + Framer) | 1, 4, 5 |
| §3.1 embedded JSON encoder, SkipLineEnding, EncodeEntry assembly | 4 |
| §3.2 timestamp (UTC, ≤6 frac, NILVALUE) | 2, 4 |
| §3.3 header sanitization | 2, 3 |
| §3.4 severity mapping + PRI normalization | 2, 4 |
| §3.5 body-format seam (`bodyFormat`/`jsonBody`; SD deferred) | 4 (seam implemented; `structuredData` not built — YAGNI) |
| §3.6 concurrency | 4 (Clone), 5 (-race test) |
| §4 framer (octet-counting / LF) | 1 |
| §5 options & defaults | 3 |
| §6 public API (NewEncoder/NewWriter/NewCore) | 4, 5 |
| §7 error handling | 4 (EncodeEntry), 5 (NewWriter/NewCore errors) |
| §8 testing matrix | 1–5 |
| §10 module/deps (stdlib-only, msgp-free) | 6 (dep check) |

**Out of scope (per spec §9):** UDP/unixgram transport, RFC5424 STRUCTURED-DATA body, RFC3164,
and the real-daemon opt-in integration test — none are built by this plan.

---

## Plan-review pass-1 resolutions

| Finding | Resolution |
|---|---|
| **P0** Task 5 `syslog_test.go` used `strings.TrimSpace`/`TrimRight` but the import note omitted `strings` (compile blocker) | Task 5 import note now lists `strings` (and `io`), and enumerates the imports already present from Task 3; the standalone "add io" note was folded in. |
| **P1** The design's §3.5 `bodyFormat` seam was silently dropped — Task 4 hard-wired the SD/MSG inline | Task 4 now defines the unexported `bodyFormat` interface + `jsonBody` implementation (appends `" - "` + optional BOM + JSON MSG); `EncodeEntry` delegates via `e.body.appendBody(...)`; `Clone` carries `body`; a `TestJSONBody_AppendBody` unit test covers it. Only `jsonBody` is built (`structuredData` deferred per spec §9). |
| **P2** Per-commit steps ran only `make lint`, not the repo gate (AGENTS.md / rule 600: `go fix` → `make lint` → `make test`) | The "Conventions" block and every task's commit step now run `go fix ./syslog/...`, `make lint`, `make test` before commit; Task 6's final gate adds `go fix`. |
