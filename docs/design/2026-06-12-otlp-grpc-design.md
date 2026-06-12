# OTLP/gRPC transport — design (zapwire/otlp v0.2)

Status: implemented (plan: docs/plans/zapwire-v2-otlp-grpc.md)
Date: 2026-06-12
Module: `github.com/arloliu/zapwire/otlp` (released `otlp/v0.1.0`; this ships as `otlp/v0.2.0`)
Predecessor: `2026-06-11-otlp-logs-design.md` (OTLP/HTTP exporter; gRPC was an explicit v1 non-goal)

## 1. Goal

Add the OTLP/gRPC transport to the existing `otlp` subpackage so zapwire covers both OTLP
protocols (`grpc` on 4317, `http/protobuf` on 4318), without changing the module's
dependency footprint or any released API. This also unblocks lifting the `grpc` protocol
rejection in otx's `zaplog` adapter (separate repo, out of scope here).

## 2. Decision record: hand-rolled gRPC framing, not grpc-go

The roadmap left "own module with grpc-go vs hand-rolled framing" open. Three spikes
(2026-06-12, code under `tmp/spike-handrolled-grpc/`, `tmp/spike-grpcgo-cost/`) settled it:

**Hand-rolled works with pure stdlib — zero new dependencies.**
A unary OTLP/gRPC export is an HTTP/2 POST of `1-byte flag + 4-byte big-endian length +
ExportLogsServiceRequest` to a fixed method path, with `grpc-status` in trailers. The
request bytes are exactly what `envelope.assemble` already emits; the response is the same
`ExportLogsServiceResponse` that `decodePartialSuccess` already parses. Go ≥1.24
(`net/http.Protocols.SetUnencryptedHTTP2`) provides h2c client support natively — the
anticipated `golang.org/x/net/http2` fallback proved unnecessary. The spike client
interoperated fully with a real grpc-go OTLP server: request decode, partial-success
decode, trailers-only errors, `RetryInfo` recovery (exact delay), per-message gzip, and
connection reuse. Transport code is ~250 lines.

**grpc-go is disproportionately expensive here.**
A conventional `otlp/grpc` submodule (google.golang.org/grpc + official stubs) measures:
+28 modules in every consumer's `go list -m all` (envoy control-plane ×3, cel, xds, gonum,
spiffe, …), go.sum 24 → 44 lines, binary +6.2 MiB standalone and **+9.3 MiB (~2×) marginal**
for an app already linking `zapwire/otlp`. Architecturally it would force extracting
~1,100 lines (writer pipeline, envelope, frame machinery, options struct) into
`otlp/internal/pipeline` consumed across a module boundary, with lockstep releases of two
modules. It would also defeat the otx goal: otx is a library and cannot adopt a grpc-go
exporter without inheriting the graph.

**Decision: gRPC transport inside the `otlp` module, hand-rolled wire code, stdlib only.**
A benchmark against a grpc-go-based exporter (§9.3) keeps the decision honest; the heavy
deps stay quarantined in `otlp/internal/conformance` (own go.mod), per the repo dependency
policy and design §11 of the v1 doc.

What hand-rolled gives up, knowingly: grpc-go's channel machinery (keepalive pings,
load-balancing policies, name resolution beyond DNS, channel-state introspection). A log
exporter shipping to a local agent or collector needs none of these; connection loss is
absorbed by the existing retry loop and stdlib re-dial (§6.5).

## 3. Architecture: the transport seam

The `Writer` pipeline — bounded queue, single flush goroutine, byte-aware batching, OTLP
retry loop, Sync/Close lifecycle with the admit-lock discipline — is transport-neutral and
is reused untouched. The HTTP-specific surface today is `attempt()` plus the whole-body
gzip block in `export()`. Both collapse behind one internal interface:

```go
// transport is the per-protocol ship layer. prepare runs ONCE per batch
// (compression + framing — a retrying batch must not re-gzip per attempt,
// matching today's HTTP behavior); attempt runs once per try.
type transport interface {
    prepare(msg []byte) prepared
    attempt(p prepared) (*acceptance, *ExportError)
}

// prepared is the wire-ready body. warn carries non-fatal prepare
// diagnostics (gzip failure → shipped uncompressed, mirroring HTTP today).
type prepared struct {
    body       []byte
    compressed bool
    warn       *ExportError
}

// acceptance is the server-accepted outcome: partial-success accounting
// data. nil acceptance + nil error = clean accept. Transports produce it
// via a shared resolveAccept(respMsg, base ExportError) helper around
// decodePartialSuccess, so the OTLP partial-success switch (rejected →
// counted drop; warning → handler; malformed → observability-only) lives
// in exactly one place and the writer applies it transport-agnostically.
type acceptance struct {
    rejected int64
    event    *ExportError
}
```

- `http_transport.go` — relocates `attempt`, `retryableStatus`, `parseRetryAfter`,
  `excerpt`, and the gzip block from `export()`. Behavior byte-identical to v0.1.0.
- `grpc_transport.go` — the new hand-rolled client (§6).
- `export()` keeps ownership of the retry loop (backoff, jitter, deadline, drop
  accounting). Transports communicate retryability through the existing
  `ExportError{Retryable, retryAfter}` contract: gRPC maps status codes + `RetryInfo`
  onto it exactly as HTTP maps 429/5xx + `Retry-After` today.
- `Writer` fields `endpoint`, `client`, `headers`, `compression`, `timeout` move into the
  transports; `Writer` gains a single `tr transport` field. Estimated `writer.go` diff:
  60–90 lines, mechanical.
- Partial-success accounting (`drop`/`errFn` on 0-rejection warnings) stays in the
  transports via the same small helpers the HTTP path uses today; the proto decode is
  `decodePartialSuccess`, shared byte-for-byte (the gRPC response message is the same
  `ExportLogsServiceResponse`, minus the 5-byte frame prefix).

No exported symbol changes meaning; no pipeline code is duplicated.

## 4. Public API

### 4.1 Constructors

```go
// NewWriter — unchanged, canonical OTLP/HTTP constructor (the spec's default
// protocol). Godoc gains: "Equivalent to NewHTTPWriter."
func NewWriter(endpoint string, opts ...Option) (*Writer, error)

// NewHTTPWriter is the explicit symmetric counterpart of NewGRPCWriter.
// A real function (not a var alias) delegating to NewWriter.
func NewHTTPWriter(endpoint string, opts ...Option) (*Writer, error)

// NewGRPCWriter builds the OTLP/gRPC exporter. See §5 for endpoint forms.
func NewGRPCWriter(endpoint string, opts ...Option) (*Writer, error)

// Core constructors follow the same symmetry: NewCore (unchanged, HTTP,
// godoc notes the equivalence), NewHTTPCore (explicit name, delegates to
// NewCore), NewGRPCCore (encoder + NewGRPCWriter + trace-aware core).
func NewHTTPCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error)
func NewGRPCCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error)
```

`NewWriter` never auto-detects protocol: an endpoint does not encode the protocol (both
run on any port), runtime probing makes failures ambiguous, and silently re-interpreting
`http://host:4317` would be a semantic break of the released contract. Deployment-time
protocol switching is served by `ProtocolFromEnv` (§4.3).

### 4.2 New options

```go
// WithInsecure selects plaintext h2c for SCHEME-LESS gRPC endpoints
// ("host:4317"). An explicit http/https scheme always takes precedence
// (OTel exporter-spec precedence). Documented no-op on the HTTP path.
func WithInsecure() Option

// WithTLSConfig supplies the gRPC TLS configuration (custom CA, mTLS).
// Implies TLS for scheme-less endpoints. Combining it with a plaintext
// endpoint ("http://" scheme, or bare + WithInsecure) is a construction
// error — a silently ignored TLS config would mask a security misconfig.
// Documented no-op on the HTTP path (use WithHTTPClient there).
func WithTLSConfig(c *tls.Config) Option
```

### 4.3 Protocol env helper

```go
type Protocol string

const (
    ProtocolGRPC         Protocol = "grpc"
    ProtocolHTTPProtobuf Protocol = "http/protobuf"
)

// ProtocolFromEnv resolves OTEL_EXPORTER_OTLP_LOGS_PROTOCOL then
// OTEL_EXPORTER_OTLP_PROTOCOL. Returns "" when neither is set. Explicit
// opt-in like EndpointFromEnv — zapwire never reads env behind the
// caller's back (v1 design §5.5).
func ProtocolFromEnv() Protocol
```

Callers dispatch themselves:

```go
switch otlp.ProtocolFromEnv() {
case otlp.ProtocolGRPC:
    w, err = otlp.NewGRPCWriter(otlp.EndpointFromEnv())
default:
    w, err = otlp.NewHTTPWriter(otlp.EndpointFromEnv())
}
```

(`"http/json"` is a valid env value per spec; it maps to the default branch — this
package only implements `http/protobuf`. Documented on the helper.)

### 4.4 Existing options on the gRPC path

| Option | gRPC semantics |
|---|---|
| `WithTimeout` | per-attempt deadline (context) AND `grpc-timeout` request header |
| `WithCompression(Gzip)` | per-message compression: frame flag 1 + `grpc-encoding: gzip` |
| `WithHeaders` | request metadata, validated at construction (`NewGRPCWriter` returns an error): reserved keys rejected (`grpc-` prefix, `-bin` suffix, `content-type`, `te`, pseudo-headers) AND values must be printable ASCII (0x20–0x7E) without leading/trailing whitespace, the gRPC ASCII-metadata value rule — a deterministic config error must not become a send-time export event |
| `WithHTTPClient` | documented no-op: the gRPC transport must own an HTTP/2-only client; a user client with HTTP/1 enabled breaks gRPC (§6.1) |
| `WithRetry`, `WithQueueSize`, `WithBatchSize`, `WithMaxRequestBytes`, `WithFlushInterval`, `WithDropPolicy`, `WithErrorHandler` | identical, shared pipeline |
| envelope/encoder options | identical, shared encoder/envelope |

`WithMaxRequestBytes` continues to bound the uncompressed `ExportLogsServiceRequest`
message; the 5-byte gRPC frame prefix is excluded (constant, negligible).

## 5. gRPC endpoint resolution & TLS

`resolveGRPCEndpoint(endpoint string, o options) (target string, tlsCfg *tls.Config, err error)`:

| Endpoint form | Transport security |
|---|---|
| `host:4317` (no scheme) | TLS (spec default `insecure=false`) — `WithInsecure()` opts into h2c; `WithTLSConfig` supplies the config |
| `http://host:4317` | plaintext h2c; `WithInsecure` redundant but allowed; `WithTLSConfig` is a construction error (§4.2) |
| `https://host:4317` | TLS; scheme wins; `WithTLSConfig` honored |

Default ports when omitted: scheme-less → 4317 (the OTLP/gRPC default); `http://` → 80,
`https://` → 443 (URL conventions — collectors behind standard LBs).

Validation errors (all at construction): empty endpoint (`ErrNoEndpoint`), unsupported
scheme, non-empty URL path/query/fragment (gRPC's `:path` is always the fixed method path
`/opentelemetry.proto.collector.logs.v1.LogsService/Export`; a user-supplied path is a
misconfiguration and is rejected loudly rather than ignored).

The OTel exporter spec's scheme-precedence rule is followed verbatim: "A scheme of https
indicates a secure connection and takes precedence over the insecure configuration
setting. A scheme of http indicates an insecure connection and takes precedence over the
insecure configuration setting."

## 6. The gRPC transport (`grpc_transport.go`)

### 6.1 HTTP/2 client construction

- Plaintext: `&http.Transport{Protocols: p}` where `p` enables **only**
  `UnencryptedHTTP2`. Spike-proven gotcha: if HTTP/1 is also enabled, stdlib picks
  HTTP/1 for `http://` URLs and the dial fails on the server's SETTINGS frame. This
  transport is private to the writer — hence `WithHTTPClient` being a no-op.
- TLS: `&http.Transport{Protocols: HTTP2 only, TLSClientConfig: cfg}` — ALPN `h2`.
- One `http.Client` per writer; stdlib reuses a single HTTP/2 connection across exports
  (spike: `GotConnInfo.Reused=true` for every request after the first) and transparently
  re-dials after connection loss on the next attempt.

### 6.2 Request

```
:method: POST
:path:   /opentelemetry.proto.collector.logs.v1.LogsService/Export
content-type: application/grpc
te: trailers
grpc-accept-encoding: identity,gzip
grpc-timeout: <WithTimeout, e.g. "10000m">   (ms unit; spec caps at 8 digits)
grpc-encoding: gzip                          (only when compressing)
user-agent: zapwire-otlp/<version>
<user metadata from WithHeaders>
```

Body: `flag(1B) | len(4B big-endian) | message`. With `WithCompression(Gzip)` the message
bytes are gzipped independently per message and flag is 1; otherwise flag 0 (spec: flag
must be 0 when `grpc-encoding` is absent). Per-attempt context timeout mirrors
`grpc-timeout`. The request body is fully known up front (`bytes.Reader`), so stdlib sends
the DATA frame with END_STREAM — the required unary half-close.

### 6.3 Response handling

Read body to EOF first (`io.LimitReader` 1 MiB, as HTTP) — stdlib populates
`resp.Trailer` only after EOF. Then resolve `grpc-status` with trailer-first precedence:

1. `resp.Trailer["Grpc-Status"]` — normal headers+body+trailers response.
2. `resp.Header["Grpc-Status"]` — **trailers-only** response (grpc-go sends immediate
   errors as a single HEADERS frame; status, message, AND `grpc-status-details-bin`
   surface as response headers; spike scenarios c/d).
3. Neither present (proxy interference, non-gRPC server): synthesize a status from the
   HTTP status code per the canonical gRPC HTTP-mapping (400→INTERNAL,
   401→UNAUTHENTICATED, 403→PERMISSION_DENIED, 404→UNIMPLEMENTED, 429/502/503/504→
   UNAVAILABLE, else UNKNOWN), keep `ExportError.StatusCode` set to the HTTP status for
   observability.

The HTTP `:status` is 200 even for gRPC errors — never consulted except in case 3.

**Transport-level failures are retryable on gRPC** (unlike the HTTP path, where
v0.1.0's released behavior keeps `client.Do` errors terminal — that asymmetry is
deliberate and preserved). A `client.Do` error or a response-body read failure maps to
`UNAVAILABLE` (retryable), or `DEADLINE_EXCEEDED` (retryable) when the attempt's local
timeout elapsed — exactly how grpc-go classifies connection loss, GOAWAY, resets, and
deadline expiry. This realizes §6.5's promise that a dead connection is absorbed by
the retry loop + stdlib re-dial within the retry budget. A failed body read also means
trailers are unreliable: the attempt returns the retryable error instead of resolving
status from incomplete metadata.

**Status 0 (OK):** strip the 5-byte frame prefix (gunzip the message when flag 1 +
`grpc-encoding: gzip` — we advertise gzip in `grpc-accept-encoding`, so servers may
compress), then `decodePartialSuccess` — identical semantics to HTTP: rejected>0 →
counted drop + handler; rejected==0 with message → warning; malformed body →
observability-only error, batch counted delivered.

**Non-OK:** `grpc-message` is percent-decoded (RFC 3986 %XX, tolerant of invalid
sequences). Retryability per the OTLP spec table:

| Class | Codes |
|---|---|
| Retryable | CANCELLED(1), DEADLINE_EXCEEDED(4), ABORTED(10), OUT_OF_RANGE(11), UNAVAILABLE(14), DATA_LOSS(15) |
| Retryable **only with RetryInfo** | RESOURCE_EXHAUSTED(8) |
| Non-retryable | everything else (UNKNOWN, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED, UNAUTHENTICATED, FAILED_PRECONDITION, UNIMPLEMENTED, INTERNAL) |

**RetryInfo (throttling):** `grpc-status-details-bin` (trailer or header, matching where
status was found) is base64 — unpadded canonical per gRPC spec, decoder accepts padded
too (`RawStdEncoding`, fallback `StdEncoding`). Payload is `google.rpc.Status` (code=1,
message=2, details=3 repeated Any); an Any with
`type_url == "type.googleapis.com/google.rpc.RetryInfo"` carries
`RetryInfo{retry_delay=1: Duration{seconds=1, nanos=2}}`. The recovered delay lands in
`ExportError.retryAfter` — the shared retry loop then prefers it over backoff exactly as
it does HTTP `Retry-After` today. Decoding reuses the existing hand-rolled proto reader
(`findField`/`findVarint`); malformed details degrade gracefully to plain backoff.

### 6.4 Error model

`ExportError` gains one field; nothing else changes:

```go
type ExportError struct {
    StatusCode int  // HTTP status; 0 for transport/encode errors and for
                    // gRPC responses (which are HTTP 200 by construction —
                    // set only when a non-gRPC intermediary answered, §6.3.3)
    GRPCStatus int  // gRPC status code (0=OK); meaningful only on writers
                    // built by NewGRPCWriter. NEW in v0.2.0.
    // Retryable, Rejected, Warning, Message, Err — unchanged semantics;
    // Message carries the percent-decoded grpc-message or the
    // partial_success error_message.
}
```

`Error()` includes `grpc=<code>` when `GRPCStatus != 0`. Adding a field to a struct that
has no exported constructor is backward-compatible.

### 6.5 Knowingly absent (non-goals at this layer)

- No client keepalive pings / channel-state machine: a dead connection surfaces as a
  failed attempt; the retry loop + stdlib re-dial absorb it within the retry budget.
- No proxy support in v0.2 for either endpoint form: the transport is constructed with
  `Proxy: nil` (plaintext h2c cannot traverse HTTP/1 forward proxies; CONNECT tunneling
  for TLS endpoints is deferred until someone needs it). Documented on NewGRPCWriter.
- No load balancing / service config / name-resolution beyond DNS.
- Unary only — OTLP defines no streaming export.

If operational reality later demands keepalives, that is the signal to revisit grpc-go —
the benchmark (§9.3) and this section are the recorded baseline for that conversation.

## 7. Options & defaults (delta)

New `options` fields: `insecure bool`, `tlsConfig *tls.Config`. Defaults: zero values
(bare endpoints → TLS with system roots). `normalize()` untouched — both fields are
valid at zero. All other defaults shared with HTTP (`timeout` 10s ≡ spec default).

## 8. doc.go & module facts

- `doc.go` gains the protocol-selection paragraph (NewWriter/NewHTTPWriter = 4318
  http/protobuf; NewGRPCWriter = 4317 grpc; ProtocolFromEnv for env-driven dispatch).
- Dependencies: **unchanged** (stdlib + zap + otel/trace + testify). Go ≥1.24 required
  for `http.Protocols` — go.mod already declares 1.25.0.
- Released as `otlp/v0.2.0`; `otlp/v0.1.0` API fully preserved.

## 9. Testing

### 9.1 Unit tests (otlp package, no new deps)

Go ≥1.24 `http.Server.Protocols` speaks unencrypted HTTP/2 server-side, so the gRPC
transport is unit-testable against an in-package fake gRPC server built on stdlib only:

- endpoint resolution matrix (scheme × WithInsecure × WithTLSConfig, path rejection,
  default port, conflict errors)
- frame encode (flag/length), per-message gzip round-trip
- status resolution precedence: trailers, trailers-only (status in headers), neither
  (HTTP-mapping fallback)
- retryability table, RESOURCE_EXHAUSTED with/without RetryInfo
- `grpc-status-details-bin` decode: unpadded + padded base64, malformed → backoff
- `grpc-message` percent-decode
- partial-success over gRPC (rejected / warning / malformed)
- header validation (reserved keys rejected at construction)
- lifecycle parity: the existing writer lifecycle tests run against a grpcTransport-backed
  writer where transport-agnostic (table-driven over both transports where cheap)

### 9.2 Conformance (otlp/internal/conformance, heavy deps quarantined)

Interop against a **real grpc-go** `LogsService` server (the spike scenarios, hardened):
success, partial success, Unavailable+RetryInfo (delay recovered exactly),
InvalidArgument, gzip request accepted, gzip response decoded, trailers-only handling,
connection reuse, and full-fidelity payload comparison via the official protobuf stubs
(every encoder field round-trips).

### 9.3 Benchmark vs grpc-go (conformance module)

Same in-process grpc-go server, two clients exporting identical batches:
(a) zapwire `grpcTransport`, (b) a conventional grpc-go client with official stubs.
Report ns/op, B/op, allocs/op per export at representative batch sizes (1, 64, 512
records). Result goes into the PR description and the user guide's protocol section.
Acceptance: hand-rolled within ~2× of grpc-go per-export CPU (it should win on allocs;
if it badly loses, the decision record §2 gets revisited before release).

### 9.4 Integration (opt-in build tags, existing pattern)

- `otelcollector` tag: extend the existing real-otelcol end-to-end test with a gRPC
  receiver variant (`receivers.otlp.protocols.grpc`), asserting body/attrs/trace fields
  land identically to the HTTP variant.
- `fluentbit` / `vector` gRPC variants: nice-to-have follow-ups, not gating (both
  ingest OTLP gRPC via their opentelemetry sources; the otelcol test already proves the
  wire).

### 9.5 Validation gate

Per repo rules: `go fix ./otlp/...`, `make lint`, `make test` (-race) before every
commit; conformance + integration runs before the PR.

## 10. Docs & examples

- User guide: extend the OTLP section with a protocol-selection table (when grpc vs
  http/protobuf), `NewGRPCWriter` usage (TLS forms), `ProtocolFromEnv` dispatch snippet,
  and the benchmark table.
- Runnable example mirroring the existing OTLP examples: gRPC export to a local
  collector (`examples/` pattern already established).
- README feature matrix row update (OTLP: gRPC + HTTP).

## 11. Scope

In scope: everything above. Out of scope: otx `zaplog` grpc-rejection lifting (separate
repo/PR, after `otlp/v0.2.0` tags), OTLP traces/metrics, streaming, grpc-go transport,
keepalive tuning, `http/json` protocol.
