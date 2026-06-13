# Changelog

All notable changes to this project are documented here. The repository hosts
two released modules — `github.com/arloliu/zapwire` (root, with the `fluent`,
`ndjson`, and `syslog` subpackages) and `github.com/arloliu/zapwire/otlp` —
which version independently; entries are grouped per module.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and both modules adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## v1.0.1 — 2026-06-13

Security hardening, no API changes. From a codebase security audit (defensive
log-shipping threat model: app-provided content, hostile receiver responses,
configuration):

- **fluent:** reject a batch whose total size would exceed the msgpack `bin32`
  length header (> 4 GiB) instead of emitting a truncated header that desyncs the
  receiver's parser. (Pathological reachability; closes a wire-integrity gap.)
- **fluent:** cap container nesting depth so a pathological/attacker-shaped
  object/array marshaler cannot recurse the goroutine stack to exhaustion (an
  uncatchable fatal throw on the logging path); over-deep fields degrade to
  `<key>Error` and the entry still ships.
- **core:** clamp `WithBufferSize` to a high-side ceiling (1<<24) so a
  fat-fingered value cannot OOM-panic the queue allocation at construction
  (extends the existing "clamp, don't error" contract to the high side).

## otlp/v0.5.0 — 2026-06-13

- **Added:** `WithDrainTimeout(d)` — bounds the total time `Sync`/`Close` spend
  draining the queue before dropping the remainder (counted), capping the
  worst-case shutdown latency a stalled or repeatedly-failing receiver can
  impose. Default 0 keeps the unbounded barrier (no behavior change).
- **Security (no API change):** cap container nesting depth in the OTLP encoder
  (same uncatchable-recursion fix as the root encoders; the JSON transcoder is
  bounded transitively); clamp `WithQueueSize` / `WithMaxRequestBytes` to
  high-side ceilings; pin the gRPC TLS `MinVersion` to 1.2 explicitly rather than
  relying on the stdlib default; set the transport-owned `Content-Type` /
  `Content-Encoding` last so `WithHeaders` cannot override them.

## otlp/v0.4.0 — 2026-06-13

- **BREAKING:** removed `NewWriter` and `NewCore`. Use the protocol-explicit
  `NewHTTPWriter` / `NewHTTPCore` (exact drop-in replacements — OTLP/HTTP is the
  spec's default protocol) or `NewGRPCWriter` / `NewGRPCCore`. This removes the
  ambiguous aliases ahead of the otlp module's v1.0.0 API freeze, leaving one
  constructor name per transport (`NewHTTP*` / `NewGRPC*`). Migration is a
  rename: `otlp.NewCore(` → `otlp.NewHTTPCore(`, `otlp.NewWriter(` →
  `otlp.NewHTTPWriter(`.

## v1.0.0 — 2026-06-13

Stability release. No API changes since v0.1.0; this tag formalises the
exported-API compatibility promise described in the README.

## otlp/v0.3.0 — 2026-06-13

- **Added:** `WithEncoding(otlp.JSON)` — OTLP/JSON encoding on the HTTP
  transport: spec-exact JSON Protobuf Encoding, `Content-Type: application/json`,
  lowerCamelCase field names, lowercase-hex `traceId`/`spanId`, decimal-string
  64-bit integers. The protobuf encode path is untouched; JSON mode pays one
  extra transcode per batch. Construction error on the gRPC constructors
  (OTLP/gRPC is protobuf-only by spec).
- **Changed (docs):** `WithMaxRequestBytes` now documents that in JSON mode the
  cap governs the protobuf-equivalent request size — JSON wire bodies are
  1.5–3× larger; lower the cap accordingly when targeting a receiver body limit.

## otlp/v0.2.0 — 2026-06-13

- **Added:** OTLP/gRPC transport (`NewGRPCWriter` / `NewGRPCCore`) — a
  hand-rolled unary gRPC client over stdlib HTTP/2 (h2c via `WithInsecure`,
  TLS via bare endpoints / `https://` / `WithTLSConfig`); gRPC status
  classification, RetryInfo, per-message gzip. No grpc-go dependency.
- **Added:** `ExportError.GRPCStatus`; `ProtocolFromEnv` for
  `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL`-driven dispatch.
- **Added:** `WithTraceCorrelationAttributes` — flat lowercase-hex
  `trace_id`/`span_id` string attributes for non-OTLP conversion pipelines.

## otlp/v0.1.0 — 2026-06-12

- Initial release of the OTLP logs exporter: OTLP/HTTP binary protobuf to
  `/v1/logs` with a hand-rolled proto encoder (no protobuf dependency),
  ctx-based trace correlation, resource/scope configuration, gzip, OTLP
  retry semantics, partial-success accounting, byte-aware batching.

## v0.1.0 — 2026-06-12

- Initial release of the root module: the reconnecting, bounded,
  drop-on-stall `Writer` core with UDS/TCP transports; `fluent` (Fluent
  Forward msgpack — transcode and native encoder paths), `ndjson`
  (newline-delimited JSON), and `syslog` (RFC5424 over UDS/TCP) subpackages.
