# Changelog

All notable changes to this project are documented here. The repository hosts
two released modules — `github.com/arloliu/zapwire` (root, with the `fluent`,
`ndjson`, and `syslog` subpackages) and `github.com/arloliu/zapwire/otlp` —
which version independently; entries are grouped per module.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and both modules adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### otlp

- **Added:** OTLP/JSON encoding via `WithEncoding(otlp.JSON)` — spec-exact
  JSON Protobuf Encoding on the HTTP transport (transcode at ship time; the
  protobuf path is untouched). Construction error on the gRPC constructors.
- **Added:** gRPC integration-test variants against real receivers: Fluent Bit
  ingest + relay fidelity, Vector ingest, otel-collector TLS; OTLP/JSON
  ingest against a real otel-collector.
- **Changed (docs):** `WithMaxRequestBytes` documents that in JSON mode the
  cap governs the protobuf-equivalent request size (JSON wire bodies run
  1.5–3× larger).

### CI

- otel-collector (otelcol-contrib 0.154.0) and Vector (0.56.0) integration
  suites now run on every push/PR alongside the existing Fluent Bit (5.0.6)
  suite.

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
