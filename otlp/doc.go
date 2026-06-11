// Package otlp ships zap logs as native OTLP/HTTP binary protobuf
// (ExportLogsServiceRequest) with logs-to-traces correlation populated from
// context.Context as LogRecord proto fields. See
// docs/design/2026-06-11-otlp-logs-design.md in the repository for the full
// design. The package is its own Go module so importing it never adds
// OpenTelemetry dependencies to plain zapwire users.
package otlp
