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
// Sync mode performs an inline, deadline-bounded write, so a sync caller waits for its own
// write up to WithWriteTimeout, and concurrent sync callers serialize on the single in-flight
// write; the wait is bounded and never unbounded. Async mode is the truly non-blocking path:
// Write enqueues and returns. In both modes a stalled or absent consumer drops logs and counts
// them (see Writer.DroppedLogs) rather than blocking indefinitely. Buffered logs are lost on a
// hard crash — zapwire is an at-most-once shipper, not a write-ahead log.
package zapwire
