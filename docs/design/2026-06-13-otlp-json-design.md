# OTLP/JSON Encoding for the `otlp` Subpackage

Date: 2026-06-13. Status: approved for implementation (v2 — addresses all
P0/P1 findings of `tmp/2026-06-13-otlp-json-design_v1_review.md`).
Builds on: `2026-06-11-otlp-logs-design.md` (HTTP transport, encoder, envelope),
`2026-06-12-otlp-grpc-design.md` (transport seam).

## 1. Motivation & scope

The OTLP logs design (2026-06-11 §10) deferred OTLP/JSON as "spec-MAY for servers;
cheap to add behind the same encoder seam later". This design adds it: an opt-in
**OTLP/HTTP JSON Protobuf Encoding** mode (`Content-Type: application/json` to
`/v1/logs`) for receivers that only accept JSON or for debugging pipelines where a
human-readable wire payload is worth the CPU.

**In scope:** OTLP/JSON over HTTP; spec-exact JSON Protobuf Encoding (hex trace IDs,
integer enums, lowerCamelCase, 64-bit-as-string); JSON partial-success handling; gzip
composition; conformance against the collector's reference `pdata` JSON unmarshaler;
real-collector integration test; docs.

**Out of scope:** JSON over gRPC (does not exist — gRPC is protobuf by definition;
`WithEncoding(JSON)` + gRPC constructor is a construction error). OTLP/JSON for
traces/metrics (no such signals here). Reading `OTEL_EXPORTER_OTLP_PROTOCOL`
(env handling stays explicit and opt-in, design 2026-06-11 precedent).

## 2. Decision record: transcode at the transport boundary

Two candidate architectures:

1. **Parallel JSON encoder** — a second `zapcore.Encoder` emitting JSON LogRecord
   fragments, plus a JSON envelope. Rejected: duplicates the entire field-mapping
   surface (every `AddX`, namespaces, arrays, reflected handling) and invites
   protobuf/JSON divergence — two encoders can drift; one cannot.
2. **Proto→JSON transcode at `prepare()`** — the existing encoder + envelope produce
   the assembled `ExportLogsServiceRequest` protobuf exactly as today; in JSON mode
   the HTTP transport transcodes those bytes to JSON once per batch, then gzips.
   **Chosen.** Single encode path stays the source of truth; the transcoder is a
   ~small table-driven walker over a fixed, frozen schema (OTLP logs proto is stable
   v1.x); the protobuf path pays zero cost.

Consequences accepted:
- JSON mode costs one extra pass + allocation per batch. Fine: JSON is a
  compatibility/debug mode; throughput-sensitive users stay on protobuf/gRPC.
- `WithMaxRequestBytes` continues to govern the **protobuf-equivalent** request size
  (batch cutting happens before transcoding). The JSON body is typically 1.5–3×
  larger on the wire. This is a **load-bearing documentation change**, not a
  footnote: the `WithMaxRequestBytes` godoc itself, the `WithEncoding` godoc, the
  guide, and the README all state that in JSON mode the cap is protobuf-equivalent
  and callers targeting a receiver's body limit must lower the cap accordingly
  (e.g. a 4 MiB receiver limit → cap at ~1.3 MiB). A separate JSON-wire-size cap
  is **deliberately rejected**: enforcing it would require transcoding before
  batch cutting (moving the JSON cost onto every record) or post-transcode
  splitting, and splitting alters the at-most-once batching story inherited from
  the parent design (its §10 already rejects 413 split-and-retry for the same
  reason). A 413 stays a non-retryable counted drop surfaced through
  `WithErrorHandler`, same as protobuf.

## 3. Wire mapping (spec-exact)

Per opentelemetry-proto `docs/specification.md` "JSON Protobuf Encoding":

| Rule | Mapping |
|---|---|
| Field names | lowerCamelCase (`timeUnixNano`, `severityNumber`, `resourceLogs`, …) |
| `trace_id` / `span_id` | lowercase hex strings (OTLP deviation — NOT base64) |
| Other `bytes` (AnyValue.bytes_value) | standard base64 |
| 64-bit ints (fixed64 times, AnyValue.int_value) | decimal **strings** (proto3 JSON) |
| Enums (`severityNumber`) | **integer** values (OTLP deviation — names prohibited) |
| `flags` (fixed32) | JSON number |
| double | JSON number; `NaN`/`Infinity`/`-Infinity` as strings (proto3 JSON) |
| Absent fields | omitted (we transcode only fields present in the proto bytes) |
| Content-Type | `application/json` on request; server MUST mirror it on response |

## 4. Transcoder (`json.go`)

A table-driven walker over the proto bytes, stdlib-only (`encoding/base64`,
`encoding/hex`, `strconv`, `math`, `unicode/utf8` — **no** `encoding/json`: output is
appended to a `[]byte` like every other writer in the package, with a local
JSON-string escaper).

- Schema tables: `map[fieldNum]jsonField{name, kind}` per message type. The
  tables carry **every field of the frozen OTLP logs v1 schema**, including ones
  this package never emits today, so the transcoder stays correct if the encoder
  grows. Kinds: `string`, `bytesHex`, `bytesB64`, `u64str` (fixed64→string),
  `i64str` (varint int64→string), `uintNum` (varint→number: enums, uint32
  counts), `fixed32num`, `doubleNum`, `boolVal`, `msg`, and `repeated` variants
  of `msg`.

  Full field inventory (proto number → JSON name; ✱ = emitted by the current
  encoder/envelope, the rest are schema-complete headroom):

  | Message | Fields |
  |---|---|
  | ExportLogsServiceRequest | 1✱ resourceLogs |
  | ResourceLogs | 1✱ resource, 2✱ scopeLogs, 3 schemaUrl |
  | Resource | 1✱ attributes, 2 droppedAttributesCount |
  | ScopeLogs | 1✱ scope, 2✱ logRecords, 3 schemaUrl |
  | InstrumentationScope | 1✱ name, 2✱ version, 3 attributes, 4 droppedAttributesCount |
  | LogRecord | 1✱ timeUnixNano, 2✱ severityNumber, 3✱ severityText, 5✱ body, 6✱ attributes, 7 droppedAttributesCount, 8✱ flags, 9✱ traceId, 10✱ spanId, 11✱ observedTimeUnixNano, 12 eventName |
  | KeyValue | 1✱ key, 2✱ value |
  | AnyValue | 1✱ stringValue, 2✱ boolValue, 3✱ intValue, 4✱ doubleValue, 5✱ arrayValue, 6✱ kvlistValue, 7✱ bytesValue |
  | ArrayValue | 1✱ values |
  | KeyValueList | 1✱ values |

  **Rule:** any future encoder/envelope change that emits a new field must update
  the schema table and the golden tests in the same change.
- Two-phase per message: scan once collecting `(fieldNum → values in wire order)`,
  then emit grouped — handles repeated fields regardless of contiguity (proto
  permits interleaving even though our own assembly is contiguous) and applies
  proto's last-one-wins for duplicated scalars.
- Input is **always our own assembly** (encoder + envelope), so an unknown field
  number or wire-type mismatch is an internal invariant violation: transcode fails
  and the batch becomes a fatal prepare outcome (§6) — never shipped malformed.
  This cannot happen short of a bug; the error path exists so a bug is loud, not
  silent.

## 5. API surface

```go
type Encoding int

const (
    Protobuf Encoding = iota // default — binary protobuf (v0.1.0 behavior)
    JSON                     // OTLP/HTTP JSON Protobuf Encoding
)

func WithEncoding(e Encoding) Option
```

- Mirrors the existing `Compression`/`WithCompression(Gzip)` pattern. The zero
  value is `Protobuf`, so existing callers are untouched.
- Validation ownership (construction-time, like every deterministic config
  error — §4.4 precedent in the gRPC design):
  - `NewWriter` / `NewHTTPWriter` / `NewCore` / `NewHTTPCore`: honored;
    **undefined** `Encoding` values → construction error in `newHTTPTransport`.
  - `NewGRPCWriter` / `NewGRPCCore` + `WithEncoding(JSON)`: construction error
    in `newGRPCTransport` (same seam as the `WithInsecure`/`WithTLSConfig`
    conflicts).
  - `NewEncoder`: ignores the option (documented). It cannot return an error
    and emits the same bare-protobuf `LogRecord` bytes regardless of transport
    encoding — the transcode lives entirely at the ship layer.

## 6. HTTP transport delta

`httpTransport` gains `jsonOn bool`:

- **Fatal prepare seam (new, P0 fix):** `prepared` gains a `fail *ExportError`
  field — a fatal prepare outcome, distinct from the non-fatal `warn`. In
  `writer.export`, a non-nil `p.fail` short-circuits **before** the retry loop:
  `w.drop(len(records), p.fail)` (whole batch counted, error handler invoked via
  the existing drop path) and `attempt` is **never called** — there is no valid
  body to ship. This holds identically on the normal flush path and the
  Close-drain path (both funnel through `export`). The gRPC and protobuf-HTTP
  transports never set `fail`, so their behavior is bit-for-bit unchanged.
- `prepare()`: in JSON mode, transcode proto→JSON **before** gzip (per-batch, once —
  a retrying batch must not re-transcode; same rule as compression). A transcode
  failure sets `prepared.fail` (§4 invariant violation → counted drop).
- `attempt()`: `Content-Type: application/json` in JSON mode.
- **Partial success — decode/classify split:** the shared classification is
  extracted as `classifyAccept(rejected, msg, decodeErr, base)`; `resolveAccept`
  (proto decode) and `resolveAcceptJSON` (JSON decode) both route through it, so
  the writer-visible semantics (rejected>0 → counted drop; message-only →
  warning; malformed → observability-only; empty → clean accept) are decoder-
  independent.
- **JSON-mode response policy (content-type mismatch):** the spec requires the
  server to mirror `application/json`, but proxies/receivers violate it. Policy:
  in JSON mode the 200-response decoder is selected by the **response**
  `Content-Type` — `application/x-protobuf` → the proto decoder (so a
  spec-violating-but-honest protobuf partial success still counts its
  rejections); anything else (including missing) → the JSON decoder, whose
  parse failure lands in the existing malformed/observability-only class.
  Protobuf mode is untouched (v0.1.0 verbatim — no sniffing).
- **Exact int64 parsing:** `rejectedLogRecords` is accepted as a JSON number or
  decimal string (proto3 JSON emits int64 as string; real receivers send both),
  parsed with `strconv.ParseInt` on the raw token — never through float64.
  Fractions, non-decimal strings, and overflow are malformed (observability-only);
  negative counts are likewise rejected as malformed. Empty body / `{}` /
  `null` → clean accept, as today.
- Error responses (4xx/5xx): unchanged — body excerpt into `Message` works for the
  JSON `Status` payload as-is.
- Retry semantics, `Retry-After`, retryable status classes: completely unchanged.

## 7. Testing

**Oracle hierarchy (P1 fix):** exact **golden JSON tests are the load-bearing
spec-compliance oracle** — they pin every OTLP deviation byte-for-byte
(lowerCamelCase names, integer `severityNumber`, decimal-string
`timeUnixNano`/`observedTimeUnixNano`/`intValue`, lowercase-hex
`traceId`/`spanId`, base64 `bytesValue`, `"NaN"`/`"Infinity"`/`"-Infinity"`).
The pdata round-trip is a **semantic acceptance oracle** only: it proves a real
receiver ingests our JSON identically to our protobuf, but pdata's unmarshaler
is permissive (it accepts enum names, base64-ish forms, etc.), so it cannot
prove deviation compliance by itself.

1. **Unit (otlp package, stdlib-only) — load-bearing:**
   - Golden JSON for representative requests: every AnyValue kind (string
     escaping incl. control chars and invalid UTF-8 replacement, bool, negative
     int64, NaN/±Inf doubles, nested array/kvlist, bytes), trace-context fields
     (hex IDs, flags), resource/scope blobs, multi-record batches.
   - Transcode-failure path: a corrupt-bytes fixture proving `prepared.fail` →
     whole batch counted dropped, error handler invoked, `attempt` never called
     (counting fake transport), on both flush and Close-drain paths.
   - Gzip ordering: capture a JSON+gzip request, decompress, assert JSON bytes.
   - Retry single-transcode: a retrying JSON batch transcodes once (counting
     transcoder seam or request-body capture across attempts).
   - `decodePartialSuccessJSON` table: empty body, `{}`, `null`, rejected as
     string, as number, at `math.MaxInt64`, fractional, negative, overflow,
     non-decimal string, message-only warning, malformed JSON.
   - Response-mismatch policy: JSON request answered by (a) protobuf body +
     `application/x-protobuf` (rejections still counted), (b) wrong/missing
     content type with JSON body, (c) malformed body.
   - Constructor matrix: zero-value default, undefined Encoding on HTTP
     writer/core, JSON+gRPC rejection, `NewEncoder(WithEncoding(JSON))` no-op.
2. **Conformance (internal/conformance — heavy deps allowed) — semantic:**
   build zap entries → our proto request → (a) `plogotlp.ExportRequest.
   UnmarshalProto`, (b) our transcoder → `plogotlp.ExportRequest.UnmarshalJSON`
   → assert (a) and (b) marshal to identical proto bytes. Plus a small probe
   test documenting what pdata's JSON unmarshaler accepts (so future reviewers
   know what this oracle does and does not prove). Adds
   `go.opentelemetry.io/collector/pdata` (pinned in go.mod like every dep) to
   the conformance module only — the otlp module itself stays protobuf-free
   (dependency-quarantine check: `go list -deps` greps, same discipline as the
   root module's msgp guarantee).
3. **Integration (otelcollector tag):** `TestCollectorEndToEndJSON` — real
   collector OTLP/HTTP receiver ingesting `WithEncoding(JSON)` output, same
   file-exporter oracle as the existing variants.

## 8. Docs

- `WithEncoding` godoc: spec mapping summary, the `WithMaxRequestBytes`
  protobuf-equivalent caveat, gRPC rejection.
- `WithMaxRequestBytes` godoc: gains the JSON-mode caveat (protobuf-equivalent
  cap; wire body 1.5–3× larger; lower the cap when targeting receiver limits).
- guide.md OTLP protocol section: when to choose http/json (receiver
  compatibility, debugging) vs protobuf (everything else), incl. the cap
  caveat; README matrix cell.

## 9. Module & dependencies

No new dependencies in `github.com/arloliu/zapwire/otlp` (stdlib additions only).
`internal/conformance` (test-only module) adds `go.opentelemetry.io/collector/pdata`.
