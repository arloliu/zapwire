package otlp

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// OTLP/JSON transcoding (design 2026-06-13). The encoder and envelope keep
// producing binary-protobuf ExportLogsServiceRequest bytes — the single
// source of truth — and in JSON mode the HTTP transport transcodes those
// bytes to the spec's "JSON Protobuf Encoding" once per batch:
//
//   - field names in lowerCamelCase;
//   - trace_id/span_id as lowercase hex strings (OTLP deviation, not base64);
//   - other bytes (AnyValue.bytes_value) as standard base64;
//   - 64-bit integers (fixed64 timestamps, AnyValue.int_value) as decimal
//     strings (proto3 JSON);
//   - enums (severityNumber) as integer values (OTLP deviation — names are
//     prohibited);
//   - doubles as JSON numbers, with NaN/±Inf as the proto3 JSON strings
//     "NaN"/"Infinity"/"-Infinity";
//   - absent fields omitted (only fields present in the proto bytes appear).
//
// The walker is table-driven over the frozen OTLP logs v1 schema. Its input
// is always this package's own assembly, so a schema miss or wire-type
// mismatch is an internal invariant violation: the transcode fails and the
// batch becomes a counted drop — loud, never shipped malformed.

// jsonKind is the JSON projection of one proto field.
type jsonKind int

const (
	jkString   jsonKind = iota // len-delimited → JSON string (escaped)
	jkBytesHex                 // len-delimited → lowercase-hex JSON string
	jkBytesB64                 // len-delimited → base64 JSON string
	jkU64Str                   // fixed64 → decimal JSON string
	jkI64Str                   // varint int64 → decimal JSON string
	jkUintNum                  // varint → JSON number (uint32 counts, enums)
	jkFixed32                  // fixed32 → JSON number
	jkDouble                   // fixed64 bits → JSON number / "NaN"/"±Infinity"
	jkBool                     // varint → true/false
	jkMsg                      // len-delimited → nested object via sub schema
)

// schemaID indexes jsonSchemas; an int indirection (rather than nested map
// literals) because AnyValue ↔ KeyValueList ↔ KeyValue is cyclic.
type schemaID int

const (
	schExportRequest schemaID = iota
	schResourceLogs
	schResource
	schScopeLogs
	schScope
	schLogRecord
	schKeyValue
	schAnyValue
	schArrayValue
	schKVList
	schNone // scalar fields
)

type jsonField struct {
	name     string
	kind     jsonKind
	repeated bool
	sub      schemaID // jkMsg only
}

// jsonSchemas mirrors the OTLP logs v1 proto schema (frozen on the wire).
// Every field of every message is present — including ones this package
// never emits (schemaUrl, droppedAttributesCount, eventName) — so the
// transcoder stays correct if the encoder grows.
var jsonSchemas = [...]map[int]jsonField{
	schExportRequest: {
		1: {name: "resourceLogs", kind: jkMsg, repeated: true, sub: schResourceLogs},
	},
	schResourceLogs: {
		1: {name: "resource", kind: jkMsg, sub: schResource},
		2: {name: "scopeLogs", kind: jkMsg, repeated: true, sub: schScopeLogs},
		3: {name: "schemaUrl", kind: jkString},
	},
	schResource: {
		1: {name: "attributes", kind: jkMsg, repeated: true, sub: schKeyValue},
		2: {name: "droppedAttributesCount", kind: jkUintNum},
	},
	schScopeLogs: {
		1: {name: "scope", kind: jkMsg, sub: schScope},
		2: {name: "logRecords", kind: jkMsg, repeated: true, sub: schLogRecord},
		3: {name: "schemaUrl", kind: jkString},
	},
	schScope: {
		1: {name: "name", kind: jkString},
		2: {name: "version", kind: jkString},
		3: {name: "attributes", kind: jkMsg, repeated: true, sub: schKeyValue},
		4: {name: "droppedAttributesCount", kind: jkUintNum},
	},
	schLogRecord: {
		1:  {name: "timeUnixNano", kind: jkU64Str},
		2:  {name: "severityNumber", kind: jkUintNum},
		3:  {name: "severityText", kind: jkString},
		5:  {name: "body", kind: jkMsg, sub: schAnyValue},
		6:  {name: "attributes", kind: jkMsg, repeated: true, sub: schKeyValue},
		7:  {name: "droppedAttributesCount", kind: jkUintNum},
		8:  {name: "flags", kind: jkFixed32},
		9:  {name: "traceId", kind: jkBytesHex},
		10: {name: "spanId", kind: jkBytesHex},
		11: {name: "observedTimeUnixNano", kind: jkU64Str},
		12: {name: "eventName", kind: jkString},
	},
	schKeyValue: {
		1: {name: "key", kind: jkString},
		2: {name: "value", kind: jkMsg, sub: schAnyValue},
	},
	schAnyValue: {
		1: {name: "stringValue", kind: jkString},
		2: {name: "boolValue", kind: jkBool},
		3: {name: "intValue", kind: jkI64Str},
		4: {name: "doubleValue", kind: jkDouble},
		5: {name: "arrayValue", kind: jkMsg, sub: schArrayValue},
		6: {name: "kvlistValue", kind: jkMsg, sub: schKVList},
		7: {name: "bytesValue", kind: jkBytesB64},
	},
	schArrayValue: {
		1: {name: "values", kind: jkMsg, repeated: true, sub: schAnyValue},
	},
	schKVList: {
		1: {name: "values", kind: jkMsg, repeated: true, sub: schKeyValue},
	},
}

// wireTypeFor is the wire type each kind must arrive with; a mismatch is an
// invariant violation (the bytes are our own assembly).
func wireTypeFor(k jsonKind) int {
	switch k {
	case jkString, jkBytesHex, jkBytesB64, jkMsg:
		return 2
	case jkU64Str, jkDouble:
		return 1
	case jkFixed32:
		return 5
	case jkI64Str, jkUintNum, jkBool:
		return 0
	}

	return -1
}

// jsonVal is one decoded wire value: payload for len-delimited fields,
// num for varint/fixed fields.
type jsonVal struct {
	payload []byte
	num     uint64
}

// appendRequestJSON transcodes an assembled ExportLogsServiceRequest to its
// OTLP/JSON form, appending to dst.
func appendRequestJSON(dst, msg []byte) ([]byte, error) {
	return appendMsgJSON(dst, msg, schExportRequest)
}

// appendMsgJSON transcodes one proto message. Two phases: scan the wire
// fields in order (grouping repeated fields by first appearance — proto
// permits interleaving even though our own assembly is contiguous; scalar
// duplicates apply proto's last-one-wins), then emit.
func appendMsgJSON(dst, b []byte, sid schemaID) ([]byte, error) {
	schema := jsonSchemas[sid]
	vals := make(map[int][]jsonVal, len(schema))
	var order []int

	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		f, ok := schema[num]
		if !ok {
			return nil, fmt.Errorf("otlp: json transcode: unknown field %d in schema %d", num, sid)
		}
		if wt != wireTypeFor(f.kind) {
			return nil, fmt.Errorf("otlp: json transcode: field %d (%s) has wire type %d, want %d", num, f.name, wt, wireTypeFor(f.kind))
		}
		var v jsonVal
		switch wt {
		case 0: // varint
			x, vn := binary.Uvarint(b)
			if vn <= 0 {
				return nil, errTruncatedResponse
			}
			v.num = x
			b = b[vn:]
		case 1: // fixed64
			if len(b) < 8 {
				return nil, errTruncatedResponse
			}
			v.num = binary.LittleEndian.Uint64(b)
			b = b[8:]
		case 2: // len-delimited
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l { //nolint:gosec // ln <= len(b) after the Uvarint check
				return nil, errTruncatedResponse
			}
			v.payload = b[ln : ln+int(l)] //nolint:gosec
			b = b[ln+int(l):]             //nolint:gosec
		case 5: // fixed32
			if len(b) < 4 {
				return nil, errTruncatedResponse
			}
			v.num = uint64(binary.LittleEndian.Uint32(b))
			b = b[4:]
		}
		if _, seen := vals[num]; !seen {
			order = append(order, num)
		}
		vals[num] = append(vals[num], v)
	}

	dst = append(dst, '{')
	for i, num := range order {
		f := schema[num]
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, f.name)
		dst = append(dst, ':')
		var err error
		if f.repeated {
			dst = append(dst, '[')
			for j, v := range vals[num] {
				if j > 0 {
					dst = append(dst, ',')
				}
				dst, err = appendValJSON(dst, v, f)
				if err != nil {
					return nil, err
				}
			}
			dst = append(dst, ']')

			continue
		}
		dst, err = appendValJSON(dst, vals[num][len(vals[num])-1], f) // last wins
		if err != nil {
			return nil, err
		}
	}

	return append(dst, '}'), nil
}

func appendValJSON(dst []byte, v jsonVal, f jsonField) ([]byte, error) {
	switch f.kind {
	case jkString:
		return appendJSONString(dst, string(v.payload)), nil
	case jkBytesHex:
		dst = append(dst, '"')
		dst = hex.AppendEncode(dst, v.payload)

		return append(dst, '"'), nil
	case jkBytesB64:
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, v.payload)

		return append(dst, '"'), nil
	case jkU64Str:
		dst = append(dst, '"')
		dst = strconv.AppendUint(dst, v.num, 10)

		return append(dst, '"'), nil
	case jkI64Str:
		dst = append(dst, '"')
		dst = strconv.AppendInt(dst, int64(v.num), 10) //nolint:gosec // proto int64: two's-complement round-trip

		return append(dst, '"'), nil
	case jkUintNum:
		return strconv.AppendUint(dst, v.num, 10), nil
	case jkFixed32:
		return strconv.AppendUint(dst, v.num, 10), nil
	case jkBool:
		if v.num != 0 {
			return append(dst, "true"...), nil
		}

		return append(dst, "false"...), nil
	case jkDouble:
		fv := math.Float64frombits(v.num)
		switch {
		case math.IsNaN(fv):
			return append(dst, `"NaN"`...), nil
		case math.IsInf(fv, 1):
			return append(dst, `"Infinity"`...), nil
		case math.IsInf(fv, -1):
			return append(dst, `"-Infinity"`...), nil
		}

		return strconv.AppendFloat(dst, fv, 'g', -1, 64), nil
	case jkMsg:
		return appendMsgJSON(dst, v.payload, f.sub)
	}

	return nil, fmt.Errorf("otlp: json transcode: unhandled kind %d", f.kind)
}

// appendJSONString appends s as a JSON string: quotes and backslashes
// escaped, control characters as \n/\r/\t or \u00XX, invalid UTF-8 replaced
// with U+FFFD (encoding/json behavior). No HTML escaping — this is a wire
// payload, not markup.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"':
				dst = append(dst, '\\', '"')
			case c == '\\':
				dst = append(dst, '\\', '\\')
			case c == '\n':
				dst = append(dst, '\\', 'n')
			case c == '\r':
				dst = append(dst, '\\', 'r')
			case c == '\t':
				dst = append(dst, '\\', 't')
			case c < 0x20:
				const hexDigits = "0123456789abcdef"
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			default:
				dst = append(dst, c)
			}
			i++

			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, "�"...)
			i++

			continue
		}
		dst = append(dst, s[i:i+size]...)
		i += size
	}

	return append(dst, '"')
}

// decodePartialSuccessJSON parses a JSON ExportLogsServiceResponse body —
// the server mirrors the request Content-Type (spec), so JSON-mode 200
// responses are JSON. rejectedLogRecords is accepted as a JSON number or a
// decimal string (proto3 JSON emits int64 as string; real receivers send
// both). Empty body / {} → clean accept, matching the proto path.
func decodePartialSuccessJSON(body []byte) (rejected int64, msg string, err error) {
	if len(body) == 0 {
		return 0, "", nil
	}
	var doc struct {
		PartialSuccess struct {
			Rejected json.RawMessage `json:"rejectedLogRecords"`
			Message  string          `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if uerr := json.Unmarshal(body, &doc); uerr != nil {
		return 0, "", uerr
	}
	raw := strings.TrimSpace(string(doc.PartialSuccess.Rejected))
	raw = strings.Trim(raw, `"`)
	if raw == "" || raw == "null" {
		return 0, doc.PartialSuccess.Message, nil
	}
	n, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return 0, "", fmt.Errorf("otlp: malformed rejectedLogRecords %q: %w", raw, perr)
	}
	if n < 0 {
		return 0, "", fmt.Errorf("otlp: negative rejectedLogRecords %d", n)
	}

	return n, doc.PartialSuccess.Message, nil
}

// resolveAcceptJSON is resolveAccept's JSON-mode counterpart: same
// partial-success classification, JSON decode.
func resolveAcceptJSON(respBody []byte, base ExportError) *acceptance {
	rejected, msg, derr := decodePartialSuccessJSON(respBody)

	return classifyAccept(rejected, msg, derr, base)
}
