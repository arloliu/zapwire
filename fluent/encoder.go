package fluent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Encoder is the v1 transcode encoder: it parses zap's JSON log line and emits a msgpack
// [time, record] Entry payload. It implements zapwire.Encoder. The TimeCodec controls which
// field carries the timestamp and how it is parsed.
type Encoder struct {
	codec TimeCodec
}

// NewEncoder returns a transcode Encoder with the default codec (AutoEpochCodec at "ts").
func NewEncoder() Encoder { return Encoder{codec: defaultTimeCodec()} }

// NewEncoderWithCodec returns a transcode Encoder using codec. An invalid (zero) codec
// falls back to the default.
func NewEncoderWithCodec(codec TimeCodec) Encoder {
	if !codec.valid() {
		codec = defaultTimeCodec()
	}

	return Encoder{codec: codec}
}

// Encode parses record (a JSON object) and appends its msgpack Entry payload to dst.
//
// JSON is decoded with UseNumber so numeric record fields keep their integer type and full
// precision on the msgpack wire: a plain json.Unmarshal decodes every JSON number to float64,
// which silently truncates int64/uint64 values above 2^53. The time field is extracted (and
// converted to float64 for the codec) before the remaining fields are normalized, so the
// codecs' float64 Decode contract is unaffected.
func (e Encoder) Encode(dst, record []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(record))
	dec.UseNumber()
	var rec map[string]any
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("fluent: unmarshal log record: %w", err)
	}

	t := e.extractTime(rec)
	normalizeNumbers(rec)

	entry := Entry{Time: EventTime(t), Record: rec}
	out, err := entry.MarshalMsg(dst)
	if err != nil {
		return nil, fmt.Errorf("fluent: marshal entry: %w", err)
	}

	return out, nil
}

// extractTime lifts the codec's time key out of rec and decodes it. The value is a json.Number
// (UseNumber); it is converted to float64 so the codecs' float64 Decode contract holds. An
// absent or unparseable value falls back to time.Now().
func (e Encoder) extractTime(rec map[string]any) time.Time {
	if v, present := rec[e.codec.Key]; present {
		delete(rec, e.codec.Key)
		if n, ok := v.(json.Number); ok {
			if f, err := n.Float64(); err == nil {
				v = f
			}
		}
		if t, ok := e.codec.Decode(v); ok && !t.IsZero() {
			return t
		}
	}

	return time.Now()
}

// normalizeNumbers replaces every json.Number in v (recursing into maps and slices) with a
// concrete numeric type — int64 or uint64 when integral and exactly representable, else
// float64 — so msgp.AppendIntf writes a msgpack integer rather than a lossy float64.
func normalizeNumbers(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = normalizeNumber(val)
		}
	case []any:
		for i, val := range t {
			t[i] = normalizeNumber(val)
		}
	}
}

// normalizeNumber converts a single decoded value: a json.Number to its tightest numeric type,
// a map/slice by recursing, anything else unchanged.
func normalizeNumber(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if u, err := strconv.ParseUint(t.String(), 10, 64); err == nil {
			return u
		}
		f, _ := t.Float64()

		return f
	case map[string]any, []any:
		normalizeNumbers(t)

		return t
	default:
		return v
	}
}
