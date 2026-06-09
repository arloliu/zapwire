package ndjson

import (
	"bytes"

	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// Encoder produces the per-line payload from zap's JSON output: the JSON object with any
// trailing newline trimmed (the Framer adds exactly one). It implements zapwire.Encoder.
type Encoder struct{}

// NewEncoder returns an NDJSON Encoder.
//
// Returns:
//   - Encoder: an encoder that trims a trailing newline from each record
func NewEncoder() Encoder { return Encoder{} }

// Encode appends record to dst with any trailing newline trimmed (the Framer
// adds exactly one). It implements zapwire.Encoder.
//
// Parameters:
//   - dst: buffer to append to; may be nil or a pooled slice to reuse
//   - record: one log entry as JSON (zap's output line)
//
// Returns:
//   - []byte: dst extended with the trimmed record
//   - error: always nil (kept for the zapwire.Encoder contract)
func (Encoder) Encode(dst, record []byte) ([]byte, error) {
	return append(dst, bytes.TrimRight(record, "\n")...), nil
}

// Framer terminates each payload with a single newline. It implements zapwire.Framer.
type Framer struct{}

// NewFramer returns an NDJSON Framer.
//
// Returns:
//   - Framer: a framer that terminates each payload with a single newline
func NewFramer() Framer { return Framer{} }

// Frame appends each payload followed by a single '\n' to dst. It implements
// zapwire.Framer.
//
// Parameters:
//   - dst: buffer to append to; may be nil or a pooled slice to reuse
//   - payloads: per-line payloads; each is written then newline-terminated
//
// Returns:
//   - []byte: dst extended with the newline-delimited payloads
//   - error: always nil (kept for the zapwire.Framer contract)
func (Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		dst = append(dst, p...)
		dst = append(dst, '\n')
	}

	return dst, nil
}

// NewWriter builds a zapwire.Writer that ships newline-delimited JSON over t.
//
// Parameters:
//   - t: transport to ship lines over; must be non-nil
//   - opts: core zapwire options (mode, buffer, timeouts, …)
//
// Returns:
//   - *zapwire.Writer: the writer; the caller owns it and must Close it
//   - error: a non-nil error from the underlying zapwire.New
func NewWriter(t zapwire.Transport, opts ...zapwire.Option) (*zapwire.Writer, error) {
	return zapwire.New(t, NewEncoder(), NewFramer(), opts...)
}

// NewCore builds a zapcore.Core (JSON-encoding into the NDJSON writer) plus the
// underlying writer, which the caller must Close.
//
// Parameters:
//   - t: transport to ship lines over; must be non-nil
//   - level: minimum level an entry must meet to be encoded
//   - encCfg: zap JSON encoder config
//   - opts: core zapwire options (mode, buffer, timeouts, …)
//
// Returns:
//   - zapcore.Core: a JSON-encoding core writing newline-delimited output
//   - *zapwire.Writer: the underlying writer; the caller must Close it
//   - error: a non-nil error if the writer cannot be built
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...zapwire.Option,
) (zapcore.Core, *zapwire.Writer, error) {
	w, err := NewWriter(t, opts...)
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(zapcore.NewJSONEncoder(encCfg), w, level), w, nil
}
