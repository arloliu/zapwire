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
func NewEncoder() Encoder { return Encoder{} }

// Encode appends record (trailing newline trimmed) to dst.
func (Encoder) Encode(dst, record []byte) ([]byte, error) {
	return append(dst, bytes.TrimRight(record, "\n")...), nil
}

// Framer terminates each payload with a single newline. It implements zapwire.Framer.
type Framer struct{}

// NewFramer returns an NDJSON Framer.
func NewFramer() Framer { return Framer{} }

// Frame appends each payload followed by '\n' to dst.
func (Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		dst = append(dst, p...)
		dst = append(dst, '\n')
	}

	return dst, nil
}

// NewWriter builds a zapwire.Writer that ships newline-delimited JSON over t.
func NewWriter(t zapwire.Transport, opts ...zapwire.Option) (*zapwire.Writer, error) {
	return zapwire.New(t, NewEncoder(), NewFramer(), opts...)
}

// NewCore builds a zapcore.Core (JSON-encoding into the NDJSON writer) plus the underlying
// writer, which the caller must Close.
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
