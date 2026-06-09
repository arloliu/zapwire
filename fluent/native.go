package fluent

import (
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// NewMsgpackEncoder returns a zapcore.Encoder that emits Fluent Forward
// [EventTime, record] msgpack payloads directly from zap's structured fields,
// with no JSON round-trip. Envelope time comes from zapcore.Entry.Time
// (exact); TimeCodec / EncoderConfig time settings do NOT affect it
// (design §3.8).
//
// Parameters:
//   - cfg: zap encoder config controlling field keys and value encoding
//     (its time settings are ignored; see above)
//
// Returns:
//   - zapcore.Encoder: a native msgpack encoder; pair it with NewNativeWriter
//     (or a Passthrough writer) and the PackedForward framer
func NewMsgpackEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder { return newMsgpackEncoder(cfg) }

// NewNativeWriter builds a zapwire.Writer for the native path: a Passthrough
// encoder plus the PackedForward Framer. Pair it with NewMsgpackEncoder to
// build a zapcore.Core yourself (e.g. inside zapcore.NewTee or a sampler).
//
// WithTimeCodec / WithTimeKey are accepted but are no-ops here: native envelope
// time is structural (zapcore.Entry.Time → the EventTime extension), not read
// from a record field.
//
// Parameters:
//   - t: transport to ship frames over; must be non-nil
//   - opts: fluent options; only WithTag and WithZapwireOptions take effect
//
// Returns:
//   - *zapwire.Writer: the native-path writer; the caller owns it and must
//     Close it
//   - error: a non-nil error from the underlying zapwire.New (e.g. a nil input)
func NewNativeWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}

	return buildWriter(t, o, zapwire.Passthrough())
}

// NewNativeCore wires NewMsgpackEncoder and NewNativeWriter into a ready
// zapcore.Core plus its Writer. WithTimeCodec / WithTimeKey are no-ops on the
// native path (see NewNativeWriter).
//
// Parameters:
//   - t: transport to ship frames over; must be non-nil
//   - level: minimum level an entry must meet to be encoded
//   - encCfg: zap encoder config (field keys / value encoding; time ignored)
//   - opts: fluent options; only WithTag and WithZapwireOptions take effect
//
// Returns:
//   - zapcore.Core: a core that msgpack-encodes directly into the writer
//   - *zapwire.Writer: the underlying writer; the caller must Close it
//   - error: a non-nil error if the writer cannot be built
func NewNativeCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}

	w, err := buildWriter(t, o, zapwire.Passthrough())
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(NewMsgpackEncoder(encCfg), w, level), w, nil
}
