package fluent

import (
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// NewMsgpackEncoder returns a zapcore.Encoder that emits Fluent Forward [EventTime, record]
// msgpack payloads directly from zap's structured fields — no JSON round-trip. Envelope time comes
// from zapcore.Entry.Time (exact); TimeCodec / EncoderConfig time settings do NOT affect it
// (design §3.8).
func NewMsgpackEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder { return newMsgpackEncoder(cfg) }

// NewNativeWriter builds a zapwire.Writer for the native path: a Passthrough encoder + the
// PackedForward Framer. Pair it with NewMsgpackEncoder to build a zapcore.Core yourself (e.g.
// inside zapcore.NewTee or a sampler). The caller owns the returned *zapwire.Writer and must
// Close it when done.
//
// WithTimeCodec / WithTimeKey are accepted but are no-ops here: native envelope time is structural
// (zapcore.Entry.Time → the EventTime extension), not read from a record field.
func NewNativeWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}

	return buildWriter(t, o, zapwire.Passthrough())
}

// NewNativeCore wires NewMsgpackEncoder + NewNativeWriter into a ready zapcore.Core plus its
// Writer (which the caller must Close). WithTimeCodec / WithTimeKey are no-ops (see above).
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
