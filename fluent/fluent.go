package fluent

import (
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

const defaultTag = "app.logs"

type options struct {
	tag         string
	codec       TimeCodec
	codecSet    bool
	keyOverride string
	wireOpts    []zapwire.Option
}

// resolveCodec returns the effective TimeCodec: the configured codec (or the default), with
// any WithTimeKey override applied. Order-independent.
func (o options) resolveCodec() TimeCodec {
	c := o.codec
	if !o.codecSet || !c.valid() {
		c = defaultTimeCodec()
	}
	if o.keyOverride != "" {
		c.Key = o.keyOverride
	}

	return c
}

// Option configures a fluent writer preset.
type Option func(*options)

// WithTag sets the Fluent Forward tag stamped on every frame (default "app.logs").
func WithTag(tag string) Option { return func(o *options) { o.tag = tag } }

// WithTimeCodec sets the TimeCodec used to read the timestamp out of each log (and, in
// NewCore, to configure zap's matching time encoder). Defaults to AutoEpochCodec("ts").
func WithTimeCodec(c TimeCodec) Option {
	return func(o *options) { o.codec = c; o.codecSet = true }
}

// WithTimeKey overrides just the JSON time key on the active codec, keeping its format.
func WithTimeKey(key string) Option { return func(o *options) { o.keyOverride = key } }

// WithZapwireOptions forwards core zapwire options (mode, buffer, timeouts, …).
func WithZapwireOptions(opts ...zapwire.Option) Option {
	return func(o *options) { o.wireOpts = append(o.wireOpts, opts...) }
}

// NewWriter builds a zapwire.Writer that ships Fluent Forward PackedForward frames over t.
//
// Timestamp contract: the transcode Encoder reads the time field per the configured
// TimeCodec (default AutoEpochCodec("ts")). The magnitude-tolerant default decodes common
// epoch units (s/ms/µs/ns) correctly, so a bring-your-own-core caller on zap's default
// float-seconds encoder still decodes to ~now. Callers wiring their own zapcore.Core (rather
// than using NewCore) with an explicit non-Auto codec MUST align the encode end with the same
// codec — call codec.ApplyTo(&encoderConfig) — or the timestamp will be misread.
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}

	return buildWriter(t, o)
}

// NewCore builds a zapcore.Core (JSON-encoding into the fluent writer) plus the underlying
// writer, which the caller must Close. It wires BOTH ends of the time contract from the
// configured TimeCodec, so the JSON encoder and the transcode decoder always agree.
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}
	o.resolveCodec().ApplyTo(&encCfg)

	w, err := buildWriter(t, o)
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(zapcore.NewJSONEncoder(encCfg), w, level), w, nil
}

func buildWriter(t zapwire.Transport, o options) (*zapwire.Writer, error) {
	if o.tag == "" {
		o.tag = defaultTag
	}

	return zapwire.New(t, NewEncoderWithCodec(o.resolveCodec()), NewFramer(o.tag), o.wireOpts...)
}
