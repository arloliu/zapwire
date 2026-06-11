package syslog

import (
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// header is the per-encoder, sanitized, immutable RFC5424 header config.
type header struct {
	facility   Facility
	hostname   string
	appName    string
	procID     string
	msgID      string
	bom        bool
	severityOf func(zapcore.Level) Severity
}

type options struct {
	facility   Facility
	severityOf func(zapcore.Level) Severity
	hostname   string
	appName    string
	procID     string
	msgID      string
	bom        bool
	framing    Framing
	wireOpts   []zapwire.Option
}

// defaultOptions pre-populates the computed defaults; With* options overwrite the raw field,
// and resolveHeader sanitizes — so WithHostname("") yields "-" while an unset hostname keeps
// os.Hostname().
func defaultOptions() options {
	host, _ := os.Hostname()

	return options{
		facility:   LOCAL0,
		severityOf: defaultSeverityMapper,
		hostname:   host,
		appName:    filepath.Base(os.Args[0]),
		procID:     strconv.Itoa(os.Getpid()),
		msgID:      nilValue,
		framing:    OctetCounting,
	}
}

func apply(opts []Option) options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

func (o options) resolveHeader() header {
	return header{
		facility:   validateFacility(o.facility),
		hostname:   sanitizeField(o.hostname, 255),
		appName:    sanitizeField(o.appName, 48),
		procID:     sanitizeField(o.procID, 128),
		msgID:      sanitizeField(o.msgID, 32),
		bom:        o.bom,
		severityOf: o.severityOf,
	}
}

// Option configures a syslog preset. Header/severity/BOM options affect NewEncoder and
// NewCore; WithFraming and WithZapwireOptions affect NewWriter and NewCore.
type Option func(*options)

// WithFacility sets the syslog facility (default LOCAL0). An out-of-range value falls back to
// LOCAL0 at construction.
func WithFacility(f Facility) Option { return func(o *options) { o.facility = f } }

// WithSeverityMapper overrides the zap-level → syslog-severity mapping. The result is clamped
// to 0..7 per entry. A nil mapper is ignored (the default is kept).
func WithSeverityMapper(fn func(zapcore.Level) Severity) Option {
	return func(o *options) {
		if fn != nil {
			o.severityOf = fn
		}
	}
}

// WithHostname sets the HOSTNAME field (default os.Hostname()); empty → "-".
func WithHostname(h string) Option { return func(o *options) { o.hostname = h } }

// WithAppName sets the APP-NAME field (default the process name); empty → "-".
func WithAppName(a string) Option { return func(o *options) { o.appName = a } }

// WithProcID sets the PROCID field (default the OS pid); empty → "-".
func WithProcID(p string) Option { return func(o *options) { o.procID = p } }

// WithMsgID sets the MSGID field (default "-").
func WithMsgID(m string) Option { return func(o *options) { o.msgID = m } }

// WithBOM prepends a UTF-8 BOM to the MSG (strict RFC5424 MSG-UTF8). Default off — a leading
// BOM trips naive JSON consumers.
func WithBOM(on bool) Option { return func(o *options) { o.bom = on } }

// WithFraming selects the stream framing (default OctetCounting).
func WithFraming(f Framing) Option { return func(o *options) { o.framing = f } }

// WithZapwireOptions forwards core zapwire options (mode, buffer, timeouts, …).
func WithZapwireOptions(opts ...zapwire.Option) Option {
	return func(o *options) { o.wireOpts = append(o.wireOpts, opts...) }
}

// Framer satisfies the core zapwire.Framer seam (asserted here, where zapwire is imported, so
// framer.go stays stdlib-only).
var _ zapwire.Framer = Framer{}

func buildWriter(t zapwire.Transport, o options) (*zapwire.Writer, error) {
	return zapwire.New(t, zapwire.Passthrough(), NewFramer(o.framing), o.wireOpts...)
}

// NewWriter builds a zapwire.Writer for the syslog path: Passthrough + the syslog Framer over
// t. Only WithFraming and WithZapwireOptions take effect here; header/severity/BOM options are
// no-ops (supply those to NewEncoder for a BYO-core path). Pair with NewEncoder to build a core
// yourself (e.g. inside zapcore.NewTee).
//
// Parameters:
//   - t: transport to ship messages over; must be non-nil
//   - opts: syslog options (WithFraming, WithZapwireOptions take effect)
//
// Returns:
//   - *zapwire.Writer: the writer; the caller owns it and must Close it
//   - error: a non-nil error from the underlying zapwire.New
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	return buildWriter(t, apply(opts))
}

// NewCore wires NewEncoder + NewWriter into a ready core plus its writer, which the caller must
// Close. A single opts list feeds both ends (each option sets only the fields it owns).
//
// Parameters:
//   - t: transport to ship messages over; must be non-nil
//   - level: minimum level an entry must meet to be encoded
//   - encCfg: zap JSON encoder config for the message body
//   - opts: syslog options
//
// Returns:
//   - zapcore.Core: an RFC5424-encoding core writing into the syslog writer
//   - *zapwire.Writer: the underlying writer; the caller must Close it
//   - error: a non-nil error if the writer cannot be built
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	w, err := buildWriter(t, apply(opts))
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(NewEncoder(encCfg, opts...), w, level), w, nil
}
