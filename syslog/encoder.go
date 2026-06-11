package syslog

import (
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Facility is a syslog facility (RFC5424 §6.2.1), valid range 0..23.
type Facility int

// Syslog facilities. PRI = int(facility)*8 + int(severity).
const (
	KERN Facility = iota
	USER
	MAIL
	DAEMON
	AUTH
	SYSLOG
	LPR
	NEWS
	UUCP
	CRON
	AUTHPRIV
	FTP
	NTP
	LOGAUDIT
	LOGALERT
	CLOCK
	LOCAL0
	LOCAL1
	LOCAL2
	LOCAL3
	LOCAL4
	LOCAL5
	LOCAL6
	LOCAL7
)

// Severity is a syslog severity (RFC5424 §6.2.1), valid range 0..7.
type Severity int

// Syslog severities, most-to-least severe.
const (
	Emergency Severity = iota
	Alert
	Critical
	Error
	Warning
	Notice
	Informational
	Debug
)

const nilValue = "-"

// validateFacility returns f when in 0..23, else the default LOCAL0 (resolves spec §3.4 /
// pass-1 P0: a typed but out-of-range facility must not produce an invalid PRI).
func validateFacility(f Facility) Facility {
	if f < KERN || f > LOCAL7 {
		return LOCAL0
	}

	return f
}

// clampSeverity coerces s into the valid 0..7 range (>7→Debug, <0→Emergency), so any custom
// mapper still yields a syntactically valid PRI (spec §3.4 / pass-1 P0).
func clampSeverity(s Severity) Severity {
	switch {
	case s < Emergency:
		return Emergency
	case s > Debug:
		return Debug
	default:
		return s
	}
}

// defaultSeverityMapper is the conservative zap-level → syslog-severity mapping (spec §3.4):
// it never auto-emits Emergency/Alert (0..1).
func defaultSeverityMapper(l zapcore.Level) Severity {
	switch {
	case l <= zapcore.DebugLevel:
		return Debug
	case l == zapcore.InfoLevel:
		return Informational
	case l == zapcore.WarnLevel:
		return Warning
	case l == zapcore.ErrorLevel:
		return Error
	default: // DPanic, Panic, Fatal
		return Critical
	}
}

// sanitizeField normalizes one RFC5424 header field: drop bytes outside printable US-ASCII
// (33..126), truncate to maxLen, and map an empty result to the NILVALUE "-" (spec §3.3).
func sanitizeField(s string, maxLen int) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(out) < maxLen; i++ {
		if c := s[i]; c >= 33 && c <= 126 {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nilValue
	}

	return string(out)
}

// appendRFC3339Micros appends t in UTC as RFC3339 with 6 fractional digits — the RFC5424
// TIMESTAMP profile (TIME-SECFRAC ≤ 6). A zero time appends the NILVALUE "-" (spec §3.2).
func appendRFC3339Micros(b *buffer.Buffer, t time.Time) {
	if t.IsZero() {
		b.AppendString(nilValue)

		return
	}
	b.AppendTime(t.UTC(), "2006-01-02T15:04:05.000000Z07:00")
}

// encoder is a zapcore.Encoder that prepends an RFC5424 header to a JSON message body. It
// embeds an inner JSON encoder so every ObjectEncoder method is inherited; only EncodeEntry
// and Clone are overridden.
type encoder struct {
	zapcore.Encoder // embedded inner JSON encoder (field handling inherited)
	cfg             header
	body            bodyFormat // renders the SD + MSG section (design §3.5 seam)
	pool            buffer.Pool
}

// bodyFormat renders the STRUCTURED-DATA + MSG section of a SYSLOG-MSG (the leading SP, the SD
// value, optional BOM, and the message). Only jsonBody is implemented; a future structuredData
// mode is a second implementation, leaving the header assembly and framer untouched (§3.5).
type bodyFormat interface {
	appendBody(dst *buffer.Buffer, bom bool, jsonMsg []byte)
}

// jsonBody emits STRUCTURED-DATA "-" and carries the structured fields as the JSON MSG body.
type jsonBody struct{}

func (jsonBody) appendBody(dst *buffer.Buffer, bom bool, msg []byte) {
	dst.AppendString(" - ") // SP + STRUCTURED-DATA "-" + SP
	if bom {
		dst.AppendString("\uFEFF")
	}
	dst.AppendBytes(msg) // MSG = bare JSON body (no trailing terminator)
}

var _ zapcore.Encoder = (*encoder)(nil)

// NewEncoder builds the RFC5424 zapcore.Encoder. It copies encCfg (a value parameter, so the
// caller's config is untouched) and sets SkipLineEnding=true so the inner JSON body carries no
// trailing terminator (spec §3.1). Pair it with NewWriter (Passthrough + the syslog Framer) to
// build a core yourself, or use NewCore.
//
// Parameters:
//   - encCfg: zap JSON encoder config for the message body (its LineEnding is overridden)
//   - opts: syslog options (WithFacility, WithHostname, WithSeverityMapper, WithBOM, …)
//
// Returns:
//   - zapcore.Encoder: the RFC5424 encoder
func NewEncoder(encCfg zapcore.EncoderConfig, opts ...Option) zapcore.Encoder {
	encCfg.SkipLineEnding = true

	return &encoder{
		Encoder: zapcore.NewJSONEncoder(encCfg),
		cfg:     apply(opts).resolveHeader(),
		body:    jsonBody{},
		pool:    buffer.NewPool(),
	}
}

// Clone deep-clones the inner encoder and copies the immutable header config.
func (e *encoder) Clone() zapcore.Encoder {
	return &encoder{Encoder: e.Encoder.Clone(), cfg: e.cfg, body: e.body, pool: e.pool}
}

// EncodeEntry assembles one SYSLOG-MSG: the RFC5424 header (from ent) + SP + optional BOM +
// the bare JSON body. It returns a pooled buffer that zap's ioCore frees after Write.
func (e *encoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	body, err := e.Encoder.EncodeEntry(ent, fields)
	if err != nil {
		return nil, err // surface the inner encoder error verbatim
	}
	defer body.Free()

	pri := int(e.cfg.facility)*8 + int(clampSeverity(e.cfg.severityOf(ent.Level)))

	line := e.pool.Get()
	line.AppendByte('<')
	line.AppendInt(int64(pri))
	line.AppendString(">1 ") // PRI + VERSION(1) + SP
	appendRFC3339Micros(line, ent.Time)
	line.AppendByte(' ')
	line.AppendString(e.cfg.hostname)
	line.AppendByte(' ')
	line.AppendString(e.cfg.appName)
	line.AppendByte(' ')
	line.AppendString(e.cfg.procID)
	line.AppendByte(' ')
	line.AppendString(e.cfg.msgID)
	e.body.appendBody(line, e.cfg.bom, body.Bytes()) // " - " [BOM] MSG (design §3.5 seam)

	return line, nil
}
