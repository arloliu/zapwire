package otlp

import (
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var recordPool = buffer.NewPool()

type encConfig struct {
	severityOf    func(zapcore.Level) SeverityNumber
	callerAttrs   bool
	loggerNameKey string
}

// encoder is the OTLP zapcore.Encoder: EncodeEntry emits the bare LogRecord
// message bytes for one entry (design §3). The embedded encState is the
// persistent With-field state AND the ObjectEncoder surface that stock
// ioCore.With dispatches into.
type encoder struct {
	encState
	cfg   encConfig
	sc    trace.SpanContext // With-stash; written only on fresh clones (§3.7)
	scSet bool
}

var _ zapcore.Encoder = (*encoder)(nil)

func newEncoder(o options) *encoder {
	e := &encoder{
		encState: *newEncState(),
		cfg: encConfig{
			severityOf:    o.severityOf,
			callerAttrs:   o.callerAttrs,
			loggerNameKey: o.loggerNameKey,
		},
	}
	e.armSink()

	return e
}

// armSink points the embedded state's trace sink at THIS encoder's stash so
// the AddReflected hook (eager-helper fields through stock ioCore.With)
// stashes on the right struct. Must run after every clone.
func (e *encoder) armSink() {
	e.scSink = &e.sc
	e.scSinkSet = &e.scSet
}

func (e *encoder) Clone() zapcore.Encoder { return e.cloneTyped() }

func (e *encoder) cloneTyped() *encoder {
	c := &encoder{encState: *e.clone(), cfg: e.cfg, sc: e.sc, scSet: e.scSet}
	c.armSink()

	return c
}

func (e *encoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	// Per-call working copy: the receiver is never mutated (§3.7).
	work := e.clone()
	defer work.free()

	// Trace resolution: With-stash seeds; per-call fields override (last
	// wins). Trace values surfacing through nested marshalers reach the
	// per-call locals via the sink.
	sc, scSet := e.sc, e.scSet
	work.scSink, work.scSinkSet = &sc, &scSet

	// Entry-metadata attributes → ROOT frame (pinned order: With, meta, call).
	e.addMeta(work, ent)

	for i := range fields {
		if fsc, ok := spanContextFromField(fields[i]); ok {
			sc, scSet = fsc, true

			continue
		}

		applyField(work, fields[i])
	}

	work.sealAll()

	rec := getFrameBuf()
	defer putFrameBuf(rec)

	if t := ent.Time.UnixNano(); t != 0 {
		rec = appendTaggedFixed64(rec, 0x09, uint64(t)) //nolint:gosec
	}

	if sev := clampSeverity(e.cfg.severityOf(ent.Level)); sev != SeverityUnspecified {
		rec = appendTaggedUvarint(rec, 0x10, uint64(sev)) //nolint:gosec
	}

	if txt := ent.Level.String(); txt != "" {
		rec = appendTaggedString(rec, 0x1a, txt)
	}

	// body: AnyValue{string_value: Message}; a set oneof is always emitted.
	bodyLen := 1 + uvarintLen(uint64(len(ent.Message))) + len(ent.Message)
	rec = append(rec, 0x2a)
	rec = appendUvarint(rec, uint64(bodyLen)) //nolint:gosec
	rec = appendTaggedString(rec, 0x0a, ent.Message)

	// attributes: root frame holds already-tagged 0x32 entries.
	rec = append(rec, work.stack[0].buf...)

	if scSet && sc.IsValid() {
		// flags == 0 (unsampled) is OMITTED: proto.Marshal drops a zero
		// fixed32, and conformance demands byte identity (plan cheat sheet).
		if f := uint32(sc.TraceFlags()); f != 0 {
			rec = appendTaggedFixed32(rec, 0x45, f)
		}

		tid, sid := sc.TraceID(), sc.SpanID()
		rec = appendTaggedBytes(rec, 0x4a, tid[:])
		rec = appendTaggedBytes(rec, 0x52, sid[:])
	}

	if t := ent.Time.UnixNano(); t != 0 {
		rec = appendTaggedFixed64(rec, 0x59, uint64(t)) // observed == time (§3.1) //nolint:gosec
	}

	out := recordPool.Get()
	_, _ = out.Write(rec)

	return out, nil
}

// applyField dispatches one zap field into the state, transactionally.
// zap's Field.AddTo calls MarshalLogObject(enc) DIRECTLY for
// InlineMarshalerType — no child frame, no error-to-Error conversion until
// after partial bytes are written (zapcore/field.go:122-124,183-185) — so the
// snapshot/rollback must wrap the dispatch here (design §3.3, pass-2 P0).
// Every other fallible type is transactional inside encState's own methods.
// Shared by EncodeEntry, the custom core's With, and resource-field encoding.
func applyField(work *encState, f zapcore.Field) {
	if f.Type == zapcore.InlineMarshalerType {
		sn := work.snap()
		if err := f.Interface.(zapcore.ObjectMarshaler).MarshalLogObject(work); err != nil {
			work.rollback(sn)
			// Mirror zap's convention exactly: <key>Error (bare "Error" for
			// zap.Inline's empty key), added AFTER rollback.
			work.AddString(f.Key+"Error", err.Error())
		}

		return
	}

	f.AddTo(work)
}

// addMeta appends entry-metadata attributes to the ROOT frame (design §3.5).
func (e *encoder) addMeta(work *encState, ent zapcore.Entry) {
	if k := e.cfg.loggerNameKey; k != "" && ent.LoggerName != "" {
		work.addKVRoot(k, work.anyString(ent.LoggerName))
	}

	if e.cfg.callerAttrs && ent.Caller.Defined {
		if fn := ent.Caller.Function; fn != "" {
			work.addKVRoot("code.function.name", work.anyString(fn))
		}

		work.addKVRoot("code.file.path", work.anyString(ent.Caller.File))
		work.addKVRoot("code.line.number", work.anyInt(int64(ent.Caller.Line))) //nolint:gosec
	}

	if ent.Stack != "" {
		work.addKVRoot("code.stacktrace", work.anyString(ent.Stack))
	}
}
