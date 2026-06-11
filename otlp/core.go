package otlp

import "go.uber.org/zap/zapcore"

// core is ioCore plus one behavior: With pre-scans the RAW field slice for
// trace context before zap's Field.AddTo dispatch can erase it — zap.Any
// classifies stdlib contexts as StringerType, so an encoder behind the stock
// zapcore.NewCore only ever sees the stringified context (design §2.2). The
// official contrib bridge intercepts in its own Core for the same reason.
type core struct {
	zapcore.LevelEnabler
	enc *encoder
	out zapcore.WriteSyncer
}

func newOTLPCore(enc *encoder, ws zapcore.WriteSyncer, level zapcore.LevelEnabler) zapcore.Core {
	return &core{LevelEnabler: level, enc: enc, out: ws}
}

func (c *core) With(fields []zapcore.Field) zapcore.Core {
	clone := &core{LevelEnabler: c.LevelEnabler, enc: c.enc.cloneTyped(), out: c.out}
	for i := range fields {
		if sc, ok := spanContextFromField(fields[i]); ok {
			// Stash on the fresh clone — never mutated after this loop (§3.7).
			clone.enc.sc, clone.enc.scSet = sc, true

			continue
		}
		applyField(&clone.enc.encState, fields[i]) // transactional incl. zap.Inline
	}

	return clone
}

func (c *core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}

	return ce
}

func (c *core) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	buf, err := c.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	_, werr := c.out.Write(buf.Bytes())
	buf.Free()
	if werr != nil {
		return werr
	}
	if ent.Level > zapcore.ErrorLevel {
		// Mirror ioCore: best-effort sync on Panic/Fatal so the batch flushes
		// before the process dies.
		_ = c.Sync()
	}

	return nil
}

func (c *core) Sync() error { return c.out.Sync() }

// NewEncoder builds the OTLP zapcore.Encoder. Only encoder-end options take
// effect (design §3.6). Pair with NewWriter for a BYO-core setup — but note
// the §2.2 compatibility matrix: behind a stock zapcore.NewCore, sticky
// zap.Any("context", ctx) degrades to a stringified attribute; use NewCore
// or the eager SpanContext helper.
func NewEncoder(opts ...Option) zapcore.Encoder {
	return newEncoder(applyOptions(opts))
}

// NewCore wires NewEncoder + NewWriter into the custom trace-aware core
// (design §2.2) plus its Writer, which the caller must Close. One opts list
// feeds all three ends (each option sets only the fields it owns).
//
// Returns:
//   - zapcore.Core: the OTLP core (sticky zap.Any("context", ctx) works here)
//   - *Writer: the underlying exporter; the caller owns it and must Close it
//   - error: a non-nil error from NewWriter (e.g. ErrNoEndpoint)
func NewCore(endpoint string, level zapcore.LevelEnabler, opts ...Option) (zapcore.Core, *Writer, error) {
	w, err := NewWriter(endpoint, opts...)
	if err != nil {
		return nil, nil, err
	}

	return newOTLPCore(newEncoder(applyOptions(opts)), w, level), w, nil
}
