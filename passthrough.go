package zapwire

// passthrough copies an already-final per-entry payload through unchanged. Use it when the
// bytes handed to the Writer are ALREADY the final per-entry wire payload — e.g. a native
// zapcore.Encoder (see fluent.NewMsgpackEncoder) that emits the framed entry directly.
type passthrough struct{}

// Passthrough returns an Encoder that copies the per-entry payload through
// unchanged.
//
// It is correct for both Writer modes with no special-casing: in sync mode the
// caller passes a pooled dst and append reuses it; in async mode the caller
// passes dst == nil, so append(nil, record...) allocates a fresh owning copy
// that survives the source buffer being freed/reused.
//
// Returns:
//   - Encoder: an identity encoder safe for both sync and async writers
func Passthrough() Encoder { return passthrough{} }

func (passthrough) Encode(dst, record []byte) ([]byte, error) {
	return append(dst, record...), nil
}
