package zapwire

// Encoder converts the bytes zap hands the WriteSyncer (a JSON log line, in the v1
// transcode path) into a single per-entry wire payload. It appends to dst and returns the
// extended slice so callers can pool the backing buffer. It must NOT add framing.
type Encoder interface {
	Encode(dst, record []byte) ([]byte, error)
}

// Framer wraps one or more per-entry payloads into a single wire frame written to the
// socket. len(payloads) == 1 in sync mode and N in async/batched mode. It appends to dst
// and returns the extended slice.
type Framer interface {
	Frame(dst []byte, payloads [][]byte) ([]byte, error)
}
