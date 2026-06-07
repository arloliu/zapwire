package fluent

import "github.com/tinylib/msgp/msgp"

// Framer wraps per-entry msgpack [time, record] payloads into a Fluent Forward
// PackedForward message: [tag, <entries bin>, {"size": N}]. It implements zapwire.Framer.
type Framer struct {
	tag string
}

// NewFramer returns a PackedForward Framer that stamps every frame with tag.
func NewFramer(tag string) Framer { return Framer{tag: tag} }

// Frame appends the PackedForward message for payloads to dst.
func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	total := 0
	for _, p := range payloads {
		total += len(p)
	}

	dst = msgp.AppendArrayHeader(dst, 3)
	dst = msgp.AppendString(dst, f.tag)
	dst = appendBinHeader(dst, total) // entries carried as a single msgpack bin
	for _, p := range payloads {
		dst = append(dst, p...)
	}
	dst = msgp.AppendMapHeader(dst, 1)
	dst = msgp.AppendString(dst, "size")
	dst = msgp.AppendInt(dst, len(payloads))

	return dst, nil
}

// appendBinHeader writes a msgpack bin8/bin16/bin32 header for a payload of n bytes.
// Each case guards n against the header width it selects, so the byte truncations are safe.
//
//nolint:gosec // intentional byte truncation: each case bounds n before packing
func appendBinHeader(b []byte, n int) []byte {
	switch {
	case n < 1<<8:
		return append(b, 0xc4, byte(n))
	case n < 1<<16:
		return append(b, 0xc5, byte(n>>8), byte(n))
	default:
		return append(b, 0xc6, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}
