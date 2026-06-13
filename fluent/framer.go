package fluent

import (
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

// Framer wraps per-entry msgpack [time, record] payloads into a Fluent Forward
// PackedForward message: [tag, <entries bin>, {"size": N}]. It implements zapwire.Framer.
type Framer struct {
	tag string
}

// NewFramer returns a PackedForward Framer that stamps every frame with tag.
//
// Parameters:
//   - tag: Fluent Forward tag written into every frame
//
// Returns:
//   - Framer: a PackedForward framer for the given tag
func NewFramer(tag string) Framer { return Framer{tag: tag} }

// Frame appends the PackedForward message for payloads to dst:
// [tag, <entries bin>, {"size": N}]. It implements zapwire.Framer.
//
// Parameters:
//   - dst: buffer to append to; may be nil or a pooled slice to reuse
//   - payloads: the per-entry [time, record] payloads to pack into one frame
//
// Returns:
//   - []byte: dst extended with the PackedForward frame
//   - error: non-nil only when the batch exceeds the msgpack bin32 limit (4 GiB)
func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	if err := checkBinSize(total); err != nil {
		return dst, err
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

// checkBinSize rejects a batch whose total byte size exceeds the msgpack bin32
// header limit (2^32-1). Beyond that the bin header would silently truncate to
// the low 32 bits and desync the receiver's parser (trailing bytes reinterpreted
// as the next object). Split out so the guard is unit-testable without
// allocating 4 GiB. uint64() keeps the comparison correct on 32-bit (where a
// >2 GiB int would have overflowed) and avoids a 1<<32 constant overflow.
func checkBinSize(total int) error {
	// total is a sum of slice lengths, so always >= 0: the uint64 conversion is
	// exact (it cannot wrap), and the comparison rejects > 2^32-1 on 64-bit
	// while being a harmless no-op on 32-bit (which cannot hold a 4 GiB batch).
	if uint64(total) > 0xFFFFFFFF { //nolint:gosec // total >= 0 (sum of len()); conversion is exact
		return fmt.Errorf("fluent: batch is %d bytes, exceeds the msgpack bin32 limit (4 GiB)", total)
	}

	return nil
}

// appendBinHeader writes a msgpack bin8/bin16/bin32 header for a payload of n bytes.
// The bin8/bin16 cases bound n against the header width they select; the bin32
// (default) case relies on the caller (Frame) having already rejected any batch
// with n > 2^32-1, so the byte truncation cannot fire.
//
//nolint:gosec // bin8/bin16 bound n; bin32 is guarded by Frame's 4 GiB check
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
