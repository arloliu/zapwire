//go:generate go tool msgp

package fluent

import (
	"fmt"
	"time"

	"github.com/tinylib/msgp/msgp"
)

// Entry is one Fluent Forward event encoded as a 2-element msgpack array [time, record].
// Entries are concatenated into the PackedForward entries stream by the Framer.
//
//msgp:tuple Entry
type Entry struct {
	Time   EventTime `msg:"time,extension"`
	Record any       `msg:"record"`
}

// EventTime is the Forward protocol's EventTime extension, carrying second + nanosecond
// components for sub-second precision.
//
// Spec: https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1
type EventTime time.Time

const (
	extensionType = 0
	length        = 8
)

func init() {
	msgp.RegisterExtension(extensionType, func() msgp.Extension { return new(EventTime) })
}

// ExtensionType reports the Forward EventTime msgpack extension type.
// Implements msgp.Extension.
//
// Returns:
//   - int8: the registered extension type id for EventTime
func (t *EventTime) ExtensionType() int8 { return extensionType }

// Len reports the EventTime payload length in bytes. Implements msgp.Extension.
//
// Returns:
//   - int: the fixed payload size (8 bytes)
func (t *EventTime) Len() int { return length }

// MarshalBinaryTo writes 4 bytes of seconds + 4 bytes of nanoseconds
// (big-endian, UTC). Implements msgp.Extension.
//
// Parameters:
//   - b: destination slice of exactly Len() (8) bytes
//
// Returns:
//   - error: always nil; the fixed-width packing cannot fail
func (t *EventTime) MarshalBinaryTo(b []byte) error {
	utc := time.Time(*t).UTC()
	sec := uint32(utc.Unix()) //nolint:gosec // Forward EventTime is 32-bit seconds (Y2106)
	nsec := utc.Nanosecond()
	b[0], b[1], b[2], b[3] = byte(sec>>24), byte(sec>>16), byte(sec>>8), byte(sec)     //nolint:gosec // intentional byte truncation: big-endian packing
	b[4], b[5], b[6], b[7] = byte(nsec>>24), byte(nsec>>16), byte(nsec>>8), byte(nsec) //nolint:gosec // intentional byte truncation: big-endian packing

	return nil
}

// UnmarshalBinary decodes the 8-byte EventTime payload (used by tests).
// Implements msgp.Extension.
//
// Parameters:
//   - b: an 8-byte EventTime payload
//
// Returns:
//   - error: non-nil if b is not exactly 8 bytes
func (t *EventTime) UnmarshalBinary(b []byte) error {
	if len(b) != length {
		return fmt.Errorf("fluent: invalid EventTime length: %d", len(b))
	}
	sec := int64(b[0])<<24 | int64(b[1])<<16 | int64(b[2])<<8 | int64(b[3])
	nsec := int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
	*t = EventTime(time.Unix(sec, nsec))

	return nil
}
