package otlp

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

// Proto3 wire-format append helpers. All functions append to dst and return
// the extended slice (the core zapwire dst-append contract). Tags are
// precomputed single bytes — see the wire cheat sheet in the plan / design §3.1.

func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}

	return append(dst, byte(v))
}

// uvarintLen returns the encoded size of v without encoding it.
func uvarintLen(v uint64) int {
	return (bits.Len64(v|1) + 6) / 7
}

// appendVarint encodes a signed int64 as proto "int64" (two's-complement,
// NOT zigzag — AnyValue.int_value is int64, not sint64).
func appendVarint(dst []byte, v int64) []byte {
	return appendUvarint(dst, uint64(v)) //nolint:gosec
}

func appendTaggedUvarint(dst []byte, tag byte, v uint64) []byte { //nolint:unparam // tag varies in tests and future tasks
	return appendUvarint(append(dst, tag), v)
}

func appendTaggedVarint(dst []byte, tag byte, v int64) []byte {
	return appendVarint(append(dst, tag), v)
}

func appendTaggedFixed64(dst []byte, tag byte, v uint64) []byte {
	dst = append(dst, tag)

	return binary.LittleEndian.AppendUint64(dst, v)
}

func appendTaggedFixed32(dst []byte, tag byte, v uint32) []byte {
	dst = append(dst, tag)

	return binary.LittleEndian.AppendUint32(dst, v)
}

func appendTaggedBytes(dst []byte, tag byte, b []byte) []byte {
	dst = appendUvarint(append(dst, tag), uint64(len(b)))

	return append(dst, b...)
}

func appendTaggedString(dst []byte, tag byte, s string) []byte {
	dst = appendUvarint(append(dst, tag), uint64(len(s)))

	return append(dst, s...)
}

var errTruncatedResponse = errors.New("otlp: truncated export response")

// decodePartialSuccess parses an ExportLogsServiceResponse body. Unknown
// fields are skipped (receivers may add fields within 1.x). The only
// len-delimited submessage we descend into is partial_success (field 1).
func decodePartialSuccess(body []byte) (rejected int64, msg string, err error) {
	ps, err := findField(body, 1)
	if err != nil || ps == nil {
		return 0, "", err
	}
	rejRaw, err := findVarint(ps, 1)
	if err != nil {
		return 0, "", err
	}
	msgRaw, err := findField(ps, 2)
	if err != nil {
		return 0, "", err
	}

	return int64(rejRaw), string(msgRaw), nil //nolint:gosec
}

// findField scans a proto message for the last occurrence of a len-delimited
// field with the given number, returning its payload (nil if absent).
func findField(b []byte, field int) (payload []byte, err error) {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0: // varint
			_, vn := binary.Uvarint(b)
			if vn <= 0 {
				return nil, errTruncatedResponse
			}
			adv = vn
		case 1: // fixed64
			adv = 8
		case 2: // len-delimited
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l { //nolint:gosec
				return nil, errTruncatedResponse
			}
			if num == field {
				payload = b[ln : ln+int(l)] //nolint:gosec
			}
			adv = ln + int(l) //nolint:gosec
		case 5: // fixed32
			adv = 4
		default:
			return nil, errTruncatedResponse
		}
		if len(b) < adv {
			return nil, errTruncatedResponse
		}
		b = b[adv:]
	}

	return payload, nil
}

// findVarint scans for the last varint field with the given number.
func findVarint(b []byte, field int) (uint64, error) {
	var out uint64
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0:
			v, vn := binary.Uvarint(b)
			if vn <= 0 {
				return 0, errTruncatedResponse
			}
			if num == field {
				out = v
			}
			adv = vn
		case 1:
			adv = 8
		case 2:
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l { //nolint:gosec
				return 0, errTruncatedResponse
			}
			adv = ln + int(l) //nolint:gosec
		case 5:
			adv = 4
		default:
			return 0, errTruncatedResponse
		}
		if len(b) < adv {
			return 0, errTruncatedResponse
		}
		b = b[adv:]
	}

	return out, nil
}

// forEachLenField invokes fn for every len-delimited occurrence of field in
// b, in order. fn returns false to stop early. Unknown fields are skipped
// with the same wire-type discipline as findField.
func forEachLenField(b []byte, field int, fn func(payload []byte) bool) error {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return errTruncatedResponse
		}
		b = b[n:]
		num, wt := int(tag>>3), int(tag&0x7)
		var adv int
		switch wt {
		case 0: // varint
			_, vn := binary.Uvarint(b)
			if vn <= 0 {
				return errTruncatedResponse
			}
			adv = vn
		case 1: // fixed64
			adv = 8
		case 2: // len-delimited
			l, ln := binary.Uvarint(b)
			if ln <= 0 || uint64(len(b)-ln) < l { //nolint:gosec
				return errTruncatedResponse
			}
			if num == field && !fn(b[ln:ln+int(l)]) { //nolint:gosec
				return nil
			}
			adv = ln + int(l) //nolint:gosec
		case 5: // fixed32
			adv = 4
		default:
			return errTruncatedResponse
		}
		if len(b) < adv {
			return errTruncatedResponse
		}
		b = b[adv:]
	}

	return nil
}
