package otlp

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendUvarint(t *testing.T) {
	cases := []struct {
		v    uint64
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x01}},
		{300, []byte{0xac, 0x02}}, // protobuf docs example
		{1<<64 - 1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}},
	}
	for _, c := range cases {
		require.Equal(t, c.want, appendUvarint(nil, c.v), "value %d", c.v)
		require.Equal(t, len(c.want), uvarintLen(c.v), "len %d", c.v)
	}
}

func TestAppendVarintNegative(t *testing.T) {
	// int64(-1) as two's-complement uint64 → ten 0xff-leading bytes.
	got := appendVarint(nil, -1)
	require.Equal(t, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, got)
}

func TestAppendTaggedHelpers(t *testing.T) {
	// severity_text=3 ("info"): tag 0x1a, len 4, bytes.
	require.Equal(t, []byte{0x1a, 0x04, 'i', 'n', 'f', 'o'},
		appendTaggedString(nil, 0x1a, "info"))
	// time_unix_nano=1 fixed64 LE.
	require.Equal(t, []byte{0x09, 0x01, 0, 0, 0, 0, 0, 0, 0},
		appendTaggedFixed64(nil, 0x09, 1))
	// flags=8 fixed32 LE.
	require.Equal(t, []byte{0x45, 0x01, 0, 0, 0},
		appendTaggedFixed32(nil, 0x45, 1))
	// bytes payload.
	require.Equal(t, []byte{0x4a, 0x02, 0xde, 0xad},
		appendTaggedBytes(nil, 0x4a, []byte{0xde, 0xad}))
	// varint payload (severity_number=2, value 9).
	require.Equal(t, []byte{0x10, 0x09}, appendTaggedUvarint(nil, 0x10, 9))
}

func TestDecodePartialSuccess(t *testing.T) {
	// ExportLogsServiceResponse{partial_success:{rejected_log_records:3, error_message:"bad"}}
	body := []byte{
		0x0a, 0x07, // partial_success, len 7
		0x08, 0x03, // rejected_log_records = 3
		0x12, 0x03, 'b', 'a', 'd', // error_message = "bad"
	}
	rejected, msg, err := decodePartialSuccess(body)
	require.NoError(t, err)
	require.Equal(t, int64(3), rejected)
	require.Equal(t, "bad", msg)

	// Empty body → success, nothing rejected.
	rejected, msg, err = decodePartialSuccess(nil)
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Empty(t, msg)

	// Truncated message → error, never panic.
	_, _, err = decodePartialSuccess([]byte{0x0a, 0x07, 0x08})
	require.Error(t, err)

	// Unknown extra field in response → ignored (forward compat).
	withUnknown := append([]byte{0x1a, 0x01, 0x00}, body...) // fake field 3, then real
	rejected, msg, err = decodePartialSuccess(withUnknown)
	require.NoError(t, err)
	require.Equal(t, int64(3), rejected)
	require.Equal(t, "bad", msg)
}

// TestUvarintLenBoundaries checks that uvarintLen agrees with the actual byte
// count produced by appendUvarint at every 7-bit boundary.
func TestUvarintLenBoundaries(t *testing.T) {
	// Edge sentinels.
	values := []uint64{0, math.MaxUint64}

	// Boundary pairs at each 7-bit threshold: 2^k-1 and 2^k.
	for _, k := range []uint{7, 14, 21, 28, 35, 42, 49, 56, 63} {
		values = append(values, (1<<k)-1, 1<<k)
	}

	for _, v := range values {
		encoded := appendUvarint(nil, v)
		require.Equal(t, len(encoded), uvarintLen(v), "mismatch for value %d", v)
	}
}

// TestFindFieldSkipsWireTypes builds a message that contains wiretype-0 (varint),
// wiretype-1 (fixed64), and wiretype-5 (fixed32) fields before the target
// len-delimited / varint field and asserts that both functions skip past them
// and still find the correct value.
func TestFindFieldSkipsWireTypes(t *testing.T) {
	// Build a message:
	//   field 1, wiretype 0 (varint): value 42
	//   field 2, wiretype 1 (fixed64): value 0x0102030405060708
	//   field 3, wiretype 5 (fixed32): value 0xDEADBEEF
	//   field 4, wiretype 2 (len-delimited): payload "hello"
	//   field 5, wiretype 0 (varint): value 99
	var msg []byte
	// field 1, wiretype 0: tag = (1<<3)|0 = 0x08, value 42
	msg = appendUvarint(msg, 0x08)
	msg = appendUvarint(msg, 42)
	// field 2, wiretype 1: tag = (2<<3)|1 = 0x11
	msg = appendTaggedFixed64(msg, 0x11, 0x0102030405060708)
	// field 3, wiretype 5: tag = (3<<3)|5 = 0x1d
	msg = appendTaggedFixed32(msg, 0x1d, 0xDEADBEEF)
	// field 4, wiretype 2: tag = (4<<3)|2 = 0x22
	msg = appendTaggedString(msg, 0x22, "hello")
	// field 5, wiretype 0: tag = (5<<3)|0 = 0x28, value 99
	msg = appendUvarint(msg, 0x28)
	msg = appendUvarint(msg, 99)

	payload, err := findField(msg, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), payload)

	val, err := findVarint(msg, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(99), val)
}

// TestFindFieldLastOccurrenceWins verifies that when the same field number
// appears more than once, both findField and findVarint return the LAST value.
func TestFindFieldLastOccurrenceWins(t *testing.T) {
	// Encode field 1 (len-delimited) twice: first "first", then "last".
	var msg []byte
	msg = appendTaggedString(msg, 0x0a, "first")
	msg = appendTaggedString(msg, 0x0a, "last")

	payload, err := findField(msg, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("last"), payload)

	// Encode field 2 (varint) twice: first 11, then 22.
	var msg2 []byte
	msg2 = appendTaggedUvarint(msg2, 0x10, 11)
	msg2 = appendTaggedUvarint(msg2, 0x10, 22)

	val, err := findVarint(msg2, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(22), val)
}

// TestDecodePartialSuccessEmptySubmessage verifies that a partial_success field
// present but with zero length (0x0a 0x00) yields (0, "", nil) — the nil
// short-circuit in decodePartialSuccess must not fire when findField returns a
// non-nil empty slice.
func TestDecodePartialSuccessEmptySubmessage(t *testing.T) {
	body := []byte{0x0a, 0x00} // partial_success present, length 0
	rejected, msg, err := decodePartialSuccess(body)
	require.NoError(t, err)
	require.Equal(t, int64(0), rejected)
	require.Equal(t, "", msg)
}

// TestDecodePartialSuccessWarning verifies the warning-classification signal:
// rejected_log_records absent (defaults to 0) but error_message present.
func TestDecodePartialSuccessWarning(t *testing.T) {
	// partial_success: { error_message: "hi!" }  (rejected_log_records omitted)
	inner := appendTaggedString(nil, 0x12, "hi!")
	body := appendTaggedBytes(nil, 0x0a, inner)

	rejected, msg, err := decodePartialSuccess(body)
	require.NoError(t, err)
	require.Equal(t, int64(0), rejected)
	require.Equal(t, "hi!", msg)
}
