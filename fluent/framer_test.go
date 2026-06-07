package fluent

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

// binHeaderOf locates the entries bin within a PackedForward frame and returns
// its msgpack bin marker byte plus the decoded payload length it advertises.
func binHeaderOf(t *testing.T, frame []byte) (byte, int) {
	t.Helper()
	_, o, err := msgp.ReadArrayHeaderBytes(frame)
	require.NoError(t, err)
	_, o, err = msgp.ReadStringBytes(o)
	require.NoError(t, err)

	require.NotEmpty(t, o)
	switch marker := o[0]; marker {
	case 0xc4: // bin8
		require.GreaterOrEqual(t, len(o), 2)

		return marker, int(o[1])
	case 0xc5: // bin16
		require.GreaterOrEqual(t, len(o), 3)

		return marker, int(o[1])<<8 | int(o[2])
	case 0xc6: // bin32
		require.GreaterOrEqual(t, len(o), 5)

		return marker, int(o[1])<<24 | int(o[2])<<16 | int(o[3])<<8 | int(o[4])
	default:
		t.Fatalf("unexpected bin marker 0x%02x", marker)

		return 0, 0
	}
}

// decodePackedForward parses a PackedForward frame into its tag, entries, and size option.
func decodePackedForward(t *testing.T, b []byte) (string, []Entry, int) {
	t.Helper()
	n, o, err := msgp.ReadArrayHeaderBytes(b)
	require.NoError(t, err)
	require.Equal(t, uint32(3), n)

	tag, o, err := msgp.ReadStringBytes(o)
	require.NoError(t, err)

	entriesBin, o, err := msgp.ReadBytesBytes(o, nil)
	require.NoError(t, err)

	var entries []Entry
	rest := entriesBin
	for len(rest) > 0 {
		var e Entry
		rest, err = e.UnmarshalMsg(rest)
		require.NoError(t, err)
		entries = append(entries, e)
	}

	mh, o, err := msgp.ReadMapHeaderBytes(o)
	require.NoError(t, err)
	require.Equal(t, uint32(1), mh)
	key, o, err := msgp.ReadStringBytes(o)
	require.NoError(t, err)
	require.Equal(t, "size", key)
	size, o, err := msgp.ReadIntBytes(o)
	require.NoError(t, err)
	require.Empty(t, o, "no bytes must remain after decoding the PackedForward frame")

	return tag, entries, size
}

func encodeEntries(t *testing.T, recs ...map[string]any) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(recs))
	for _, r := range recs {
		e := Entry{Time: EventTime(time.Unix(1, 0)), Record: r}
		b, err := e.MarshalMsg(nil)
		require.NoError(t, err)
		out = append(out, b)
	}

	return out
}

func TestFramer_SingleEntry(t *testing.T) {
	f := NewFramer("app.logs")
	payloads := encodeEntries(t, map[string]any{"msg": "one"})

	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	require.Equal(t, "one", entries[0].Record.(map[string]any)["msg"])
}

func TestFramer_Bin16Header(t *testing.T) {
	f := NewFramer("t")
	// A handful of ~50-byte records pushes the entries bin well past 256 bytes,
	// forcing the 0xc5 (bin16) length-prefix branch.
	recs := make([]map[string]any, 0, 8)
	for i := range 8 {
		recs = append(recs, map[string]any{"i": int64(i), "msg": strings.Repeat("x", 40)})
	}
	payloads := encodeEntries(t, recs...)

	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	require.GreaterOrEqual(t, total, 1<<8, "test must exceed bin8 range to exercise bin16")
	require.Less(t, total, 1<<16)

	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc5), marker, "entries bin must use a bin16 header")
	require.Equal(t, total, length, "bin16 length must equal the total entries size (big-endian)")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "t", tag)
	require.Equal(t, len(recs), size)
	require.Len(t, entries, len(recs))
	for i, e := range entries {
		require.EqualValues(t, i, e.Record.(map[string]any)["i"])
	}
}

func TestFramer_Bin32Header(t *testing.T) {
	f := NewFramer("t")
	// One record carrying a >64KiB string forces the 0xc6 (bin32) default branch.
	big := strings.Repeat("y", 1<<16)
	payloads := encodeEntries(t, map[string]any{"i": int64(0), "blob": big})

	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	require.GreaterOrEqual(t, total, 1<<16, "test must exceed bin16 range to exercise bin32")

	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc6), marker, "entries bin must use a bin32 header")
	require.Equal(t, total, length, "bin32 length must equal the total entries size (big-endian)")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "t", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	require.EqualValues(t, 0, entries[0].Record.(map[string]any)["i"])
	require.Equal(t, big, entries[0].Record.(map[string]any)["blob"])
}

func TestFramer_BatchPreservesOrder(t *testing.T) {
	f := NewFramer("t")
	payloads := encodeEntries(t,
		map[string]any{"i": int64(0)},
		map[string]any{"i": int64(1)},
		map[string]any{"i": int64(2)},
	)
	frame, err := f.Frame(nil, payloads)
	require.NoError(t, err)

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "t", tag)
	require.Equal(t, 3, size)
	require.Len(t, entries, 3)
	for i, e := range entries {
		require.EqualValues(t, i, e.Record.(map[string]any)["i"])
	}
}
