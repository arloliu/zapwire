package syslog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFramer_OctetCounting(t *testing.T) {
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{[]byte("<13>1 msg")})
	require.NoError(t, err)
	require.Equal(t, "9 <13>1 msg", string(out)) // len("<13>1 msg") == 9
}

func TestFramer_OctetCounting_IsByteLengthNotRuneCount(t *testing.T) {
	msg := []byte("<13>1 \xEF\xBB\xBF{}") // 6 ASCII + 3-byte BOM + 2 = 11 bytes, 9 runes
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{msg})
	require.NoError(t, err)
	require.Equal(t, "11 "+string(msg), string(out))
}

func TestFramer_LFTerminated(t *testing.T) {
	out, err := NewFramer(LFTerminated).Frame(nil, [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, "a\nb\n", string(out))
}

func TestFramer_OctetCounting_MultiPayloadBatch(t *testing.T) {
	out, err := NewFramer(OctetCounting).Frame(nil, [][]byte{[]byte("aa"), []byte("bbb")})
	require.NoError(t, err)
	require.Equal(t, "2 aa3 bbb", string(out))
}

func TestFramer_DefaultIsOctetCounting(t *testing.T) {
	// The zero Framing value is OctetCounting.
	out, err := NewFramer(Framing(0)).Frame(nil, [][]byte{[]byte("xy")})
	require.NoError(t, err)
	require.Equal(t, "2 xy", string(out))
}
