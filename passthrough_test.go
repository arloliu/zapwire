package zapwire

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPassthrough_SyncAppendsIntoDst(t *testing.T) {
	dst := make([]byte, 0, 16)
	out, err := Passthrough().Encode(dst, []byte("abc"))
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), out)
}

func TestPassthrough_AsyncReturnsOwningCopy(t *testing.T) {
	src := []byte("hello")
	out, err := Passthrough().Encode(nil, src)
	require.NoError(t, err)
	src[0] = 'J' // mutate the source after encoding (simulates buffer reuse)
	require.Equal(t, []byte("hello"), out, "async copy (dst==nil) must not alias the source")
}

func TestPassthrough_EmptyRecord(t *testing.T) {
	out, err := Passthrough().Encode(nil, nil)
	require.NoError(t, err)
	require.Empty(t, out)
}
