package otlp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// psBody builds an ExportLogsServiceResponse with partial_success{rejected, msg}.
func psBody(rejected int64, msg string) []byte {
	var ps []byte
	if rejected != 0 {
		ps = appendTaggedVarint(ps, 0x08, rejected)
	}
	if msg != "" {
		ps = appendTaggedString(ps, 0x12, msg)
	}

	return appendTaggedBytes(nil, 0x0a, ps)
}

func TestResolveAccept(t *testing.T) {
	t.Run("clean accept", func(t *testing.T) {
		require.Nil(t, resolveAccept(nil, ExportError{StatusCode: 200}))
		require.Nil(t, resolveAccept(appendTaggedBytes(nil, 0x0a, nil), ExportError{}))
	})
	t.Run("rejected", func(t *testing.T) {
		a := resolveAccept(psBody(3, "boom"), ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.EqualValues(t, 3, a.rejected)
		require.NotNil(t, a.event)
		require.Equal(t, 200, a.event.StatusCode)
		require.EqualValues(t, 3, a.event.Rejected)
		require.Equal(t, "boom", a.event.Message)
		require.False(t, a.event.Warning)
	})
	t.Run("warning", func(t *testing.T) {
		a := resolveAccept(psBody(0, "heads-up"), ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.Zero(t, a.rejected)
		require.True(t, a.event.Warning)
		require.Equal(t, "heads-up", a.event.Message)
	})
	t.Run("malformed body is observability-only", func(t *testing.T) {
		a := resolveAccept([]byte{0xff}, ExportError{StatusCode: 200})
		require.NotNil(t, a)
		require.Zero(t, a.rejected)
		require.Error(t, a.event.Err)
	})
}
