package zapwire

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_RequiresDeps(t *testing.T) {
	_, err := New(nil, rawEncoder{}, lineFramer{})
	require.ErrorIs(t, err, ErrNoTransport)
	_, err = New(UDS("/x"), nil, lineFramer{})
	require.ErrorIs(t, err, ErrNoEncoder)
	_, err = New(UDS("/x"), rawEncoder{}, nil)
	require.ErrorIs(t, err, ErrNoFramer)
}

func TestWriter_Sync_DeliversFrame(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())

	n, err := w.Write([]byte(`{"msg":"hello"}`))
	require.NoError(t, err)
	require.Equal(t, len(`{"msg":"hello"}`), n)

	select {
	case got := <-srv.recv:
		require.Equal(t, "{\"msg\":\"hello\"}\n", string(got))
	case <-time.After(time.Second):
		t.Fatal("did not receive framed log")
	}
}

func TestWriter_EncodeError_Returned(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), errEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()

	n, err := w.Write([]byte(`{}`))
	require.Error(t, err)
	require.Equal(t, 0, n)
}

func TestWriter_NoConn_DropsButReportsConsumed(t *testing.T) {
	// No server: connection never establishes.
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, WithMaxRetries(1))
	require.NoError(t, err)
	defer w.Close()
	require.False(t, w.IsConnected())

	n, err := w.Write([]byte(`{"a":1}`))
	require.NoError(t, err)
	require.Equal(t, len(`{"a":1}`), n)
	require.Positive(t, w.DroppedLogs())
}

func TestWriter_ClosedIsIdempotent(t *testing.T) {
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, WithMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	n, err := w.Write([]byte(`x`))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestWriter_ReconnectsAfterDrop(t *testing.T) {
	path := randomSocketPath(t)
	defer func() { _ = os.Remove(path) }()

	srv := startRawUDSServer(t, path)
	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())
	srv.waitConnected()

	_, _ = w.Write([]byte(`{"a":1}`))
	srv.close()

	require.Eventually(t, func() bool {
		_, werr := w.Write([]byte(`{"a":1}`))
		require.NoError(t, werr)

		return !w.IsConnected()
	}, 3*time.Second, 20*time.Millisecond)

	srv2 := startRawUDSServer(t, path)
	defer srv2.close()

	require.Eventually(t, func() bool {
		_, werr := w.Write([]byte(`{"a":1}`))
		require.NoError(t, werr)

		return w.IsConnected()
	}, 5*time.Second, 50*time.Millisecond)
	require.GreaterOrEqual(t, w.ReconnectCount(), uint64(1))
}
