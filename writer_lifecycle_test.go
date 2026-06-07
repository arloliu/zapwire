package zapwire

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriter_Close_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for _, async := range []bool{false, true} {
		opts := []Option{WithMaxRetries(1)}
		if async {
			opts = append(opts, WithAsyncMode())
		}
		w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, opts...)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}

	// Poll from THIS goroutine (require.Eventually's spawned goroutine would inflate the count).
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before,
		"reconnect and flush goroutines must exit after Close")
}

// TestWriter_Close_ReturnsWhileWriteStalled proves Close returns within a bounded time and
// leaves no lingering goroutines even when the flush goroutine is mid-write to a connected-but-
// non-reading consumer. The deaf server accepts but never reads, so a large async frame fills
// the socket buffer and conn.Write blocks until SetWriteDeadline (short WithWriteTimeout) fires.
func TestWriter_Close_ReturnsWhileWriteStalled(t *testing.T) {
	path := randomSocketPath(t)
	srv := startDeafServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithWriteTimeout(100*time.Millisecond),
		WithFlushInterval(10*time.Millisecond), WithBufferSize(64), WithMaxRetries(1))
	require.NoError(t, err)
	srv.waitConnected(t)

	// Baseline after the deaf server's accept goroutine is up and the writer's goroutines have
	// started, so the measurement isn't inflated by the server.
	before := runtime.NumGoroutine()

	// A frame large enough to overflow the socket send buffer so conn.Write actually stalls.
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = 'a'
	}
	_, _ = w.Write(big)

	// Let the flush goroutine enter the stalled conn.Write before we tear down.
	time.Sleep(50 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	select {
	case cerr := <-done:
		require.NoError(t, cerr)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return while a write was stalled")
	}

	// Poll from THIS goroutine (see leak test above) until goroutines drain.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before,
		"no zapwire goroutine may linger after Close during a stalled write")
}
