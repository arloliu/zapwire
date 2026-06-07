package zapwire

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recvNewlines accumulates bytes from srv.recv until want newline-terminated records have
// arrived (lineFramer emits one '\n' per record) or the deadline elapses.
func recvNewlines(t *testing.T, srv *readServer, want int) int {
	t.Helper()
	var got int
	deadline := time.After(2 * time.Second)
	for got < want {
		select {
		case b := <-srv.recv:
			got += bytes.Count(b, []byte{'\n'})
		case <-deadline:
			return got
		}
	}

	return got
}

func TestWriter_Async_BatchesAndFlushesOnSync(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(time.Hour)) // only Sync() triggers a flush
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())

	for i := range 5 {
		_, werr := w.Write([]byte{'a' + byte(i)})
		require.NoError(t, werr)
	}
	require.NoError(t, w.Sync())

	select {
	case got := <-srv.recv:
		require.Equal(t, "a\nb\nc\nd\ne\n", string(got)) // one batched frame
	case <-time.After(2 * time.Second):
		t.Fatal("no flushed batch received")
	}
}

func TestWriter_Async_FlushesOnInterval(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer w.Close()

	_, _ = w.Write([]byte("x"))

	select {
	case got := <-srv.recv:
		require.Equal(t, "x\n", string(got))
	case <-time.After(2 * time.Second):
		t.Fatal("interval flush did not fire")
	}
}

func TestWriter_Async_DropNewestWhenFull(t *testing.T) {
	// Tiny buffer, no flushing (huge interval), no server: everything piles up then drops.
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithBufferSize(2), WithFlushInterval(time.Hour),
		WithMaxRetries(1), WithDropPolicy(DropNewest))
	require.NoError(t, err)
	defer w.Close()

	for range 50 {
		_, werr := w.Write([]byte("y"))
		require.NoError(t, werr)
	}
	require.Positive(t, w.DroppedLogs())
}

func TestWriter_Async_CloseFlushesRemaining(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	require.True(t, w.IsConnected())

	_, _ = w.Write([]byte("z"))
	require.NoError(t, w.Close()) // must flush "z\n" before closing the conn

	select {
	case got := <-srv.recv:
		require.True(t, bytes.Equal(got, []byte("z\n")))
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not flush buffered log")
	}
}

func TestWriter_Async_SyncFlushesWholeBufferAcrossBatches(t *testing.T) {
	// batchSize 2 with 50 queued records forces multiple flush batches; Sync() must drain
	// the entire queue, not just the first batch (regression guard for single-batch flush).
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	const n = 50
	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithBatchSize(2), WithFlushInterval(time.Hour)) // only Sync triggers flush
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())

	for range n {
		_, werr := w.Write([]byte("r"))
		require.NoError(t, werr)
	}
	require.NoError(t, w.Sync())

	require.Equal(t, n, recvNewlines(t, srv, n), "Sync must flush every queued record")
}

// blockingEncoder parks every Encode call between the Write-level closed-check and the
// lifecycle gate (encode runs outside the gate), letting a test hold async writers in the
// exact window where the Write/Close race lives.
type blockingEncoder struct {
	entered chan struct{} // signalled once per Encode, before blocking
	release chan struct{} // closed to let all parked encoders proceed
}

func (b blockingEncoder) Encode(dst, record []byte) ([]byte, error) {
	b.entered <- struct{}{}
	<-b.release

	return append(dst, record...), nil
}

func TestWriter_Async_ConcurrentWriteCloseNoStrandedPayload(t *testing.T) {
	// Regression: a Write goroutine preempted past the closed-check must not enqueue a payload
	// after Close's final flush, which would strand it in the buffered queue with no consumer
	// and no drop accounting. The lifecycle gate guarantees that once Close returns the queue
	// is fully drained.
	//
	// The blocking encoder parks N writers in the post-closed-check / pre-enqueue window, then
	// Close runs to completion, then the writers are released. Unfixed: releasees send to the
	// still-open queue (no consumer left) -> len(queue)==N. Fixed: releasees re-check closed
	// under the gate and drop -> len(queue)==0.
	const n = 8
	enc := blockingEncoder{entered: make(chan struct{}), release: make(chan struct{})}

	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), enc, lineFramer{},
		WithAsyncMode(), WithBufferSize(64), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	require.True(t, w.IsConnected())

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, _ = w.Write([]byte("q"))
		})
	}

	// Wait until all writers are parked mid-encode (past the closed-check, before the gate).
	for range n {
		<-enc.entered
	}

	// Close runs fully while the writers are parked: the queue is empty, so the final flush
	// drains nothing and flushLoop exits.
	require.NoError(t, w.Close())

	// Release the parked writers; they now race past the gate against the completed Close.
	close(enc.release)
	wg.Wait()

	// White-box invariant: no payload may strand in the queue after Close.
	require.Empty(t, w.queue, "payload stranded in queue after Close")
}

func TestWriter_Async_CloseFlushesWholeBufferAcrossBatches(t *testing.T) {
	// As above but the whole-buffer flush happens on the Close() shutdown path.
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	const n = 50
	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithBatchSize(2), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	require.True(t, w.IsConnected())

	for range n {
		_, werr := w.Write([]byte("r"))
		require.NoError(t, werr)
	}
	require.NoError(t, w.Close()) // shutdown path must flush every queued record

	require.Equal(t, n, recvNewlines(t, srv, n), "Close must flush every queued record")
}
