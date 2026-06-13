package otlp

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// deepObject / deepArray are marshalers that recurse n levels deep, modeling a
// pathological or attacker-shaped value used to prove the encoder caps nesting
// instead of recursing the goroutine stack to an uncatchable fatal throw.
type deepObject struct{ n int }

func (d deepObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if d.n <= 0 {
		return nil
	}

	return enc.AddObject("child", deepObject{n: d.n - 1})
}

type deepArray struct{ n int }

func (d deepArray) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	if d.n <= 0 {
		return nil
	}

	return enc.AppendArray(deepArray{n: d.n - 1})
}

// TestEncoderDepthCapNoCrash drives marshalers far past the depth cap. With the
// guard they degrade to an <key>Error field; without it the goroutine stack
// would exhaust (a fatal, unrecoverable throw). The JSON transcoder is bounded
// transitively because it only parses the proto this encoder produces.
func TestEncoderDepthCapNoCrash(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field zapcore.Field
	}{
		{"object", zap.Object("root", deepObject{n: maxEncodeDepth * 10})},
		{"array", zap.Array("root", deepArray{n: maxEncodeDepth * 10})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := NewEncoder().EncodeEntry(zapcore.Entry{}, []zapcore.Field{tc.field})
			require.NoError(t, err) // entry still ships; the field degrades to <key>Error
			require.NotNil(t, buf)
			require.NotZero(t, buf.Len())
		})
	}
}

// TestEncoderNormalNestingUnaffected confirms the cap does not perturb encoding
// of legitimately shallow nesting.
func TestEncoderNormalNestingUnaffected(t *testing.T) {
	buf, err := NewEncoder().EncodeEntry(zapcore.Entry{}, []zapcore.Field{
		zap.Object("root", deepObject{n: 8}),
	})
	require.NoError(t, err)
	require.NotZero(t, buf.Len())
}

// TestSizeClampsHighSide pins the high-side clamps that stop a fat-fingered
// config from OOM-panicking the queue / request-buffer allocation, plus the
// negative-drain-timeout normalization.
func TestSizeClampsHighSide(t *testing.T) {
	o := applyOptions([]Option{
		WithQueueSize(maxQueueSize + 1),
		WithMaxRequestBytes(maxRequestBytesCeil + 1),
		WithDrainTimeout(-time.Second),
	})
	require.Equal(t, maxQueueSize, o.queueSize)
	require.Equal(t, maxRequestBytesCeil, o.maxRequestBytes)
	require.Equal(t, time.Duration(0), o.drainTimeout, "negative drain timeout disables the bound")
}

// TestWithDrainTimeoutBoundsSync proves a hostile/broken receiver (always 503,
// a retryable status) cannot stretch Sync to the full per-batch retry budget
// when WithDrainTimeout is set: Sync returns within the drain bound and the
// records are counted as dropped.
func TestWithDrainTimeoutBoundsSync(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusServiceUnavailable) // retryable forever
	}))
	defer srv.Close()

	// MaxElapsed is 30s, so without the drain bound Sync would block ~30s.
	w, err := NewHTTPWriter(srv.URL,
		WithRetry(RetryConfig{Initial: 20 * time.Millisecond, MaxInterval: 50 * time.Millisecond, MaxElapsed: 30 * time.Second}),
		WithDrainTimeout(200*time.Millisecond),
		WithBatchSize(512), WithFlushInterval(time.Hour)) // keep records buffered until Sync
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	for range 3 {
		_, _ = w.Write([]byte{0x10, 0x09})
	}

	start := time.Now()
	require.NoError(t, w.Sync())
	require.Less(t, time.Since(start), 5*time.Second, "Sync must honor WithDrainTimeout, not the 30s retry budget")
	require.Equal(t, uint64(3), w.DroppedLogs(), "all buffered records dropped once the drain budget was spent")
}

// TestUserHeadersCannotOverrideContentType pins that WithHeaders cannot clobber
// the transport-owned Content-Type/Content-Encoding (a mismatch would make the
// receiver misparse the body), while ordinary custom headers still pass through.
func TestUserHeadersCannotOverrideContentType(t *testing.T) {
	var gotCT, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotCustom = r.Header.Get("X-Custom")
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL,
		WithHeaders(map[string]string{"Content-Type": "text/evil", "X-Custom": "ok"}),
		WithFlushInterval(time.Hour))
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, _ = w.Write([]byte{0x10, 0x09})
	require.NoError(t, w.Sync())

	require.Equal(t, "application/x-protobuf", gotCT, "transport Content-Type must win over WithHeaders")
	require.Equal(t, "ok", gotCustom, "non-reserved custom headers still pass through")
}
