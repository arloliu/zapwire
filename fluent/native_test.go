package fluent

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

func TestNewMsgpackEncoder_ImplementsEncoder(t *testing.T) {
	// The explicit type pins the public return-type contract; that is the point of the test.
	var _ zapcore.Encoder = NewMsgpackEncoder(zap.NewProductionEncoderConfig()) //nolint:staticcheck // QF1011: explicit type asserts the public contract
}

func TestNewNativeCore_EndToEndThroughWriterAndFramer(t *testing.T) {
	path := randomSocketPath(t) // from Step 0's native_testsupport_test.go
	srv := startReadServer(t, path)
	defer srv.stop()

	cfg := zap.NewProductionEncoderConfig()
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, cfg, WithTag("app.logs"))
	require.NoError(t, err)
	defer w.Close()
	require.Eventually(t, w.IsConnected, time.Second, 5*time.Millisecond)

	logger := zap.New(core)
	logger.Info("hello", zap.String("k", "v"), zap.Int("n", 1))
	require.NoError(t, w.Sync())

	frame := <-srv.recv
	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	rec := entries[0].Record.(map[string]any)
	require.Equal(t, "hello", rec["msg"])
	require.Equal(t, "v", rec["k"])
	require.EqualValues(t, 1, rec["n"])
}

func TestNewNativeCore_TimeOptionsAreNoOpNotError(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()
	cfg := zap.NewProductionEncoderConfig()
	_, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, cfg,
		WithTimeCodec(EpochMillisCodec("whatever")), WithTimeKey("ignored"))
	require.NoError(t, err, "time options are accepted as no-ops on the native path")
	_ = w.Close()
}

// Framer integration (design §5.4): multiple native payloads concatenate into one PackedForward
// entries bin; the bin length equals the sum of payloads and size equals the payload count. Enough
// ~50-byte entries push the bin past 256 bytes into the bin16 (0xc5) branch. Reuses binHeaderOf /
// decodePackedForward from framer_test.go.
func TestNative_FramerIntegration_MultiPayloadBin16(t *testing.T) {
	enc := NewMsgpackEncoder(zap.NewProductionEncoderConfig())
	mkPayload := func(i int) []byte {
		buf, err := enc.EncodeEntry(
			zapcore.Entry{Level: zapcore.InfoLevel, Message: strings.Repeat("x", 40) + fmt.Sprint(i)}, nil)
		require.NoError(t, err)
		b := make([]byte, len(buf.Bytes())) // own the bytes (zap would free buf after Write)
		copy(b, buf.Bytes())

		return b
	}
	payloads := make([][]byte, 0, 8)
	for i := range 8 {
		payloads = append(payloads, mkPayload(i))
	}
	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	require.GreaterOrEqual(t, total, 1<<8, "must exceed bin8 to exercise bin16")
	require.Less(t, total, 1<<16)

	frame, err := NewFramer("app.logs").Frame(nil, payloads)
	require.NoError(t, err)

	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc5), marker, "entries bin must use a bin16 header")
	require.Equal(t, total, length, "bin length == sum of native payload lengths")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, len(payloads), size, "size option == payload count")
	require.Len(t, entries, len(payloads))
}

// Framer bin-size boundaries with native payloads (design §5.4): a single small native payload
// produces a bin8 (0xc4) entries blob; a single payload >= 64 KiB produces a bin32 (0xc6) blob.
// (bin16 / 0xc5 is covered by TestNative_FramerIntegration_MultiPayloadBin16.) Together these prove
// native payloads of every size flow through the framer's variable bin-size selection.
func TestNative_FramerIntegration_Bin8(t *testing.T) {
	enc := NewMsgpackEncoder(zap.NewProductionEncoderConfig())
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "small"}, nil)
	require.NoError(t, err)
	payload := make([]byte, len(buf.Bytes()))
	copy(payload, buf.Bytes())
	require.Less(t, len(payload), 1<<8, "single small native payload stays under the bin8 limit")

	frame, err := NewFramer("app.logs").Frame(nil, [][]byte{payload})
	require.NoError(t, err)
	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc4), marker, "small entries blob uses a bin8 header")
	require.Equal(t, len(payload), length, "bin length == payload length")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
}

// deepObj is a recursive ObjectMarshaler: it nests `depth` levels of single-key objects, with a
// string leaf at the bottom. Used to exercise arbitrary-depth structural nesting (§3.2/§3.7).
type deepObj struct{ depth int }

func (d deepObj) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if d.depth <= 0 {
		enc.AddString("leaf", "bottom")

		return nil
	}

	return enc.AddObject("n", deepObj{depth: d.depth - 1})
}

// Deep structural nesting (design §5.4 line 617 + the "arbitrary depth" claim of §3.2/§3.7): a
// deeply-nested object whose encoded payload is large enough to push the framed entries blob across
// the bin8→bin16 boundary. Proves the frame-stack seal + per-level count bookkeeping are exact at
// depth, and that a deep native payload flows through the framer's bin selection.
func TestNative_DeepNesting_CrossesBinBoundary(t *testing.T) {
	const depth = 150
	enc := NewMsgpackEncoder(zap.NewProductionEncoderConfig())
	buf, err := enc.EncodeEntry(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "deep"},
		[]zapcore.Field{zap.Object("root", deepObj{depth: depth})})
	require.NoError(t, err)
	payload := make([]byte, len(buf.Bytes()))
	copy(payload, buf.Bytes())
	require.GreaterOrEqual(t, len(payload), 1<<8, "deep nesting pushes the payload past the bin8 boundary")
	require.Less(t, len(payload), 1<<16)

	frame, err := NewFramer("app.logs").Frame(nil, [][]byte{payload})
	require.NoError(t, err)
	marker, _ := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc5), marker, "deeply-nested native payload crosses into a bin16 entries blob")

	// Decode and walk all `depth` levels — proves seal/count bookkeeping is exact at depth.
	_, entries, size := decodePackedForward(t, frame)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	rec := entries[0].Record.(map[string]any)
	require.Equal(t, "deep", rec["msg"])
	cur := rec["root"].(map[string]any)
	for range depth {
		cur = cur["n"].(map[string]any)
	}
	require.Equal(t, "bottom", cur["leaf"], "the innermost field survives arbitrary-depth nesting")
}

func TestNative_FramerIntegration_Bin32(t *testing.T) {
	enc := NewMsgpackEncoder(zap.NewProductionEncoderConfig())
	// A single native payload >= 64 KiB forces the framer's bin32 branch.
	buf, err := enc.EncodeEntry(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: strings.Repeat("x", 1<<16)}, nil)
	require.NoError(t, err)
	payload := make([]byte, len(buf.Bytes()))
	copy(payload, buf.Bytes())
	require.GreaterOrEqual(t, len(payload), 1<<16, "single large native payload exceeds the bin16 limit")

	frame, err := NewFramer("app.logs").Frame(nil, [][]byte{payload})
	require.NoError(t, err)
	marker, length := binHeaderOf(t, frame)
	require.Equal(t, byte(0xc6), marker, "large entries blob uses a bin32 header")
	require.Equal(t, len(payload), length, "bin length == payload length")

	tag, entries, size := decodePackedForward(t, frame)
	require.Equal(t, "app.logs", tag)
	require.Equal(t, 1, size)
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Record.(map[string]any)["msg"].(string), 1<<16)
}
