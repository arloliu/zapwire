package fluent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

func socketPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_fluent_%d.sock", time.Now().UnixNano()))
}

// mockFluentd accepts one UDS connection and surfaces decoded PackedForward frames.
type mockFluentd struct {
	ln   net.Listener
	recv chan []byte
	path string
}

func startMock(t *testing.T, path string) *mockFluentd {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	m := &mockFluentd{ln: ln, recv: make(chan []byte, 16), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				m.recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()

	return m
}

func (m *mockFluentd) stop() { _ = m.ln.Close(); _ = os.Remove(m.path) }

func TestNewWriter_EndToEnd(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	core, writer, err := NewCore(zapwire.UDS(path), zap.InfoLevel,
		zap.NewProductionEncoderConfig(), WithTag("svc.logs"))
	require.NoError(t, err)
	defer writer.Close()

	logger := zap.New(core)
	logger.Info("hello", zap.String("k", "v"))

	select {
	case frame := <-m.recv:
		tag, entries, size := decodePackedForward(t, frame)
		require.Equal(t, "svc.logs", tag)
		require.Equal(t, 1, size)
		require.Len(t, entries, 1)
		rec := entries[0].Record.(map[string]any)
		require.Equal(t, "hello", rec["msg"])
		require.Equal(t, "v", rec["k"])
		_, hasTS := rec["ts"]
		require.False(t, hasTS, "ts must be lifted into EventTime, not left in the record")
		// Regression guard for the ts encoder/decoder contract: a real zap log's decoded
		// EventTime must be ~now, not ~1970 (which is what a seconds/nanos mismatch yields).
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}

// TestNewWriter_CallerOwnedCoreEpochSeconds covers the supported opt-in path where the caller
// builds their own zapcore.Core feeding fluent.NewWriter, using zap.NewProductionEncoderConfig
// (TimeKey="ts", EpochTimeEncoder -> float SECONDS). The transcode decoder must read those
// seconds as seconds, not nanoseconds. Without magnitude detection the ~1.7e9 value would be
// read as nanos and decode to ~1970; the assertion that EventTime is ~now discriminates.
func TestNewWriter_CallerOwnedCoreEpochSeconds(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	w, err := NewWriter(zapwire.UDS(path), WithTag("svc.logs"))
	require.NoError(t, err)
	defer w.Close()

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), w, zap.InfoLevel)
	logger := zap.New(core)
	logger.Info("hello", zap.String("k", "v"))

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		rec := entries[0].Record.(map[string]any)
		require.Equal(t, "hello", rec["msg"])
		_, hasTS := rec["ts"]
		require.False(t, hasTS, "ts must be lifted into EventTime, not left in the record")
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}

// TestNewCore_PinsTimeKeyForNonTSConfig guards the contract that NewCore forces the
// encoder TimeKey to "ts" even when the caller supplies a config with a different key.
// zap.NewDevelopmentEncoderConfig uses TimeKey="T"; without pinning, the JSON encoder would
// emit the timestamp under "T", the transcode decoder (which hardcodes "ts") would never see
// it, and the original timestamp would survive in the record. The discriminating assertion is
// that no "T" key leaks into the decoded record.
func TestNewCore_PinsTimeKeyForNonTSConfig(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	core, writer, err := NewCore(zapwire.UDS(path), zap.InfoLevel,
		zap.NewDevelopmentEncoderConfig(), WithTag("svc.logs"))
	require.NoError(t, err)
	defer writer.Close()

	logger := zap.New(core)
	logger.Info("hello")

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		rec := entries[0].Record.(map[string]any)
		_, hasT := rec["T"]
		require.False(t, hasT, "TimeKey must be pinned to ts; no T key may leak into the record")
		_, hasTS := rec["ts"]
		require.False(t, hasTS, "ts must be lifted into EventTime, not left in the record")
		// Sanity: timestamp decodes to ~now, not ~1970.
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}

func TestNewCore_CustomCodecRoundTrips(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	// RFC3339Nano string codec at a custom key "time" — NewCore must wire BOTH ends.
	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		WithTimeCodec(RFC3339NanoCodec("time")))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("hello")

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
		_, hasTime := entries[0].Record.(map[string]any)["time"]
		require.False(t, hasTime, "custom time key must be lifted out of the record")
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}

func TestWithTimeKey_OverridesActiveCodecKey(t *testing.T) {
	o := options{}
	WithTimeCodec(EpochMillisCodec("a"))(&o)
	WithTimeKey("b")(&o)
	require.Equal(t, "b", o.resolveCodec().Key)
	// Order-independent: key override applies regardless of option order.
	o2 := options{}
	WithTimeKey("b")(&o2)
	WithTimeCodec(EpochMillisCodec("a"))(&o2)
	require.Equal(t, "b", o2.resolveCodec().Key)
}

func TestResolveCodec_DefaultWhenUnset(t *testing.T) {
	o := options{}
	require.Equal(t, "ts", o.resolveCodec().Key)
}

// TestNewWriter_BYOCore_ProductionConfigDecodesToNow is the Finding 3 regression guard: a
// bring-your-own-core caller using zap's DEFAULT production config (epoch float seconds)
// must decode to ~now, not ~1970, thanks to the magnitude-tolerant default codec.
func TestNewWriter_BYOCore_ProductionConfigDecodesToNow(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	w, err := NewWriter(zapwire.UDS(path))
	require.NoError(t, err)
	defer w.Close()

	// Caller wires their own core with the stock production encoder config (EpochTimeEncoder).
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), w, zap.InfoLevel)
	zap.New(core).Info("hello")

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}
