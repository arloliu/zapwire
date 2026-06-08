// fluent/native_race_test.go
package fluent

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

// nativeLogger uses a DRAINING server (not the queueing startReadServer): the through-core smoke
// tests below log 16×200 entries and never read frames, so a backpressuring server would stall
// sync writes against the write deadline. The decoded correctness assertions live in the direct
// EncodeEntry tests above, which need no socket at all.
func nativeLogger(t *testing.T) (*zap.Logger, func()) {
	t.Helper()
	path := randomSocketPath(t)
	srv := startDrainServer(t, path)
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig())
	require.NoError(t, err)
	require.Eventually(t, w.IsConnected, time.Second, 5*time.Millisecond)

	return zap.New(core), func() { _ = w.Close(); srv.stop() }
}

// Direct-EncodeEntry concurrency WITH assertions: N goroutines call EncodeEntry on ONE shared
// Clone (the carried With context) with distinct call-site fields. We copy each payload, then
// after the barrier decode every one and assert no cross-talk and that the With field survived —
// proving EncodeEntry never mutates the receiver (design §3.1 / §5.5). Run with -race.
func TestNative_Concurrent_EncodeEntry_NoCrossTalk(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	shared := base.Clone()
	zap.String("shared", "ctx").AddTo(shared)

	const G, N = 16, 200
	raw := make([][][]byte, G)
	var wg sync.WaitGroup

	for g := range G {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([][]byte, 0, N)

			for i := range N {
				buf, err := shared.EncodeEntry(zapcore.Entry{Message: "m"},
					[]zapcore.Field{zap.Int("g", g), zap.Int("i", i)})
				if err != nil {
					panic(err) // t.Fatal is not goroutine-safe
				}

				b := make([]byte, len(buf.Bytes()))
				copy(b, buf.Bytes())
				out = append(out, b)
			}

			raw[g] = out
		}(g)
	}

	wg.Wait()

	for g := range G {
		for i, b := range raw[g] {
			rec := decodeEntry(t, b).Record.(map[string]any)
			require.Equal(t, "ctx", rec["shared"], "carried With field intact (receiver not mutated)")
			require.Equal(t, "m", rec["msg"])
			require.EqualValues(t, g, rec["g"], "no cross-talk between goroutines")
			require.EqualValues(t, i, rec["i"])
			require.Len(t, rec, 4)
		}
	}
}

// A bad With field shared across goroutines degrades to <key>Error on EVERY concurrent entry
// without poisoning others (design §3.9 / §5.5).
func TestNative_Concurrent_BadWith_NoPoison(t *testing.T) {
	base := newMsgpackEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	shared := base.Clone()
	zap.Any("bad", make(chan int)).AddTo(shared) // → badError baked into the shared stack

	const G, N = 8, 100
	raw := make([][][]byte, G)
	var wg sync.WaitGroup

	for g := range G {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([][]byte, 0, N)

			for i := range N {
				buf, err := shared.EncodeEntry(zapcore.Entry{Message: "m"}, []zapcore.Field{zap.Int("i", i)})
				if err != nil {
					panic(err)
				}

				b := make([]byte, len(buf.Bytes()))
				copy(b, buf.Bytes())
				out = append(out, b)
			}

			raw[g] = out
		}(g)
	}

	wg.Wait()

	for g := range G {
		for i, b := range raw[g] {
			rec := decodeEntry(t, b).Record.(map[string]any)
			require.Contains(t, rec, "badError")
			require.EqualValues(t, i, rec["i"])
			require.Equal(t, "m", rec["msg"])
		}
	}
}

// Through-the-core -race smoke (integration path): concurrent logging through one core. The
// assertion is "no race, no panic" across the Writer/Framer/socket path.
func TestNative_Concurrent_ThroughCore(t *testing.T) {
	logger, cleanup := nativeLogger(t)
	defer cleanup()
	shared := logger.With(zap.String("shared", "ctx"))
	var wg sync.WaitGroup

	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for i := range 200 {
				shared.Info("m", zap.Int("g", g), zap.Int("i", i))
			}
		}(g)
	}

	wg.Wait()
}

func TestNative_Concurrent_ReflectedScratch(t *testing.T) {
	logger, cleanup := nativeLogger(t)
	defer cleanup()
	var wg sync.WaitGroup

	for g := range 16 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for i := range 200 {
				// Mix Tier 1 (map) and Tier 2 (struct) through the same core.
				logger.Info("m",
					zap.Any("m", map[string]any{"g": g, "i": i}),
					zap.Any("s", struct {
						A int `json:"a"`
					}{A: i}),
				)
			}
		}(g)
	}

	wg.Wait()
}
