package zapwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewCore_LogsFlowToWriter(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()

	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	logger := zap.New(NewCore(enc, w, zap.InfoLevel))
	logger.Info("hello", zap.Int("n", 1))

	select {
	case got := <-srv.recv:
		require.Contains(t, string(got), `"msg":"hello"`)
		require.Contains(t, string(got), `"n":1`)
	case <-time.After(time.Second):
		t.Fatal("log did not reach the writer")
	}
}
