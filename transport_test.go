package zapwire

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUDS_DialsListeningSocket(t *testing.T) {
	path := filepath.Join(os.TempDir(), "zapwire_uds_test.sock")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer func() { _ = ln.Close(); _ = os.Remove(path) }()

	tr := UDS(path)
	require.Equal(t, "unix", tr.Network())
	require.Equal(t, path, tr.Address())

	conn, err := tr.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestTCP_DialsListeningPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	tr := TCP(ln.Addr().String())
	require.Equal(t, "tcp", tr.Network())

	conn, err := tr.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestUDS_DialMissingSocketFailsFast(t *testing.T) {
	tr := UDS(filepath.Join(os.TempDir(), "zapwire_absent.sock"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := tr.Dial(ctx)
	require.Error(t, err)
}
