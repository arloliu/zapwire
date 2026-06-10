package zapwire

import (
	"context"
	"net"
	"time"
)

const defaultDialTimeout = 3 * time.Second

// Transport is a reconnectable byte-stream endpoint (UDS or TCP).
type Transport interface {
	// Dial establishes a new connection, honoring ctx for cancellation/timeout.
	Dial(ctx context.Context) (net.Conn, error)
	// Network reports the net package network ("unix" or "tcp").
	Network() string
	// Address reports the dial address (socket path or host:port).
	Address() string
}

type netTransport struct {
	network string
	address string
	timeout time.Duration
}

// UDS returns a Transport that connects to a Unix domain socket at path. Each
// (re)connect dial is bounded by a 3s timeout; dialing runs only on the
// background (re)connect path, never on the log-write path.
//
// Parameters:
//   - path: filesystem path of the Unix domain socket to dial
//
// Returns:
//   - Transport: a transport that dials path on each (re)connect
func UDS(path string) Transport {
	return &netTransport{network: "unix", address: path, timeout: defaultDialTimeout}
}

// TCP returns a Transport that connects to a TCP host:port address. Each
// (re)connect dial is bounded by a 3s timeout; dialing runs only on the
// background (re)connect path, never on the log-write path.
//
// Parameters:
//   - addr: TCP address to dial, in host:port form
//
// Returns:
//   - Transport: a transport that dials addr on each (re)connect
func TCP(addr string) Transport {
	return &netTransport{network: "tcp", address: addr, timeout: defaultDialTimeout}
}

func (t *netTransport) Dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: t.timeout}

	return d.DialContext(ctx, t.network, t.address) //nolint:wrapcheck // surfaced verbatim by caller
}

func (t *netTransport) Network() string { return t.network }
func (t *netTransport) Address() string { return t.address }
