package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// grpcMethodPath is the only :path OTLP/gRPC ever uses; user-supplied URL
// paths are a misconfiguration and are rejected (design 2026-06-12 §5).
const grpcMethodPath = "/opentelemetry.proto.collector.logs.v1.LogsService/Export"

const grpcDefaultPort = "4317"

// resolveGRPCEndpoint maps the accepted endpoint forms to (base URL, TLS):
//
//	host[:port]          TLS by default (spec insecure=false); WithInsecure → h2c
//	http://host[:port]   plaintext h2c; scheme wins; +WithTLSConfig errors
//	https://host[:port]  TLS; scheme wins
//
// Scheme-less endpoints default to port 4317; http/https leave the URL's
// own port semantics (80/443) to the HTTP client.
func resolveGRPCEndpoint(endpoint string, insecure, hasTLSCfg bool) (base string, useTLS bool, err error) {
	if endpoint == "" {
		return "", false, ErrNoEndpoint
	}
	if !strings.Contains(endpoint, "://") {
		if strings.ContainsAny(endpoint, "/?#") {
			return "", false, fmt.Errorf("otlp: grpc endpoint %q must be host[:port] — gRPC uses the fixed method path", endpoint)
		}
		host := endpoint
		if _, _, sperr := net.SplitHostPort(host); sperr != nil {
			host = net.JoinHostPort(host, grpcDefaultPort)
		}
		if insecure {
			if hasTLSCfg {
				return "", false, fmt.Errorf("otlp: WithInsecure and WithTLSConfig conflict for endpoint %q", endpoint)
			}

			return "http://" + host, false, nil
		}

		return "https://" + host, true, nil
	}

	u, perr := url.Parse(endpoint)
	if perr != nil {
		return "", false, fmt.Errorf("otlp: invalid grpc endpoint %q: %w", endpoint, perr)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("otlp: grpc endpoint %q has no host", endpoint)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false, fmt.Errorf("otlp: grpc endpoint %q must not carry a path/query/fragment — gRPC uses the fixed method path", endpoint)
	}
	switch u.Scheme {
	case "http":
		if hasTLSCfg {
			return "", false, fmt.Errorf("otlp: WithTLSConfig conflicts with plaintext scheme in %q", endpoint)
		}

		return "http://" + u.Host, false, nil
	case "https":
		return "https://" + u.Host, true, nil
	default:
		return "", false, fmt.Errorf("otlp: grpc endpoint %q must be host:port or an http(s) URL", endpoint)
	}
}

// validateGRPCHeaders rejects WithHeaders entries that would corrupt the
// gRPC request (design §4.4): reserved keys (grpc-* metadata, -bin suffix,
// framing headers, pseudo-headers — binary metadata is unsupported) and
// non-printable-ASCII values (the gRPC ASCII-metadata value rule,
// %x20-%x7E). Both fail at construction: a deterministic configuration
// error must not surface as a send-time export event.
func validateGRPCHeaders(h map[string]string) error {
	for k, v := range h {
		lk := strings.ToLower(k)
		switch {
		case strings.HasPrefix(lk, "grpc-"),
			strings.HasSuffix(lk, "-bin"),
			strings.HasPrefix(lk, ":"),
			lk == "content-type", lk == "te":
			return fmt.Errorf("otlp: header %q is reserved on the gRPC transport", k)
		}
		for i := 0; i < len(v); i++ {
			if v[i] < 0x20 || v[i] > 0x7e {
				return fmt.Errorf("otlp: header %q value contains non-printable-ASCII byte 0x%02x", k, v[i])
			}
		}
		if v != strings.TrimSpace(v) {
			return fmt.Errorf("otlp: header %q value has leading/trailing whitespace", k)
		}
	}

	return nil
}

// grpcTransport is the hand-rolled OTLP/gRPC ship layer (design 2026-06-12
// §6): a unary gRPC client over stdlib HTTP/2 — h2c for plaintext (Go ≥1.24
// Protocols), ALPN h2 for TLS. It owns its http.Client: a user-supplied
// client with HTTP/1 enabled would break gRPC, hence WithHTTPClient is a
// documented no-op here.
type grpcTransport struct {
	url        string // base + grpcMethodPath
	client     *http.Client
	headers    map[string]string
	gzipOn     bool
	timeout    time.Duration
	timeoutHdr string // precomputed grpc-timeout value ("" = omit)
	userAgent  string
}

var _ transport = (*grpcTransport)(nil)

func newGRPCTransport(endpoint string, o options) (*grpcTransport, error) {
	base, useTLS, err := resolveGRPCEndpoint(endpoint, o.insecure, o.tlsConfig != nil)
	if err != nil {
		return nil, err
	}
	if err := validateGRPCHeaders(o.headers); err != nil {
		return nil, err
	}

	protos := new(http.Protocols)
	htr := &http.Transport{Protocols: protos}
	if useTLS {
		// ALPN h2 only — no HTTP/1 fallback on this dedicated transport.
		protos.SetHTTP2(true)
		htr.TLSClientConfig = o.tlsConfig
		htr.ForceAttemptHTTP2 = true
	} else {
		// h2c (prior knowledge). HTTP/1 must stay DISABLED: with both
		// enabled, stdlib picks HTTP/1 for http:// URLs (spike gotcha #1).
		protos.SetUnencryptedHTTP2(true)
	}

	ua := "zapwire-otlp"
	if v := moduleVersion(); v != "" {
		ua += "/" + v
	}

	return &grpcTransport{
		url:        base + grpcMethodPath,
		client:     &http.Client{Transport: htr},
		headers:    o.headers,
		gzipOn:     o.compression == Gzip,
		timeout:    o.timeout,
		timeoutHdr: grpcTimeoutValue(o.timeout),
		userAgent:  ua,
	}, nil
}

// close releases the private HTTP/2 client's idle connections. The client
// is writer-private (WithHTTPClient is a no-op on the gRPC path), so
// closing idle connections is safe and required — the zero-value
// http.Transport has no IdleConnTimeout and would otherwise leak the h2c/h2
// connection after the writer is closed.
func (t *grpcTransport) close() { t.client.CloseIdleConnections() }

// grpcFrame wraps msg in the gRPC Length-Prefixed-Message framing:
// 1-byte compressed flag + 4-byte big-endian length + message.
func grpcFrame(msg []byte, compressed bool) []byte {
	body := make([]byte, 5, 5+len(msg))
	if compressed {
		body[0] = 1
	}
	binary.BigEndian.PutUint32(body[1:5], uint32(len(msg))) //nolint:gosec

	return append(body, msg...)
}

// prepare frames (and per-message gzips) once per batch. A gzip failure
// ships uncompressed rather than losing the batch (httpTransport parity).
func (t *grpcTransport) prepare(msg []byte) prepared {
	if t.gzipOn {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		if _, err := zw.Write(msg); err == nil && zw.Close() == nil {
			return prepared{body: grpcFrame(gz.Bytes(), true), compressed: true}
		}

		return prepared{body: grpcFrame(msg, false), warn: &ExportError{Message: "gzip failed; sent uncompressed"}}
	}

	return prepared{body: grpcFrame(msg, false)}
}

// attempt performs one unary Export call. Context is Background+timeout, not
// the lifecycle ctx — Close never cancels an accepted request (§5.4 parity).
func (t *grpcTransport) attempt(p prepared) (*acceptance, *ExportError) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(p.body))
	if err != nil {
		return nil, &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	req.Header.Set("Grpc-Accept-Encoding", "identity,gzip")
	req.Header.Set("User-Agent", t.userAgent)
	if t.timeoutHdr != "" {
		req.Header.Set("Grpc-Timeout", t.timeoutHdr)
	}
	if p.compressed {
		req.Header.Set("Grpc-Encoding", "gzip")
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		// Transport failures (dial, reset, GOAWAY, local deadline) are
		// RETRYABLE on gRPC — grpc-go's own classification, and the §6.5
		// promise that connection loss is absorbed by retry + re-dial.
		// (Deliberate asymmetry: the HTTP path keeps v0.1.0's terminal
		// transport errors.)
		return nil, t.transportError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Body MUST be read to EOF before resp.Trailer is populated (spike
	// gotcha #3). A failed read also means trailers are unreliable —
	// return retryable rather than resolving status from incomplete metadata.
	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if rerr != nil {
		return nil, t.transportError(ctx, rerr)
	}

	// Trailer-first status resolution; trailers-only errors surface the
	// status (and details) in resp.Header instead (spike gotcha #2).
	statusStr := resp.Trailer.Get("Grpc-Status")
	meta := resp.Trailer
	if statusStr == "" {
		statusStr = resp.Header.Get("Grpc-Status")
		meta = resp.Header
	}
	if statusStr == "" {
		// Not a gRPC response (proxy interference): canonical HTTP mapping.
		code := httpStatusToGRPCStatus(resp.StatusCode)

		return nil, &ExportError{
			StatusCode: resp.StatusCode,
			GRPCStatus: code,
			Retryable:  retryableGRPCStatus(code, false),
			Message:    excerpt(respBody),
		}
	}

	code, cerr := strconv.Atoi(statusStr)
	if cerr != nil {
		// non-retryable by intent: an unparseable grpc-status is a server defect
		return nil, &ExportError{Message: "malformed grpc-status " + excerpt([]byte(statusStr)), Err: cerr}
	}
	if code == grpcOK {
		msg, derr := deframeGRPCResponse(respBody, resp.Header.Get("Grpc-Encoding"))
		if derr != nil {
			// Accepted by the server; the malformed frame is observability-only.
			return &acceptance{event: &ExportError{Err: derr}}, nil
		}
		if msg == nil {
			return nil, nil
		}

		return resolveAccept(msg, ExportError{}), nil
	}

	var retryAfter time.Duration
	hasRetryInfo := false
	if det := meta.Get("Grpc-Status-Details-Bin"); det != "" {
		if raw, berr := decodeBinHeader(det); berr == nil {
			if d, ok := retryDelayFromStatus(raw); ok {
				retryAfter, hasRetryInfo = d, true
			}
		}
	}

	return nil, &ExportError{
		GRPCStatus: code,
		Retryable:  retryableGRPCStatus(code, hasRetryInfo),
		Message:    percentDecode(meta.Get("Grpc-Message")),
		retryAfter: retryAfter,
	}
}

// transportError classifies a client.Do / body-read failure: UNAVAILABLE
// (connection-level), or DEADLINE_EXCEEDED when the attempt's local timeout
// elapsed. Both are in the OTLP retryable class (design §6.3).
func (t *grpcTransport) transportError(ctx context.Context, err error) *ExportError {
	code := grpcUnavailable
	if ctx.Err() != nil {
		code = grpcDeadlineExceeded
	}

	return &ExportError{GRPCStatus: code, Retryable: true, Err: err}
}

// deframeGRPCResponse strips the 5-byte prefix and gunzips a compressed
// message frame. A zero-length body (no response frame) returns (nil, nil).
// A unary response is exactly one frame: the body must be exactly 5+mlen
// bytes. Bodies shorter than 5+mlen return errTruncatedResponse; bodies
// longer (trailing bytes or a second frame) return a distinct error — both
// cases are observability-only on an otherwise OK status (the server accepted
// the batch; the malformed framing is a server defect).
func deframeGRPCResponse(body []byte, encoding string) ([]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}
	if len(body) < 5 {
		return nil, errTruncatedResponse
	}
	mlen := binary.BigEndian.Uint32(body[1:5])
	if uint64(len(body)-5) < uint64(mlen) { //nolint:gosec // len(body) >= 5 verified above
		return nil, errTruncatedResponse
	}
	if extra := uint64(len(body)-5) - uint64(mlen); extra > 0 { //nolint:gosec // len(body) >= 5 verified above
		return nil, fmt.Errorf("otlp: %d trailing bytes after unary response frame", extra)
	}
	msg := body[5 : 5+mlen]
	if body[0]&1 == 0 {
		return msg, nil
	}
	if encoding != "gzip" {
		return nil, fmt.Errorf("otlp: compressed response frame with grpc-encoding %q", encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()

	return io.ReadAll(io.LimitReader(zr, 1<<20))
}
