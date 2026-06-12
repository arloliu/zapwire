package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"
)

// httpTransport is the OTLP/HTTP ship layer — the v0.1.0 behavior, verbatim,
// behind the transport seam.
type httpTransport struct {
	endpoint string
	client   *http.Client
	headers  map[string]string
	gzipOn   bool
	timeout  time.Duration
}

var _ transport = (*httpTransport)(nil)

func newHTTPTransport(endpoint string, o options) (*httpTransport, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	return &httpTransport{
		endpoint: ep,
		client:   o.client,
		headers:  o.headers,
		gzipOn:   o.compression == Gzip,
		timeout:  o.timeout,
	}, nil
}

// prepare applies whole-body gzip (Content-Encoding) once per batch. A gzip
// failure ships uncompressed rather than losing the batch (v0.1.0 behavior).
func (t *httpTransport) prepare(msg []byte) prepared {
	if !t.gzipOn {
		return prepared{body: msg}
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(msg); err == nil && zw.Close() == nil {
		return prepared{body: gz.Bytes(), compressed: true}
	}

	return prepared{body: msg, warn: &ExportError{Message: "gzip failed; sent uncompressed"}}
}

// close is a no-op: the http.Client may be caller-supplied (WithHTTPClient)
// and is therefore never torn down here.
func (t *httpTransport) close() {}

// attempt performs one POST. Its context is Background+timeout — NOT the
// lifecycle ctx — so Close never cancels a request the server may have
// already accepted (§5.4); Close interrupts the backoff sleeps instead.
func (t *httpTransport) attempt(p prepared) (*acceptance, *ExportError) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(p.body))
	if err != nil {
		return nil, &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if p.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, &ExportError{Err: err} // transport errors are non-retryable (§5.3)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		return resolveAccept(respBody, ExportError{StatusCode: http.StatusOK}), nil // never retried (§5.3)
	case retryableStatus(resp.StatusCode):
		return nil, &ExportError{
			StatusCode: resp.StatusCode,
			Retryable:  true,
			Message:    excerpt(respBody),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	default: // 400, 413, anything else: non-retryable
		return nil, &ExportError{StatusCode: resp.StatusCode, Message: excerpt(respBody)}
	}
}

// retryableStatus implements the OTLP/HTTP retryable class (§5.3).
func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}

	return false
}

// parseRetryAfter accepts delta-seconds or an HTTP-date (spec clarification).
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(h); err == nil {
		if d := time.Until(at); d > 0 {
			return d
		}
	}

	return 0
}

func excerpt(b []byte) string {
	const max = 256 //nolint:predeclared // local response-excerpt cap; shadowing the builtin is harmless here
	if len(b) > max {
		b = b[:max]
	}

	return string(b)
}
