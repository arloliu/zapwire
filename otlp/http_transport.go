package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// httpTransport is the OTLP/HTTP ship layer — the v0.1.0 behavior, verbatim,
// behind the transport seam. jsonOn selects the OTLP/JSON encoding mode
// (design 2026-06-13): transcode at prepare, application/json content type,
// JSON partial-success resolution.
type httpTransport struct {
	endpoint string
	client   *http.Client
	headers  map[string]string
	gzipOn   bool
	jsonOn   bool
	timeout  time.Duration
}

var _ transport = (*httpTransport)(nil)

func newHTTPTransport(endpoint string, o options) (*httpTransport, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if o.encoding > JSON {
		return nil, fmt.Errorf("otlp: undefined Encoding %d", o.encoding)
	}

	return &httpTransport{
		endpoint: ep,
		client:   o.client,
		headers:  o.headers,
		gzipOn:   o.compression == Gzip,
		jsonOn:   o.encoding == JSON,
		timeout:  o.timeout,
	}, nil
}

// prepare transcodes to OTLP/JSON when configured, then applies whole-body
// gzip (Content-Encoding) — both once per batch (a retrying batch must not
// re-transcode or re-gzip). A gzip failure ships uncompressed rather than
// losing the batch (v0.1.0 behavior); a transcode failure CANNOT ship
// (there is no valid body) and surfaces as prepared.fail → counted drop.
func (t *httpTransport) prepare(msg []byte) prepared {
	if t.jsonOn {
		j, err := appendRequestJSON(nil, msg)
		if err != nil {
			return prepared{fail: &ExportError{Err: err, Message: "json transcode failed; batch dropped"}}
		}
		msg = j
	}
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
	// User headers first, then transport-owned headers LAST so WithHeaders can
	// never override the Content-Type/Content-Encoding the body is framed with
	// (a mismatched Content-Type would make the receiver misparse the payload).
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	ct := "application/x-protobuf"
	if t.jsonOn {
		ct = "application/json"
	}
	req.Header.Set("Content-Type", ct)
	// Own Content-Encoding in BOTH directions: a user-supplied value must never
	// describe a body it doesn't match (an uncompressed body tagged gzip makes
	// the receiver misparse it). Del covers the default no-compression case.
	if p.compressed {
		req.Header.Set("Content-Encoding", "gzip")
	} else {
		req.Header.Del("Content-Encoding")
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, &ExportError{Err: err} // transport errors are non-retryable (§5.3)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		return t.resolve(respBody, resp.Header.Get("Content-Type")), nil // never retried (§5.3)
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

// resolve picks the 200-response partial-success decoder. Protobuf mode is
// v0.1.0 verbatim (no sniffing). In JSON mode the spec requires the server to
// mirror application/json, but proxies/receivers violate it: an explicit
// application/x-protobuf response routes to the proto decoder (so a
// spec-violating-but-honest protobuf partial success still counts its
// rejections); anything else parses as JSON, with parse failures landing in
// the malformed/observability-only class (design 2026-06-13 §6).
func (t *httpTransport) resolve(respBody []byte, respCT string) *acceptance {
	base := ExportError{StatusCode: http.StatusOK}
	if !t.jsonOn {
		return resolveAccept(respBody, base)
	}
	if ct, _, _ := strings.Cut(respCT, ";"); strings.TrimSpace(ct) == "application/x-protobuf" {
		return resolveAccept(respBody, base)
	}

	return resolveAcceptJSON(respBody, base)
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
