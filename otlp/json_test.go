package otlp

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// assembleRequest builds a full ExportLogsServiceRequest from encoded records
// using the same envelope path as the writer.
func assembleRequest(t *testing.T, opts []Option, records ...[]byte) []byte {
	t.Helper()
	env := newEnvelope(applyOptions(opts))

	return env.assemble(nil, records)
}

func transcode(t *testing.T, req []byte) string {
	t.Helper()
	out, err := appendRequestJSON(nil, req)
	require.NoError(t, err)
	require.True(t, json.Valid(out), "transcoder must emit valid JSON: %s", out)

	return string(out)
}

// goldenOpts pins resource/scope content so goldens do not depend on the
// module version or binary name.
func goldenOpts() []Option {
	return []Option{WithServiceName("svc"), WithScopeName("scope"), WithScopeVersion("v1")}
}

// TestJSONGoldenMinimal pins the full-request OTLP/JSON shape byte-for-byte:
// lowerCamelCase names, integer severityNumber, decimal-string timestamps —
// the load-bearing spec-compliance oracle (design 2026-06-13 §7).
func TestJSONGoldenMinimal(t *testing.T) {
	rec := encodeRecord(t, NewEncoder(), testEntry())
	got := transcode(t, assembleRequest(t, goldenOpts(), rec))
	want := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},` +
		`"scopeLogs":[{"scope":{"name":"scope","version":"v1"},` +
		`"logRecords":[{"timeUnixNano":"1","severityNumber":9,"severityText":"info",` +
		`"body":{"stringValue":"m"},"observedTimeUnixNano":"1"}]}]}]}`
	require.Equal(t, want, got)
}

// TestJSONGoldenMultiRecordBatch pins repeated-field grouping: two records →
// one logRecords array.
func TestJSONGoldenMultiRecordBatch(t *testing.T) {
	e := NewEncoder()
	r1 := encodeRecord(t, e, testEntry())
	ent2 := testEntry()
	ent2.Message = "m2"
	r2 := encodeRecord(t, e, ent2)
	got := transcode(t, assembleRequest(t, goldenOpts(), r1, r2))
	want := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},` +
		`"scopeLogs":[{"scope":{"name":"scope","version":"v1"},` +
		`"logRecords":[` +
		`{"timeUnixNano":"1","severityNumber":9,"severityText":"info","body":{"stringValue":"m"},"observedTimeUnixNano":"1"},` +
		`{"timeUnixNano":"1","severityNumber":9,"severityText":"info","body":{"stringValue":"m2"},"observedTimeUnixNano":"1"}` +
		`]}]}]}`
	require.Equal(t, want, got)
}

// TestJSONGoldenAnyValueKinds pins every AnyValue projection: bool, negative
// int64 as decimal string, doubles (finite as number, NaN/±Inf as proto3 JSON
// strings), bytes as base64, string escaping (quote, backslash, newline,
// control char, invalid UTF-8 → U+FFFD), and nested arrayValue.
func TestJSONGoldenAnyValueKinds(t *testing.T) {
	rec := encodeRecord(
		t, NewEncoder(), testEntry(),
		zap.Bool("b", true),
		zap.Int64("i", -42),
		zap.Float64("d", 1.5),
		zap.Float64("nan", math.NaN()),
		zap.Float64("pinf", math.Inf(1)),
		zap.Float64("ninf", math.Inf(-1)),
		zap.Binary("bin", []byte{0x01, 0x02}),
		zap.String("esc", "a\"b\\c\nd\x01e\xfff"),
		zap.Strings("arr", []string{"x", "y"}),
	)
	got := transcode(t, assembleRequest(t, goldenOpts(), rec))
	wantAttrs := `"attributes":[` +
		`{"key":"b","value":{"boolValue":true}},` +
		`{"key":"i","value":{"intValue":"-42"}},` +
		`{"key":"d","value":{"doubleValue":1.5}},` +
		`{"key":"nan","value":{"doubleValue":"NaN"}},` +
		`{"key":"pinf","value":{"doubleValue":"Infinity"}},` +
		`{"key":"ninf","value":{"doubleValue":"-Infinity"}},` +
		`{"key":"bin","value":{"bytesValue":"AQI="}},` +
		`{"key":"esc","value":{"stringValue":"a\"b\\c\nd\u0001e�f"}},` +
		`{"key":"arr","value":{"arrayValue":{"values":[{"stringValue":"x"},{"stringValue":"y"}]}}}` +
		`]`
	require.Contains(t, got, wantAttrs)
}

// TestJSONGoldenNestedKVList pins kvlistValue via zap.Namespace.
func TestJSONGoldenNestedKVList(t *testing.T) {
	rec := encodeRecord(
		t, NewEncoder(), testEntry(),
		zap.Namespace("ns"), zap.String("k", "v"), zap.Int64("n", 7),
	)
	got := transcode(t, assembleRequest(t, goldenOpts(), rec))
	require.Contains(t, got,
		`"attributes":[{"key":"ns","value":{"kvlistValue":{"values":[`+
			`{"key":"k","value":{"stringValue":"v"}},`+
			`{"key":"n","value":{"intValue":"7"}}]}}}]`)
}

// TestJSONGoldenTraceContext pins the OTLP deviations for trace context:
// lowercase-hex traceId/spanId (NOT base64) and numeric flags.
func TestJSONGoldenTraceContext(t *testing.T) {
	sc, sctx := testSpanContext(t)
	rec := encodeRecord(t, NewEncoder(), testEntry(), SpanContext(sctx))
	got := transcode(t, assembleRequest(t, goldenOpts(), rec))
	require.Contains(t, got, `"flags":1`)
	require.Contains(t, got, fmt.Sprintf(`"traceId":"%s"`, sc.TraceID()))
	require.Contains(t, got, fmt.Sprintf(`"spanId":"%s"`, sc.SpanID()))
}

func TestAppendJSONStringEscaping(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", `"plain"`},
		{`q"uote`, `"q\"uote"`},
		{`back\slash`, `"back\\slash"`},
		{"nl\ncr\rtab\t", `"nl\ncr\rtab\t"`},
		{"ctrl\x00\x1f", "\"ctrl\\u0000\\u001f\""},
		{"unicode ✓ ok", `"unicode ✓ ok"`},
		{"bad\xffutf8", "\"bad�utf8\""},
		{"", `""`},
	}
	for _, c := range cases {
		require.Equal(t, c.want, string(appendJSONString(nil, c.in)), "input %q", c.in)
	}
}

// TestAppendRequestJSONCorruptBytes proves the invariant-violation path:
// bytes that are not a valid request must error, never emit JSON.
func TestAppendRequestJSONCorruptBytes(t *testing.T) {
	for _, b := range [][]byte{
		{0xff},                   // truncated tag
		{0x0a, 0x05, 0x01},       // length beyond buffer
		{0x3a, 0x00},             // unknown field 7 in ExportLogsServiceRequest
		{0x09, 1, 2, 3, 4, 5, 6}, // wire type 1 where field 1 is len-delimited
	} {
		_, err := appendRequestJSON(nil, b)
		require.Error(t, err, "bytes %x", b)
	}
}

func TestDecodePartialSuccessJSONTable(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		rejected int64
		msg      string
		wantErr  bool
	}{
		{name: "empty body", body: ""},
		{name: "empty object", body: "{}"},
		{name: "null partialSuccess", body: `{"partialSuccess":null}`},
		{name: "rejected as string", body: `{"partialSuccess":{"rejectedLogRecords":"2","errorMessage":"x"}}`, rejected: 2, msg: "x"},
		{name: "rejected as number", body: `{"partialSuccess":{"rejectedLogRecords":3}}`, rejected: 3},
		{name: "max int64 as string", body: fmt.Sprintf(`{"partialSuccess":{"rejectedLogRecords":"%d"}}`, int64(math.MaxInt64)), rejected: math.MaxInt64},
		{name: "message only warning", body: `{"partialSuccess":{"errorMessage":"slow"}}`, msg: "slow"},
		{name: "null rejected", body: `{"partialSuccess":{"rejectedLogRecords":null,"errorMessage":"m"}}`, msg: "m"},
		{name: "fractional", body: `{"partialSuccess":{"rejectedLogRecords":1.5}}`, wantErr: true},
		{name: "negative", body: `{"partialSuccess":{"rejectedLogRecords":-1}}`, wantErr: true},
		{name: "overflow", body: `{"partialSuccess":{"rejectedLogRecords":"92233720368547758080"}}`, wantErr: true},
		{name: "non-decimal string", body: `{"partialSuccess":{"rejectedLogRecords":"abc"}}`, wantErr: true},
		{name: "malformed json", body: `{"partialSuccess":`, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rejected, msg, err := decodePartialSuccessJSON([]byte(c.body))
			if c.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, c.rejected, rejected)
			require.Equal(t, c.msg, msg)
		})
	}
}

// protoPartialSuccess builds a binary ExportLogsServiceResponse with
// partial_success{rejected_log_records, error_message}.
func protoPartialSuccess(rejected int64, msg string) []byte {
	var inner []byte
	inner = appendTaggedVarint(inner, 0x08, rejected)
	inner = appendTaggedString(inner, 0x12, msg)

	return appendTaggedBytes(nil, 0x0a, inner)
}

// newTestJSONTransport builds a JSON-mode httpTransport against url.
func newTestJSONTransport(t *testing.T, url string, opts ...Option) *httpTransport {
	t.Helper()
	tr, err := newHTTPTransport(url, applyOptions(append([]Option{WithEncoding(JSON)}, opts...)))
	require.NoError(t, err)
	require.True(t, tr.jsonOn)

	return tr
}

// TestJSONResponsePolicy pins the JSON-mode 200-response decoder selection
// (design 2026-06-13 §6): explicit protobuf content type → proto decoder
// (rejections still counted); anything else → JSON decoder; malformed JSON →
// observability-only event.
func TestJSONResponsePolicy(t *testing.T) {
	cases := []struct {
		name         string
		respCT       string
		respBody     []byte
		wantRejected int64
		wantEvent    bool
	}{
		{
			name: "json string count", respCT: "application/json",
			respBody: []byte(`{"partialSuccess":{"rejectedLogRecords":"2","errorMessage":"x"}}`), wantRejected: 2, wantEvent: true,
		},
		{
			name: "json number count", respCT: "application/json; charset=utf-8",
			respBody: []byte(`{"partialSuccess":{"rejectedLogRecords":3}}`), wantRejected: 3, wantEvent: true,
		},
		{
			name: "protobuf response despite json request", respCT: "application/x-protobuf",
			respBody: protoPartialSuccess(4, "proxy"), wantRejected: 4, wantEvent: true,
		},
		{
			name: "wrong content type with json body", respCT: "text/plain",
			respBody: []byte(`{"partialSuccess":{"rejectedLogRecords":1}}`), wantRejected: 1, wantEvent: true,
		},
		{
			name: "malformed json body", respCT: "application/json",
			respBody: []byte(`{"partialSuccess":`), wantEvent: true,
		},
		{name: "empty body clean accept", respCT: "application/json", respBody: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
				rw.Header().Set("Content-Type", c.respCT)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(c.respBody)
			}))
			defer srv.Close()

			tr := newTestJSONTransport(t, srv.URL)
			rec := encodeRecord(t, NewEncoder(), testEntry())
			p := tr.prepare(assembleRequest(t, goldenOpts(), rec))
			require.Nil(t, p.fail)
			accept, expErr := tr.attempt(p)
			require.Nil(t, expErr)
			if c.wantRejected == 0 && !c.wantEvent {
				require.Nil(t, accept)

				return
			}
			require.NotNil(t, accept)
			require.Equal(t, c.wantRejected, accept.rejected)
			require.Equal(t, c.wantEvent, accept.event != nil)
		})
	}
}

// TestJSONTransportContentTypeAndGzip captures a JSON-mode request and
// asserts (a) Content-Type application/json, (b) with gzip the transcode
// runs BEFORE compression: Content-Encoding gzip and the decompressed body
// is the JSON document, not protobuf.
func TestJSONTransportContentTypeAndGzip(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", gz), func(t *testing.T) {
			var mu sync.Mutex
			var gotCT, gotEnc string
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				gotCT, gotEnc, gotBody = r.Header.Get("Content-Type"), r.Header.Get("Content-Encoding"), body
				mu.Unlock()
				rw.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			opts := []Option{}
			if gz {
				opts = append(opts, WithCompression(Gzip))
			}
			tr := newTestJSONTransport(t, srv.URL, opts...)
			rec := encodeRecord(t, NewEncoder(), testEntry())
			p := tr.prepare(assembleRequest(t, goldenOpts(), rec))
			require.Nil(t, p.fail)
			accept, expErr := tr.attempt(p)
			require.Nil(t, expErr)
			require.Nil(t, accept)

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, "application/json", gotCT)
			body := gotBody
			if gz {
				require.Equal(t, "gzip", gotEnc)
				zr, err := gzip.NewReader(newByteReader(body))
				require.NoError(t, err)
				body, err = io.ReadAll(zr)
				require.NoError(t, err)
			} else {
				require.Empty(t, gotEnc)
			}
			require.True(t, json.Valid(body), "wire body must be JSON: %q", body)
			require.Contains(t, string(body), `"resourceLogs"`)
		})
	}
}

func newByteReader(b []byte) io.Reader { return &byteReader{b: b} }

type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]

	return n, nil
}

// failTransport counts prepare/attempt calls; prepare always fails.
type failTransport struct {
	prepares atomic.Int64
	attempts atomic.Int64
}

func (f *failTransport) prepare([]byte) prepared {
	f.prepares.Add(1)

	return prepared{fail: &ExportError{Message: "boom"}}
}

func (f *failTransport) attempt(prepared) (*acceptance, *ExportError) {
	f.attempts.Add(1)

	return nil, nil
}

func (f *failTransport) close() {}

// TestPrepareFailDropsBatchWithoutAttempt proves the fatal-prepare seam
// (design 2026-06-13 §6): the whole batch is counted dropped, the error
// handler fires exactly once, and attempt is never called — identically on
// the retrying flush path and the single-attempt Close-drain path.
func TestPrepareFailDropsBatchWithoutAttempt(t *testing.T) {
	for _, allowRetry := range []bool{true, false} {
		t.Run(fmt.Sprintf("allowRetry=%v", allowRetry), func(t *testing.T) {
			f := &failTransport{}
			var events atomic.Int64
			w := newWriterCore(f, applyOptions([]Option{
				WithErrorHandler(func(error) { events.Add(1) }),
			}))
			rec := encodeRecord(t, NewEncoder(), testEntry())
			w.export([][]byte{rec, rec, rec}, allowRetry)

			require.Equal(t, uint64(3), w.DroppedLogs())
			require.Equal(t, int64(1), events.Load())
			require.Equal(t, int64(1), f.prepares.Load())
			require.Zero(t, f.attempts.Load(), "attempt must never run on fatal prepare")
		})
	}
}

// countingTransport wraps a real transport, counting prepare calls.
type countingTransport struct {
	inner    transport
	prepares atomic.Int64
}

func (c *countingTransport) prepare(msg []byte) prepared {
	c.prepares.Add(1)

	return c.inner.prepare(msg)
}

func (c *countingTransport) attempt(p prepared) (*acceptance, *ExportError) {
	return c.inner.attempt(p)
}
func (c *countingTransport) close() { c.inner.close() }

// TestJSONRetryTranscodesOnce proves a retrying JSON batch transcodes once:
// prepare runs once per export while attempt retries (503 → 200).
func TestJSONRetryTranscodesOnce(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			rw.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	inner := newTestJSONTransport(t, srv.URL)
	ct := &countingTransport{inner: inner}
	w := newWriterCore(ct, applyOptions([]Option{
		WithRetry(RetryConfig{Initial: time.Millisecond, MaxInterval: 2 * time.Millisecond, MaxElapsed: time.Second}),
	}))
	rec := encodeRecord(t, NewEncoder(), testEntry())
	w.export([][]byte{rec}, true)

	require.Equal(t, int64(2), calls.Load(), "503 then 200")
	require.Equal(t, int64(1), ct.prepares.Load(), "transcode/gzip must run once per batch")
	require.Zero(t, w.DroppedLogs())
}

// TestEncodingConstructorMatrix pins validation ownership (design §5):
// undefined values fail on the HTTP constructors, JSON fails on the gRPC
// constructors, the zero value is Protobuf, and NewEncoder ignores the
// option entirely.
func TestEncodingConstructorMatrix(t *testing.T) {
	w, err := NewWriter("http://127.0.0.1:1")
	require.NoError(t, err, "zero value (Protobuf) must construct")
	require.NoError(t, w.Close())

	w, err = NewWriter("http://127.0.0.1:1", WithEncoding(JSON))
	require.NoError(t, err, "JSON must construct on HTTP")
	require.NoError(t, w.Close())

	_, err = NewWriter("http://127.0.0.1:1", WithEncoding(Encoding(255)))
	require.ErrorContains(t, err, "undefined Encoding")

	_, _, err = NewHTTPCore("http://127.0.0.1:1", zapcore.InfoLevel, WithEncoding(Encoding(7)))
	require.ErrorContains(t, err, "undefined Encoding")

	_, err = NewGRPCWriter("127.0.0.1:1", WithInsecure(), WithEncoding(JSON))
	require.ErrorContains(t, err, "gRPC")

	_, _, err = NewGRPCCore("127.0.0.1:1", zapcore.InfoLevel, WithInsecure(), WithEncoding(JSON))
	require.ErrorContains(t, err, "gRPC")

	// NewEncoder ignores writer-end options: same bytes either way.
	plain := encodeRecord(t, NewEncoder(), testEntry())
	withOpt := encodeRecord(t, NewEncoder(WithEncoding(JSON)), testEntry())
	require.Equal(t, plain, withOpt)
}

// TestJSONWriterEndToEnd ships through the public NewWriter path in JSON
// mode and asserts the wire request parses as the documented shape.
func TestJSONWriterEndToEnd(t *testing.T) {
	type anyDoc = map[string]any
	var mu sync.Mutex
	var docs []anyDoc
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var doc anyDoc
		if json.Unmarshal(body, &doc) == nil {
			mu.Lock()
			docs = append(docs, doc)
			mu.Unlock()
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewWriter(srv.URL, WithEncoding(JSON), WithServiceName("jtest"),
		WithFlushInterval(10*time.Millisecond))
	require.NoError(t, err)
	rec := encodeRecord(t, NewEncoder(), testEntry())
	_, err = w.Write(rec)
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, docs)
	rl, ok := docs[0]["resourceLogs"].([]any)
	require.True(t, ok, "resourceLogs must be a JSON array: %v", docs[0])
	require.Len(t, rl, 1)
}
