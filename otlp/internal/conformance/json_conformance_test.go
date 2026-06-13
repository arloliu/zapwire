package conformance

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/arloliu/zapwire/otlp"
)

// fixedClock pins zap's entry timestamps so the protobuf and JSON captures
// of the same log sequence are bit-identical inputs.
type fixedClock time.Time

func (c fixedClock) Now() time.Time                       { return time.Time(c) }
func (c fixedClock) NewTicker(time.Duration) *time.Ticker { return time.NewTicker(time.Hour) }

// shipAndCapture logs one fixed sequence through a writer with the given
// encoding and returns the single captured request body.
func shipAndCapture(t *testing.T, enc otlp.Encoding) []byte {
	t.Helper()
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, w, err := otlp.NewHTTPCore(srv.URL, zapcore.DebugLevel,
		otlp.WithEncoding(enc),
		otlp.WithServiceName("json-conf"),
		otlp.WithResource(zap.String("deployment.environment.name", "test")),
		otlp.WithScopeName("conformance"), otlp.WithScopeVersion("v1"))
	require.NoError(t, err)
	logger := zap.New(core, zap.WithClock(fixedClock(time.Unix(7, 42))))
	_, ctx := spanCtx(t)
	logger.Info("one", otlp.SpanContext(ctx),
		zap.String("k", "v"),
		zap.Int64("neg", -42),
		zap.Uint64("umax", 1<<63+1),
		zap.Float64("f", 3.5),
		zap.Bool("b", true),
		zap.Binary("bin", []byte{0xde, 0xad}),
		zap.Strings("arr", []string{"x", "y"}),
		zap.Namespace("ns"), zap.String("inner", "w"),
	)
	logger.Warn("two")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 1)

	return bodies[0]
}

// TestJSONSemanticRoundTrip is the SEMANTIC acceptance oracle for OTLP/JSON
// (design 2026-06-13 §7): the collector pdata JSON unmarshaler — the
// reference receiver implementation — must decode our JSON into exactly the
// same data as our protobuf. It does NOT prove deviation compliance by
// itself (pdata is permissive; see TestPdataOraclePermissivenessProbe) —
// the byte-exact goldens in the otlp package are the load-bearing oracle.
func TestJSONSemanticRoundTrip(t *testing.T) {
	protoBody := shipAndCapture(t, otlp.Protobuf)
	jsonBody := shipAndCapture(t, otlp.JSON)

	protoReq := plogotlp.NewExportRequest()
	require.NoError(t, protoReq.UnmarshalProto(protoBody))
	jsonReq := plogotlp.NewExportRequest()
	require.NoError(t, jsonReq.UnmarshalJSON(jsonBody), "reference receiver must accept our JSON: %s", jsonBody)

	pb1, err := protoReq.MarshalProto()
	require.NoError(t, err)
	pb2, err := jsonReq.MarshalProto()
	require.NoError(t, err)
	require.Equal(t, pb1, pb2, "JSON and protobuf must decode to identical data")
}

// TestPdataOraclePermissivenessProbe documents what the pdata JSON
// unmarshaler tolerates beyond the spec's sender requirements, so future
// readers know what TestJSONSemanticRoundTrip does and does not prove:
// pdata accepts enum NAMES for severityNumber and snake_case field names,
// both of which OTLP/JSON senders MUST NOT emit. Compliance with the
// sender-side deviations is pinned by the golden tests in the otlp package.
func TestPdataOraclePermissivenessProbe(t *testing.T) {
	permissive := `{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[` +
		`{"time_unix_nano":"1","severityNumber":"SEVERITY_NUMBER_INFO","body":{"stringValue":"m"}}` +
		`]}]}]}`
	req := plogotlp.NewExportRequest()
	err := req.UnmarshalJSON([]byte(permissive))
	require.NoError(t, err, "pdata accepts non-compliant sender forms — hence goldens are load-bearing")
	rec := req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	require.EqualValues(t, 9, rec.SeverityNumber())
	require.EqualValues(t, 1, rec.Timestamp())
}
