package otlp

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestDefaultSeverityMapper(t *testing.T) {
	cases := []struct {
		lvl  zapcore.Level
		want SeverityNumber
	}{
		{zapcore.DebugLevel, SeverityDebug},  // 5
		{zapcore.InfoLevel, SeverityInfo},    // 9
		{zapcore.WarnLevel, SeverityWarn},    // 13
		{zapcore.ErrorLevel, SeverityError},  // 17
		{zapcore.DPanicLevel, SeverityFatal}, // 21
		{zapcore.PanicLevel, SeverityFatal2}, // 22
		{zapcore.FatalLevel, SeverityFatal3}, // 23
		{zapcore.Level(42), SeverityUnspecified},
	}
	for _, c := range cases {
		require.Equal(t, c.want, defaultSeverityMapper(c.lvl), "level %v", c.lvl)
	}
}

func TestClampSeverity(t *testing.T) {
	require.Equal(t, SeverityNumber(0), clampSeverity(-3))
	require.Equal(t, SeverityNumber(24), clampSeverity(99))
	require.Equal(t, SeverityInfo, clampSeverity(SeverityInfo))
}

func TestDefaultsAndNormalize(t *testing.T) {
	o := applyOptions(nil)
	require.Equal(t, 2048, o.queueSize)
	require.Equal(t, 512, o.batchSize)
	require.Equal(t, 4<<20, o.maxRequestBytes)
	require.Equal(t, time.Second, o.flushInterval)
	require.Equal(t, 10*time.Second, o.timeout)
	require.Equal(t, DropNewest, o.dropPolicy)
	require.Equal(t, RetryConfig{Initial: 5 * time.Second, MaxInterval: 30 * time.Second, MaxElapsed: time.Minute}, o.retry)
	require.Equal(t, NoCompression, o.compression)
	require.True(t, o.callerAttrs)
	require.Equal(t, "logger", o.loggerNameKey)
	require.Contains(t, o.serviceName, "unknown_service:")
	require.Equal(t, "github.com/arloliu/zapwire/otlp", o.scopeName)
	require.NotNil(t, o.severityOf)
	require.NotNil(t, o.errFn) // no-op, never nil
	require.NotNil(t, o.client)

	// Non-positive values clamp back to defaults (core normalizeConfig discipline).
	o = applyOptions([]Option{
		WithQueueSize(-1), WithBatchSize(0), WithMaxRequestBytes(-5),
		WithFlushInterval(0), WithTimeout(-time.Second),
		WithRetry(RetryConfig{}), // zero fields → defaults
	})
	require.Equal(t, 2048, o.queueSize)
	require.Equal(t, 512, o.batchSize)
	require.Equal(t, 4<<20, o.maxRequestBytes)
	require.Equal(t, time.Second, o.flushInterval)
	require.Equal(t, 10*time.Second, o.timeout)
	require.Equal(t, 5*time.Second, o.retry.Initial)
	require.Equal(t, 30*time.Second, o.retry.MaxInterval)
	require.Equal(t, time.Minute, o.retry.MaxElapsed)
}

func TestOptionSetters(t *testing.T) {
	hc := &http.Client{}
	called := false
	o := applyOptions([]Option{
		WithServiceName("svc"),
		WithScopeName("scope"), WithScopeVersion("v9"),
		WithSeverityMapper(func(zapcore.Level) SeverityNumber { return 7 }),
		WithSeverityMapper(nil), // nil mapper ignored, previous kept
		WithCallerAttributes(false),
		WithLoggerNameKey(""), // empty disables
		WithDropPolicy(DropOldest),
		WithHeaders(map[string]string{"x-api-key": "k"}),
		WithHTTPClient(hc),
		WithCompression(Gzip),
		WithErrorHandler(func(error) { called = true }),
	})
	require.Equal(t, "svc", o.serviceName)
	require.Equal(t, "scope", o.scopeName)
	require.Equal(t, "v9", o.scopeVersion)
	require.Equal(t, SeverityNumber(7), o.severityOf(zapcore.InfoLevel))
	require.False(t, o.callerAttrs)
	require.Empty(t, o.loggerNameKey)
	require.Equal(t, DropOldest, o.dropPolicy)
	require.Equal(t, "k", o.headers["x-api-key"])

	// WithHeaders merges across two calls; later value wins on collision.
	o2 := applyOptions([]Option{
		WithHeaders(map[string]string{"a": "1"}),
		WithHeaders(map[string]string{"b": "2", "a": "overwritten"}),
	})
	require.Equal(t, "overwritten", o2.headers["a"])
	require.Equal(t, "2", o2.headers["b"])

	require.Same(t, hc, o.client)
	require.Equal(t, Gzip, o.compression)
	o.errFn(nil)
	require.True(t, called)
}

func TestResolveEndpoint(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{"http://collector:4318", "http://collector:4318/v1/logs", false},
		{"http://collector:4318/", "http://collector:4318/v1/logs", false},
		{"https://collector:4318/custom/path", "https://collector:4318/custom/path", false},
		{"http://h:4318?x=1", "http://h:4318/v1/logs?x=1", false},
		{"", "", true},               // ErrNoEndpoint
		{"://bad", "", true},         // parse error
		{"collector:4318", "", true}, // no http(s) scheme
	}
	for _, c := range cases {
		got, err := resolveEndpoint(c.in)
		if c.wantErr {
			require.Error(t, err, "input %q", c.in)
			if c.in == "" {
				require.ErrorIs(t, err, ErrNoEndpoint)
			}

			continue
		}
		require.NoError(t, err, "input %q", c.in)
		require.Equal(t, c.want, got)
	}
}

func TestEndpointFromEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	require.Empty(t, EndpointFromEnv())

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://base:4318")
	require.Equal(t, "http://base:4318", EndpointFromEnv())

	// Signal-specific endpoint wins.
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://logs:4318/v1/logs")
	require.Equal(t, "http://logs:4318/v1/logs", EndpointFromEnv())
}

func TestExportError(t *testing.T) {
	inner := errTruncatedResponse
	e := &ExportError{StatusCode: 503, Retryable: true, Message: "busy", Err: inner}
	require.ErrorIs(t, e, errTruncatedResponse)
	require.Contains(t, e.Error(), "503")
	require.Contains(t, e.Error(), "busy")
}
