package otlp

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SeverityNumber is the OTLP severity (data model: 1-24, 0 = unspecified).
type SeverityNumber int32

const (
	SeverityUnspecified SeverityNumber = 0
	SeverityTrace       SeverityNumber = 1
	SeverityDebug       SeverityNumber = 5
	SeverityInfo        SeverityNumber = 9
	SeverityWarn        SeverityNumber = 13
	SeverityError       SeverityNumber = 17
	SeverityFatal       SeverityNumber = 21
	SeverityFatal2      SeverityNumber = 22
	SeverityFatal3      SeverityNumber = 23

	maxSeverity SeverityNumber = 24
)

// defaultSeverityMapper is the contrib-bridge canonical mapping (design §3.2).
func defaultSeverityMapper(l zapcore.Level) SeverityNumber {
	switch l {
	case zapcore.DebugLevel:
		return SeverityDebug
	case zapcore.InfoLevel:
		return SeverityInfo
	case zapcore.WarnLevel:
		return SeverityWarn
	case zapcore.ErrorLevel:
		return SeverityError
	case zapcore.DPanicLevel:
		return SeverityFatal
	case zapcore.PanicLevel:
		return SeverityFatal2
	case zapcore.FatalLevel:
		return SeverityFatal3
	default:
		return SeverityUnspecified
	}
}

// clampSeverity bounds mapper output per entry (design §3.2).
func clampSeverity(s SeverityNumber) SeverityNumber {
	if s < 0 {
		return 0
	}
	if s > maxSeverity {
		return maxSeverity
	}

	return s
}

// DropPolicy selects which record is dropped when the queue is full.
type DropPolicy uint8

const (
	DropNewest DropPolicy = iota
	DropOldest
)

// Compression selects the request body encoding.
type Compression uint8

const (
	NoCompression Compression = iota
	Gzip
)

// RetryConfig bounds the §5.3 retry loop. Zero fields fall back to defaults.
type RetryConfig struct {
	Initial     time.Duration // first backoff delay (default 5s)
	MaxInterval time.Duration // backoff cap (default 30s)
	MaxElapsed  time.Duration // total retry budget per batch (default 60s)
}

// ExportError is delivered to WithErrorHandler for every ship-path event.
// Handlers receive terminal outcomes only: a Retryable error has already
// exhausted its retry budget, and Warning reports a partial success that
// rejected nothing.
type ExportError struct {
	StatusCode int    // HTTP status; 0 for transport/encode errors
	Retryable  bool   // whether the failure was in the retryable class
	Rejected   int64  // partial-success rejected_log_records
	Warning    bool   // partial success with Rejected == 0 and a message
	Message    string // partial-success error_message or short response excerpt
	Err        error  // wrapped underlying error, may be nil

	retryAfter time.Duration // parsed Retry-After; internal to the retry loop
}

func (e *ExportError) Error() string {
	return fmt.Sprintf("otlp export: status=%d retryable=%v rejected=%d warning=%v msg=%q err=%v",
		e.StatusCode, e.Retryable, e.Rejected, e.Warning, e.Message, e.Err)
}

func (e *ExportError) Unwrap() error { return e.Err }

// ErrNoEndpoint is returned by NewWriter/NewCore for an empty endpoint.
var ErrNoEndpoint = errors.New("otlp: no endpoint")

type options struct {
	// envelope end
	serviceName    string
	resourceFields []zap.Field
	scopeName      string
	scopeVersion   string
	// encoder end
	severityOf    func(zapcore.Level) SeverityNumber
	callerAttrs   bool
	loggerNameKey string
	// writer end
	queueSize       int
	batchSize       int
	maxRequestBytes int
	flushInterval   time.Duration
	timeout         time.Duration
	dropPolicy      DropPolicy
	retry           RetryConfig
	headers         map[string]string
	client          *http.Client
	compression     Compression
	errFn           func(error)
}

// Option configures the otlp preset. Envelope/encoder options are documented
// no-ops on NewWriter-only paths and writer options are no-ops on NewEncoder,
// per the subpackage convention (design §6).
type Option func(*options)

func defaultOptions() options {
	return options{
		serviceName:     "unknown_service:" + filepath.Base(os.Args[0]),
		scopeName:       "github.com/arloliu/zapwire/otlp",
		scopeVersion:    moduleVersion(),
		severityOf:      defaultSeverityMapper,
		callerAttrs:     true,
		loggerNameKey:   "logger",
		queueSize:       2048,
		batchSize:       512,
		maxRequestBytes: 4 << 20,
		flushInterval:   time.Second,
		timeout:         10 * time.Second,
		dropPolicy:      DropNewest,
		retry:           RetryConfig{Initial: 5 * time.Second, MaxInterval: 30 * time.Second, MaxElapsed: time.Minute},
		client:          &http.Client{},
		errFn:           func(error) {},
	}
}

func applyOptions(opts []Option) options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return normalize(o)
}

// normalize clamps invalid values back to defaults (core normalizeConfig
// discipline, options.go:171 in the root module).
func normalize(o options) options {
	d := defaultOptions()
	if o.queueSize <= 0 {
		o.queueSize = d.queueSize
	}
	if o.batchSize <= 0 {
		o.batchSize = d.batchSize
	}
	if o.maxRequestBytes <= 0 {
		o.maxRequestBytes = d.maxRequestBytes
	}
	if o.flushInterval <= 0 {
		o.flushInterval = d.flushInterval
	}
	if o.timeout <= 0 {
		o.timeout = d.timeout
	}
	if o.retry.Initial <= 0 {
		o.retry.Initial = d.retry.Initial
	}
	if o.retry.MaxInterval <= 0 {
		o.retry.MaxInterval = d.retry.MaxInterval
	}
	if o.retry.MaxElapsed <= 0 {
		o.retry.MaxElapsed = d.retry.MaxElapsed
	}
	if o.severityOf == nil {
		o.severityOf = d.severityOf
	}
	if o.client == nil {
		o.client = d.client
	}
	if o.errFn == nil {
		o.errFn = d.errFn
	}

	return o
}

// moduleVersion best-efforts this module's version from build info ("" in
// dev/test builds).
func moduleVersion() string {
	// runtime/debug.ReadBuildInfo walks deps of the *main* module; when otlp
	// is a dependency its version appears there. Inside this module's own
	// tests it is "(devel)" or absent — both render as "".
	return readModuleVersion()
}

// readModuleVersion is split out so tests can exercise the "" path without
// faking build info.
func readModuleVersion() string {
	bi, ok := debugReadBuildInfo()
	if !ok {
		return ""
	}
	const path = "github.com/arloliu/zapwire/otlp"
	if bi.Main.Path == path && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	for _, d := range bi.Deps {
		if d.Path == path {
			return d.Version
		}
	}

	return ""
}

// debugReadBuildInfo is a seam for tests.
var debugReadBuildInfo = debug.ReadBuildInfo

// WithServiceName sets the Resource service.name (default "unknown_service:<exe>").
func WithServiceName(s string) Option { return func(o *options) { o.serviceName = s } }

// WithResource appends extra Resource attributes, encoded through the same
// proto ObjectEncoder as record attributes (design §4).
func WithResource(fields ...zap.Field) Option {
	return func(o *options) { o.resourceFields = append(o.resourceFields, fields...) }
}

// WithScopeName overrides the InstrumentationScope name.
func WithScopeName(s string) Option { return func(o *options) { o.scopeName = s } }

// WithScopeVersion overrides the InstrumentationScope version.
func WithScopeVersion(s string) Option { return func(o *options) { o.scopeVersion = s } }

// WithSeverityMapper overrides the zap-level → SeverityNumber mapping; results
// are clamped to 0..24 per entry. A nil mapper is ignored.
func WithSeverityMapper(fn func(zapcore.Level) SeverityNumber) Option {
	return func(o *options) {
		if fn != nil {
			o.severityOf = fn
		}
	}
}

// WithCallerAttributes toggles code.* attributes from Entry.Caller (default on).
func WithCallerAttributes(on bool) Option { return func(o *options) { o.callerAttrs = on } }

// WithLoggerNameKey sets the attribute key carrying Entry.LoggerName (default
// "logger"; empty disables the attribute).
func WithLoggerNameKey(k string) Option { return func(o *options) { o.loggerNameKey = k } }

// WithQueueSize bounds the ingest queue (records; default 2048).
func WithQueueSize(n int) Option { return func(o *options) { o.queueSize = n } }

// WithBatchSize caps records per request (default 512).
func WithBatchSize(n int) Option { return func(o *options) { o.batchSize = n } }

// WithMaxRequestBytes caps the uncompressed request body (default 4 MiB);
// batches are cut early and oversized single records are dropped at Write.
func WithMaxRequestBytes(n int) Option { return func(o *options) { o.maxRequestBytes = n } }

// WithFlushInterval caps batch latency (default 1s).
func WithFlushInterval(d time.Duration) Option { return func(o *options) { o.flushInterval = d } }

// WithDropPolicy selects the queue-full policy (default DropNewest).
func WithDropPolicy(p DropPolicy) Option { return func(o *options) { o.dropPolicy = p } }

// WithTimeout bounds each HTTP attempt (default 10s).
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithRetry overrides retry/backoff bounds; zero fields keep defaults.
func WithRetry(rc RetryConfig) Option { return func(o *options) { o.retry = rc } }

// WithHeaders adds headers to every export request (auth, api keys).
func WithHeaders(h map[string]string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(map[string]string, len(h))
		}
		maps.Copy(o.headers, h)
	}
}

// WithHTTPClient supplies the http.Client (TLS, proxies); nil keeps the default.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) {
		if c != nil {
			o.client = c
		}
	}
}

// WithCompression selects request compression (default NoCompression).
func WithCompression(c Compression) Option { return func(o *options) { o.compression = c } }

// WithErrorHandler observes ship-path events (*ExportError); nil keeps the
// no-op. The handler is invoked synchronously from Write (oversized records)
// and from the flush goroutine (export failures) — it must be fast and
// non-blocking, but it MAY call the Writer's own methods (Close/Sync): no
// internal lock is held across handler invocations.
func WithErrorHandler(fn func(error)) Option {
	return func(o *options) {
		if fn != nil {
			o.errFn = fn
		}
	}
}

// resolveEndpoint validates the endpoint URL and appends /v1/logs when the
// path is empty (design §5.5).
func resolveEndpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", ErrNoEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("otlp: invalid endpoint %q: %w", endpoint, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("otlp: endpoint %q must be an http(s) URL", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/logs"
	}

	return u.String(), nil
}

// EndpointFromEnv resolves OTEL_EXPORTER_OTLP_LOGS_ENDPOINT (used as-is) then
// OTEL_EXPORTER_OTLP_ENDPOINT (base URL; NewWriter appends /v1/logs when the
// path is empty). Returns "" when neither is set. Env handling is explicit
// and opt-in — zapwire never reads env behind the caller's back (design §5.5).
func EndpointFromEnv() string {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"); v != "" {
		return v
	}

	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}
