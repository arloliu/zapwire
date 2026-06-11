package syslog

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestValidateFacility(t *testing.T) {
	require.Equal(t, KERN, validateFacility(KERN))
	require.Equal(t, LOCAL7, validateFacility(LOCAL7)) // 23, the max
	require.Equal(t, LOCAL0, validateFacility(Facility(24)))
	require.Equal(t, LOCAL0, validateFacility(Facility(-1)))
}

func TestClampSeverity(t *testing.T) {
	require.Equal(t, Emergency, clampSeverity(Emergency)) // 0
	require.Equal(t, Debug, clampSeverity(Debug))         // 7
	require.Equal(t, Debug, clampSeverity(Severity(9)))
	require.Equal(t, Emergency, clampSeverity(Severity(-1)))
}

func TestDefaultSeverityMapper(t *testing.T) {
	cases := map[zapcore.Level]Severity{
		zapcore.DebugLevel:  Debug,
		zapcore.InfoLevel:   Informational,
		zapcore.WarnLevel:   Warning,
		zapcore.ErrorLevel:  Error,
		zapcore.DPanicLevel: Critical,
		zapcore.PanicLevel:  Critical,
		zapcore.FatalLevel:  Critical,
	}
	for lvl, want := range cases {
		require.Equalf(t, want, defaultSeverityMapper(lvl), "level %v", lvl)
	}
}

func TestSanitizeField(t *testing.T) {
	require.Equal(t, "host", sanitizeField("host", 255))
	require.Equal(t, "-", sanitizeField("", 48))            // empty → NILVALUE
	require.Equal(t, "-", sanitizeField("   ", 48))         // spaces (0x20) are not 33..126
	require.Equal(t, "abc", sanitizeField("a b\tc", 48))    // drop space + tab
	require.Equal(t, "ab", sanitizeField("abcdef", 2))      // truncate to cap
	require.Equal(t, "ok", sanitizeField("o\x00k\x7f", 48)) // drop NUL + DEL(127)
}

func TestAppendRFC3339Micros(t *testing.T) {
	b := buffer.NewPool().Get()
	appendRFC3339Micros(b, time.Date(2026, 6, 10, 22, 14, 15, 3456000, time.UTC))
	require.Equal(t, "2026-06-10T22:14:15.003456Z", b.String())

	z := buffer.NewPool().Get()
	appendRFC3339Micros(z, time.Time{})
	require.Equal(t, "-", z.String()) // zero time → NILVALUE
}

// goldenEncCfg yields a minimal JSON body: only the message key, so the body is
// {"msg":...} plus the call-site fields — keeping golden assertions focused on the header.
func goldenEncCfg() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{MessageKey: "msg"}
}

func TestEncoder_Golden_InfoLine(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithHostname("h"), WithAppName("app"), WithProcID("123"))
	ent := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2026, 6, 10, 22, 14, 15, 3456000, time.UTC),
		Message: "hi",
	}
	buf, err := enc.EncodeEntry(ent, []zapcore.Field{zap.String("k", "v")})
	require.NoError(t, err)
	want := `<134>1 2026-06-10T22:14:15.003456Z h app 123 - - {"msg":"hi","k":"v"}`
	require.Equal(t, want, buf.String())
}

func TestEncoder_PRI_Boundaries(t *testing.T) {
	emerg := func(zapcore.Level) Severity { return Emergency }
	encMin := NewEncoder(goldenEncCfg(), WithFacility(KERN), WithSeverityMapper(emerg),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := encMin.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<0>1 "), buf.String()) // 0*8+0

	dbg := func(zapcore.Level) Severity { return Debug }
	encMax := NewEncoder(goldenEncCfg(), WithFacility(LOCAL7), WithSeverityMapper(dbg),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ = encMax.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<191>1 "), buf.String()) // 23*8+7
}

func TestEncoder_PRI_OutOfRangeInputsAreBounded(t *testing.T) {
	bad := func(zapcore.Level) Severity { return Severity(9) } // clamped to 7
	enc := NewEncoder(goldenEncCfg(), WithFacility(Facility(99)), WithSeverityMapper(bad),
		WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<135>1 "), buf.String()) // LOCAL0(16)*8+7
}

func TestEncoder_LineEndingIndependence(t *testing.T) {
	for _, tc := range []struct {
		ending string
		skip   bool
	}{{"\n", false}, {"\r\n", false}, {"", false}, {"END\n", false}, {"\n", true}, {"\r\n", true}} {
		cfg := goldenEncCfg()
		cfg.LineEnding = tc.ending
		cfg.SkipLineEnding = tc.skip
		enc := NewEncoder(cfg, WithHostname("h"), WithAppName("a"), WithProcID("1"))
		buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
		require.NoError(t, err)
		line := buf.String()
		assert.Falsef(t, strings.HasSuffix(line, "\n"), "no trailing LF for %+v: %q", tc, line)
		assert.NotContainsf(t, line, "\r", "no stray CR for %+v: %q", tc, line)
		assert.Truef(t, strings.HasSuffix(line, "}"), "ends with JSON body for %+v: %q", tc, line)
	}
}

func TestEncoder_BOM(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithBOM(true), WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.Contains(t, buf.String(), " - \uFEFF{") // SD, SP, BOM, then JSON
}

// The §3.5 body-format seam: jsonBody emits SP + SD "-" + SP + optional BOM + the JSON MSG.
func TestJSONBody_AppendBody(t *testing.T) {
	b := buffer.NewPool().Get()
	jsonBody{}.appendBody(b, false, []byte(`{"a":1}`))
	require.Equal(t, ` - {"a":1}`, b.String())

	b2 := buffer.NewPool().Get()
	jsonBody{}.appendBody(b2, true, []byte(`{"a":1}`))
	require.Equal(t, " - \uFEFF{\"a\":1}", b2.String())
}

func TestEncoder_ZeroTimeIsNilValue(t *testing.T) {
	enc := NewEncoder(goldenEncCfg(), WithHostname("h"), WithAppName("a"), WithProcID("1"))
	buf, _ := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "x"}, nil)
	require.True(t, strings.HasPrefix(buf.String(), "<134>1 - h a 1 - - {"), buf.String())
}

// JSON-body parity (spec §8): the MSG equals a plain JSON encoder body built from the SAME
// SkipLineEnding=true config, including With fields and a namespace.
func TestEncoder_JSONBodyParity_WithAndNamespace(t *testing.T) {
	cfg := goldenEncCfg()
	syslogEnc := NewEncoder(cfg, WithHostname("h"), WithAppName("a"), WithProcID("1"))

	oracleCfg := cfg
	oracleCfg.SkipLineEnding = true // NewEncoder forces this internally; match it here
	oracle := zapcore.NewJSONEncoder(oracleCfg)

	sysClone := syslogEnc.Clone()
	oraClone := oracle.Clone()
	for _, f := range []zapcore.Field{zap.String("svc", "x"), zap.Namespace("ns")} {
		f.AddTo(sysClone)
		f.AddTo(oraClone)
	}
	ent := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hi"}
	call := []zapcore.Field{zap.Int("n", 7)}

	sysBuf, err := sysClone.EncodeEntry(ent, call)
	require.NoError(t, err)
	oraBuf, err := oraClone.EncodeEntry(ent, call)
	require.NoError(t, err)

	i := strings.IndexByte(sysBuf.String(), '{') // header fields contain no '{' here
	require.GreaterOrEqual(t, i, 0)
	require.Equal(t, oraBuf.String(), sysBuf.String()[i:])
}
