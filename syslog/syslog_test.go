package syslog

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	require.Equal(t, LOCAL0, o.facility)
	require.Equal(t, OctetCounting, o.framing)
	require.False(t, o.bom)
	require.Equal(t, strconv.Itoa(os.Getpid()), o.procID)
	require.Equal(t, filepath.Base(os.Args[0]), o.appName)
	require.NotNil(t, o.severityOf)
}

func TestApply_OverridesAndResolveHeader(t *testing.T) {
	o := apply([]Option{
		WithFacility(LOCAL3),
		WithHostname("h\x00ost"), // illegal byte dropped on resolve
		WithAppName("my-app"),
		WithProcID("4123"),
		WithMsgID("ID7"),
		WithBOM(true),
		WithFraming(LFTerminated),
	})
	require.Equal(t, LFTerminated, o.framing)

	h := o.resolveHeader()
	require.Equal(t, LOCAL3, h.facility)
	require.Equal(t, "host", h.hostname) // NUL dropped
	require.Equal(t, "my-app", h.appName)
	require.Equal(t, "4123", h.procID)
	require.Equal(t, "ID7", h.msgID)
	require.True(t, h.bom)
}

func TestResolveHeader_EmptyFieldsBecomeNilValue(t *testing.T) {
	h := apply([]Option{WithHostname(""), WithAppName(""), WithProcID(""), WithMsgID("")}).resolveHeader()
	require.Equal(t, "-", h.hostname)
	require.Equal(t, "-", h.appName)
	require.Equal(t, "-", h.procID)
	require.Equal(t, "-", h.msgID)
}

func TestWithSeverityMapper_NilKeepsDefault(t *testing.T) {
	o := apply([]Option{WithSeverityMapper(nil)})
	require.Equal(t, Informational, o.severityOf(zapcore.InfoLevel)) // still the default
}

func TestResolveHeader_OutOfRangeFacilityFallsBack(t *testing.T) {
	h := apply([]Option{WithFacility(Facility(99))}).resolveHeader()
	require.Equal(t, LOCAL0, h.facility)
}

// rfc5424Re matches HEADER SP STRUCTURED-DATA SP MSG (SD is "-" in JSON-body mode).
var rfc5424Re = regexp.MustCompile(`^<(\d{1,3})>1 (\S+) (\S+) (\S+) (\S+) (\S+) - (.*)$`)

func assertValidRFC5424(t *testing.T, msg string) {
	t.Helper()
	m := rfc5424Re.FindStringSubmatch(msg)
	require.NotNilf(t, m, "not a valid RFC5424 line: %q", msg)
	pri, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	require.LessOrEqual(t, pri, 191)
	require.True(t, json.Valid([]byte(m[7])), "MSG must be valid JSON: %q", m[7])
}

func dialableUDS(t *testing.T) (string, net.Listener) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "syslog.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)

	return path, ln
}

func TestNewCore_EndToEnd_OctetCounting(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()

	recv := make(chan []byte, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		// Octet-counted frame: "<len> <SYSLOG-MSG>". Read the count, then that many bytes.
		r := bufio.NewReader(conn)
		countField, _ := r.ReadString(' ')
		n, _ := strconv.Atoi(strings.TrimSpace(countField))
		msg := make([]byte, n)
		_, _ = io.ReadFull(r, msg)
		recv <- msg
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg(),
		WithHostname("h"), WithAppName("app"), WithProcID("123"))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("hello", zap.Int("n", 7))

	select {
	case msg := <-recv:
		assertValidRFC5424(t, string(msg))
		require.Contains(t, string(msg), `"msg":"hello"`)
		require.Contains(t, string(msg), `"n":7`)
	case <-time.After(2 * time.Second):
		t.Fatal("no syslog message received")
	}
}

// TestNewCore_EndToEnd_Async exercises async mode: the encoder returns a pooled buffer, so the
// async path must take an owning copy before zap frees it (design §7). Run under -race, this
// asserts the decoded record is intact — a use-after-free would corrupt the JSON body.
func TestNewCore_EndToEnd_Async(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()

	recv := make(chan []byte, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		countField, _ := r.ReadString(' ')
		n, _ := strconv.Atoi(strings.TrimSpace(countField))
		msg := make([]byte, n)
		_, _ = io.ReadFull(r, msg)
		recv <- msg
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg(),
		WithZapwireOptions(zapwire.WithAsyncMode()),
		WithHostname("h"), WithAppName("app"), WithProcID("123"))
	require.NoError(t, err)
	defer w.Close()

	logger := zap.New(core)
	logger.Info("async", zap.Int("n", 9))
	require.NoError(t, logger.Sync()) // flush the async buffer

	select {
	case msg := <-recv:
		assertValidRFC5424(t, string(msg))
		require.Contains(t, string(msg), `"msg":"async"`)
		require.Contains(t, string(msg), `"n":9`)
	case <-time.After(2 * time.Second):
		t.Fatal("no async syslog message received")
	}
}

func TestNewCore_EndToEnd_LFFraming(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()

	recv := make(chan string, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		line, _ := bufio.NewReader(conn).ReadString('\n')
		recv <- strings.TrimRight(line, "\n")
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg(),
		WithFraming(LFTerminated), WithHostname("h"), WithAppName("app"), WithProcID("1"))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("lf")

	select {
	case line := <-recv:
		assertValidRFC5424(t, line)
	case <-time.After(2 * time.Second):
		t.Fatal("no LF-terminated message received")
	}
}

func TestNewWriter_HeaderOptionsAreNoOps(t *testing.T) {
	// NewWriter builds only Passthrough + Framer; header options must be accepted and ignored
	// without error (a BYO-core caller supplies them to NewEncoder).
	path, ln := dialableUDS(t)
	defer ln.Close()
	w, err := NewWriter(zapwire.UDS(path), WithHostname("ignored"), WithFacility(KERN))
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func TestConcurrent_Logging_Race(t *testing.T) {
	path, ln := dialableUDS(t)
	defer ln.Close()
	go func() { // drain
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c); _ = c.Close() }(conn)
		}
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, goldenEncCfg())
	require.NoError(t, err)
	defer w.Close()

	base := zap.New(core)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			base.Info("shared", zap.Int("g", i))
			base.With(zap.String("ctx", "c")).Info("with")
			base.With(zap.Namespace("ns")).Info("nested", zap.Int("k", i))
		}(i)
	}
	wg.Wait()
}
