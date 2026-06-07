# Phase 05 — Configurable time handling (`fluent.TimeCodec`)

Replaces the hard-coded `"ts"` + `EpochNanos` time handling in `fluent/` with a `TimeCodec`
that bundles the key, the zap time encoder, and the decoder as one unit — so the encode and
decode ends cannot drift. Adds built-in codecs and supports custom ones. See design §12.

> **Ordering:** integrate this AFTER the whole-branch post-impl review has landed, so it
> builds on the final `fluent/encoder.go` + `fluent/fluent.go`. This phase supersedes the
> hard-coded time handling (`extractTime` reading `"ts"`; `NewCore` pinning
> `TimeKey="ts"`/`EpochNanosTimeEncoder`).
>
> **Scope:** `fluent/` only. `ndjson/` passes zap's JSON through untouched and needs no codec.

---

### Task 5.1: `TimeCodec` type, built-in codecs, `ApplyTo`

**Files:**
- Create: `fluent/timecodec.go`
- Test: `fluent/timecodec_test.go`

- [ ] **Step 1: Write the failing tests**

`fluent/timecodec_test.go`:
```go
package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// roundTrip encodes want with the codec's ZapEncoder, captures the JSON-ish value zap would
// write, then feeds it back through codec.Decode — proving both ends agree.
func encodeTimeValue(t *testing.T, c TimeCodec, want time.Time) any {
	t.Helper()
	// Drive the codec's zap TimeEncoder through a JSON encoder and read the field back.
	cfg := zapcore.EncoderConfig{
		TimeKey: c.Key, MessageKey: "msg", EncodeTime: c.ZapEncoder,
	}
	enc := zapcore.NewJSONEncoder(cfg)
	buf, err := enc.EncodeEntry(zapcore.Entry{Time: want, Message: "x"}, nil)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, jsonUnmarshal(buf.Bytes(), &m))

	return m[c.Key]
}

func TestBuiltinCodecs_RoundTrip(t *testing.T) {
	want := time.Unix(1692959400, 123000000).UTC() // ms-aligned so all codecs can match
	codecs := map[string]TimeCodec{
		"epochNanos":   EpochNanosCodec("ts"),
		"epochMillis":  EpochMillisCodec("ts"),
		"epochSeconds": EpochSecondsCodec("ts"),
		"rfc3339nano":  RFC3339NanoCodec("ts"),
		"rfc3339":      RFC3339Codec("ts"),
		"iso8601":      ISO8601Codec("ts"),
	}
	for name, c := range codecs {
		t.Run(name, func(t *testing.T) {
			v := encodeTimeValue(t, c, want)
			got, ok := c.Decode(v)
			require.True(t, ok, "codec must decode its own encoded value")
			// Allow up to 1s slop for second-precision codecs (rfc3339, epochSeconds float).
			require.WithinDuration(t, want, got, time.Second)
		})
	}
}

func TestEpochMillisCodec_ExactToMillis(t *testing.T) {
	want := time.Unix(1692959400, 123000000).UTC()
	v := encodeTimeValue(t, EpochMillisCodec("ts"), want)
	got, ok := EpochMillisCodec("ts").Decode(v)
	require.True(t, ok)
	require.Equal(t, want.UnixMilli(), got.UnixMilli())
}

func TestRFC3339NanoCodec_ExactToNanos(t *testing.T) {
	want := time.Unix(1692959400, 123456789).UTC()
	v := encodeTimeValue(t, RFC3339NanoCodec("ts"), want)
	got, ok := RFC3339NanoCodec("ts").Decode(v)
	require.True(t, ok)
	require.True(t, got.Equal(want), "RFC3339Nano is exact: got %v want %v", got, want)
}

func TestDecode_WrongType(t *testing.T) {
	_, ok := EpochNanosCodec("ts").Decode("not-a-number")
	require.False(t, ok)
	_, ok = RFC3339NanoCodec("ts").Decode(12345)
	require.False(t, ok)
}

func TestApplyTo_SetsBothEnds(t *testing.T) {
	c := EpochMillisCodec("when")
	var cfg zapcore.EncoderConfig
	c.ApplyTo(&cfg)
	require.Equal(t, "when", cfg.TimeKey)
	require.NotNil(t, cfg.EncodeTime)
}
```

Add a tiny JSON helper to the test (keeps the test self-contained):
```go
import "encoding/json"

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run 'TestBuiltinCodecs|TestEpochMillis|TestRFC3339Nano|TestDecode_WrongType|TestApplyTo'`
Expected: FAIL — `undefined: EpochNanosCodec` etc.

- [ ] **Step 3: Write `fluent/timecodec.go`**

```go
package fluent

import (
	"time"

	"go.uber.org/zap/zapcore"
)

// TimeCodec bundles the two ends of the timestamp round-trip so they cannot drift: how zap
// writes the time field on the JSON wire (Key + ZapEncoder) and how the transcode Encoder
// reads it back into the Fluent EventTime (Key + Decode). Use a built-in (EpochNanosCodec,
// EpochMillisCodec, RFC3339NanoCodec, …) or supply your own.
type TimeCodec struct {
	// Key is the JSON field holding the timestamp (e.g. "ts").
	Key string
	// ZapEncoder is the zapcore time encoder for the encode end; NewCore (and ApplyTo)
	// wire it onto the EncoderConfig so the encode side matches Decode.
	ZapEncoder zapcore.TimeEncoder
	// Decode converts the JSON-decoded value at Key into a time. ok=false means
	// "absent or unparseable" → the Encoder falls back to time.Now().
	Decode func(value any) (time.Time, bool)
}

// ApplyTo wires this codec's encode end onto a zapcore.EncoderConfig (TimeKey + EncodeTime).
// Callers building their own zapcore.Core (instead of using NewCore) should call this so the
// encode side matches Decode.
func (c TimeCodec) ApplyTo(cfg *zapcore.EncoderConfig) {
	cfg.TimeKey = c.Key
	cfg.EncodeTime = c.ZapEncoder
}

// valid reports whether the codec is usable (a zero TimeCodec is not).
func (c TimeCodec) valid() bool { return c.Key != "" && c.ZapEncoder != nil && c.Decode != nil }

// defaultTimeCodec is used when no codec is configured: a magnitude-tolerant epoch decoder
// at key "ts", so a bring-your-own-core caller using zap's default float-seconds encoder is
// decoded correctly instead of to ~1970.
func defaultTimeCodec() TimeCodec { return AutoEpochCodec("ts") }

// AutoEpochCodec decodes a numeric epoch timestamp, auto-detecting its unit (s/ms/µs/ns) by
// magnitude — robust for log timestamps (always ~now, ~3 orders of magnitude apart per
// unit). Its encode end is EpochNanosTimeEncoder, so NewCore round-trips exactly while the
// decoder tolerates other units on the bring-your-own-core path. This is the default codec.
func AutoEpochCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochNanosTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return epochToTime(f), true
			}

			return time.Time{}, false
		},
	}
}

// Magnitude thresholds for detecting the unit of a numeric epoch timestamp. Today: seconds
// ~1.7e9, millis ~1.7e12, micros ~1.7e15, nanos ~1.7e18. Only pre-~2001 nanosecond values
// (which never occur in logs) would be misclassified.
const (
	epochMillisThreshold = 1e12 // >= this: at least milliseconds
	epochMicrosThreshold = 1e15 // >= this: at least microseconds
	epochNanosThreshold  = 1e18 // >= this: nanoseconds
)

// epochToTime converts a numeric epoch timestamp to a time, detecting its unit by magnitude.
func epochToTime(v float64) time.Time {
	switch {
	case v >= epochNanosThreshold:
		return time.Unix(0, int64(v))
	case v >= epochMicrosThreshold:
		return time.Unix(0, int64(v*1e3))
	case v >= epochMillisThreshold:
		return time.Unix(0, int64(v*1e6))
	default:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)

		return time.Unix(sec, nsec)
	}
}

// EpochNanosCodec encodes/decodes integer epoch nanoseconds (zapcore.EpochNanosTimeEncoder).
// Note: JSON numbers decode to float64, so nanosecond magnitudes lose ~tens of ns of
// precision. Use RFC3339NanoCodec when exact nanoseconds matter.
func EpochNanosCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochNanosTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return time.Unix(0, int64(f)), true
			}

			return time.Time{}, false
		},
	}
}

// EpochMillisCodec encodes/decodes integer epoch milliseconds (zapcore.EpochMillisTimeEncoder).
func EpochMillisCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochMillisTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				return time.UnixMilli(int64(f)), true
			}

			return time.Time{}, false
		},
	}
}

// EpochSecondsCodec encodes/decodes floating-point epoch seconds — zap's default
// EpochTimeEncoder. Lets zap's out-of-the-box config work without misreading the time.
func EpochSecondsCodec(key string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: zapcore.EpochTimeEncoder,
		Decode: func(v any) (time.Time, bool) {
			if f, ok := v.(float64); ok {
				sec := int64(f)
				nsec := int64((f - float64(sec)) * float64(time.Second))

				return time.Unix(sec, nsec), true
			}

			return time.Time{}, false
		},
	}
}

// RFC3339NanoCodec encodes/decodes RFC3339 timestamps with nanoseconds
// (zapcore.RFC3339NanoTimeEncoder). Exact to the nanosecond.
func RFC3339NanoCodec(key string) TimeCodec {
	return stringCodec(key, zapcore.RFC3339NanoTimeEncoder, time.RFC3339Nano)
}

// RFC3339Codec encodes/decodes RFC3339 timestamps at second precision
// (zapcore.RFC3339TimeEncoder).
func RFC3339Codec(key string) TimeCodec {
	return stringCodec(key, zapcore.RFC3339TimeEncoder, time.RFC3339)
}

// ISO8601Codec encodes/decodes ISO8601 millisecond-precision strings
// (zapcore.ISO8601TimeEncoder), e.g. "2006-01-02T15:04:05.000Z0700".
func ISO8601Codec(key string) TimeCodec {
	return stringCodec(key, zapcore.ISO8601TimeEncoder, "2006-01-02T15:04:05.000Z0700")
}

// stringCodec builds a codec for string timestamps. It tries the primary layout, then a few
// common variants, so minor format differences still parse.
func stringCodec(key string, enc zapcore.TimeEncoder, layout string) TimeCodec {
	return TimeCodec{
		Key:        key,
		ZapEncoder: enc,
		Decode: func(v any) (time.Time, bool) {
			s, ok := v.(string)
			if !ok {
				return time.Time{}, false
			}
			for _, l := range []string{layout, time.RFC3339Nano, time.RFC3339} {
				if t, err := time.Parse(l, s); err == nil {
					return t, true
				}
			}

			return time.Time{}, false
		},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./fluent/... -run 'TestBuiltinCodecs|TestEpochMillis|TestRFC3339Nano|TestDecode_WrongType|TestApplyTo' -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fluent/timecodec.go fluent/timecodec_test.go
git commit -m "feat(fluent): add TimeCodec with built-in time codecs"
```

---

### Task 5.2: Encoder uses the codec

**Files:**
- Modify: `fluent/encoder.go`
- Test: `fluent/encoder_test.go` (add cases)

- [ ] **Step 1: Write the failing tests (append to `fluent/encoder_test.go`)**

```go
func TestEncoderWithCodec_CustomKeyAndFormat(t *testing.T) {
	enc := NewEncoderWithCodec(EpochMillisCodec("timestamp"))
	want := time.Unix(1692959400, 456000000).UTC()
	out, err := enc.Encode(nil, []byte(fmt.Sprintf(`{"msg":"x","timestamp":%d}`, want.UnixMilli())))
	require.NoError(t, err)

	e := decodeEntry(t, out)
	require.Equal(t, want.UnixMilli(), time.Time(e.Time).UnixMilli())
	rec := e.Record.(map[string]any)
	_, hasKey := rec["timestamp"]
	require.False(t, hasKey, "the configured time key must be lifted out of the record")
}

func TestEncoderWithCodec_RFC3339NanoExact(t *testing.T) {
	enc := NewEncoderWithCodec(RFC3339NanoCodec("ts"))
	want := time.Unix(1692959400, 123456789).UTC()
	out, err := enc.Encode(nil, []byte(fmt.Sprintf(`{"msg":"x","ts":%q}`, want.Format(time.RFC3339Nano))))
	require.NoError(t, err)
	require.True(t, time.Time(decodeEntry(t, out).Time).Equal(want))
}

func TestEncoderWithCodec_MissingKeyFallsBackToNow(t *testing.T) {
	before := time.Now().Add(-time.Second)
	enc := NewEncoderWithCodec(EpochMillisCodec("timestamp"))
	out, err := enc.Encode(nil, []byte(`{"msg":"no time field"}`))
	require.NoError(t, err)
	require.False(t, time.Time(decodeEntry(t, out).Time).Before(before))
}

func TestNewEncoder_DefaultIsMagnitudeTolerant(t *testing.T) {
	// The zero-arg NewEncoder uses the magnitude-tolerant default: it must decode epoch
	// nanos, seconds (zap's default), AND millis to the correct instant — never 1970.
	want := time.Unix(1692959400, 123456789).UTC()
	for _, in := range []string{
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.UnixNano()),  // nanoseconds
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.Unix()),      // seconds (zap default)
		fmt.Sprintf(`{"msg":"x","ts":%d}`, want.UnixMilli()), // milliseconds
	} {
		out, err := NewEncoder().Encode(nil, []byte(in))
		require.NoError(t, err)
		got := time.Time(decodeEntry(t, out).Time)
		require.WithinDuration(t, want, got, time.Second, "input %s", in)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run TestEncoderWithCodec -run TestNewEncoder_Defaults`
Expected: FAIL — `undefined: NewEncoderWithCodec`.

- [ ] **Step 3: Rewrite `fluent/encoder.go`**

```go
package fluent

import (
	"encoding/json"
	"fmt"
	"time"
)

// Encoder is the v1 transcode encoder: it parses zap's JSON log line and emits a msgpack
// [time, record] Entry payload. It implements zapwire.Encoder. The TimeCodec controls which
// field carries the timestamp and how it is parsed.
type Encoder struct {
	codec TimeCodec
}

// NewEncoder returns a transcode Encoder with the default codec (epoch nanoseconds at "ts").
func NewEncoder() Encoder { return Encoder{codec: defaultTimeCodec()} }

// NewEncoderWithCodec returns a transcode Encoder using codec. An invalid (zero) codec
// falls back to the default.
func NewEncoderWithCodec(codec TimeCodec) Encoder {
	if !codec.valid() {
		codec = defaultTimeCodec()
	}

	return Encoder{codec: codec}
}

// Encode parses record (a JSON object) and appends its msgpack Entry payload to dst.
func (e Encoder) Encode(dst, record []byte) ([]byte, error) {
	var rec map[string]any
	if err := json.Unmarshal(record, &rec); err != nil {
		return nil, fmt.Errorf("fluent: unmarshal log record: %w", err)
	}

	entry := Entry{Time: EventTime(e.extractTime(rec)), Record: rec}
	out, err := entry.MarshalMsg(dst)
	if err != nil {
		return nil, fmt.Errorf("fluent: marshal entry: %w", err)
	}

	return out, nil
}

// extractTime lifts the codec's time key out of rec and decodes it. An absent or
// unparseable value falls back to time.Now().
func (e Encoder) extractTime(rec map[string]any) time.Time {
	if v, present := rec[e.codec.Key]; present {
		delete(rec, e.codec.Key)
		if t, ok := e.codec.Decode(v); ok && !t.IsZero() {
			return t
		}
	}

	return time.Now()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./fluent/... -race`
Expected: PASS (new codec tests + the existing encoder tests, which use the default codec).

- [ ] **Step 5: Commit**

```bash
git add fluent/encoder.go fluent/encoder_test.go
git commit -m "feat(fluent): drive Encoder timestamp parsing from TimeCodec"
```

---

### Task 5.3: Presets (`WithTimeCodec`/`WithTimeKey`) + docs

**Files:**
- Modify: `fluent/fluent.go`
- Test: `fluent/fluent_test.go` (add cases)
- Modify: `README.md`

- [ ] **Step 1: Write the failing tests (append to `fluent/fluent_test.go`)**

```go
func TestNewCore_CustomCodecRoundTrips(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	// RFC3339Nano string codec at a custom key "time" — NewCore must wire BOTH ends.
	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		WithTimeCodec(RFC3339NanoCodec("time")))
	require.NoError(t, err)
	defer w.Close()

	zap.New(core).Info("hello")

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
		_, hasTime := entries[0].Record.(map[string]any)["time"]
		require.False(t, hasTime, "custom time key must be lifted out of the record")
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}

func TestWithTimeKey_OverridesActiveCodecKey(t *testing.T) {
	o := options{}
	WithTimeCodec(EpochMillisCodec("a"))(&o)
	WithTimeKey("b")(&o)
	require.Equal(t, "b", o.resolveCodec().Key)
	// Order-independent: key override applies regardless of option order.
	o2 := options{}
	WithTimeKey("b")(&o2)
	WithTimeCodec(EpochMillisCodec("a"))(&o2)
	require.Equal(t, "b", o2.resolveCodec().Key)
}

func TestResolveCodec_DefaultWhenUnset(t *testing.T) {
	o := options{}
	require.Equal(t, "ts", o.resolveCodec().Key)
}

// TestNewWriter_BYOCore_ProductionConfigDecodesToNow is the Finding 3 regression guard: a
// bring-your-own-core caller using zap's DEFAULT production config (epoch float seconds)
// must decode to ~now, not ~1970, thanks to the magnitude-tolerant default codec.
func TestNewWriter_BYOCore_ProductionConfigDecodesToNow(t *testing.T) {
	path := socketPath(t)
	m := startMock(t, path)
	defer m.stop()

	w, err := NewWriter(zapwire.UDS(path))
	require.NoError(t, err)
	defer w.Close()

	// Caller wires their own core with the stock production encoder config (EpochTimeEncoder).
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), w, zap.InfoLevel)
	zap.New(core).Info("hello")

	select {
	case frame := <-m.recv:
		_, entries, _ := decodePackedForward(t, frame)
		require.Len(t, entries, 1)
		require.WithinDuration(t, time.Now(), time.Time(entries[0].Time), 5*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("no frame received")
	}
}
```

> Add `"go.uber.org/zap/zapcore"` to `fluent_test.go`'s imports for the BYO-core test.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./fluent/... -run 'TestNewCore_CustomCodec|TestWithTimeKey|TestResolveCodec'`
Expected: FAIL — `undefined: WithTimeCodec` / `o.resolveCodec`.

- [ ] **Step 3: Rewrite `fluent/fluent.go`**

```go
package fluent

import (
	"go.uber.org/zap/zapcore"

	"github.com/arloliu/zapwire"
)

const defaultTag = "app.logs"

type options struct {
	tag         string
	codec       TimeCodec
	codecSet    bool
	keyOverride string
	wireOpts    []zapwire.Option
}

// resolveCodec returns the effective TimeCodec: the configured codec (or the default), with
// any WithTimeKey override applied. Order-independent.
func (o options) resolveCodec() TimeCodec {
	c := o.codec
	if !o.codecSet || !c.valid() {
		c = defaultTimeCodec()
	}
	if o.keyOverride != "" {
		c.Key = o.keyOverride
	}

	return c
}

// Option configures a fluent writer preset.
type Option func(*options)

// WithTag sets the Fluent Forward tag stamped on every frame (default "app.logs").
func WithTag(tag string) Option { return func(o *options) { o.tag = tag } }

// WithTimeCodec sets the TimeCodec used to read the timestamp out of each log (and, in
// NewCore, to configure zap's matching time encoder). Defaults to EpochNanosCodec("ts").
func WithTimeCodec(c TimeCodec) Option {
	return func(o *options) { o.codec = c; o.codecSet = true }
}

// WithTimeKey overrides just the JSON time key on the active codec, keeping its format.
func WithTimeKey(key string) Option { return func(o *options) { o.keyOverride = key } }

// WithZapwireOptions forwards core zapwire options (mode, buffer, timeouts, …).
func WithZapwireOptions(opts ...zapwire.Option) Option {
	return func(o *options) { o.wireOpts = append(o.wireOpts, opts...) }
}

// NewWriter builds a zapwire.Writer that ships Fluent Forward PackedForward frames over t.
//
// Timestamp contract: the transcode Encoder reads the time field per the configured
// TimeCodec (default EpochNanosCodec("ts")). Callers wiring their own zapcore.Core (rather
// than using NewCore) MUST align the encode end with the same codec — call
// codec.ApplyTo(&encoderConfig) — or zap's default float-seconds time encoding will be
// misread (decoding to ~1970).
func NewWriter(t zapwire.Transport, opts ...Option) (*zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}

	return buildWriter(t, o)
}

// NewCore builds a zapcore.Core (JSON-encoding into the fluent writer) plus the underlying
// writer, which the caller must Close. It wires BOTH ends of the time contract from the
// configured TimeCodec, so the JSON encoder and the transcode decoder always agree.
func NewCore(
	t zapwire.Transport,
	level zapcore.LevelEnabler,
	encCfg zapcore.EncoderConfig,
	opts ...Option,
) (zapcore.Core, *zapwire.Writer, error) {
	o := options{tag: defaultTag}
	for _, opt := range opts {
		opt(&o)
	}
	o.resolveCodec().ApplyTo(&encCfg)

	w, err := buildWriter(t, o)
	if err != nil {
		return nil, nil, err
	}

	return zapwire.NewCore(zapcore.NewJSONEncoder(encCfg), w, level), w, nil
}

func buildWriter(t zapwire.Transport, o options) (*zapwire.Writer, error) {
	if o.tag == "" {
		o.tag = defaultTag
	}

	return zapwire.New(t, NewEncoderWithCodec(o.resolveCodec()), NewFramer(o.tag), o.wireOpts...)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./fluent/... -race`
Expected: PASS (new preset tests + the existing end-to-end test on the default codec).

- [ ] **Step 5: Update README time-handling docs**

In `README.md`, under the fluent usage, add a short subsection:

````markdown
### Timestamps

`fluent.NewCore` wires both ends of the time round-trip from a `TimeCodec`, so the JSON
encoder and the decoder always agree. Choose a built-in or supply your own:

```go
core, w, _ := fluent.NewCore(zapwire.UDS(path), zap.InfoLevel, cfg,
    fluent.WithTimeCodec(fluent.RFC3339NanoCodec("ts")))
```

Built-ins: `EpochNanosCodec` (default), `EpochMillisCodec`, `EpochSecondsCodec`,
`RFC3339NanoCodec`, `RFC3339Codec`, `ISO8601Codec`. Override just the key with
`fluent.WithTimeKey("timestamp")`, or pass a custom `fluent.TimeCodec{Key, ZapEncoder, Decode}`.

If you build your own `zapcore.Core` (via `fluent.NewWriter`) instead of `NewCore`, align the
encode end yourself: `codec.ApplyTo(&encoderConfig)`.
````

- [ ] **Step 6: Full pass, lint, commit**

```bash
go mod tidy
make lint && go test ./... -race
git add fluent/fluent.go fluent/fluent_test.go README.md
git commit -m "feat(fluent): configurable time handling via TimeCodec presets"
```

---

**Phase 05 done when:** `make lint` + `go test ./... -race` pass; a custom codec + custom key
round-trips end-to-end through a real `zap.Logger`; the default (no options) behavior is
unchanged (epoch nanos at `"ts"`); and dependency isolation still holds. Update design §12 if
the API shifted during implementation.
