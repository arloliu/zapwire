package otlp

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// rootBytes seals all frames and returns the root attribute bytes.
func rootBytes(t *testing.T, s *encState) []byte {
	t.Helper()
	s.sealAll()

	return append([]byte(nil), s.stack[0].buf...)
}

func TestAddStringGolden(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("k", "v")
	// KeyValue{key:"k", value:AnyValue{string_value:"v"}}
	// kv body: 0x0a 0x01 'k'  0x12 0x03 (0x0a 0x01 'v')  → len 8
	want := []byte{0x32, 0x08, 0x0a, 0x01, 'k', 0x12, 0x03, 0x0a, 0x01, 'v'}
	require.Equal(t, want, rootBytes(t, s))
}

func TestScalarShapes(t *testing.T) {
	cases := []struct {
		name string
		add  func(s *encState)
		av   []byte // expected AnyValue bytes
	}{
		{
			name: "bool_true",
			add:  func(s *encState) { s.AddBool("k", true) },
			av:   []byte{0x10, 0x01},
		},
		{
			name: "bool_false",
			add:  func(s *encState) { s.AddBool("k", false) },
			av:   []byte{0x10, 0x00},
		},
		{
			name: "int",
			add:  func(s *encState) { s.AddInt64("k", 3) },
			av:   []byte{0x18, 0x03},
		},
		{
			name: "int_negative",
			add:  func(s *encState) { s.AddInt64("k", -1) },
			av:   append([]byte{0x18}, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
		},
		{
			name: "double",
			add:  func(s *encState) { s.AddFloat64("k", 1.0) },
			av:   []byte{0x21, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f},
		},
		{
			name: "binary",
			add:  func(s *encState) { s.AddBinary("k", []byte{0xde}) },
			av:   []byte{0x3a, 0x01, 0xde},
		},
		{
			name: "bytestring_is_string",
			add:  func(s *encState) { s.AddByteString("k", []byte("txt")) },
			av:   []byte{0x0a, 0x03, 't', 'x', 't'},
		},
		{
			name: "duration_nanos",
			add:  func(s *encState) { s.AddDuration("k", 2*time.Nanosecond) },
			av:   []byte{0x18, 0x02},
		},
		{
			name: "time_unixnanos",
			add:  func(s *encState) { s.AddTime("k", time.Unix(0, 5)) },
			av:   []byte{0x18, 0x05},
		},
		{
			name: "uint64_overflow_string",
			add:  func(s *encState) { s.AddUint64("k", 1<<63+1) },
			av:   append([]byte{0x0a, 0x13}, []byte("9223372036854775809")...),
		},
		{
			name: "complex",
			add:  func(s *encState) { s.AddComplex128("k", complex(1, 2)) },
			av:   append([]byte{0x0a, 0x04}, []byte("1+2i")...),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newEncState()
			defer s.free()
			c.add(s)
			kvLen := 3 + 2 + len(c.av) // key part (0x0a 0x01 'k') + value header (0x12 len)
			want := append([]byte{0x32, byte(kvLen), 0x0a, 0x01, 'k', 0x12, byte(len(c.av))}, c.av...)
			require.Equal(t, want, rootBytes(t, s))
		})
	}
}

func TestNamespaceNesting(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("a", "1")
	s.OpenNamespace("ns")
	s.AddString("b", "2")
	got := rootBytes(t, s)

	// inner kv: KeyValue{"b", "2"} tagged for KeyValueList → 0x0a 0x08 ...
	innerKV := []byte{0x0a, 0x08, 0x0a, 0x01, 'b', 0x12, 0x03, 0x0a, 0x01, '2'}
	// ns AnyValue: kvlist_value → 0x32 0x0a innerKV
	nsAV := append([]byte{0x32, byte(len(innerKV))}, innerKV...)
	// ns KeyValue at root: 0x32 len 0x0a 0x02 'n' 's' 0x12 len(nsAV) nsAV
	nsKV := append([]byte{0x0a, 0x02, 'n', 's', 0x12, byte(len(nsAV))}, nsAV...)
	first := []byte{0x32, 0x08, 0x0a, 0x01, 'a', 0x12, 0x03, 0x0a, 0x01, '1'}
	want := append(first, append([]byte{0x32, byte(len(nsKV))}, nsKV...)...)
	require.Equal(t, want, got)
}

type okObj struct{}

func (okObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("x", "y")

	return nil
}

type failObj struct{ partial bool }

func (f failObj) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if f.partial {
		e.AddString("written", "before-failing")
		e.OpenNamespace("opened")
	}

	return errors.New("marshal failed")
}

type intArr []int64

func (a intArr) MarshalLogArray(e zapcore.ArrayEncoder) error {
	for _, v := range a {
		e.AppendInt64(v)
	}

	return nil
}

func TestAddObjectAndArray(t *testing.T) {
	s := newEncState()
	defer s.free()
	require.NoError(t, s.AddObject("o", okObj{}))
	require.NoError(t, s.AddArray("arr", intArr{1, 2}))
	got := rootBytes(t, s)

	// o → kvlist with KeyValue{"x","y"}
	xy := []byte{0x0a, 0x08, 0x0a, 0x01, 'x', 0x12, 0x03, 0x0a, 0x01, 'y'}
	oAV := append([]byte{0x32, byte(len(xy))}, xy...)
	oKV := append([]byte{0x0a, 0x01, 'o', 0x12, byte(len(oAV))}, oAV...)
	want := append([]byte{0x32, byte(len(oKV))}, oKV...)
	// arr → ArrayValue{AnyValue{1}, AnyValue{2}} → elements 0x0a 0x02 0x18 v
	elems := []byte{0x0a, 0x02, 0x18, 0x01, 0x0a, 0x02, 0x18, 0x02}
	aAV := append([]byte{0x2a, byte(len(elems))}, elems...)
	aKV := append([]byte{0x0a, 0x03, 'a', 'r', 'r', 0x12, byte(len(aAV))}, aAV...)
	want = append(want, append([]byte{0x32, byte(len(aKV))}, aKV...)...)
	require.Equal(t, want, got)
}

func TestRollbackNoPartialBytes(t *testing.T) {
	// Failing object marshaler that wrote an attr and opened a namespace
	// before erroring: rollback must leave the state byte-identical to never
	// having added the field (design §3.3 / pass-2 P0).
	clean := newEncState()
	defer clean.free()
	clean.AddString("a", "1")
	want := rootBytes(t, clean)

	dirty := newEncState()
	defer dirty.free()
	dirty.AddString("a", "1")
	require.Error(t, dirty.AddObject("bad", failObj{partial: true}))
	require.Equal(t, want, rootBytes(t, dirty))
}

func TestAddReflectedJSONAndSink(t *testing.T) {
	s := newEncState()
	defer s.free()
	// JSON fallback (loose shape assertion; the conformance module decodes
	// reflected values through the official stubs).
	require.NoError(t, s.AddReflected("r", map[string]int{"n": 1}))
	got := rootBytes(t, s)
	require.Contains(t, string(got), `{"n":1}`)

	// Unmarshalable value → error, nothing written (transactional).
	s2 := newEncState()
	defer s2.free()
	require.Error(t, s2.AddReflected("bad", make(chan int)))
	require.Empty(t, rootBytes(t, s2))
}

func TestSnapshotRollbackDiscardsFrames(t *testing.T) {
	s := newEncState()
	defer s.free()
	s.AddString("keep", "1")
	sn := s.snap()
	s.OpenNamespace("n1")
	s.OpenNamespace("n2")
	s.AddString("drop", "2")
	s.rollback(sn)
	require.Len(t, s.stack, 1)

	clean := newEncState()
	defer clean.free()
	clean.AddString("keep", "1")
	require.Equal(t, rootBytes(t, clean), rootBytes(t, s))
}
