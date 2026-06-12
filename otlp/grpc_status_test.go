package otlp

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryableGRPCStatus(t *testing.T) {
	// OTLP spec table (design §6.3).
	for _, code := range []int{grpcCancelled, grpcDeadlineExceeded, grpcAborted, grpcOutOfRange, grpcUnavailable, grpcDataLoss} {
		require.True(t, retryableGRPCStatus(code, false), "code %d", code)
	}
	require.True(t, retryableGRPCStatus(grpcResourceExhausted, true), "RESOURCE_EXHAUSTED with RetryInfo")
	require.False(t, retryableGRPCStatus(grpcResourceExhausted, false), "RESOURCE_EXHAUSTED without RetryInfo")
	for _, code := range []int{
		grpcOK, grpcUnknown, grpcInvalidArgument, grpcNotFound, grpcAlreadyExists,
		grpcPermissionDenied, grpcFailedPrecondition, grpcUnimplemented, grpcInternal, grpcUnauthenticated,
	} {
		require.False(t, retryableGRPCStatus(code, false), "code %d", code)
	}
}

func TestHTTPStatusToGRPCStatus(t *testing.T) {
	cases := map[int]int{
		400: grpcInternal, 401: grpcUnauthenticated, 403: grpcPermissionDenied,
		404: grpcUnimplemented, 429: grpcUnavailable, 502: grpcUnavailable,
		503: grpcUnavailable, 504: grpcUnavailable, 418: grpcUnknown,
	}
	for in, want := range cases {
		require.Equal(t, want, httpStatusToGRPCStatus(in), "http %d", in)
	}
}

func TestPercentDecode(t *testing.T) {
	require.Equal(t, "plain", percentDecode("plain"))
	require.Equal(t, "a b", percentDecode("a%20b"))
	require.Equal(t, "ümlaut", percentDecode("%C3%BCmlaut"))
	// Invalid sequences pass through literally (gRPC spec tolerance).
	require.Equal(t, "100%", percentDecode("100%"))
	require.Equal(t, "%zz", percentDecode("%zz"))
}

func TestDecodeBinHeader(t *testing.T) {
	raw := []byte{0x08, 0x0e}
	for name, enc := range map[string]string{
		"unpadded": base64.RawStdEncoding.EncodeToString(raw),
		"padded":   base64.StdEncoding.EncodeToString(raw),
	} {
		got, err := decodeBinHeader(enc)
		require.NoError(t, err, name)
		require.Equal(t, raw, got, name)
	}
	_, err := decodeBinHeader("!!not-base64!!")
	require.Error(t, err)
}

// statusBin builds a serialized google.rpc.Status carrying a RetryInfo detail.
// Field numbers: Status{code=1,message=2,details=3}; Any{type_url=1,value=2};
// RetryInfo{retry_delay=1}; Duration{seconds=1,nanos=2}.
func statusBin(code int64, msg string, delay time.Duration, withRetry bool) []byte {
	st := appendTaggedVarint(nil, 0x08, code)
	st = appendTaggedString(st, 0x12, msg)
	if withRetry {
		var dur []byte
		if s := int64(delay / time.Second); s != 0 {
			dur = appendTaggedVarint(dur, 0x08, s)
		}
		if n := int64(delay % time.Second); n != 0 {
			dur = appendTaggedVarint(dur, 0x10, n)
		}
		ri := appendTaggedBytes(nil, 0x0a, dur)
		anyMsg := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.RetryInfo")
		anyMsg = appendTaggedBytes(anyMsg, 0x12, ri)
		st = appendTaggedBytes(st, 0x1a, anyMsg) // field 3, wt 2 = Status.details (repeated Any)
	}

	return st
}

func TestRetryDelayFromStatus(t *testing.T) {
	d, ok := retryDelayFromStatus(statusBin(14, "throttled", 7*time.Second+500*time.Millisecond, true))
	require.True(t, ok)
	require.Equal(t, 7*time.Second+500*time.Millisecond, d)

	_, ok = retryDelayFromStatus(statusBin(14, "no details", 0, false))
	require.False(t, ok)

	// A foreign detail BEFORE RetryInfo must be skipped, not aborted: build
	// Status{code=14, details=[DebugInfo, RetryInfo(3s)]} by hand.
	foreignAny := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.DebugInfo")
	foreignAny = appendTaggedBytes(foreignAny, 0x12, []byte("x"))
	dur := appendTaggedVarint(nil, 0x08, 3) // Duration{seconds: 3}
	ri := appendTaggedBytes(nil, 0x0a, dur)
	retryAny := appendTaggedString(nil, 0x0a, "type.googleapis.com/google.rpc.RetryInfo")
	retryAny = appendTaggedBytes(retryAny, 0x12, ri)
	st := appendTaggedVarint(nil, 0x08, 14)
	st = appendTaggedBytes(st, 0x1a, foreignAny)
	st = appendTaggedBytes(st, 0x1a, retryAny)
	d2, ok2 := retryDelayFromStatus(st)
	require.True(t, ok2)
	require.Equal(t, 3*time.Second, d2)

	// Malformed bytes → (0, false), never panic.
	_, ok = retryDelayFromStatus([]byte{0xff, 0xff})
	require.False(t, ok)
}

func TestGRPCTimeoutValue(t *testing.T) {
	require.Equal(t, "", grpcTimeoutValue(0))
	require.Equal(t, "10000m", grpcTimeoutValue(10*time.Second))
	require.Equal(t, "1m", grpcTimeoutValue(500*time.Microsecond)) // sub-ms clamps up to 1m
	// Beyond 8 digits of ms (~27.7h) falls back to seconds.
	require.Equal(t, "108000S", grpcTimeoutValue(30*time.Hour))
}
