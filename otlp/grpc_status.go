package otlp

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// gRPC status codes (the subset the OTLP spec classifies; google.golang.org/
// grpc/codes values, hand-pinned — no dependency).
const (
	grpcOK                 = 0
	grpcCancelled          = 1
	grpcUnknown            = 2
	grpcInvalidArgument    = 3
	grpcDeadlineExceeded   = 4
	grpcNotFound           = 5
	grpcAlreadyExists      = 6
	grpcPermissionDenied   = 7
	grpcResourceExhausted  = 8
	grpcFailedPrecondition = 9
	grpcAborted            = 10
	grpcOutOfRange         = 11
	grpcUnimplemented      = 12
	grpcInternal           = 13
	grpcUnavailable        = 14
	grpcDataLoss           = 15
	grpcUnauthenticated    = 16
)

// retryableGRPCStatus implements the OTLP/gRPC retryable class (design §6.3):
// RESOURCE_EXHAUSTED is retryable ONLY when the server attached RetryInfo.
func retryableGRPCStatus(code int, hasRetryDelay bool) bool {
	switch code {
	case grpcCancelled, grpcDeadlineExceeded, grpcAborted, grpcOutOfRange,
		grpcUnavailable, grpcDataLoss:
		return true
	case grpcResourceExhausted:
		return hasRetryDelay
	}

	return false
}

// httpStatusToGRPCStatus is the canonical gRPC HTTP→status mapping, used only
// when a response carries no grpc-status (non-gRPC intermediary, §6.3 case 3).
func httpStatusToGRPCStatus(code int) int {
	switch code {
	case http.StatusBadRequest:
		return grpcInternal
	case http.StatusUnauthorized:
		return grpcUnauthenticated
	case http.StatusForbidden:
		return grpcPermissionDenied
	case http.StatusNotFound:
		return grpcUnimplemented
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return grpcUnavailable
	}

	return grpcUnknown
}

// percentDecode reverses gRPC's grpc-message percent-encoding (RFC 3986 %XX);
// invalid sequences pass through literally per the gRPC spec's tolerance rule.
func percentDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if hi, ok1 := unhex(s[i+1]); ok1 {
				if lo, ok2 := unhex(s[i+2]); ok2 {
					b.WriteByte(hi<<4 | lo)
					i += 2

					continue
				}
			}
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}

	return 0, false
}

// decodeBinHeader decodes gRPC -bin metadata: canonical emission is unpadded
// base64, but the spec requires accepting both (design §6.3).
func decodeBinHeader(s string) ([]byte, error) {
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return b, nil
	}

	return base64.StdEncoding.DecodeString(s)
}

const retryInfoTypeURL = "type.googleapis.com/google.rpc.RetryInfo"

// retryDelayFromStatus extracts RetryInfo.retry_delay from a serialized
// google.rpc.Status (grpc-status-details-bin payload). Malformed input
// degrades to (0, false) — plain backoff, never an error (design §6.3).
func retryDelayFromStatus(raw []byte) (time.Duration, bool) {
	var delay time.Duration
	found := false
	_ = forEachLenField(raw, 3, func(anyMsg []byte) bool { // Status.details (repeated Any)
		tu, err := findField(anyMsg, 1) // Any.type_url
		if err != nil || string(tu) != retryInfoTypeURL {
			return true // keep scanning
		}
		val, err := findField(anyMsg, 2) // Any.value
		if err != nil || val == nil {
			return true
		}
		dur, err := findField(val, 1) // RetryInfo.retry_delay
		if err != nil || dur == nil {
			return true
		}
		secs, err := findVarint(dur, 1)
		if err != nil {
			return true
		}
		nanos, err := findVarint(dur, 2)
		if err != nil {
			return true
		}
		if d := time.Duration(secs)*time.Second + time.Duration(nanos); d > 0 { //nolint:gosec
			delay, found = d, true
		}

		return false // RetryInfo found — stop
	})

	return delay, found
}

// grpcTimeoutValue renders WithTimeout as a grpc-timeout header value:
// milliseconds up to the spec's 8-digit cap, then seconds (design §6.2).
func grpcTimeoutValue(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	ms := max(d.Milliseconds(), 1)
	const maxDigits = 99999999
	if ms <= maxDigits {
		return strconv.FormatInt(ms, 10) + "m"
	}
	s := min(int64(d/time.Second), maxDigits)

	return strconv.FormatInt(s, 10) + "S"
}
