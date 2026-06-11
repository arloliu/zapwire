package syslog

import "strconv"

// Framing selects the RFC 6587 stream-framing method for syslog over a byte stream.
type Framing int

const (
	// OctetCounting prefixes each message with its byte length: "MSG-LEN SP SYSLOG-MSG"
	// (RFC 6587 §3.4.1). The default — unambiguous and robust to any payload byte.
	OctetCounting Framing = iota
	// LFTerminated terminates each message with a single '\n' (RFC 6587 §3.4.2,
	// non-transparent framing). Simpler, but safe only when the message contains no LF.
	LFTerminated
)

// Framer wraps each per-entry SYSLOG-MSG payload for an octet stream. It implements
// zapwire.Framer.
type Framer struct{ octetCounting bool }

// NewFramer returns a Framer using f (the zero Framing value, OctetCounting, is the default).
//
// Parameters:
//   - f: the framing method (OctetCounting or LFTerminated)
//
// Returns:
//   - Framer: a framer for the chosen method
func NewFramer(f Framing) Framer { return Framer{octetCounting: f == OctetCounting} }

// Frame appends each payload to dst, framed per the configured method. MSG-LEN in
// octet-counting mode is the byte length of the payload. It implements zapwire.Framer.
//
// Parameters:
//   - dst: buffer to append to; may be nil or a pooled slice to reuse
//   - payloads: per-entry SYSLOG-MSG payloads (one in sync mode, N when batched)
//
// Returns:
//   - []byte: dst extended with the framed payloads
//   - error: always nil (kept for the zapwire.Framer contract)
func (f Framer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		if f.octetCounting {
			dst = strconv.AppendInt(dst, int64(len(p)), 10)
			dst = append(dst, ' ')
			dst = append(dst, p...)
		} else {
			dst = append(dst, p...)
			dst = append(dst, '\n')
		}
	}

	return dst, nil
}
