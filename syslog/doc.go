// Package syslog provides a zapwire preset that ships zap logs as RFC5424 syslog
// messages (with a JSON message body) to rsyslog, syslog-ng, Vector, Logstash, and other
// syslog receivers over Unix domain sockets or TCP.
//
// A custom zapcore.Encoder prepends the RFC5424 header — PRI (facility+severity), version,
// RFC3339 timestamp, hostname, app-name, procid, msgid — to a JSON body encoded by an inner
// zap JSON encoder, and ships the result through the core Writer via zapwire.Passthrough and
// a Framer (RFC6587 octet-counting by default, or LF-terminated). The header severity is
// mapped from the zap level (configurable); all header fields are configurable and sanitized
// to RFC5424 limits.
package syslog
