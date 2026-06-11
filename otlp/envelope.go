package otlp

// envelope wraps batches of bare LogRecord payloads into an
// ExportLogsServiceRequest. Resource and InstrumentationScope bytes are
// immutable per writer, built once at construction (design §4).
type envelope struct {
	resourceBlob []byte // Resource message bytes (attributes only)
	scopeBlob    []byte // InstrumentationScope message bytes; empty = omit scope
}

func newEnvelope(o options) *envelope {
	// Resource.attributes (field 1) entries are tagged 0x0a — identical to
	// KeyValueList.values — so a kvlist frame builds them directly.
	s := newEncState()
	s.openFrame(frameKVList, "")
	depth := len(s.stack)
	s.AddString("service.name", o.serviceName)
	for i := range o.resourceFields {
		applyField(s, o.resourceFields[i]) // transactional incl. zap.Inline
	}
	s.sealDownTo(depth) // close namespaces a WithResource field may have opened
	res := append([]byte(nil), s.cur().buf...)
	s.free()

	var scope []byte
	if o.scopeName != "" {
		scope = appendTaggedString(scope, 0x0a, o.scopeName)
	}
	if o.scopeVersion != "" {
		scope = appendTaggedString(scope, 0x12, o.scopeVersion)
	}

	return &envelope{resourceBlob: res, scopeBlob: scope}
}

// recordCost is the tagged size of one record inside ScopeLogs.log_records.
func (e *envelope) recordCost(recLen int) int {
	return 1 + uvarintLen(uint64(recLen)) + recLen //nolint:gosec
}

// scopePartLen is the tagged size of the scope field inside ScopeLogs (0 when omitted).
func (e *envelope) scopePartLen() int {
	if len(e.scopeBlob) == 0 {
		return 0
	}

	return 1 + uvarintLen(uint64(len(e.scopeBlob))) + len(e.scopeBlob) //nolint:gosec
}

// sizeFor returns the exact request size for records whose recordCost sum is
// taggedRecords. Used for byte-aware batch cutting and the Write-time
// oversized-record guard (design §5.1/§5.2).
func (e *envelope) sizeFor(taggedRecords int) int {
	scopeLogs := e.scopePartLen() + taggedRecords
	resourceLogs := 1 + uvarintLen(uint64(len(e.resourceBlob))) + len(e.resourceBlob) + //nolint:gosec
		1 + uvarintLen(uint64(scopeLogs)) + scopeLogs //nolint:gosec

	return 1 + uvarintLen(uint64(resourceLogs)) + resourceLogs //nolint:gosec
}

// assemble appends the full ExportLogsServiceRequest to dst.
// dst will be non-nil when called from writer.go for buffer reuse.
func (e *envelope) assemble(dst []byte, records [][]byte) []byte { //nolint:unparam
	tagged := 0
	for _, r := range records {
		tagged += e.recordCost(len(r))
	}
	scopeLogsLen := e.scopePartLen() + tagged
	resourceLogsLen := 1 + uvarintLen(uint64(len(e.resourceBlob))) + len(e.resourceBlob) + //nolint:gosec
		1 + uvarintLen(uint64(scopeLogsLen)) + scopeLogsLen //nolint:gosec

	dst = append(dst, 0x0a)                            // ExportLogsServiceRequest.resource_logs
	dst = appendUvarint(dst, uint64(resourceLogsLen))  //nolint:gosec
	dst = appendTaggedBytes(dst, 0x0a, e.resourceBlob) // ResourceLogs.resource
	dst = append(dst, 0x12)                            // ResourceLogs.scope_logs
	dst = appendUvarint(dst, uint64(scopeLogsLen))     //nolint:gosec
	if len(e.scopeBlob) != 0 {
		dst = appendTaggedBytes(dst, 0x0a, e.scopeBlob) // ScopeLogs.scope
	}
	for _, r := range records {
		dst = appendTaggedBytes(dst, 0x12, r) // ScopeLogs.log_records
	}

	return dst
}
