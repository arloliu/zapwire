package otlp

// transport is the per-protocol ship layer (grpc design §3). prepare
// runs ONCE per batch (compression + framing — a retrying batch must not
// re-gzip per attempt); attempt runs once per try. Transports never touch
// writer state: outcomes flow back through acceptance / *ExportError and
// the writer applies the shared OTLP semantics.
//
// close releases transport-owned connection resources after the flush loop
// has drained. It must not touch caller-supplied resources.
type transport interface {
	prepare(msg []byte) prepared
	attempt(p prepared) (*acceptance, *ExportError)
	close()
}

// prepared is the wire-ready body. warn carries non-fatal prepare
// diagnostics (gzip failure → shipped uncompressed).
type prepared struct {
	body       []byte
	compressed bool
	warn       *ExportError
}

// acceptance is a server-accepted outcome that still needs accounting:
// partial-success rejections (counted drops) or observability-only events.
// nil acceptance == clean accept.
type acceptance struct {
	rejected int64
	event    *ExportError
}

// resolveAccept decodes an ExportLogsServiceResponse and applies the OTLP
// partial-success classification (§5.3): rejected>0 →
// counted drop; rejected==0 with message → warning; malformed body →
// observability-only (the server accepted the batch). base stamps transport
// identity (StatusCode 200 for HTTP, GRPCStatus 0 for gRPC).
func resolveAccept(respMsg []byte, base ExportError) *acceptance {
	rejected, msg, derr := decodePartialSuccess(respMsg)
	switch {
	case derr != nil:
		e := base
		e.Err = derr

		return &acceptance{event: &e}
	case rejected > 0:
		e := base
		e.Rejected = rejected
		e.Message = msg

		return &acceptance{rejected: rejected, event: &e}
	case msg != "":
		e := base
		e.Warning = true
		e.Message = msg

		return &acceptance{event: &e}
	}

	return nil
}
