# 700 - Performance and Security

Apply when editing the encode / framing / write hot path, transport code, or
anything handling log payloads or connection endpoints.

## Performance

zapwire sits on every log call, so the encode → frame → write path is hot.

- **Allocations:** the per-entry path should aim for zero steady-state
  allocations. Reuse buffers via the existing pooling (`scratchPool`,
  `buffer.Pool`); pre-size with `make([]T, 0, n)` when the size is known.
  Confirm with `make bench` (`-benchmem`) before and after — the native
  encoder's value is measured in allocs/op, not just ns/op.
- **Copy discipline:** know who owns a `[]byte`. Async delivery hands payloads
  across a goroutine boundary, so it must own its copy; sync delivery can reuse
  a pooled destination. Do not return a pooled slice to a caller that outlives
  the pool checkout (see `Passthrough` and `Writer`'s sync/async modes).
- **Interfaces:** `Encoder`/`Framer`/`Transport` are interfaces by design;
  keep their per-entry methods small and avoid adding indirection inside the
  inner loop beyond those seams.
- **Don't block the logger:** a stalled consumer must drop (counted) rather
  than block unbounded — sync writes honour the bounded write timeout, async
  enqueue never blocks. Preserve this contract when touching delivery.
- **Profile before optimizing:** use `pprof` / `make bench` to find the real
  bottleneck; don't micro-optimize on a hunch.

## Security

- **Never log or leak secrets.** This is a logging library — error messages,
  reconnect logs, and debug output must not echo payload contents or
  credentials embedded in endpoints.
- **Validate external config:** endpoint paths/addresses, timeouts, and buffer
  sizes come from callers; normalize and bound them (the codebase already
  normalizes config). Fail closed on nil transport/encoder/framer.
- **Transport:** UDS paths should be created with least-privilege permissions;
  for TCP to a remote processor, prefer an encrypted/authenticated channel.
- **Never commit secrets** (test fixtures included).
