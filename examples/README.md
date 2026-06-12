# zapwire examples

Runnable, self-contained examples. Most stand up a local sink on the loopback
interface, ship logs to it, and print what arrived. The OTLP example instead
ships to an OTLP endpoint and reports delivery health — it is safe to run
without a collector (drops are expected and counted). For the full reference,
see the [User Guide](../docs/guide.md).

Run any example from this directory:

```bash
go run ./ndjson-tcp
```

| Example | Shows |
|---|---|
| [`ndjson-tcp`](ndjson-tcp) | NDJSON over TCP, default **sync** mode, end-to-end delivery |
| [`fluent-native-uds`](fluent-native-uds) | Fluent Forward **native** msgpack over a Unix socket, with frame decoding on the sink |
| [`async-observability`](async-observability) | **Async** mode + tuning, and the `DroppedLogs` / `ReconnectCount` / `IsConnected` health counters |
| [`tee-console`](tee-console) | Fan-out with `zapcore.NewTee`: console + wire, with a different level per core |
| [`otlp-trace-correlation`](otlp-trace-correlation) | OTLP/HTTP protobuf export with trace correlation; the three correlation forms and the `infoCtx` app-layer boundary; graceful shutdown and `DroppedLogs` |
| [`otlp-tee-cost-control`](otlp-tee-cost-control) | Cost-control tee pattern: console core at Info + OTLP core gated to Warn+ via `zap.AtomicLevel`; runtime dial; expected drops reported |

## A note on layout

These examples live in their own Go module (`examples/go.mod`) so the library's
lint, vet, and coverage stay focused on the package itself. A `replace` directive
builds them against the local source, so they always track the API in this tree.

Build them all at once:

```bash
cd examples && go build ./...
```
