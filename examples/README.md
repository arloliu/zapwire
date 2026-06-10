# zapwire examples

Runnable, self-contained examples. Each one stands up a local sink on the
loopback interface, ships logs to it, and prints what arrived — no external log
processor required. For the full reference, see the [User Guide](../docs/guide.md).

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

## A note on layout

These examples live in their own Go module (`examples/go.mod`) so the library's
lint, vet, and coverage stay focused on the package itself. A `replace` directive
builds them against the local source, so they always track the API in this tree.

Build them all at once:

```bash
cd examples && go build ./...
```
