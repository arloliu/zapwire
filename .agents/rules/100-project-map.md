# 100 - Project Map

- Project: zapwire — high-performance zap WriteSyncer for log processors.
- Module: `github.com/arloliu/zapwire`  |  Go 1.25  |  golangci-lint v2.11.4 (`make lint`).

## Structure
- Root `zapwire`: core — Transport, Encoder/Framer interfaces, Options, Writer
  (conn manager + reconnect + sync/async dispatch), NewCore. Stdlib + zapcore.
- `fluent/`: Fluent Forward (msgpack). Owns `tinylib/msgp`. Encoder (transcode), Framer
  (PackedForward), presets.
- `ndjson/`: newline-delimited JSON. Stdlib + zapcore. Encoder, Framer, presets.

## Dependency policy
See `AGENTS.md`. No msgp/grpc/protobuf in root or `ndjson`. Hybrid module policy: heavy-dep
processors get their own `go.mod` (design §11). Ask before adding any dependency.
