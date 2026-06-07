# zapwire Agent Configuration

Authoritative entrypoint for coding agents in this repository. Claude Code imports this
file from `CLAUDE.md`; other agents read `AGENTS.md` directly.

zapwire (`github.com/arloliu/zapwire`) is a high-performance zap `WriteSyncer` that ships
structured logs to log processors (Fluentd, Fluent-bit, Vector, …) over UDS/TCP. Design:
`docs/design/2026-06-07-zapwire-design.md`. Detailed rules live under `.agents/rules/`.

## Detailed Rules

Read `.agents/rules/AGENTS.md` first — it maps task triggers to rule files. Always follow
`.agents/rules/000-agent-contract.md`.

## Dependency policy (load-bearing)

- Root `zapwire` and `ndjson/`: stdlib + `go.uber.org/zap/zapcore` only.
- `fluent/`: may add `github.com/tinylib/msgp`. No other third-party deps.
- **No log-processor-format dependency (msgp/grpc/protobuf) may leak into root or `ndjson`.**
- Heavy-dep processors (future `otlp`) get their own `go.mod` (design §11). Ask before
  adding any new dependency.

## Validation gate (before every commit)

1. `go fix ./<changed-pkg>/...` (touched packages only).
2. `make lint` — fix all issues.
3. `make test` — unit tests with `-race` must pass.
Never add `Co-Authored-By` or any attribution trailer to commits.
