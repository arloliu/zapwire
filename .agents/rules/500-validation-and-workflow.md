# 500 - Validation and Workflow

Apply before validation, commits, or PR work.

## Validation gates

- Run `make lint` after any Go change and fix every issue (see
  [600-go-after-write.md](600-go-after-write.md) for the `go fix` → lint loop).
- Run `make test` (unit tests with `-race`) before calling implementation work
  done.
- Run `make bench` when a change touches the encode / framing / write hot path,
  to confirm allocations and throughput have not regressed
  ([700-performance-security.md](700-performance-security.md)).
- Update godoc and the README when exported API changes
  ([400-docs.md](400-docs.md)).

## Code review checklist

- [ ] Correctness (especially reconnect, drop-counting, and Close ordering).
- [ ] No unnecessary allocations in the encode / write hot path.
- [ ] Test coverage for new code; golden wire bytes AND round-trip decode for
      Encoder/Framer changes ([300-testing.md](300-testing.md)).
- [ ] Docs updated for exported API changes.
- [ ] Dependency policy respected — no format dep leaks into root or `ndjson`
      ([100-project-map.md](100-project-map.md), `AGENTS.md`).

## Git conventions

See [550-git-conventions.md](550-git-conventions.md) for branches, commit
format, body wrapping, the plan/review jargon prohibition, and PR conventions.

## Make targets

```bash
make help               # List all targets
make lint               # golangci-lint (verifies the pinned version first)
make fmt                # golangci-lint fmt (gofmt + goimports)
make vet                # go vet ./...
make test               # Unit tests with the race detector
make bench              # Benchmarks (no race; reports allocations)
make coverage           # Coverage profile
make generate           # go generate (msgpack codegen)
make gomod-tidy         # go mod tidy + verify
make clean-linter-cache # Clear golangci-lint's on-disk cache
make ci                 # Full local gate: lint + vet + test + coverage
```
