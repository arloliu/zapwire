# Phase 00 — Repo, agent config & tooling

Sets up the professional scaffolding (spec tasks 2 & 3) before any library code: isolated
`golangci-lint`, Makefile, agent contracts, and the codegen/test tool dependencies.
Modeled on `~/projects/parti`.

---

### Task 0.1: Isolated golangci-lint tool module

**Files:**
- Create: `.golangci-lint.go.mod`
- Create: `.golangci.yaml`

- [ ] **Step 1: Create the linter tool module file**

Create `.golangci-lint.go.mod` with just the module/go header — the `tool` and `require`
lines are populated by the next step:

```
module github.com/arloliu/zapwire/tools

go 1.25.0
```

- [ ] **Step 2: Install golangci-lint into the isolated module**

Run:
```bash
go get -tool -modfile=.golangci-lint.go.mod github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
go mod verify -modfile=.golangci-lint.go.mod 2>/dev/null || true
```
Expected: `.golangci-lint.go.mod` now has a `tool` directive and a large `require` block;
a `.golangci-lint.go.sum` is created. This keeps every linter dependency out of the main
module's `go.mod`.

- [ ] **Step 3: Verify it runs**

Run: `go tool -modfile=.golangci-lint.go.mod golangci-lint --version`
Expected: `golangci-lint has version 2.11.4 ...`

- [ ] **Step 4: Create `.golangci.yaml`**

```yaml
# golangci-lint configuration (v2 schema). Strategy: broad opt-in for correctness,
# security and style; narrow only where signal:noise is poor, with a comment per change.
# Pinned to golangci-lint v2.11.4 (see Makefile GOLANGCI_LINT_VERSION).
version: "2"

linters:
  enable:
    # -- Correctness --
    - bodyclose
    - copyloopvar
    - durationcheck
    - errname
    - errorlint
    - exhaustive
    - makezero
    - nilnil
    - reassign
    - wastedassign
    # -- Security --
    - asasalint
    - asciicheck
    - bidichk
    - gosec
    # -- Style / readability --
    - misspell
    - nakedret
    - nlreturn
    - predeclared
    - unconvert
    - unparam
    - usestdlibvars
    - whitespace
    # -- Complexity --
    - cyclop
  settings:
    cyclop:
      max-complexity: 22
    exhaustive:
      default-signifies-exhaustive: true
  exclusions:
    generated: lax
    rules:
      # Generated msgpack code is exempt from style/complexity/security lints.
      - path: proto_gen\.go
        linters: [gosec, cyclop, gocritic, unparam, errcheck, govet, staticcheck]
      # Tests may use fixed-bit conversions and unchecked helpers freely.
      - path: _test\.go
        linters: [gosec, errcheck, cyclop, unparam]

formatters:
  enable:
    - gofumpt
    - goimports
  settings:
    goimports:
      local-prefixes: [github.com/arloliu/zapwire]
```

- [ ] **Step 5: Commit**

```bash
git add .golangci-lint.go.mod .golangci-lint.go.sum .golangci.yaml
git commit -m "chore: add isolated golangci-lint tool module and config"
```

---

### Task 0.2: Makefile

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Create the Makefile**

```makefile
# zapwire Makefile

TEST_TIMEOUT          ?= 5m
LINT_TIMEOUT          ?= 3m
COVERAGE_DIR          := ./.coverage
COVERAGE_OUT          := $(COVERAGE_DIR)/coverage.out
LINTER_GOMOD          := -modfile=.golangci-lint.go.mod
GOLANGCI_LINT_VERSION := 2.11.4

.DEFAULT_GOAL := help
.PHONY: help test test-race bench coverage lint linter-update linter-version clean-linter-cache fmt vet gomod-tidy generate ci

## help: Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## test: Run unit tests with the race detector
test:
	@go test ./... -timeout=$(TEST_TIMEOUT) -race

## test-race: Alias for test
test-race: test

## bench: Run benchmarks (no race; report allocations)
bench:
	@go test ./... -run='^$$' -bench=. -benchmem -timeout=$(TEST_TIMEOUT)

## coverage: Generate a coverage profile
coverage:
	@mkdir -p $(COVERAGE_DIR)
	@go test ./... -coverprofile=$(COVERAGE_OUT) -covermode=atomic -timeout=$(TEST_TIMEOUT)
	@go tool cover -func=$(COVERAGE_OUT) | tail -1

## generate: Run go generate (msgpack codegen)
generate:
	@go generate ./...

## fmt: Format the code
fmt:
	@go tool $(LINTER_GOMOD) golangci-lint fmt

## vet: Run go vet
vet:
	@go vet ./...

## lint: Run linters (verifies the pinned version first)
lint:
	@INSTALLED=$$(go tool $(LINTER_GOMOD) golangci-lint --version 2>/dev/null | grep -oE 'version [^ ]+' | cut -d' ' -f2 || echo none); \
	if [ "$$INSTALLED" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "golangci-lint version mismatch (have $$INSTALLED, want $(GOLANGCI_LINT_VERSION)); run 'make linter-update'"; exit 1; fi
	@go tool $(LINTER_GOMOD) golangci-lint run --timeout=$(LINT_TIMEOUT)

## linter-update: Install/update the pinned golangci-lint
linter-update:
	@go get -tool $(LINTER_GOMOD) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION)
	@go mod verify $(LINTER_GOMOD)

## linter-version: Print the installed linter version
linter-version:
	@go tool $(LINTER_GOMOD) golangci-lint --version

## clean-linter-cache: Clear golangci-lint's on-disk cache
clean-linter-cache:
	@go tool $(LINTER_GOMOD) golangci-lint cache clean

## gomod-tidy: Tidy and verify modules
gomod-tidy:
	@go mod tidy
	@go mod verify

## ci: Full local gate
ci: lint vet test coverage
```

- [ ] **Step 2: Verify lint runs on the empty module**

Run: `make lint`
Expected: PASS (no Go files yet, exits 0) or a clean "no go files" message.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: add Makefile with isolated lint/test/bench targets"
```

---

### Task 0.3: Bootstrap runtime & codegen tool dependencies

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add the msgp code generator as a tool dependency**

Run:
```bash
go get -tool github.com/tinylib/msgp@latest
```
Expected: `go.mod` gains a `tool github.com/tinylib/msgp` directive. (`//go:generate go tool
msgp ...` in Phase 02 will use it — no global install needed.)

- [ ] **Step 2: Pre-add the runtime/test deps so later phases resolve cleanly**

Run:
```bash
go get go.uber.org/zap@latest
go get github.com/stretchr/testify@latest
go mod tidy
```
Expected: `go.mod` requires `go.uber.org/zap` and `github.com/stretchr/testify`. (They show
as indirect until imported; the first phase that imports them flips them to direct on the
next `go mod tidy`.)

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add zap, testify and msgp tool dependencies"
```

---

### Task 0.4: Agent configuration (CLAUDE.md + AGENTS.md)

**Files:**
- Create: `CLAUDE.md`
- Create: `AGENTS.md`

- [ ] **Step 1: Create `CLAUDE.md`**

```markdown
# zapwire — Claude Code Configuration

@AGENTS.md

Claude Code imports shared instructions from `AGENTS.md`. Invoke skills by slash name.
```

- [ ] **Step 2: Create `AGENTS.md`**

```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md AGENTS.md
git commit -m "docs: add agent configuration entrypoints"
```

---

### Task 0.5: Agent rules (`.agents/rules/`)

**Files:**
- Create: `.agents/rules/AGENTS.md`, `000-agent-contract.md`, `100-project-map.md`,
  `200-go-style.md`, `300-testing.md`, `550-git-conventions.md`, `600-go-after-write.md`

- [ ] **Step 1: Create `.agents/rules/AGENTS.md` (trigger map)**

```markdown
# zapwire — Agent Rules Index

Read `000-agent-contract.md` for every task, then the files whose triggers match.

- Always: `000-agent-contract.md`
- Before code changes: `100-project-map.md`, `200-go-style.md`
- Before adding/changing tests: `300-testing.md`
- Before commits: `550-git-conventions.md`
- After modifying Go files: `600-go-after-write.md`
```

- [ ] **Step 2: Create `000-agent-contract.md`**

```markdown
# 000 - Agent Contract

Always apply. The operating contract for every task here.

- State assumptions; if uncertain, ask rather than guess. Do not guess when source, tests,
  benchmarks, docs, or grep can answer.
- Make the minimum change that solves the problem. No speculative features or drive-by
  refactors. Touch only what you must.
- If two patterns conflict, pick one explicitly and explain why. Do not blend them.
- Tests encode WHY behavior matters; a test that can't fail when logic changes is wrong.
- Fail loud: define success criteria and loop until verified. Never claim "done" or "tests
  pass" if anything was skipped.
```

- [ ] **Step 3: Create `100-project-map.md`**

```markdown
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
```

- [ ] **Step 4: Create `200-go-style.md`**

```markdown
# 200 - Go Style

- Idioms: Effective Go. `any` over `interface{}`. Use `slices`/`maps` stdlib.
- Errors: `errors.New` for static; `fmt.Errorf("ctx: %w", err)` to wrap; `errors.Is/As` to
  check. Sentinels `Err*`; error types `*Error`. Errors are the last return value.
- Type assert with comma-ok. Interface assertions: `var _ Iface = (*T)(nil)` after the type
  (or in `_test.go` for public packages that would otherwise import-cycle).
- File layout: package → imports → consts → vars → types → constructors → exported funcs →
  unexported funcs → exported methods → unexported methods.
- Functions ≤ 100 lines (prefer < 50), complexity ≤ 22. Short, consistent receivers.
```

- [ ] **Step 5: Create `300-testing.md`**

```markdown
# 300 - Testing

- Table-driven tests with `testify` (`require` for fatal, `assert` for soft).
- Run with `-race`. Network tests use ephemeral UDS paths under `os.TempDir()` and a real
  `net.Listen` mock server; clean up sockets in `defer`/cleanup.
- For goroutine-leak / reconnect assertions, poll from the test goroutine (not inside
  `require.Eventually`'s spawned goroutine) when counting `runtime.NumGoroutine()`.
- Encoder/Framer: assert golden wire bytes AND round-trip decode.
```

- [ ] **Step 6: Create `550-git-conventions.md`**

```markdown
# 550 - Git Conventions

- Branches: `feat/`, `fix/`, `docs/`, `chore/`, `test/`, `refactor/`, `perf/`.
- Conventional Commits, present tense; first line ≤ 50 chars (hard cap 100). Body explains
  WHY + the high-level WHAT.
- No plan/review jargon in messages (no `Phase 02`, `Task 1.4`, review-round IDs). Citing a
  spec file path is fine.
- **Never** add `Co-Authored-By` or any attribution trailer.
```

- [ ] **Step 7: Create `600-go-after-write.md`**

```markdown
# 600 - Go After Write

After modifying any `.go` file:
1. `go fix ./<pkg>/...` (touched packages only; never repo-wide in a feature commit).
2. `make lint` — fix all issues.
3. `make test` — re-run until green.

If lint output looks stale: `make clean-linter-cache && make lint`. Don't keep a `//nolint`
that a cold-cache run no longer needs.
```

- [ ] **Step 8: Commit**

```bash
git add .agents/
git commit -m "docs: add agent rules (contract, project map, style, testing, git)"
```

---

**Phase 00 done when:** `make lint` and `make linter-version` succeed, the agent config and
rules exist, and `go.mod` has the zap/testify/msgp dependencies. Proceed to `01-core.md`.
