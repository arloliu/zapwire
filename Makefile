# zapwire Makefile

TEST_TIMEOUT          ?= 5m
LINT_TIMEOUT          ?= 3m
COVERAGE_DIR          := ./.coverage
COVERAGE_OUT          := $(COVERAGE_DIR)/coverage.out
LINTER_GOMOD          := -modfile=.golangci-lint.go.mod
GOLANGCI_LINT_VERSION := 2.11.4
MODULES               := . ./otlp

.DEFAULT_GOAL := help
.PHONY: help test test-race integration bench coverage lint linter-update linter-version clean-linter-cache fmt vet gomod-tidy generate examples ci

## help: Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## test: Run unit tests with the race detector for all modules
test:
	@for m in $(MODULES); do (cd $$m && go test -race ./... -timeout=$(TEST_TIMEOUT)) || exit 1; done

## test-race: Alias for test
test-race: test

## integration: Run opt-in integration tests against a real Fluent Bit (set ZAPWIRE_FLUENT_BIT_BIN=/path/to/fluent-bit)
integration:
	@if [ -z "$(ZAPWIRE_FLUENT_BIT_BIN)" ]; then \
		echo "set ZAPWIRE_FLUENT_BIT_BIN to a fluent-bit binary, e.g. ZAPWIRE_FLUENT_BIT_BIN=/opt/fluent-bit/bin/fluent-bit make integration"; \
		exit 1; fi
	@go test ./fluent -tags fluentbit -run Integration -race -count=1 -v -timeout=$(TEST_TIMEOUT)

## bench: Run benchmarks (no race; report allocations)
bench:
	@go test ./... -run='^$$' -bench=. -benchmem -timeout=$(TEST_TIMEOUT)

# TODO(otlp Task 12): loop over $(MODULES) and merge profiles
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
	@GOWORK=off go tool $(LINTER_GOMOD) golangci-lint fmt

## vet: Run go vet
vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

## examples: Build and vet the runnable examples (separate module under examples/)
examples:
	@cd examples && GOWORK=off go build ./... && GOWORK=off go vet ./...

## lint: Run linters for all modules (verifies the pinned version first)
lint:
	@INSTALLED=$$(GOWORK=off go tool $(LINTER_GOMOD) golangci-lint --version 2>/dev/null | grep -oE 'version [^ ]+' | cut -d' ' -f2 || echo none); \
	if [ "$$INSTALLED" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "golangci-lint version mismatch (have $$INSTALLED, want $(GOLANGCI_LINT_VERSION)); run 'make linter-update'"; exit 1; fi
	@for m in $(MODULES); do (cd $$m && GOWORK=off go tool -modfile=$(CURDIR)/.golangci-lint.go.mod golangci-lint run --timeout=$(LINT_TIMEOUT)) || exit 1; done

## linter-update: Install/update the pinned golangci-lint
linter-update:
	@GOWORK=off go get -tool $(LINTER_GOMOD) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION)
	@GOWORK=off go mod verify $(LINTER_GOMOD)

## linter-version: Print the installed linter version
linter-version:
	@GOWORK=off go tool $(LINTER_GOMOD) golangci-lint --version

## clean-linter-cache: Clear golangci-lint's on-disk cache
clean-linter-cache:
	@GOWORK=off go tool $(LINTER_GOMOD) golangci-lint cache clean

## gomod-tidy: Tidy and verify modules
gomod-tidy:
	@go mod tidy
	@go mod verify

## ci: Full local gate
ci: lint vet test coverage examples
