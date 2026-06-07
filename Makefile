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
