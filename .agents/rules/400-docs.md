# 400 - Documentation Standards

Apply when editing godoc comments, the README, examples, design docs, or any
exported API.

## Godoc

- Every exported symbol has a doc comment that starts with the symbol name.
- **Exported functions and methods use the structured template below**
  (`Parameters:` / `Returns:` / `Example:` as applicable). Making the contract
  explicit is the house style for this repo — it reads more clearly than a
  prose paragraph for anything with parameters or a non-obvious return.
- Exported types, vars, consts, and trivial getters may use plain prose: a
  symbol-name-first one-liner plus optional detail paragraphs separated by a
  blank `//` line.
- Cite the design doc by section when behavior is specified there, e.g.
  `(design §3.8)`.
- Prefer runnable `ExampleXxx` tests for examples that should render on
  pkg.go.dev; the inline `Example:` block is for a short usage sketch.
- All comment lines follow the line-width convention below.

### Godoc template (exported functions / methods)

```go
// FunctionName one-line summary.
//
// Detailed description: behavior, ownership, concurrency, edge cases
// (optional but recommended for non-trivial APIs).
//
// Parameters:
//   - param1: description and constraints
//   - param2: expected values
//
// Returns:
//   - *Result: what it represents
//   - error: conditions that cause a non-nil error
//
// Example:
//
//	w, err := FunctionName(args)
//	if err != nil { ... }
func FunctionName(param1 T1, param2 T2) (*Result, error) { }
```

Omit sections when they add nothing:

- No parameters → omit `Parameters:`.
- No (or only a trivial) return → omit `Returns:`.
- Trivial getter / one-line behavior → a single prose line is enough; do not
  pad it with empty sections.

### Examples by type

Constructor:

```go
// New creates a Writer. It attempts an immediate connection so logs flow at
// once when the endpoint is already up; otherwise it starts disconnected and
// reconnects in the background.
//
// Parameters:
//   - t: transport to dial (UDS or TCP); must be non-nil
//   - enc: per-entry encoder; must be non-nil
//   - framer: wire framer; must be non-nil
//   - opts: functional options (e.g. WithAsync, WithWriteTimeout)
//
// Returns:
//   - *Writer: ready-to-use writer; the caller owns it and must Close it
//   - error: ErrNoTransport / ErrNoEncoder / ErrNoFramer on a nil input
func New(t Transport, enc Encoder, framer Framer, opts ...Option) (*Writer, error)
```

Method with error:

```go
// Close stops the background goroutines, flushes any buffered async entries,
// and closes the connection. It is safe to call more than once.
//
// Returns:
//   - error: the connection's close error, or nil
func (w *Writer) Close() error
```

## Line width (canonical — referenced by other rules)

These widths govern **doc comments and commit-message bodies**. They are a
human-readability convention, not a linter rule (no `lll` is configured), so:
apply them when writing or editing a comment; do not reflow code lines to fit;
do not reformat existing comments solely to comply.

- **Soft target: 80 columns.** Aim to keep each comment / body line ≤ 80.
- **Hard limit: 120 columns.** Never exceed it.
- **Readability beats the soft target.** Keep a complete sentence on one line
  when it fits within the hard limit (≤ 120) rather than breaking it
  mid-thought just to reach 80. Wrap a longer sentence at a natural boundary.

Why 120 for the hard limit: it matches the golangci-lint `lll` default — the
de-facto Go ecosystem standard — so turning `lll` on later needs no retrofit,
and it sits above the codebase's current widest comment (~100) with headroom.
(100 would be tighter and match the commit-subject cap, but 120 is the
ecosystem norm.) The commit *subject* keeps its own tighter cap in
[550-git-conventions.md](550-git-conventions.md); this width is for comment and
body lines.

## README and design docs

- Keep the README's install/usage in sync with the exported API.
- When exported API changes, update the relevant godoc and any affected
  `docs/design/` or `docs/plans/` references in the same change.
