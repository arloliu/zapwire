# 400 - Documentation Standards

Apply when editing godoc comments, the README, examples, design docs, or any
exported API.

## Godoc

- Every exported symbol has a doc comment.
- Start the comment with the symbol name and a one-line summary, then add
  prose detail as needed: `// Writer ships logs to a processor ...`.
- Default to idiomatic Go prose paragraphs (this is what the codebase uses).
  Separate paragraphs with a blank `//` line. Match detail to complexity:
  simple symbols get one line; complex APIs explain constraints, error
  conditions, ownership, and edge cases.
- Cite the design doc by section when behavior is specified there, e.g.
  `(design §3.8)` — matches existing comments in `fluent/`.
- Prefer runnable `ExampleXxx` tests over prose code blocks for examples that
  should render on pkg.go.dev.

Idiomatic prose (the default, mirrors `writer.go` / `fluent/native.go`):

```go
// New creates a Writer. It attempts an immediate connection so logs flow at
// once when the endpoint is already up; otherwise it starts disconnected and
// reconnects in the background. An error is returned only for nil
// transport/encoder/framer.
func New(t Transport, enc Encoder, framer Framer, opts ...Option) (*Writer, error)
```

A structured `Parameters:/Returns:` block is allowed ONLY when it genuinely
clarifies a complex exported API and prose would be harder to follow. Do not
convert existing prose comments to this shape, and do not use it for simple
constructors or getters.

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
