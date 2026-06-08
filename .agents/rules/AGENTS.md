# zapwire — Agent Rules Index

Read `000-agent-contract.md` for every task, then the files whose triggers
match. For broad or ambiguous tasks, read all rule files before editing.

## Default load

- Most Go tasks: `000`, `100`, `200`, `500`, `600`.
- Add `300` when adding or changing tests.
- Add `400` when editing godoc, README, examples, design docs, or exported API.
- Add `550` when crafting commits, branches, or PR titles/bodies.
- Add `700` only for the encode/write hot path, transport, or payload/endpoint
  handling.
- Add `800` for non-trivial design, plan, or review-loop work; add `850` when
  sequencing a review loop.

## Rules

- Always: `000-agent-contract.md` — do not guess, control scope, verify, fail
  loud.
- Before code changes: `100-project-map.md` (identity, structure, dependency
  policy), `200-go-style.md` (idioms, errors, layout, naming).
- Before tests: `300-testing.md`.
- Before docs / exported API: `400-docs.md` (godoc, README, **canonical
  line-width convention**).
- Before validation / commit / PR: `500-validation-and-workflow.md` (gates,
  checklist, `make` targets).
- Before commits / PRs: `550-git-conventions.md` (Conventional Commits, body
  wrapping, jargon prohibition, attribution).
- After modifying Go files: `600-go-after-write.md` (`go fix` → lint loop).
- Hot path / transport / payloads: `700-performance-security.md`.
- Design / review loops: `800-design-and-review-loops.md` (think within a
  round), `850-review-loop-workflow.md` (sequence the rounds).
