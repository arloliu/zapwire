# 550 - Git Conventions

Apply when crafting commits, branches, or pull-request titles and bodies.

## Branches

- Prefixes: `feat/`, `fix/`, `docs/`, `chore/`, `test/`, `refactor/`, `perf/`.

## Commit messages

### Subject (first line)

- [Conventional Commits](https://www.conventionalcommits.org/): a type prefix
  is required (`feat`, `fix`, `docs`, `chore`, `test`, `refactor`, `perf`, …);
  an optional scope goes in parentheses, e.g. `feat(fluent): ...`.
- Present tense, imperative mood.
- ≤ 50 characters when possible; hard cap 100.

### Body — wrapping

The body follows the line-width convention in
[400-docs.md](400-docs.md#line-width-canonical--referenced-by-other-rules):
**80 soft, 120 hard**; keep a complete sentence on one line when it fits within
the hard limit rather than breaking it just to reach 80. This is the rule the
recent over-long bodies violated.

Note: `git log` / `git blame` indent the body by 4 spaces when displaying it,
so a line near the 80 soft target stays clean in an 80-column terminal — a
practical reason to favour the soft target for bodies.

### Body — content

The body explains WHY the change is needed and WHAT its purpose is, then
summarises the main changes at a high level. Aim for a few short paragraphs (or
tight bullets) readable in under a minute. Bias toward the reader who finds the
commit via `git log` / `git blame` months later.

Skip detail that belongs in the code, the PR, or the spec:

- Per-function or per-file diffs (the code already shows them).
- Line-by-line walk-throughs and exhaustive test enumerations.
- Review-iteration counts or how many rounds shaped the design.

### No plan / review jargon

Future readers of `git log` have no access to in-progress plan or review
documents. Do NOT reference:

- Sequencing / work-item labels: `PR-1`, `Phase 02`, `Task 1.4`, `W15`, …
- Review-iteration jargon: review-round IDs, `plan-review pass 3`,
  `post-impl v3.1`, `Codex xhigh`, references to `tmp/*_review.md`.

Citing a spec FILE PATH is fine (e.g. `docs/design/...md`) — the path is
discoverable; the section IDs and round numbers inside it are not.

Bad: `docs: add design spec (v2). Reached plan-review consensus (pass 3).`
Good: `docs: add native msgpack encoder design spec (v2)` + a body describing
what the design does.

### Attribution

**Never** add `Co-Authored-By` or any other attribution trailer.

## Pull requests

- Title follows the same Conventional Commits format as the subject line.
- Body restates the WHY and purpose for reviewers. Linking the spec and prior
  review history is acceptable here when it is useful context, but lead with
  domain language so a reviewer who hasn't read the plan still understands the
  change.
