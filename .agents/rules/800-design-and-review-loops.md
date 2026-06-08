# 800 - Design Discipline and Review Loops

Apply when drafting a non-trivial fix or design, revising a plan after review,
or dispatching a reviewer (`/plan-review`, `/final-plan-review`,
`/post-impl-review`, or `codex:codex-rescue`).

This captures reusable discipline from prior multi-round review loops. Treat
the concrete examples as reminders of failure modes, not as claims about the
current code.

## Quick checklist

- [ ] Invariant stated explicitly in one sentence.
- [ ] Every "X is the only caller / path" claim verified by grep.
- [ ] Every code path that observes or mutates the relevant state enumerated.
- [ ] Atomicity primitive named (`sync.Mutex`, atomic CAS, etc.) where two
      operations must coordinate.
- [ ] Tests can be written against current source; missing seams/mocks listed
      as prerequisites.
- [ ] Tightly-coupled deferred issues either pulled in-scope or justified.

## Before drafting

For architectural changes, create or update the implementation plan under
`docs/plans/` and get approval before coding.

### 1. State the invariant, not the symptom

The bug is the shadow of a broken invariant. Name the invariant before
sketching a fix. "This state must always mean X regardless of which path last
touched it" tells you which paths must maintain it; the symptom only names one
path that breaks it.

### 2. Grep, don't claim

Every prose invariant like "X is the only caller of Y" or "all writes to Z go
through W" is a grep claim. Run the grep before writing it — reviewers will
check, and a wrong claim wastes the round. Grep both production code and tests
unless scope explicitly excludes one.

### 3. Enumerate paths, then design

List every path that observes the live state, then every path that mutates it,
then the atomicity needs, then pick the minimum mechanism that holds the
invariant across all of them. Designing the mechanism first and plugging paths
into it produces patches-on-patches.

## During design

### 4. Atomicity is designed in, not bolted on

If two operations must be atomic, pick the primitive first
(`sync.Mutex`, `atomic.CompareAndSwap`, …) rather than writing code, finding a
race, and patching. Warning signs you're bolting on safety: adding a state
pre-check before an op that already checks, or wrapping a call in a new mutex
without changing the operation's scope.

### 5. Tightly-coupled issues are not deferrable

If a deferred issue shares a field, a lock, or a code path with the in-scope
fix, it will be pulled back by a reviewer or a scope change. Surface the
entanglement up front — either include it, or show why the shared resource has
clean lifecycle separation.

### 6. Test plans must compile against current source

The reproducer that would fail without the fix must be writable against the
existing code. A needed clock seam, mock, or fake server is part of the plan,
not an afterthought.

## During review loops

### 7. "Approve with changes" means the changes are required

It is not "almost approved." Every listed change is a required edit.

### 8. Patching past 2-3 rounds means the design is wrong

If each patch introduces a new failure mode, stop. Reset to step 1 (state the
invariant, enumerate paths). A larger refactor that closes several coupled
defects at once is cheaper than four more single-issue patches. Reset signals:
the fix adds a second coordination mechanism for the same state; the new test
needs ever-more-precise timing to reproduce; the latest finding is about an
interaction between two earlier fixes.

### 9. Scope can shift; design must follow

When the user expands the goal mid-loop ("robust" instead of "fix this one
case"), re-examine every "out of scope" item against the new goal.

### 10. The reviewer sees what you wrote, not what you meant

If a reviewer "refutes" a claim, the plan text was usually ambiguous or
stronger than the code supported. Tighten the language; cite `file:line` for
every load-bearing claim.

## Examples appendix (illustrative)

Grounded in this repo's `Writer`, as reminders — verify before relying on them:

- **Invariant framing:** "an enqueued payload is either written or counted as a
  drop, never silently lost" — not "the second write returned the wrong error."
- **Atomicity designed in:** `writeMu` serializes `SetWriteDeadline`+`Write` so
  concurrent writers cannot extend one another's in-flight deadline;
  `lifecycleMu` gates async enqueue against `Close` so nothing is enqueued
  after the final flush. Both are chosen primitives, not patches.
- **Enumerate paths:** every site that mutates `droppedLogs` / touches `conn`
  must be listed before changing the reconnect or close ordering.

## Cross-references

- [850-review-loop-workflow.md](850-review-loop-workflow.md) — how to sequence
  the rounds (which reviewer, when to stop). 800 is how to think *within* a
  round.
