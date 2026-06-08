# 850 - Review Loop Workflow

Apply when running a plan-track or implementation-track review loop — deciding
which reviewer to dispatch, when to stop, and how to handle findings between
rounds.

For the discipline that applies *inside* a round (state the invariant, grep
don't claim, atomicity is designed in), see
[800-design-and-review-loops.md](800-design-and-review-loops.md). 800 is how to
think during a round; 850 is how to sequence the rounds.

These reviewers run through the Codex plugin (`codex:codex-rescue`) when
available, with GitHub Copilot as the fallback.

## Canonical sequence

### Plan track

```
draft plan
  → /plan-review (architectural pass — highest effort the skill offers)
  → revise per findings
  → /plan-review again until no P0/P1 architecture findings
  → /final-plan-review (precision pass)
  → revise per findings
  → hand off to implementation
```

### Implementation track

```
implement (or /simplify a draft)
  → /post-impl-review v1 (high effort)
  → fix findings
  → /post-impl-review v2 (high effort)
  → fix findings
  → /post-impl-review v3+ (step the effort down once early findings clear)
  → merge when verdict=merge and zero P0/P1
```

### Lightweight alternative

For a fresh outside-model pass over a diff without a spec-vs-impl audit, use
`/code-review` or a direct `codex:codex-rescue` review. These do not write a
versioned report and are not substitutes for `/post-impl-review` when
spec compliance is the point.

### Stopping conditions

- **Plan track:** stop when `/final-plan-review` returns no P0 and no P1, or a
  verdict equivalent to "ready to implement."
- **Impl track:** stop when `/post-impl-review` returns verdict=`merge` with
  zero P0 and zero P1. P2 polish items are not merge-blockers.

## Between-round discipline

1. **Read the full report, then triage.** Each P0/P1 gets one of: *accept*
   (apply the fix), *argue back* (the reviewer was wrong — record why with
   `file:line`), or *defer* (legitimate but out of this round's scope — file a
   follow-up). Silent drops are not allowed.
2. **Edits land in the plan or code, not in chat.** The next reviewer must see
   the change in the file.
3. **Do not auto-apply reviewer suggestions.** The report is input to your
   edit, not a patch — blind application creates new findings.
4. **Argue-back must cite source.** "I disagree, see `file:line` showing X" is
   acceptable; "I think this is fine" is not.
5. **Surface the next dispatch as a cost gate.** Each external round costs real
   tokens and minutes of wall time. Propose the next round before dispatching;
   do not chain rounds silently.

## Re-dispatch guard

Before dispatching v2+, confirm material change since the last report.
Re-reviewing unchanged input wastes budget and reproduces the prior verdict. If
nothing changed but the loop hasn't converged, the next step is a human
judgment call, not another dispatch.

## Stage escalation

- Precision pass surfaces a P0/P1 with architectural shape → reopen
  `/plan-review`. `/final-plan-review` cannot redesign.
- `/post-impl-review` surfaces a finding needing re-architecture (not just code
  fixes) → stop the impl loop, reopen `/plan-review`, then return to impl.
- More than 2-3 rounds at the same stage with new findings each time → the
  approach is wrong (see 800 §8). Reset, do not patch further.
