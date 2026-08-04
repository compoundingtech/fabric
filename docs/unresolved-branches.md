# Unresolved branches on `origin`

Three remote branches are neither merged into `main` nor safe to assume are
obsolete. This note exists so the next person meets a recorded fact instead of a
puzzle.

| Branch | Commit | Size against `main` |
| --- | --- | --- |
| `agent/production-sync-scan-counters` | `24a0fd8` Expose production sync scan counters | 7 files, +234 / −32 |
| `agent/reliable-shell-sync-noop` | `a5671e9` Restore shell compatibility and skip converged scans | 8 files, +1078 / −91 |
| `agent/suppress-sync-self-events` | `558f094` Suppress delayed sync self-events | 1 file, +551 / −38 |

Measured on 2026-08-04 against `main` at `5999384`.

## What is known

Each branch is a single commit. None is an ancestor of `main`, so
`git branch --merged` will never list them and ancestry alone will never retire
them.

They belong to the `af8fb02` line — the four pull requests (#25, #26, #28, #29)
that were **not** merged into `main` directly. That line was instead absorbed by
a separate reconciliation, which preserved the behaviour while arriving at it by
a different route. `main` is therefore *believed* to contain the substance of
all three.

## What is not known

**Nobody has proven that.** "Absorbed by a different route" is a claim about
behaviour, not about commits, and no one has gone branch by branch to confirm
that every change in each one is represented in `main` today.

The diffs are large and `main` has moved a long way since, so a three-dot diff
answers nothing on its own: it shows divergence from a shared ancestor, not
missing behaviour.

## Why they are still here

Deleting a remote branch that is not an ancestor of `main` is **not reversible
for anyone but the person who still has it locally**. Keeping a branch costs
nothing. Tidiness is not worth an irreversible action on evidence that is only
probably right.

Verifying equivalence is real archaeology and needs judgement per branch. Nobody
needs these branches today, so that cost has not been paid.

## What to do if you care

Do not delete them on the strength of this note. Either prove per branch that
`main` covers the behaviour — and record the proof here — or leave them alone.
If you prove it, say which commit in `main` covers each one, then delete.
