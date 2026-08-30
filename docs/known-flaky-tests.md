# Known flaky tests

This page names tests that fail sometimes under CI load and pass on a rerun of
the same commit. It exists so a red `deterministic` job can be told apart from a
real regression in about thirty seconds, instead of by re-deriving it each time.

**A test is on this list only after it failed on a commit that did not touch its
code, and passed on a rerun of that same commit.** If a test here starts failing
on every run, or on a commit that changed the code it exercises, treat it as a
real failure and take it off this list.

**All three below live in the daemon slice** (`tests/local_slice.rs` and
`tests/sync_slice.rs`): real daemons, real iroh, real timers, on one machine.
They share one root cause rather than three: a deadline that holds on a fast
developer machine can elapse on a loaded CI runner. That is a property of the
deadline, not of the change under test. It is written up as a candidate finding
in [failure-modes.md](failure-modes.md); until a deadline is retuned, these
reruns are expected.

## The tests

| Test | File | Symptom in the log |
| --- | --- | --- |
| `a_peer_not_permitted_for_a_service_cannot_reach_it` | `tests/local_slice.rs` | `a permitted peer could not reach the service it was just granted` / `deadline has elapsed`. A fresh dial after a permission grant did not complete inside the round-trip timeout. |
| `exec_expose_reconnect_keeps_child_bound_to_tunnel_session` | `tests/local_slice.rs` | `exec reconnect payload timed out` / `unix round trip timed out`. A tunnel that dropped and reconnected did not carry the next payload inside the timeout. |
| `production_status_exposes_exact_inbound_scan_ledger` | `tests/sync_slice.rs` | An assertion on `inbound_noop_transactions` / `full_scans` counts is off by one. A reconcile still in flight is counted on one side of the comparison and not the other; the test's own comment says so. |

## How to confirm one is the flake and not a regression

1. Read which test failed and which assertion. If it is not one of the three
   above, it is not a known flake.
2. Rerun the failed job on the SAME commit (`gh run rerun <id> --failed`). A
   known flake passes on the rerun.
3. If it fails again on the rerun, it is not this. Stop treating it as a flake.

## Why the main branch cannot answer "did my merge cause this"

The `main` workflow and the pull-request workflow run different job sets. A pull
request runs `build`, `macos`, and `deterministic`; `main` runs `Nix` and
`test`. So a green `main` may never have run the `deterministic` job a pull
request runs, and cannot on its own clear a merge of having caused a
`deterministic` failure. A squashed merge commit sometimes carries the pull
request's own check-runs, which is the only reason the check has been possible
after the fact. Aligning the two job sets would close that gap.
