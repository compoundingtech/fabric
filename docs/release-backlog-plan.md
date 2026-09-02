# Fabric release backlog plan

Status: proposed on 2026-09-02. This plan changes no code, live configuration, or release.

Remote `main` was `729ec56` during this audit. GitHub had no open pull request.

## Recommendation

Not all planned work is complete.

Ship a strict baseline first. It removes compatibility code and fixes every known release blocker.

Ship Git remotes second. The strict peer configuration makes its repository grants smaller and easier to prove.

Finish automatic degraded-path recovery third. Its foundation exists, but the daemon does not use it as designed.

The total estimate is 14 to 20 focused engineering days. CI, review, fleet coordination, and soak time are additional.

Silber.cos owns every release cut. Ask it for the current fleet build and release gate before each cut.

## Allow-list decision

Nathan decided that Fabric is an allow list. A peer gets exactly the listed services and no other service.

An absent `allow` field means an empty list. Fabric has no unrestricted peer state.

The empty state must be obvious. `fabric peers` and `fabric doctor` must identify each peer that has no grants.

A refused connection must say that the peer has no grant. It must not look like a network fault.

## 1. Remove all compatibility code

Nathan is the only user. Fabric needs no mixed-version or old-layout support after the controlled fleet migration.

The source contains 57 direct uses of the word `legacy`. Other compatibility paths use words such as `older`, `predates`, and `migration`.

The removal audit must cover behavior, not only that word count.

### Preflight on the current release

Run these steps on Silber, hetz, and bluey before the removal release:

1. Ask Silber.cos for the exact running build on each machine.
2. Save each canonical `peers.toml`, `config.toml`, `syncs.toml`, identity file, and sync state directory.
3. Record the built-in services and every persisted or ephemeral exposure on each machine.
4. Measure the exact reachable service set for every peer pair and save the result.
5. Run `fabric peers make-explicit` on each machine with the current binary.
6. Confirm that every peer entry now has an explicit `allow` list.
7. Reload peers and measure the exact reachable service set again.
8. Prove that each before and after set is identical. Investigate any difference before the release continues.
9. Confirm that canonical sync `state.json` exists for each entry.
10. Run a successful sync pass where needed, then confirm that no `manifest.json` projection remains.
11. Confirm that no old peer file or embedded peer list remains.
12. Record a 15-minute CPU, write-volume, memory, probe, sync-wire, and reconnect baseline.

Every measurement must include its start, end, machine, peer pair, and build.

The migration is part of this release. It must cover Silber, hetz, and bluey without a manual edit by Nathan.

Nathan authorized the migration. Silber.cos must coordinate the live changes and approve the release cut.

### Code deletion

Delete these compatibility surfaces after the preflight is complete:

1. Make `Peer.allow` a default-empty list. Delete unrestricted omission and `peers make-explicit` after the fleet migration.
2. Delete the old state-root peer file migration and embedded `config.toml` peer migration.
3. Delete the one-shot `fabric/shell/0` server, client fallback, wire constants, and compatibility tests.
4. Keep only the resumable shell protocol. A peer that lacks it is an error.
5. Delete old sync peer capability branches for missing digests, missing delta flags, and ignored error fields.
6. Bump the sync ALPN and use one strict wire schema on every machine.
7. Delete old sync state loaders, defaulted pre-cache fields, and the `manifest.json` cleanup projection.
8. Delete control-response defaults that exist only for an older local CLI or daemon.
9. Remove informational mtime fields retained only for old wire parsing. Keep content and executable-bit behavior unchanged.
10. Delete compatibility documentation, tests, and changelog language after the behavior is gone.

Do not delete defaults that express a current optional setting. This work removes compatibility, not valid configuration choices.

Use red tests first. Missing `allow` must grant nothing and report the empty state clearly.

Shell version zero, old sync payloads, and old state files must all fail clearly.

### Rollout shape

The strict sync and shell protocols make rolling mixed-version operation unsupported.

Build and verify every target first. Stage the same release binary on all three machines without starting it.

Use local access or SSH for the coordinated restart. Do not depend on the Fabric process that the restart replaces.

Verify NodeIDs, explicit ACLs, sync digests, exposures, shell, exec, and status after all machines start.

Repeat the service-set measurement. The result on each peer pair must match its pre-migration result.

Rollback restores the saved configuration, state, and prior binary as one set. A partial rollback is not supported.

Estimated cost: 3 to 5 engineering days, plus one coordinated fleet window and its soak.

## 2. Release and roadmap audit

| Record | Audit result | Disposition |
| --- | --- | --- |
| `path-quality-reconnect-roadmap.md` | Partial. `mux.rs` and `pathwatch.rs` exist, but the daemon does not use the mux. Pathwatch observes and never acts. | Still wanted. Finish it after Git remotes. Keep dual-path bonding parked. |
| `service-install-roadmap.md` | The Linux and macOS first slice shipped. Install, status, and uninstall exist. | Complete for Nathan's platforms. Abandon Windows, service logs, and service restart until a real need appears. |
| `liveness-and-product-gaps-2026-07-22.md` | Roaming recovery, exec, and `send-file` shipped. The 20-second peer probe still runs during ordinary traffic. | Keep the idle-only probe improvement. Abandon discovery, multi-hop, exec stdin, and a separate reverse-tunnel command for now. |
| `multi-machine-reliability-2026-07-22.md` | Roaming recovery, the Nix build, and managed services shipped. A NixOS module did not ship. | Treat the module as abandoned until a consumer asks. Keep the live roaming and relay proof as a release check. |
| `known-flaky-tests.md` | Active and incomplete. A fourth daemon-slice timeout occurred on `593a3f7`. Issue 54 is a separate suite hang. | Fix before the strict baseline release. |
| `unresolved-branches.md` | Stale. Its three branches match shipped patches exactly. | Record the proof, then delete those remote branches. Land the separate executable-bit wire proof. |
| `failure-modes.md` | The reference remains useful. Slow reconnect backoff and two live-WAN proof gaps remain. | Keep the page. Fix the backoff and run the missing proofs before release. |
| `dev-prod-isolation.md` | The main isolation controls shipped. `KillMode=process` did not ship. | Add `KillMode=process`. Abandon the optional `fabric dev` command. |
| `agent-attribution.md` | The commit trailer and pull-request rule shipped. | Complete in this repository. Distinct Git identities belong outside Fabric. |
| `hetzner-supervisor-plan.md` | Parked and cross-repository. Its standalone topology was never selected. | Abandon it as a release roadmap. Retain only accurate Fabric service facts elsewhere. |
| `git-remotes-plan.md` | Proposed and requested by Nathan. No implementation exists. | Still wanted. Ship after the strict baseline. |

The three old unresolved branches map exactly by patch identity:

- `agent/production-sync-scan-counters` maps to `af8fb02`.
- `agent/reliable-shell-sync-noop` maps to `1ea4c04`.
- `agent/suppress-sync-self-events` maps to `ed8e7ad`.

The branch `test/exec-bit-over-the-wire` at `30ac292` is not in `main`. Its clean merge adds the missing end-to-end proof.

### Open issue audit

| Issue | Current result | Action |
| --- | --- | --- |
| #54, Linux library-test hang | Live release blocker. Shutdown and several setup awaits remain unbounded. | Find the stuck await, add bounded diagnostics, and fix the lifecycle cause. |
| #52, sustained sync CPU | The delta wire path and sweep code shipped. Current production cost has not been remeasured in this audit. | Repeat the 15-minute fleet baseline. Close only if the current build proves the cost moved. |
| #50, catalog path retirement | Superseded. Catalog deletions now propagate and have an offline-peer regression test. | Close with the replacing commits and test. Do not build a retire command. |
| #27, same-content mtime | The product chose content plus executable-bit sync. README forbids mtime as liveness. | Close as abandoned by design. Remove the wire-only mtime fields during compatibility deletion. |
| #21, sleep and resumable shell | The code shipped through roaming recovery and resumable shell work. The live sleep/wake proof remains. | Run the live proof and close if it passes. Reopen only a failed behavior. |

Estimated audit cleanup cost: one engineering day. Issue #52 can add more work only if the new measurement stays high.

## 3. Fix every known release blocker

### Test stability

Replace timer guesses with readiness signals or bounded condition loops. Do not only increase the deadlines.

Fix these four daemon-slice flakes:

- `a_peer_not_permitted_for_a_service_cannot_reach_it`
- `exec_expose_reconnect_keeps_child_bound_to_tunnel_session`
- `production_status_exposes_exact_inbound_scan_ledger`
- `a_long_outage_does_not_time_out_permanently`

Fix issue #54 separately. A stuck daemon shutdown must fail one test with the blocked phase instead of killing the suite.

Run each former flake repeatedly under Linux load. Record the run count and total window.

Make pull-request and `main` workflows run the same release-gate job set. A green merge must cover the code that a pull request covered.

### Process and lock lifetime

Fix F13 in `exec.rs`. A disconnected caller must terminate its child, including a quiet child with no output.

Use `kill_on_drop` and select child output against receive EOF. Prove that the process and session permit leave.

Fix F14 in the inbound sync service. A stalled peer must not hold an entry operation guard without a deadline.

Apply a bounded wire-session deadline outside the guard. Prove that another reconcile can continue after expiry.

Add `KillMode=process` to generated systemd units.

This setting tells systemd to stop only the daemon. Systemd then leaves other processes in the service cgroup alive.

Prove that a Fabric restart cannot kill an unrelated process in its cgroup.

### Reconnect cost

Fix the known retry rough edge. Repeated 200-millisecond network flaps must not leave a 15-second recovery delay.

Derive reset behavior from successful traffic or outage duration. Keep a cap and prove that failures cannot create a retry storm.

### Missing proof

Land the executable-bit wire test from `30ac292` after rebasing it onto `main`.

Run the Mac sleep/wake test, direct-to-relay failover test, and TCP roam test on real machines.

Record each build and measurement window in `failure-modes.md`.

Estimated cost: 3 to 4 engineering days. A repeatable issue #54 root cause can increase this estimate.

## Release sequence

### Release A: strict and stable baseline

Merge the compatibility deletion and bug fixes as small reviewed pull requests. Keep one behavior change per pull request.

The release gate requires all CI jobs, repeated former-flake runs, the executable-bit proof, and the three live network proofs.

The release includes the peer migration on Silber, hetz, and bluey.

The release requires identical reachable service sets before and after that migration.

Estimated cost: 7 to 10 engineering days, including audit cleanup but excluding soak and coordination.

### Release B: Git remotes over Fabric

Use one `peers.toml`. Store host-local paths in top-level `[[git_remotes]]` entries.

Store read and write as exact peer grants named `git/<remote>/read` and `git/<remote>/write`.

The strict baseline removes every special case for an unrestricted peer. A share starts with no grant.

Use the two pull requests in `git-remotes-plan.md`: configuration and ACL first, then the helper and wire service.

Estimated cost: 4 to 6 engineering days, plus a temporary-repository fleet proof and soak.

### Release C: degraded-path recovery

Wire `PeerConnections` into daemon traffic so one peer uses one multipath QUIC connection.

Turn pathwatch evidence into a conservative degraded-path classifier. Redial only after an absolute and relative latency threshold persists.

Skip the liveness probe when recent real traffic already proves the peer is alive.

Prove direct-to-relay recovery on two real machines. Record CPU, wake, connection, and recovery costs over a 24-hour window.

Estimated cost: 3 to 4 engineering days, plus the 24-hour proof window.

Dual-path bonding stays abandoned for this release sequence. It has no usable iroh control surface and no measured need.

## Done condition

The programme is complete when all these statements are true:

1. No current source, test, config, state, wire, or documentation path supports an older Fabric layout or protocol.
2. Every migrated peer entry has an explicit ACL, and an omitted ACL grants nothing.
3. `fabric peers`, `fabric doctor`, and connection errors make an empty ACL obvious.
4. Every known flake has a fixed cause and a repeated bounded proof.
5. Issues #21, #27, #50, #52, and #54 have evidence-backed final states.
6. The stale branch note names every exact replacement, and only live branches remain.
7. Git clone, fetch, pull, and push work through explicit repository grants.
8. A persistently degraded selected path recovers without a daemon restart.
9. Silber.cos approves each release cut and records the fleet build that each soak measured.
