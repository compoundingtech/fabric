# now — fabric working state

The living handoff for whoever owns fabric next (there was none before; keep this
current). This records what is DONE, what is IN FLIGHT, and what is NEXT — the
things the repo history alone does not carry.

_Last updated: 2026-09-02 by Silber.fabric-codex. Main is `6ee46a8`.
Silber and hetz run local build `0.2.0+4548b1e`. Bluey is deferred to Nathan._

## Current handoff — 2026-09-02

Nathan ordered the release backlog completed without approval stops. The active
order is strict ACL baseline, Git remotes, then degraded-path recovery. A release
cut is not the current task.

PR #103 added `KillMode=process` to the Linux service. PR #104 bounded initial
tunnel dials. PR #105 made omitted and empty peer grants deny every service. PR
#106 added Git share declarations and exact read and write grants to
`peers.toml`. PR #107 bounded iroh endpoint close waits after deterministic CI
hung for ten minutes in a half-close recycle test.

The strict ACL migration and rollout are complete on Silber and hetz. Each
machine ran `fabric peers make-explicit` with old build `0.2.0+593a3f7` before
its binary changed. The pre-migration, post-migration, and post-swap ALPN
matrices are identical. Two-way exec works. Both sync entries are clean on both
machines. Both services are active and enabled. Hetz has `KillMode=process`.

Silber permits the five service names `echo`, `exec`, `send-file`, `shell`, and
`sync` for both peers. Hetz permits those five names plus `deskset-vnc`,
`pty-remote`, and `st-sync` for both peers. The strict daemon starts with an
omitted allow list and grants nothing. It does not refuse startup.

Bluey is Nathan's deferred task. It must run the old `make-explicit` helper and
preserve its full ALPN matrix before it receives a strict binary. Do not wait for
Bluey and do not count it as verified.

PR #108 added Git remotes and merged at `4548b1e`. Silber and hetz run that
build. Each has the relative `git-remote-fabric -> fabric` helper and zero Git
remotes. Nathan owns the first live share and grant.

PR #109 added degraded-path recovery and merged at `bbd69bb`. All outbound
services now use `fabric/mux/1` streams on one shared multipath connection per
peer pair. Simultaneous cross-dials converge on one connection. The health loop
skips a redundant probe after recent application traffic. Three samples above
one second and eight times baseline redial the peer connection. The classifier
resets on endpoint generation changes and has a 60-second per-peer cooldown.
The full local proof passed: 406 library tests, 29 daemon-slice tests, 18
folder-sync tests, 12 shell tests, and all smaller integration slices.

The live WAN proof and the 24-hour idle-cost window remain. PR #109 requires a
coordinated fleet restart because most new clients require `fabric/mux/1`.
Silber.cos owns that deployment decision. Do not deploy or cut a release without
a later native order.

PR #109 deterministic CI found two follow-up defects. A temporary debug tunnel
block became a permanent mux denial, which returned early EOF in five recovery
tests. A valid reconnect also retained the old outage backoff. PR #111 fixed
both and merged at `ff03bfc`. The five-flap proof now has a 1.5-second budget and
measured 315.95 ms locally after five 200-millisecond drops.

The sixth CI failure was portable test setup, not transport behavior. Ubuntu
made a bare Git remote whose HEAD named `master`, while the test pushed only
`main`. PR #112 points the bare HEAD at `main` and merged at `6ee46a8`.

The latest full local proof passes 407 library tests and all 29 daemon-slice
tests. Let current Linux CI prove these two follow-ups together. Do not retry the
failed PR #109 job.

After green CI, the live work is the Silber-only soak that Silber.cos described.
Measure real traffic for longer than the classifier window. Prove a false-redial
loop cannot starve the peer. Keep a console rollback that needs only the old
binary because these changes write no new state. Tell Silber.cos before any hetz
step. Do not cut a release.

## Historical handoff — 2026-08-29 (Fable session; fleet moved to Codex)

**Who wrote this and why.** A Claude Fable session spent 2026-08-29 on the fabric
review (`../cos/notes/fabric/review-2026-08-29.md`, 15 findings). Workers are
moving to Codex, so this externalizes what commits and PRs do not carry. A
running decision log also lives in st2 context under identity `Silber.fabric` —
on the message bus, NOT in any repository, so it has no history and is not
reviewable. IF YOU CAN REACH IT, run `st2 context read` for the fuller log. Treat
THIS file as the reviewable source of truth. The context is accurate as an
append-only log, with one correction appended 2026-08-29 evening: its "morning
plan" entry was written as if the same agent resumes — the actor is now you, and
cos sends the soak-clear signal.

**State of main (verified 2026-08-29).** origin/main at `343b08b`. Merged today:
#86 (finding 2, include-delete), #87 (finding 1, dial-permit leak), #88 (finding
4A, content memory) — these three are in release `v0.2.0+4bc04af`, deployed to
the fleet. Also merged: #89 (finding 3), #90 (finding 15, README).

**Open PRs:**
- #91 F5 sweep-gate · #92 known-flakes doc · #93 F6 update-timer · #94 F9
  monitor-park · #95 F12 doctor-`--home` · #96 F10 doctor service-enablement.
  These are a STACKED CHAIN: each branch is rebased onto the previous, so
  SQUASH-MERGE THEM IN ORDER 91→96. Out of order or one-at-a-time will conflict
  on CHANGELOG. (#89 and #90 already landed this way, cleanly.)
- #97 (finding 7, send-file streaming) and #98 (finding 8, adopt-honours-include)
  — NOT in the chain. After the chain lands, rebase both onto new main (CHANGELOG
  conflicts). #98 changes wire/convergence semantics — merge AFTER the soak.
- Branch `feat/peer-acls-explicit-not-legacy` (`5350765`) — 0.10 ACL groundwork,
  transcription test only so far; helper + expose warning still to write.

### Decisions + reasons (settled with cos / Nathan, not in any file)

1. **Affirmative-absence deletes — item 1 of the NEXT correctness pass.** A delete
   must be inferred ONLY from an affirmative absence: a path missing from a
   directory the scan COMPLETELY and successfully read. Split "absent from scan"
   into three answers — PRESENT / AFFIRMATIVELY-GONE / UNKNOWN — and tombstone
   only affirmatively-gone. Why: today `scan_folder` (src/sync/engine.rs) aborts
   on the first unreadable file (`read_dir?`/`file_type?`/`fs::read?`), so an
   unreadable file STALLS the whole entry rather than false-deleting; the delete
   loop only runs on a fully-successful scan. Making the scan resilient (skip
   what it can't read) would REINTRODUCE the false-delete for skipped paths. The
   three-answer split fixes both at once and retires the guard accretion — three
   guards today are one mechanism: the 2026-08-25 scan guard, #86's
   materialize-time include check, and F4-B's proposed size guard. Filed as the
   FIRST item of the next correctness pass, deliberately NOT under 1.0 (a
   correctness fix under a feature milestone inherits the wrong gate; 1.0 is
   folder sync + relay + DNS). Gate: the soak. Ceiling: the filesystem gives no
   reliable delete event for a delete while the daemon was off, so the scan stays
   the delete source — the change QUALIFIES its absences, it does not replace it.

2. **F4-B and F11 are the first instances of (1), written in that shape, NOT as a
   third guard.** F4-B: a single file over 512 MiB (`MAX_BLOB`, src/sync/wire.rs)
   makes the whole entry bail and every peer read as "unreachable". F11: `no
   local sync entry named X` and a residual oversize error must stop reading as
   "unreachable, clears when it comes back" (classification in src/sync/engine.rs
   → wording in src/doctor.rs). TRAP: the naive F4-B fix (skip oversized files at
   scan) tombstones a file that was synced under the limit and later grew past it
   — it goes absent-from-scan and bus policy reads that as a delete. That is the
   2026-08-25 loss through a new door. Do not ship the naive version.

3. **0.10 ACL groundwork — transcription vs narrowing.** `PeerBook::may(id,
   service)` (src/config.rs) treats `allow = None` as unrestricted: it reaches
   every service INCLUDING ones exposed in the future (pinned by test
   `a_peer_without_an_allow_list_may_reach_everything`). Writing each legacy
   entry's CURRENT effective permissions as an explicit `allow` list changes the
   file but not today's access — a TRANSCRIPTION, which is cos's to do. It
   becomes NARROWING (Nathan's decision) only when a list is smaller than what
   the peer can reach today. Nathan's conscious decision (via cos): ACCEPT that a
   service exposed AFTER the transcription is no longer auto-granted and must be
   added per peer — that IS the cleanup; auto-granting every future service is
   the leak the moment a peer is a friend rather than one of Nathan's own
   machines. Each entry's explicit list = the five built-in service names
   {shell, exec, sync, echo, send-file} (from `service_name_for_alpn`,
   src/daemon.rs) PLUS THAT MACHINE'S `fabric expose` names. LIVE TRAP found on
   hetz: omit an exposure and you narrow by omission and break the fleet — hetz
   exposes `pty-remote` (how cos reaches hetz agents) and `st-sync` (carries the
   bus/catalog). The helper MUST read the machine's expose names, never
   hard-code; take them from `fabric status` on that machine at transcription
   time.

4. **Release cut convention.** Releases are `v0.2.0+<short-sha>` tags at a main
   commit, NO crate-version bump. Push the tag → `.github/workflows/release.yml`
   builds and publishes per-target tarballs each with a `.sha256` (there is no
   combined SHA256SUMS; the README's checksum recipe is wrong about that).
   `fabric --version` reports `0.2.0+<sha>` (build.rs `git rev-parse --short=7`),
   so it matches the tag. Verify the artifact's checksum + `--version` before any
   rollout. The 0.9 MILESTONE shipped as `v0.2.0+4bc04af` — there will never be a
   tag named 0.9.

5. **The soak.** The fleet is on `v0.2.0+4bc04af`. cos defined "soaked"
   observably: seven conditions at every hourly sweep for 24h from the ~12:46 UTC
   (14:46 Europe/Berlin) deploy on 2026-08-29 — so the window closes ~14:46
   Berlin (12:46 UTC) on 2026-08-30: all three machines read the version;
   delta_fallbacks 0; drift clean; digests agreeing; stopped none;
   reconcile_failures 0; include-drift clean. Any one failing BREAKS the soak
   (diagnose, then restart the window); elevated bytes-per-pass with fallbacks 0
   and digests agreeing does NOT (it is the ~24 MB/restart cursor cost). cos
   measures it and SIGNALS; do not watch the clock or ask.

### Gotchas (cost time; don't rediscover the expensive way)

- **`git checkout main` without `git pull` first** gives a stale local main. A
  branch cut from it silently reverts already-merged commits and produces a
  CONFLICTING PR with no CI. This happened (PR 97). Pull local main to origin/main
  before branching, or branch from `origin/main` explicitly.
- **st2 CLI arguments are shell-evaluated** — backticks and `$(...)` in a `-m`
  body or in `context append --decision/--why` EXECUTE. Write the text to a file
  and pass via `"$(cat file)"`; never inline backticks.
- **Daemon-slice tests flake under CI load**: a_peer_not_permitted_for_a_service_cannot_reach_it,
  exec_expose_reconnect_keeps_child_bound_to_tunnel_session, and
  production_status_exposes_exact_inbound_scan_ledger. See `docs/known-flaky-tests.md`
  (PR #92). Rerun the `deterministic` job; a real regression also fails on rerun.
- **main and PR CI run DIFFERENT job sets** (main: Nix, test; PR: build, macos,
  deterministic). A green main may never have run the job a PR runs, so it cannot
  clear a merge of a `deterministic` failure.
- **Stacked-PR chains squash-merge IN ORDER.** Each branch contains its
  predecessors, so a 3-way merge folds the identical changes (it does not rely on
  ancestry, which squash breaks). Merging out of order conflicts.

### Deliberately NOT done (do not read as oversight)

- F4-B/F11, F14, and the affirmative-absence refactor NOT authored — deferred to
  fresh authoring and gated on the soak (they touch the sync scan / wire path the
  fleet is soaking). Designs are above.
- F13 (an exec child outlives a disconnected caller; src/exec.rs — needs
  `kill_on_drop` PLUS a recv-EOF disconnect arm, because a quiet child never
  triggers the write-failure path) NOT touched — cos gated it on the soak
  because exec is how cos reaches two of the three machines to diagnose anything.
- Did NOT edit live `peers.toml` on any machine — that is the
  change-what-peers-can-reach boundary. Transcription is cos's per-machine
  action; narrowing is Nathan's.
- Did NOT bump the crate version at release (convention: +sha only).
- #98 (F8) authored but flagged for POST-soak merge (wire/convergence semantics).

### What I would do next, in order (my judgement)

1. **When the soak clears (~14:46 Berlin 2026-08-30, cos signals):** author the
   affirmative-absence delete refactor (decision 1), with F4-B and F11 as its
   first instances, in the three-answer split shape. First because it is the
   correctness fix the whole delete subsystem has been accreting guards toward.
2. **Finish the 0.10 ACL slice** on `feat/peer-acls-explicit-not-legacy` (NOT
   soak-gated; merge when green): (a) the make-explicit helper that reads THIS
   machine's `fabric expose` names + the five built-ins and writes each legacy
   entry's equivalent list (never narrow by omission); (b) a `fabric expose`
   warning when no trusted peer is permitted to reach a newly-exposed service —
   one line naming the peers that would need it, NOT a refusal (model on
   `warn_if_permissions_would_stop_a_sync`, src/main.rs). The transcription
   property is already pinned by the test in that branch.
3. **After the #91–#96 chain lands:** rebase #97 and #98 onto new main (CHANGELOG
   conflicts); #98 then merges post-soak.
4. **F14** (an inbound sync session holds the entry guard with no timeout;
   src/sync/engine.rs `prepare_inbound_entry` / src/daemon.rs `handle_sync`) —
   soak-gated; add a deadline to the inbound wire session.
5. **F13** — when cos calls it (post-soak).

### Uncertain / unverified (flagged per "don't assert what you didn't verify")

- Whether a Codex successor inherits my st2 context (`Silber.fabric`). If it can,
  the log there has more detail; if not, this file stands alone.
- bluey's exposures were NOT checked (bluey is a peer but not in Silber's sync
  entries; the handbook says it has no remote exec). Check before transcribing
  any bluey entry.
- Whether the fleet is STILL on `v0.2.0+4bc04af` and the soak is intact — this
  session was paused for budget mid-afternoon and did not re-check. Re-verify
  before acting on anything soak-gated.
- #91/#93/#94/#95/#96/#97/#98 each carry a red-before-green test and were green
  locally on macOS at author time; their CI state at this handoff was not
  re-checked.



## Hotfix DEPLOYED — peer-config split (resolved 2026-07-21)

On 2026-07-21 the Mac↔hetz stopgap sync (rsync over `fabric dial`) failed in a
loop and blocked cos from reaching the hetz fleet. Root cause: the launchd
service launched the daemon as `--home ~/.local/share/fabric`, which (pre-fix)
made the dial path read `peers.toml` from under `--home` while the CLI writes
`~/.config/fabric/peers.toml`; a default-home `fabric add` migrated + deleted the
in-`--home` copy, leaving the daemon with zero peers on the dial path
(`unknown peer` in `service.err.log`) while `ping`/`status`/`shell` kept working.

- **Durable fix (commit `9f5391b`):** `FabricHome::resolve` treats an explicit
  `--home`/`FABRIC_HOME` equal to the default state root like the no-arg default
  (peers from `~/.config/fabric/`); a genuinely different `--home` stays isolated.
  Unit-tested (`resolve_from` + 6 regression tests).
- **DEPLOYED:** as of ~17:33 the Mac daemon runs the fixed binary
  (`0.2.0+9f5391b`, pid rotates under launchd) — swapped via stop-old →
  `fabric service install`. Verified: hetzner reachable, dial probe returns
  bytes, sync log clean bidirectional cycles, no fresh `unknown peer`.
- **launchd label changed:** the service is now `com.compoundingtech.fabric`
  (was `com.myobie.fabric`, org rename). The stale `com.myobie.fabric` job was
  booted out and its plist moved aside to `*.stale-501`. Use the new label.
- **Holds LIFTED:** default-home `fabric add`/`remove` and `fabric restart` are
  safe again — the daemon reads the same `~/.config/fabric/peers.toml` as the CLI.

## Recent work (2026-07-24)

- **Marker env vars shipped** (main `93e0f59`): a `fabric shell`/`exec` runs in the
  daemon's session, not the caller's, so the remote session now exports
  `FABRIC_SHELL=1` / `FABRIC_EXEC=1` / `FABRIC_PEER=<connecting NodeID>`. Lets a
  shell rc detect a fabric session and skip session-fragile startup. Unit + two-node
  iroh tests; README has an rc-guard snippet. Not deployed — ships in the next binary.
- **`fabric shell silber` hang — diagnosed, NOT the suspected commands.** Probing
  the LIVE silber daemon's own session (via the harmless `fabric exec hetzner ->
  fabric exec mac -> …` relay) showed the daemon is in an **Aqua (GUI) session** and
  xcrun / xcode-select / `security show-keychain-info` / brew all return rc 0 (no
  hang); xcode is fully usable in a fabric shell. So the hang was a prior daemon
  state, not those four. Deterministic culprit-finder for Nathan: rc-timing
  instrumentation (`set -x; PS4='+ ${EPOCHREALTIME} '`) on a fresh shell; or just
  gate fragile rc on `[ -z "$FABRIC_SHELL" ]`.
- **PR #16 (nix flake) MERGED** to main (`903d70f`) — `flake.nix` + nix CI now live.

## What fabric is

A standalone Rust CLI + local daemon that hides iroh behind local Unix sockets.
Consumer tools ask fabric for a local socket wired to a trusted remote peer and
speak their own protocol over it; only fabric touches iroh/QUIC/relays/NodeAddr
and the peer allow-list. See `README.md` and `SKILL.md`.

## In flight — `fabric sync` (generic file-sync primitive)

fabric now owns file sync. A config file (`syncs.toml`) lists sync entries; the
running daemon watches each folder and keeps it converged with its peers.

Status:
**Fully landed + pushed to origin main (fabric 0.2.0):** the config surface
(`syncs.toml`) and the property-tested reconciliation core
(`src/sync/{config,manifest,node,glob}`), the on-wire backend
(`src/sync/wire.rs`), the async `SyncEngine` (`src/sync/engine.rs`, fs-watch +
scan/materialize), the daemon wiring (`fabric/sync/1` ALPN, `IrohSyncTransport`,
engine started at boot, `SyncReload`/`SyncStatus` control ops), and the
`fabric sync add/ls/rm/reload` CLI. Proven end-to-end over real iroh by
`tests/sync_slice.rs`. All tests green; CLI smoke-tested; zero warnings.

Design decisions (why, so they are not re-litigated):
- **Backend-agnostic semantics.** The merge/conflict/delete/echo/convergence
  rules live in fabric (`sync::manifest` + `sync::node`), above a swappable
  transport. `Manifest::merge` is a semilattice join (commutative/associative/
  idempotent) → convergence and echo-freedom are structural, proven by property
  tests. Newer-wins = Lamport version + deterministic author tie-break (never a
  wall clock). The on-wire backend is verified to reach the exact same state as
  the pure reference reconcile — that is the swappable-backend guarantee.
- **Policies.** `catalog` = union + newer-wins + never-delete-on-peer + no-sweep
  (a local delete is restored; decommission is an edit, e.g. `retired = true`).
  `bus` = + tombstone deletes (modelled; sweep TTL not yet wired).
- **Config.** `~/.config/fabric/syncs.toml`, hand-editable, `fabric sync reload`
  applies live (mirrors `peers.toml` / `reload-peers`). Entry =
  `{name, folder, peers, policy, include?}`. `name` is the shared logical key
  (same name on two boxes = same sync); `peers = "*"` follows the `peers.toml`
  allow-list.

## Next

0. **ROAMING GAP — the shareability blocker (Nathan 2026-07-22). DEPLOYED
   2026-07-22 (see the deploy summary below).** A peer that changes network/public IP went
   unreachable both ways until its daemon was manually restarted. Root cause:
   fabric only reacted to LOCAL netmon changes and never probed peer reachability.
   - **Fix landed (commit `298a593`):** `run_peer_health_loop` echo-probes each peer
     every `FABRIC_PEER_HEALTH_SECS` (default 20s) and, on N consecutive failures,
     drives recovery (drop tunnels + iroh `network_change()` → re-discover/relay;
     recycle only after repeated nudges fail) — no local-change dependency. Pure
     `PeerHealthTracker` decision core with escalating backoff, fault-injection
     unit-tested. Emits per-probe latency + direct/relay telemetry. `=0` disables.
   - **Staged, NOT deployed:** the running daemon is still `9f5391b`. Deploy =
     `./install.sh` + restart, which blips cos's only path to hetz → **Nathan-gated
     window** with him present. Rollback ready: `~/.local/bin/fabric.prev` = the
     known-good `9f5391b` binary (restore + restart to revert). Live validation:
     move a laptop between networks, confirm self-heal without a manual restart.
   - Analysis + reliability data + nix design: `docs/multi-machine-reliability-2026-07-22.md`.

0b. **`fabric exec` — non-interactive remote command execution (Nathan 2026-07-22).
   DEPLOYED 2026-07-22 (see the deploy summary below).** `fabric exec <peer> --
   <cmd>` = scriptable counterpart to `shell` (stream stdout/stderr, propagate
   exit code). DEFAULT-DENY per machine via `allow_exec` (`--allow-exec` on
   up/daemon/service install), separate ALPN `fabric/exec/0`. Validated with a
   local two-node e2e test (allow + deny + stderr split + exit codes); that test
   caught a real bug — `dial_alpn` routed exec through the mux tunnel whose Hello
   frame corrupted the argv; exec now uses the raw dial path like shell.
   **DEPLOYED 2026-07-22 (tag `deploy-roaming-exec-bootout` = `1964c99`):** roaming
   self-heal (`298a593`) + `fabric exec` (`4d02a01`/`4fe8557`) + service-install
   idempotency (`83a3614`), ZERO isolation (isolation was decoupled). BOTH machines
   on `0.2.0+1964c99`: Mac (launchd-managed) + hetz (`fabric-keepalive.service`).
   Verified both ends — link direct both ways, node identities preserved, roaming
   `peer_health_probe` firing both directions (each sees the other reachable),
   `fabric exec hetzner -- echo` exit 0 (cos's test + mine). hetz exec enabled via
   `allow_exec=true` in its config.toml (keepalive unit passes only `--allow-shell`;
   both `[[exposes]]` pty-remote + st-sync preserved). Deploy method that made it
   clean: build-verify-before-swap; Nathan fired the hetz `systemctl --user restart`
   from ssh (outside the daemon cgroup); rollbacks staged (Mac `fabric.prev`=9f5391b,
   hetz `fabric.prev`=0.1.7+940afd1 recovered from `/proc`). Remaining: Nathan's
   laptop-roaming self-heal test.

   **SEPARATE follow-up (on main `a9b336e`, NOT deployed — needs NO Nathan
   window):** dev/prod isolation: service-install refuses a non-default home,
   down/restart home-mismatch guard, README FABRIC_HOME-for-dev convention.
   Standalone validation before it lands live: default/prod-home `service install`
   still succeeds; a non-default home is refused; empty-home `down`/`restart`
   warns. Design: `docs/dev-prod-isolation.md`.

   **Deferred:** the `fabric dev` subcommand (env convention covers it), the
   Erlang idle-skip probe refinement (low urgency), and `fabric cp`/discovery
   (await Nathan's product prioritization — see
   `docs/liveness-and-product-gaps-2026-07-22.md`).

1. **Fleet redeploy before the hetz proof** (CoS-coordinated). The *installed*
   fabric binaries are stale/pre-sync (`~/.local/bin/fabric` was 0.1.7+940afd1).
   A machine only serves/dials sync after its running daemon is **restarted**
   onto the 0.2.0 binary — a fresh file on disk is not enough. fabric-claude owns
   build + `./install.sh` + `fabric restart`; the Mac daemon restart blips the
   live network so the CoS sequences the window; the CoS drives the hetz
   pull+build+restart.
2. Run the **real hetz proof** (Mac → Hetzner): convoy declares the catalog on
   both (per-network name `convoy-catalog-<network>`), drop a `host=hetz` job in
   the Mac catalog, it appears on hetz, hetz's `convoy up` launches it. This is
   the done-bar.
3. Evaluate **iroh-docs 0.101.0** (iroh `^1`, range-based set reconciliation,
   LWW, iroh-blobs content) as backend #2 and green the SAME conformance suite
   against it. If healthy it likely becomes the production backend; `fast_rsync`
   stays a large-file delta optimization only.

## Open items / known gaps

- **Per-peer exec/shell ACL (feature gap, raised 2026-07-24).** `allow_shell` /
  `allow_exec` live on `FabricConfig` (daemon-global) — enabling either opens the
  capability to *every* trusted peer. There is no per-peer allow field on `Peer`
  (peers.toml), so "allow exec from the Air but not from hetz" is not expressible
  today. Real future work: per-peer capability scoping. README now documents the
  current global scope.
- **Backlog (cos, 2026-07-24, do-not-act-yet): unify `peers.toml` + config.toml +
  `syncs.toml` into ONE config file.** Parked pending greenlight.
- `bus` tombstone-sweep TTL is not implemented yet (deletes propagate; sweep is a
  no-op). Fine — bus is a later consumer (smalltalk).
- mtime is carried in the manifest but not restored to disk (ordering is logical,
  not mtime; disk mtime preservation is a future nicety).
- Changing an existing entry's `folder` needs a daemon restart to re-point its
  watcher; add/remove of entries is picked up live by `fabric sync reload`.
- The folder scan uses blocking `std::fs` on the async task; fine for small
  catalogs, revisit (spawn_blocking) if large trees appear.

## Coordination

- **convoy-claude** is the first consumer (its network catalog), wire-ready and
  standing by for the "green on main" ping.
- **cos-claude** is the supervisor; direct-on-main landing, push to origin.
