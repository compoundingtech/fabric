# now — fabric working state

The living handoff for whoever owns fabric next (there was none before; keep this
current). This records what is DONE, what is IN FLIGHT, and what is NEXT — the
things the repo history alone does not carry.

_Last updated: 2026-09-03 by Silber.fabric-codex. Main is `17a7bb5`. The
deployed code is `353de6b`. Silber and hetz run `0.2.0+353de6b`._

## Current incident — 2026-09-03

Release `v0.2.0+ebca516` is published with Apple arm64 and both Linux assets.
The required ACL migration and downgrade warnings lead its release notes.

A matched mux pair again retained a stale canonical connection after Silber
replaced its endpoint. Hetz has the lower NodeID, so both tie-break decisions
correctly select the hetz-client to Silber-server connection. Silber closed its
server-side cache and dialed a fresh noncanonical connection. Hetz retained the
old canonical client handle and rejected every fresh connection as a duplicate.
The bounded retry cannot clear a durable stale handle. A Silber daemon restart
fully closed the old endpoint and restored control.

PR #128 quarantines mux in both directions. Production dials use the
existing direct service ALPNs, and production endpoints do not advertise mux.
This keeps all services, including Git, and removes the stale shared state.
It merged at `8c6458146ee50ef57f134ba47e747cbe5990482b`. Silber and
hetz run the exact merge. Two-way exec passed after each service restart.

PR #129 is the full repair. It merged at
`353de6b2cc8755c5c286018eecfddc8f4ef13a7a`. The first red test preloads
Bluey's old recovery state, misses one hetz probe, and observes a shared
endpoint replacement. The isolated recovery fix now closes only the failed
peer's cached connection. It never changes the shared endpoint or drops another
peer's tunnel.

The second red test holds the old canonical connection while a new endpoint
generation dials. The old mux code refuses all eight replacement attempts. Mux
version 2 exchanges durable endpoint generations before it registers a shared
connection. A higher remote generation replaces stale canonical state. An old
peer rejects the new mux ALPN, so the new peer uses the existing direct service
ALPN until both builds support mux version 2.

Both original red tests pass. A full daemon test also replaces the higher
NodeID endpoint and proves two-way ping reconverges without restarting either
daemon. The library suite passed 424 tests, with two measurement tests ignored.
All 15 binary tests passed. A second full local integration run passed all 29
tests in 213.60 seconds. All three CI jobs passed.

Silber deployed first, then hetz. Mixed-version ping and exec passed in both
directions before the hetz update. After both updates, a live test replaced the
Silber endpoint from generation 1 to generation 2. Silber PID 57719 and hetz PID
851943 stayed unchanged. Both directions reconverged direct in 37 milliseconds,
and remote exec passed. Neither log contains a new duplicate refusal.

The matched-fleet soak starts at 2026-09-03 10:32Z. A release is not cut. Bluey
needs no compatibility action because mux/2 falls back to direct service ALPNs.
It needs a future release update only to receive the isolation and generation
fixes.

The mux value measurement is complete. Two fresh processes ran the exact same
test binary. Each process held 16 proven idle logical sessions for 1,800
seconds. Mux used one connection, and direct ALPN used 16 connections.

Mux used 0.604104 CPU seconds. Direct used 1.274088 CPU seconds, so mux used
52.6 percent less CPU during the 1,800-second window. Mux caused 100 package
idle wakeups and 8,336 interrupt wakeups. Direct caused 373 package idle wakeups
and 23,700 interrupt wakeups. Mux reduced those wake counts by 73.2 percent and
64.8 percent during the window.

Mux transferred 327,020 QUIC UDP bytes. Direct transferred 3,101,070 bytes, so
mux reduced idle network bytes by 89.5 percent during the window. Mux RSS moved
from 42.406 MiB to 41.406 MiB. Direct RSS moved from 44.875 MiB to 45.047 MiB.

Direct won the recovery measurement. Across 160 concurrent logical-session
samples, mux p95 was 44.026 milliseconds and direct p95 was 36.476 milliseconds.
Mux recovery was 7.550 milliseconds, or 20.7 percent, slower. The percentage is
large, but the absolute recovery cost is not perceptible.

The main win is battery life. Fabric is idle for most of its life on Nathan's
laptops. With 16 idle sessions, mux gave the machine substantially fewer reasons
to wake and reduced network bytes by 89.5 percent. Mux earns its place because
it substantially reduces every measured idle cost, with a small recovery cost.

PR #131 records the repeatable mux and direct harness and the result above. It
merged at `edd1c80169e7b856407df3c9fb17accc83dfe371`.

PR #132 adds the per-peer `roaming = true` contract. It merged at
`17a7bb53b80d8a79c46ab4f8a8adcd7fe3c2f69c`. An absent roaming peer still gets
health probes and normal sync attempts. The absence does not enter either
failure counter, close a peer connection, or appear as a stopped sync. Health
and sync log only away and return transitions. `fabric status` reports `away`.
`fabric sync ls` reports `stopped=none` and `away=<peer>`. Doctor reports both
the peer and its paused sync as OK.

The Bluey NodeID now has `roaming = true` in both live peer files. Silber names
it `bluey`; hetz names it `air`. Both old daemons reloaded the file, but build
`353de6b` ignores the new field. Bluey was reachable from both machines at
12:15Z, so no real away or return transition has been observed.

Silber.cos held deployment. Prepare v0.2.1 with the roaming change, but do not
release or deploy it. Nathan wants to use the current stable v0.2.0 fleet before
the 0.9.0 work starts. Silber.cos will ask when he wants v0.2.1.

Do not merge your own pull requests. Mark each pull request ready and wait for
Nathan to review and merge it. Do not create peer-file backup copies. Put
non-secret configuration history in git instead.

Six dated peer and sync copies on Silber and ten on hetz were deleted on
2026-09-03. The deletions are not recoverable. The four live files remain
intact. Do not create replacement copies.

The private catalog now owns their version history under
`docs/fabric/<host>/peers.toml` and `docs/fabric/<host>/syncs.toml`.
Silber.catalog committed Silber's files at `7add563`. It is adding the two live
hetz files from host-local reads. Silber.catalog is the only writer for that
folder. Send it changed bytes, then review its commit before it pushes.

The tracked file is a snapshot, not a live configuration source. Nothing
checks it against the host yet. Compare it with the live host file before using
it as current state. Never add `~/.local/share/fabric/identity.toml`; it holds
the machine's private identity key.

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

At 18:42Z, Silber lost ping and exec control to hetz while the st2 bus still
crossed the same peer pair. A dial failed after four mux reopen attempts because
hetz closed a connection as a duplicate. Silber recycled its endpoint from
generation 2 through generation 5. Hetz stayed active on generation 0 during
the incident. One LOCAL Silber daemon restart restored a direct ping and exec
at 18:51Z. If control fails again with `duplicate mux connection`, restart the
local daemon first. If this failure recurs on `0.2.0+ae755c5`, roll Silber back
without asking. Do not repeatedly restart it.

Bluey was not a closed laptop during the incident. Its host answered over
Tailscale in 62 milliseconds, but its Fabric endpoint stopped answering at
18:23Z. This absence matters. When hetz missed a probe, the health loop saw no
reachable peer and let Bluey's old failure count trigger an endpoint recycle.
The same condition caused generation 2 to 3 at 18:40Z and generation 4 to 5 at
18:50Z. This recycle trigger and the mux convergence defect are separate.

PR #122 fixes the mux defect and merged at `7f4da21`. A generation change
ignored the cached old connection but did not close it before the new endpoint
dialed. The peer retained that canonical connection and rejected the replacement
as a duplicate. The fix closes old-generation cached state before redial. It
also gives an explicit duplicate refusal eight attempts with 100-millisecond
delays. The total added wait is bounded at 700 milliseconds.

Both proofs failed before their fixes. The old cached connection survived its
generation change for more than one second. Four immediate duplicate refusals
then escaped as `open mux stream after reconnect`. Both proofs now pass without
a daemon restart. All 413 active library tests and all 15 binary tests pass.
Two measurement tests stay ignored. PR #122 is merged and deployed on Silber
and hetz as `0.2.0+7f4da21`.

The rollout used Silber first and hetz second. Silber's arm64 binary has SHA-256
`59794988e352df076e4ce44c949f334dce508362d3fe48eff6c05ec1d0f71b1e`.
Hetz built exact commit `7f4da21293732e5be914e95cf16dbf54c248a705` natively.
Its x86_64 binary has SHA-256
`c9a890cfefc531f68847393e6cafca6abf0c1b4dbcb89ceb4041a35336137bc5`.
The rollback paths on both machines report `0.2.0+ae755c5` and end with
`fabric.rollback-ae755c5-pre-7f4da21`.

Bluey returned during the final proof after Nathan updated it. One fresh-process
ping hit `duplicate mux connection` during that real topology change. The next
six fresh-process pings all passed direct in 36 to 128 milliseconds. An exec
returned real remote output. Silber PID 26309 and hetz PID 3364283 stayed
unchanged. The pair therefore reconverged without the daemon restart that the
same failure required before PR #122. Do not run the synthetic recycle tonight.

Bluey is temporarily absent from Silber's live `peers.toml`. Hetz still lists it
as `air` because Nathan uses Bluey to reach hetz. Removing `air` locked Nathan
out and was reverted from the fresh config backup before the hetz restart.
Unreachable from Silber did not mean unused by hetz. Silber now has one peer, so
one missed hetz probe can still recycle its endpoint.

The incident `fabric restart` started a healthy daemon outside launchd. The
orphan used the same home and node identity, so starting a second daemon beside
it was unsafe. Silber stopped the orphan first and started the loaded launchd
job once. Launchd owns PID 26309, and ping plus exec still work. This was the
second critical process found without supervision tonight; the port 3080 relay
was the other. Treat an unsupervised critical process as a shared host pattern,
not as an isolated service detail.

Silber permits the five service names `echo`, `exec`, `send-file`, `shell`, and
`sync` for both peers. Hetz permits those five names plus `deskset-vnc`,
`pty-remote`, and `st-sync` for both peers. The strict daemon starts with an
omitted allow list and grants nothing. It does not refuse startup.

Bluey is Nathan's deferred task. It must run the old `make-explicit` helper and
preserve its full ALPN matrix before it receives a strict binary. Do not wait for
Bluey and do not count it as verified.

PR #108 added Git remotes and merged at `4548b1e`. Silber and hetz ran that
build before the degraded-path rollout. Each has the relative
`git-remote-fabric -> fabric` helper and zero Git remotes. Nathan owns the first
live share and grant.

PR #109 added degraded-path recovery and merged at `bbd69bb`. All outbound
services now use `fabric/mux/1` streams on one shared multipath connection per
peer pair. Simultaneous cross-dials converge on one connection. The health loop
skips a redundant probe after recent application traffic. Three samples above
one second and eight times baseline redial the peer connection. The classifier
resets on endpoint generation changes and has a 60-second per-peer cooldown.
The full local proof passed: 406 library tests, 29 daemon-slice tests, 18
folder-sync tests, 12 shell tests, and all smaller integration slices.

The live WAN proof and the 24-hour idle-cost window remain. PR #114 added the
mixed-version compatibility fallback and merged at `36158f6`. A new client uses
an uncached direct ALPN only after an explicit mux ALPN rejection. Old clients
remain compatible with new servers. Silber first deployed `0.2.0+36158f6`, while
hetz stayed on `0.2.0+4548b1e` for the mixed-version soak.

The compatibility candidate passed an actual two-build proof in isolated
homes. Build `0.2.0+4548b1e` and the new candidate exchanged ping and exec
traffic in both directions. The new side wrote one fallback event across both
services. After the old side changed to the new candidate, two pings and one
exec passed, and the fallback count stayed at one. The in-process proof also
shows zero cached peers during fallback and one shared connection after mux
becomes available.

The first Silber soak found that each new stream repeated the rejected mux
handshake. The log stayed at one event, but manual pings periodically took 2.3
to 2.9 seconds. No path-quality redial occurred. PR #115 suppresses mux re-probes
for 60 seconds after an explicit rejection and merged at `ae755c5`. It then
permits one requested-stream re-probe so an upgraded peer cannot stay
downgraded. It reports cumulative fallback uses at powers of two. This makes
repeated use visible without noisy per-stream logging.

Silber and hetz previously ran `0.2.0+ae755c5`. The Silber updater kept
`/Users/myobie/.local/bin/fabric.rollback-1788366350`, which reports
`0.2.0+36158f6`. Roll Silber back without asking if control to hetz fails, the
st2 bus stops crossing machines, or the per-stream cost grows beyond the
measured cost.

The latency sample used a fresh CLI process for each requested stream. Run
`/usr/bin/time -p fabric ping hetz` repeatedly from Silber and record the
`real` value. The first soak ran 28 successful ping and exec samples. Periodic
ping samples cost 2.3 to 2.9 seconds. Use the same command and the same local
Silber-to-hetz direction after the fix. Report the first probe separately from
the later samples in the 60-second negative-capability window.

PR #115 serializes the first capability check and counts direct fallback uses.
The full library proof passes 409 active tests, with two ignored tests. The
isolated proof ran eight rapid new-to-old pings plus an exec. Candidate
`0.2.0+361e581` reported 3.3 to 7.7 milliseconds per ping. The old daemon
recorded one rejected mux handshake. The new validation log recorded one
fallback entry and cumulative use summaries at 2, 4, and 8 uses. The
in-process capability-flip proof expires the window, enables mux on the old
peer, and proves two requested streams converge on one cached mux connection.

The exact merged archive has SHA-256
`4eaf590ab6f559ac36f8e390a2a2196ff0f6c008cdab61c9481026f336e9bfdf`.
After the Silber update, 28 fresh-process pings ran from 16:26:24Z to
16:26:28Z. Real time ranged from 0.04 to 1.09 seconds. The median was 0.05
seconds, and the 95th percentile was 0.47 seconds. The daemon logged one hetz
fallback window, summaries at 2, 4, 8, 16, and 32 uses, and no hetz redial.
Control, ping, and exec work.

Silber.cos authorized the hetz update after that measurement. Hetz built exact
commit `ae755c5` on x86_64 Linux. Its one-member archive has SHA-256
`4222b76bbe50ad3de5dea1c96e2b3501d68c7ad3181387e0ca751d951508772e`.
Before replacement, `/home/myobie/.local/bin/fabric.rollback-4548b1e-pre-ae755c5`
ran and reported `0.2.0+4548b1e`. The updater staged beside the live path and
renamed the new binary into place. Its additional rollback path is
`/home/myobie/.local/bin/fabric.rollback-1788366755`.

The ordered post-update checks passed. Silber-to-hetz ping took 333
milliseconds. Hetz-to-Silber ping took 36 milliseconds. Exec reported
`0.2.0+ae755c5`, and `hetz.root` returned a native bus probe. The pair produced
one `mux_accept` event at 16:32:40Z. The last fallback-use summary was 128 before
the update. All 140 later ping streams passed from 16:34:58Z to 16:35:08Z, and
no use-256 summary appeared. Neither machine logged a post-update fallback for
the other. Hetz is active, enabled, and uses `KillMode=process`.

The first PR #115 deterministic job exposed a lost tunnel notification wake.
`a_tunnel_recovers_from_an_asymmetric_partition` stalled after a live session
resumed. The writer checked state before it created its notification future.
Data could arrive and notify existing waiters in that gap, then leave the
writer asleep with bytes ready. PR #117 creates each future before the state
check and merged at `5c035dc`. Before the fix, two targeted runs passed and the
third reproduced the 63.56-second failure. After the fix, ten consecutive runs
passed in a 54-second window. All 29 daemon-slice tests passed in 210.80 seconds,
and all 409 active library tests passed in 24.38 seconds. The two measurement
tests stayed ignored. This change is not deployed.

The session totals in `fabric status` persist across daemon restarts. They do
not cover only the current daemon uptime. The old output did not state this
window and listed retained telemetry for removed peers without a marker. That
made droppy's historical 10,885 attempts look like current dial activity.
Droppy was already absent from both authoritative `peers.toml` files. Its last
local log entry was August 12, and its last hetz log entry was August 20.

PR #119 adds the missing context and merged at `8eb296d`. New telemetry
snapshots record their exact UTC window start and retain it across restarts. An
unreadable or incompatible snapshot reports its reset reason. An existing
snapshot keeps its valid counters and states that its start is unknown. Session
and path rows mark retained entries as `[not in peers.toml]`. This change is not
deployed, and it must ride with a later release.

PR #109 deterministic CI found two follow-up defects. A temporary debug tunnel
block became a permanent mux denial, which returned early EOF in five recovery
tests. A valid reconnect also retained the old outage backoff. PR #111 fixed
both and merged at `ff03bfc`. The five-flap proof now has a 1.5-second budget and
measured 315.95 ms locally after five 200-millisecond drops.

The sixth CI failure was portable test setup, not transport behavior. Ubuntu
made a bare Git remote whose HEAD named `master`, while the test pushed only
`main`. PR #112 points the bare HEAD at `main` and merged at `6ee46a8`.

The hetz fabric checkout had a stale SSH origin at the old `myobie/fabric`
repository. Hetz has no GitHub SSH key, so fetch had failed for an unknown time.
Its origin now uses `https://github.com/compoundingtech/fabric.git`, matching
the other working hetz checkouts.

The strict ACL, Git transport, shared mux, mixed-version fallback, bounded
fallback, lost-wake, telemetry-context, and recycle-convergence fixes are
complete. The PR #122 rollout and its live convergence proof are complete.
No deployment action remains from this handoff.

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
