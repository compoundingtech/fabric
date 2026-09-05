# `fabric-sync` process extraction plan

Date: 2026-09-04

Status: approved for implementation on 2026-09-04.

Implementation: steps 1 through 5 are merged. The embedded engine remains the
production owner.

## Decisions

File sync becomes a separate `fabric-sync` process. The monorepo ships `fabric`
and `fabric-sync` together. The main daemon stays available when the companion
is missing, stopped, incompatible, or unhealthy.

After activation, an absent `fabric-sync` means that no file sync runs. Fabric
does not silently start an embedded fallback.

The current connection-health fix remains first. The blocking-walk fix is step
one of this plan. PR 153 remains independent. Memory optimization is out of
scope.

The process split keeps one identity, one peer list, and one authorization
point. `fabric-sync` never parses `peers.toml`. It names a peer to the daemon.
The daemon resolves that name, applies the `sync` permission, and either opens a
stream or returns a structured refusal.

`syncs.toml` stays beside `peers.toml` in the existing config directory. The
plan adds no config root and no `fabric-sync.toml`. The files stay separate so
the config-format migration and the process migration remain independent.

Format-preserving `peers.toml` upserts remove the data-loss reason against a
literal one-file config. They do not make that move part of this extraction.
Combining the file move with the process move would mix two migrations and make
each rollback depend on the other.

If Nathan later selects one literal file, do it in a separate change after the
upsert work lands. The daemon must parse the complete file and send validated
sync entries through IPC. `fabric-sync` must still not parse peer policy. Until
then, two sibling files provide one Fabric-owned config authority without a
second companion config surface.

## The permanent latency test

The first development commit adds one end-to-end property test. That commit is
run against today's code and must fail before production code changes.

The test starts two real Fabric nodes. The target node uses one Tokio worker so
one blocked driver cannot be hidden by test-machine core count. A real exec
child emits flushed sequence records every 10 ms. At the same time, a configured
sync entry performs continuous real scans with a debug-only 500 ms walk hold.

The producer includes its source time in each record. Over a five-second window,
the test checks both facts:

- The source interval stays below 50 ms. This proves that the child kept writing.
- The delivery interval stays below 150 ms. This proves that sync did not delay
  pipe readiness.

Today's in-process walk blocks the only runtime driver for 500 ms, so the second
check fails. Moving the blocking phase to the blocking pool makes it pass. The
test command, workload, window, and bounds do not change during extraction.
After extraction, the hold runs in `fabric-sync`, while the measured pipe stays
in `fabric`.

The injected hold makes the failure deterministic. The assertion is still the
external property: sync activity must not affect pipe delivery latency. The test
does not inspect a task type, a thread name, or a blocking-pool counter.

## Transport decision

I agree with the daemon-client design. The daemon continues to own iroh, QUIC,
mux connection recovery, identity, peer resolution, and authorization.
`fabric-sync` uses an owner-only Unix socket to request an authenticated sync
stream.

The bridge must carry raw `fabric/sync/1` bytes. The generic resumable tunnel is
not suitable. It would add a second framing and replay layer around a sync wire
protocol that already retries complete sessions.

The outbound path is:

```text
fabric-sync -> local Unix bridge -> fabric -> shared mux stream -> remote fabric
```

The inbound path is:

```text
remote fabric -> shared mux stream -> local fabric -> Unix bridge -> fabric-sync
```

This keeps one QUIC connection and the current routing behavior. It adds one
local relay and one Unix-socket transfer for every byte in each direction. That
adds copies, scheduling, and some throughput cost. A separate endpoint would
avoid the local relay, but it would add another identity, peer policy, QUIC
connection, route state, and recovery system. Those costs violate the selected
security boundary.

The throughput cost is not measured because the bridge does not exist. Before
activation, the same two-host workload measures both paths over a 10-minute
window. It reports content bytes, wire bytes, wall time, daemon CPU,
`fabric-sync` CPU, and peak resident memory. The report gives every delta.

The bridge path must deliver at least 90 percent of the embedded path's content
throughput during that fixed window. This threshold is set before measurement.
If the bridge loses more than 10 percent, work stops before activation. The
transport decision then returns to Nathan through Silber.cos. CPU and memory
remain reported costs, but this plan does not set their limits.

## Ownership

The `fabric` CLI remains the only command surface. It writes `syncs.toml` and
validates it with `peers.toml`. The shared config library keeps the existing
syntax and selector rules.

`fabric-sync` reads `syncs.toml`. It does not read identity material,
`peers.toml`, address hints, or permission lists. Unknown selectors come back
from the daemon as structured transport errors. `fabric doctor` also validates
all selectors directly, even when the companion is absent.

The companion asks the daemon to resolve wildcard and named selectors. The
daemon returns opaque peer keys, labels, and roaming state. It does not return
address hints or policy. The daemon also supplies its public NodeID as the stable
sync author. The companion never reads the private identity.

`fabric-sync` owns folder watchers, manifests, scan caches, content held for
reconcile, and durable sync state. It keeps the existing paths under
`<home>/sync/<entry>/`. `state.json` and `log.jsonl` stay authoritative. No copy
or state-root migration occurs.

An owner lease under `<home>/sync/` prevents the daemon and the companion from
opening sync state together. The legacy daemon acquires the same lease before
the extraction activates. A process must hold the lease for its complete engine
lifetime.

The daemon owns the operator report. `fabric status`, `fabric sync ls`, and
`fabric doctor` remain the commands that users run. The daemon asks the
companion for a bounded status snapshot. It combines that snapshot with the
configured entry list.

If the companion does not answer, the daemon still returns every configured
entry. Each entry says `runtime=unavailable` and gives one reason: not installed,
not running, timed out, incompatible, or state lease busy. It never returns an
empty list for configured syncs.

## Lifecycle

The OS service manager owns both processes. The daemon does not spawn the
companion.

Linux gets an independent `fabric-sync.service`. It starts after
`fabric.service` and restarts on failure. `fabric.service` does not require it.
A companion failure cannot stop or restart the daemon.

macOS gets an independent `com.compoundingtech.fabric-sync` LaunchAgent. The
companion retries its local daemon connection because launchd has no ordering
contract between the two agents. Its failure does not unload the main agent.

`fabric service install`, status, and uninstall manage and report both services.
The installer starts the daemon first and the companion second. Uninstall stops
the companion first so it releases the state lease.

If `fabric-sync` dies during a pass, the Unix bridge and remote stream close.
The daemon continues all non-sync services. The existing atomic snapshot and
append-log recovery restore the last durable state when the companion restarts.
The normal sync retry then converges the incomplete pass.

The daemon never falls back to an embedded engine after a companion failure.
Automatic fallback could create two state owners after a slow or partitioned
shutdown. Recovery is a companion restart or a complete two-binary rollback.

The updater stages and verifies both binaries before it replaces either path.
Each binary must report the same release version. A partial local update is safe
because the local handshake rejects an incompatible version before it forwards
bytes or opens sync state.

The rollback command runs the old binary on purpose because that binary is the
copy already proven on the machine. Therefore, new rollback logic cannot protect
the release that first introduces it. This is a recurring self-update property,
not an implementation exception.

A fabric-only transition release must reach each machine before that machine
receives its first paired archive. The transition release contains the complete
pair-aware rollback reader and the macOS supervisor. It contains no companion.
The reader restores both old processes when a companion existed. It removes the
companion binary and OS service when no companion existed. The first paired
archive is the later writer. This gate is per machine, so a roaming machine such
as Bluey first receives the transition release when it returns.

The reader-before-writer rule applies to every update artifact. If a new release
writes an artifact that an old rollback binary must read, the old binary must
understand that artifact before the writer reaches the machine. Service
definitions, generation records, durable state, and future install metadata all
follow this rule. A release plan must identify the reader for each new artifact
before it permits the writer.

### Paired-install rollback inventory

Every future paired-install change must add its machine effects to this table.
The rollback review answers both columns before a paired release.

| Item changed by install | Required rollback action | Harm if stale |
| --- | --- | --- |
| `fabric` binary | Restore the exact prior binary. | A bad candidate can keep every Fabric service down. |
| `fabric-sync` binary | Restore the prior binary, or remove it when none existed. | A mismatched binary fails the local handshake. An unwanted binary can restart later. |
| Main service definition | Restore or render a definition that the old binary accepts. | A new argument can make the old daemon fail at the next restart. |
| Companion service definition | Restore it when one existed. Otherwise, unload and remove it. | A definition pointing at an absent binary causes permanent restart churn. |
| Main and companion enablement | Restore each prior enabled or disabled state. | A disabled daemon does not return after login. An unwanted companion keeps retrying. |
| Git remote helper | Repair the relative helper link after restoring `fabric`. | The current relative link is safe. A future versioned target could call the wrong binary. |
| Sync IPC socket | Stop the companion and remove its socket before the old owner starts. Keep the shared run directory. | A stale socket can block bind or report a process that is gone. |
| State-owner lease | Stop every new owner before the old embedded owner starts. The unlocked lease file can remain. | A living holder prevents the restored daemon from opening sync state. |
| Durable sync state | Keep the schema readable by both releases. Do not rewrite state during binary rollback. | New-only state can make the restored engine fail or misread data. |
| Incoming staging files | Remove every uncommitted file on success and handled failure. | Hidden executable bytes waste disk and invite an unsafe manual recovery. |
| Rollback copies | Keep one exact matched prior set through the rollback window. Prune older complete sets later. | An unmatched newest set can select a mismatched pair. Unlimited sets waste disk. |
| Detached supervisor job and plist | Remove both after success and rollback. | A stale verifier can roll back a later healthy build. |
| Update generation record | Replace it atomically before each install. A supervisor acts only on an exact match. A stale or missing record makes it stop. | A stale verifier can revert a healthy later build. Guessing after a missing record has the same harm. |
| Service log files | No restore is needed. Keep bounded logs for diagnosis. | Unbounded logs waste disk. Existing bounded files are harmless. |

On macOS, the updater arms a transient launchd supervisor before the first pair
rename. The supervisor starts its timeout only after replacement is visible, so
a system sleep cannot consume the recovery window before the update starts. It
removes its plist and loaded job after success or rollback.

## Compatibility contracts

The remote `fabric/sync/1` wire stays byte-compatible during the complete mixed
fleet window. Existing error replies already cross old and new implementations.
An incompatible future wire change requires a new ALPN. It must not reinterpret
version 1 bytes.

The local bridge uses a new `fabric/sync-ipc/1` contract. Its bounded header
contains a magic value, protocol version, direction, request ID, authenticated
peer ID, optional display label, and structured result. The daemon supplies the
peer identity on inbound sessions. The companion never accepts a peer claim
from an untrusted local caller.

The socket uses the same local-user trust boundary as the current control
socket. The companion verifies the connecting user and a daemon-instance nonce.
A local user who owns the synced folders remains trusted.

The bridge negotiates before raw wire bytes begin. Different local major
versions return `incompatible` and close without touching state. Additive fields
use defaults. Unknown required fields or message kinds are errors.

The durable state schema remains readable by the release before and after the
cutover. Extraction makes no state-format change. A later incompatible state
change needs its own migration and rollback plan.

## Mixed-fleet behavior

Mixed operation is supported. It does not need a flag day.

An old daemon still runs its embedded engine and speaks `fabric/sync/1`. A new
daemon forwards the same wire protocol to its local companion. Therefore each
pair works in both directions:

- old to old stays unchanged;
- new companion to old daemon uses the old inbound server;
- old daemon to new companion uses the new inbound bridge;
- new companion to new companion crosses both local bridges.

The mixed-fleet integration test runs all four cases. It checks two-way file
changes, tombstones, equal final digests, zero drift, and an explicit unavailable
error when the new companion stops.

Rollout updates one machine at a time. Each step checks two-way sync with an old
peer before the next machine changes. Rollback stops the companion, restores the
previous binary pair, and starts the old daemon. The old daemon reads the same
durable sync state.

## Staged delivery

Each step is one pull request. Each step leaves main green. Each step has an
independent revert. No step includes allocation optimization.

### 1. Protect daemon pipe latency

First, commit and run the permanent latency test against current main. Record
the red delivery gap and its five-second window.

Then move each synchronous scan, materialization, and persistence phase to one
bounded blocking task. Snapshot the required engine state under short locks.
Release internal node and disk locks before blocking filesystem work. Reacquire
them to validate the saved mutation generation and apply the result. Keep the
per-entry operation guard so concurrent passes retain their current order.

Do not enable Tokio's experimental eager driver handoff. It needs
`tokio_unstable` and treats a long poll instead of removing it.

The property test must pass without changing its bounds or workload. Existing
sync convergence, local-edit, delete, and crash-recovery tests must also pass.
Reverting this pull request restores the old execution model without changing
any format.

### 2. Make sync process-neutral and enforce one owner

Move the sync validation and engine types behind process-neutral library APIs.
Replace the dependency on the daemon log target with a sync-owned target. Pass
the few required paths instead of giving the engine access to daemon state.

Add the lifetime state-owner lease. The embedded engine acquires it first, so
this step changes no production owner. A second engine must fail loudly before
it starts a watcher or reads state.

The existing engine and all wire tests stay on the embedded path. Reverting this
pull request removes only the new boundary and lease.

### 3. Freeze and test the local bridge

Add the versioned IPC types and a raw Unix-stream relay. Add bounded handshake,
status, shutdown, and error messages. Enforce owner-only socket permissions and
frame limits.

Use an in-process reference server only in tests. Run the existing
`SyncTransport` conformance suite through the IPC bridge. No production request
selects this path yet.

Reverting this pull request removes a dormant protocol and does not touch sync
state.

### 4. Ship the companion binary as a diagnostic

Add `fabric-sync` as a second binary in the same Cargo package. It supports
`--version` and a read-only `--check` mode first. Check mode validates
`syncs.toml`, the state paths, the owner lease, and daemon IPC compatibility. It
does not start watchers or mutate state.

Release archives contain exactly `fabric` and `fabric-sync`. The installer and
updater verify both members, both hashes, and equal versions. Rollback stores a
matched pair.

The main daemon still runs embedded sync. Removing the companion binary reverts
this step without changing behavior.

### 5. Add independent companion supervision and loud absence

Add the systemd unit and launchd agent. The companion starts in compatibility
standby when the installed daemon does not grant it the sync-owner role. Standby
does not acquire the state lease or start watchers.

Extend service status, `fabric status`, `fabric sync ls`, and doctor with the
sync runtime state. Doctor reads configured entries even when either process is
down. A configured entry can never appear as “no sync entries” because its
runtime is absent.

The embedded engine remains the production owner in this step. Reverting the
pull request removes the second service and the new report fields.

### 6. Run the full engine behind both bridge directions

Put `SyncEngine` in `fabric-sync`. Its `SyncTransport` implementation requests
outbound raw streams from the daemon. The daemon forwards authenticated inbound
streams to the companion. The daemon keeps the remote ALPN and permission gate.

Run the permanent latency test unchanged. Run the four-way mixed-fleet matrix.
Run crash tests at snapshot commit, log append, inbound prepare, materialization,
and process exit. Also run the 10-minute throughput comparison and publish its
window and deltas.

Production remains on embedded ownership until the next step. Reverting this
pull request removes a tested but inactive engine path.

### 7. Activate the process boundary

Stop constructing `SyncEngine` in the main daemon. The daemon always delegates
sync to the companion. If the companion is absent, Fabric serves shell, exec,
Git, send-file, echo, and generic tunnels normally.

A local configured entry reports `runtime=unavailable`. An inbound old peer gets
an explicit sync-unavailable wire reply. An outbound companion request gets a
structured local error. Neither case is classified as network weather.

Deploy one machine first and prove both mixed directions. Continue one machine
at a time. The release gate remains with Silber.cos. Reverting the complete
binary pair restores the embedded owner and reads the unchanged state.

No paired archive may reach a machine until that machine runs the fabric-only
transition release and has proved its rollback reader. Bluey follows the same
gate when it returns.

### 8. Remove the dormant embedded engine

After every non-roaming supported machine has passed the mixed window and
rollback period, remove the embedded startup and daemon-only sync transport
implementation. Keep the shared wire server, config validation, bridge, status
aggregation, and unavailable reply in `fabric`.

A roaming peer qualifies when either of two proofs passes. It can pass the live
mixed window when it returns. It can also pass the automated two-direction
mixed test against its exact deployed build. An absent roaming peer does not
block this cleanup. If it stays behind indefinitely, it keeps embedded sync and
uses the compatible `fabric/sync/1` wire path. Status continues to name its old
build, and the machine updates when it returns. Step 8 does not remove that wire
compatibility.

Run the permanent latency test unchanged one final time. This removal is a code
cleanup only. Reverting it does not change the active process architecture.

## Activation gates

Each implementation pull request merges when its required tests and CI pass.
Silber.cos owns the step 7 activation gate and every release and deployment
gate.

Before any release, run every ignored test that needs a real machine. Record
the machine, commit, command, and result.

| Platform | Test | Exact proof |
| --- | --- | --- |
| macOS | `a_real_launchd_supervisor_rolls_back_and_removes_its_job` | An isolated real launchd job detects a deliberately broken pair. The pair-aware reader restores both members. The plist and loaded job disappear. |
| Linux | `a_real_systemd_supervisor_rolls_back_and_removes_its_jobs` | An isolated real systemd timer starts its service over a deliberately broken pair. The pair-aware reader restores both members. Both units disappear. |

These tests use isolated paths and services. They do not prove the production
service names, install paths, home permissions, or service definitions.

Before the transition release, run one rollback through the actual production
service on Linux and macOS. Use hetz first and Silber second. Record every
observed state and any manual cleanup. Do not use Bluey for this exercise.

Test the recovery route from the actor who will use it before each deliberate
outage. A reachable host and an open SSH port do not prove that the actor can
log in. Fabric is the only working route to hetz for every agent and for
Silber.cos. If Fabric fails there and cannot recover itself, Nathan must recover
hetz personally. Silber.cos has local shell access on Silber and can repair that
machine by hand.

Both production rollback exercises passed on 2026-09-05 at exact main
`ebfd0ec`. On hetz, the broken pair installed at 03:33:02Z. The systemd reader
restored exact main at 03:34:00Z, and Fabric answered externally at 03:34:14Z.
The observer depended on Fabric because no agent or Silber.cos could complete
an SSH login. The exercise then restored fleet build `0.2.1+48208e4`.

On Silber, the broken pair installed at 03:46:14Z. The launchd reader restored
exact main on disk at 03:47:18Z. The control socket answered at 03:47:27Z. The
transient job and plist disappeared, and the reader removed the new companion.
Local shell observation stayed available. The exercise then restored fleet
build `0.2.1+48208e4`. Doctor passed on both hosts after the exercises.

The deployed `48208e4` reader also received the exact paired archive in an
isolated home. It accepted the checksum, then refused the archive because it
expected exactly one `fabric` member. It changed no executable or staging file.
This clean refusal enforces the Release A order for that deployed reader. It is
not a property of the Release A pair-aware reader.

Add each future real-machine test to this named list when the test is added.
The measurement-only ignored tests are not release gates unless this list names
them.

The process-boundary release needs all of these results:

- The permanent five-second latency test passes with its original 150 ms bound.
- All four mixed-fleet cases pass in both directions.
- A stopped, absent, and incompatible companion leaves the daemon healthy and
  appears in status and doctor.
- A crash in each durable phase recovers without dual state ownership.
- The bridge keeps at least 90 percent of embedded content throughput during the
  fixed 10-minute comparison. A larger loss stops activation and reopens the
  transport decision with Nathan.
- The Release A archive contains exactly `fabric`. The deployed `48208e4`
  reader refuses a paired archive without changing the machine.
- Step 7 restores an archive with exactly `fabric` and `fabric-sync`. The
  updater verifies that matched pair.

This plan changes where sync executes. It does not claim that the connection
cache caused Nathan's earlier pauses, and it does not optimize sync memory.
