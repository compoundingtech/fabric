# Changelog

All notable changes to fabric are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); fabric is pre-1.0 and
EXPERIMENTAL, so on-disk formats and the CLI may change without notice.

## [Unreleased]

### Added

- **Durable connection telemetry.** `fabric status` now reports, per peer, how
  many times a session lost its transport, how many came back, how many gave
  up, and how long the reconnect took. The counters persist in
  `<state>/telemetry.json`, so they survive a daemon restart.

  Fabric already logged a line for each loss, reconnect attempt, and resume. A
  line reconstructs one incident by hand and cannot answer whether resumption
  works in daily use, because the answer needed a grep over megabytes of log
  and died at the next rotation. Three things the log could not report are now
  recorded: a durable count, the measured total reconnect time (the log carried
  the backoff delay before the next attempt, which is a different number), and
  the path in use beside each loss and each resume. "It came back" and "it came
  back on the relay" are different outcomes.

  The liveness probe also keeps what it measures. It computed a round trip time
  and a path on every probe and discarded both, so comparing the direct path
  against the relay meant parsing days of log text. Latency is now summarized
  per peer and per path in bounded histogram buckets, which keeps the cost flat
  on a daemon that runs for weeks. Nothing reads these numbers to make a
  routing decision yet; this change only stops throwing them away.

- **Production sync scan ledger.** `fabric sync ls` and its JSON form now expose
  per-entry monotonic full-scan, exact-noop inbound, and guarded-inbound
  transaction counters. Operators can prove scan-free convergence and the
  expected guarded scan transaction without relying on test-only instrumentation
  or CPU inference.

- **Truthful sync status.** `fabric sync ls` now reports logical Present,
  Tombstone, and observed-on-disk counts plus explicit missing, unexpected, and
  content-mismatch drift. `fabric sync ls --json` provides the same state as a
  stable machine-readable array.

- **Safe st2 catalog sync recipe.** The README now provides a copy-pasteable,
  two-entry positive allow-list for declarations and bus data, with explicit
  machine-local PTY/runtime exclusions and an ordinary resource/message
  propagation check. A provisioning test executes and locks the documented CLI
  and resulting policy/include semantics.

- **Deterministic CI stability gate.** Pull requests and main pushes now run
  library/model tests, explicit two- and three-machine temp-folder simulation,
  provisioning, real-iroh sync/restart, and the serial local multi-node daemon
  slice under bounded timeouts.

- **`fabric exec <peer> -- <cmd...>`** — non-interactive remote command
  execution: run a command on a trusted peer with no tty, stream its stdout and
  stderr back on separate streams, and exit with the remote command's exit code.
  The scriptable counterpart to `shell`. Security mirrors `shell` and is
  **default-deny per machine** (`allow_exec`, opt-in via `--allow-exec` on
  `up`/`daemon`/`service install`), under a separate ALPN (`fabric/exec/0`) so
  allowing exec never implies shell. `fabric status` now reports `exec allowed`.

### Removed

- **The fixed 300 MiB endpoint-recycle limit.** The daemon used to recycle its
  iroh endpoint whenever RSS crossed 300 MiB. The memory was never in the
  endpoint, so recycling did not reclaim it and simply ran again: one machine
  logged 1,206 threshold crossings, 645 recycles, and 599 that its own follow-up
  sample recorded as ineffective. Every recycle tore down live shell and tunnel
  sessions, which is how a held remote shell died mid-session. RSS is now
  observed and reported on each new peak, and nothing interrupts the daemon for
  memory alone. A fixed number also cannot know a healthy working set for a
  given network size; if memory grows, that is now an operator's call.

- **The default memory ceiling in generated services.** `--memory-max-mb` is
  unset by default, so a generated systemd unit carries no `MemoryMax` and a
  generated launchd plist carries no resident-set limits. On Linux `MemoryMax`
  is a hard kill and on macOS `ResidentSetSize` biases the kernel to reclaim
  from Fabric first, and neither was justified while a healthy working set is
  unmeasured. An operator who has measured one can still declare it.

### Changed

- **Detached shells are retained for 15 minutes, not 60 seconds.** A held remote
  shell now survives a closed lid over lunch; previously it did not survive a
  coffee break. The number comes from measured cost rather than taste: an idle
  detached session buffers 0 bytes across a full window, so holding one costs a
  session struct and a PTY process and nothing that grows with time, while a
  session still producing output grows at whatever the remote writes — about
  19 KB/s for a pathological loop, roughly 17 MB over this window. Past the
  window the behaviour is unchanged and already proven: the client reports
  `remote shell could not resume`, names the expired session, and exits
  non-zero. This TTL remains the only backstop against a runaway remote process,
  because the replay buffer has no cap of its own, so raising it much further
  wants that cap first.

- **Release archives have an enforced one-file contract.** Each platform tarball
  must contain exactly one member named literal `fabric` (not `./fabric`), and
  the lockout-safe upgrade runbook verifies that shape before extraction.

- **Sync publishers must stage outside the synced folder.** The file-sync
  documentation now makes explicit that every watcher-visible path matched by
  an entry is a durable logical key. Temporary, backup, and partial files must
  be assembled outside the configured sync folder before only canonical final
  paths are moved into place; a matching sibling temp name under `catalog`
  policy is propagated and restored like any other catalog key.

- **Equal-version sync conflicts preserve updates.** Higher logical versions
  still always win, but at the same version a Present update now wins over a
  Tombstone delete before deterministic author/content-hash tie-breaking. A
  later delete advances its logical version and wins normally.

- **Roaming self-heal.** The daemon now actively echo-probes each trusted peer
  (`FABRIC_PEER_HEALTH_SECS`, default 20s) and, on repeated failures, drives
  recovery (drop tunnels → iroh `network_change()` re-discovery → endpoint
  recycle) — so a peer that roams to a new network no longer stays unreachable
  both ways until a manual daemon restart. Previously the daemon only reacted to
  *local* network changes and never checked per-peer reachability. Each probe
  also emits latency + direct/relay telemetry.

### Fixed

- **Mixed-version shells no longer fail on the first frame.** `fabric/shell/0`
  is a wire contract with every released Fabric and carries one-shot raw
  framing only; the resumable shell moved to its own `fabric/shell/1` ALPN.
  Previously the resumable path reused the legacy ALPN, so an older peer met
  tunnel framing it could not parse, rejected the first `SERVER_OUTPUT` frame as
  an unknown tunnel frame, and reconnected forever. A client that cannot
  negotiate shell/1 now falls back to shell/0, so upgrading one host at a time
  works. Signal exits restore the caller's exact terminal settings.

- **One absent peer no longer recycles the shared endpoint.** Peer health probes
  the whole round before recovering anything, so a roaming peer that is simply
  away neither drops everyone's tunnels nor escalates to a global endpoint
  recycle while other peers are answering.

- **Endpoint recycles no longer kill live sessions.** A recycle is refused while
  shell or tunnel sessions are attached; a session dying mid-command is worse
  than whatever prompted the recycle. Detached-but-resumable sessions do not
  pin the endpoint.

- **Shell recovery is visible in the daemon log.** Connection loss, reconnect
  attempt with its delay, and resume or failure are logged with the peer and
  endpoint generation. The log previously showed the drop and nothing about
  getting the session back.
- **Converged periodic syncs no longer hash the full tree twice.** An inbound
  peer whose manifest exactly matches the local node can bypass both folder
  scans when the local content store is complete. Differing manifests, missing
  content, and every potentially mutating reconcile retain the guarded
  pre-merge and completion scans that protect local archive/delete intent.


- **Inherited catalog tombstones no longer leave folders divergent.** Under
  catalog policy, any surviving physical copy now advances an inherited
  Tombstone to a higher Present version and supplies its content over the wire,
  converging every materialized folder and persisted restart. Bus policy keeps
  its existing delete semantics and removes the same stale bytes.

- **macOS startup no longer blocks forever in CoreWLAN.** The optional Wi-Fi
  transmit-rate lookup is now bounded to 250 ms and cached per interface; a
  wedged synchronous CoreWLAN XPC request falls back to unknown speed without
  preventing iroh or Fabric's control socket from binding. Repeated network
  refreshes cannot spawn duplicate blocked workers. The launchd install path
  also uses non-destructive `kickstart`, avoiding a readiness race that killed
  the PID created moments earlier by `bootstrap`.

- **Linux file reads could self-trigger an unbounded sync storm.** Linux
  inotify reports file opens as access events; treating every watcher event as
  a mutation meant Fabric's own scans could wake all peers again. Watchers now
  forward only create/modify/remove events, continuous write streams are
  coalesced into bounded two-second windows, queued inbound no-op sessions
  reuse a durable pre-merge scan, and sync-accept path snapshots are sampled
  1-in-128 in the default validation log. A Linux read-vs-write regression and
  a three-node, 2,000-file continuous-mutation stress gate cover the recovery.

- **Repeated `fabric exec`/`shell` calls exhausted daemon file descriptors.**
  Replacing a command's deterministic local dial socket removed its pathname
  but left the old listener task and file descriptor alive. The daemon now owns,
  cancels, and joins each replaced accept loop while allowing already-accepted
  sessions to finish, and deterministically closes all dial listeners on
  shutdown.

- **Bus archive/delete intent lost to an inbound reconcile.** An atomic local
  rename could remove an inbox file while Fabric's filesystem watcher was still
  inside its 150 ms debounce window; an inbound sync in that gap could
  materialize the stale Present entry and undo the archive. Inbound sessions now
  carry an explicit last-observed disk receipt across reconciliation, durably
  record local changes before merging, and guard final materialization. This
  preserves deletes in both pre-merge and post-scan races while still accepting
  genuinely new remote files, without deadlocking simultaneous peer syncs. The
  manifest and observed-disk receipt are committed together in one authoritative
  state file, so the same guarantee survives daemon restarts and time spent with
  a sync entry disabled.

- **Peer-config split when the daemon runs with `--home <default-root>`.** The
  managed service always launches the daemon as
  `--home ~/.local/share/fabric`, which made it read `peers.toml` from under that
  `--home` while the interactive CLI reads `~/.config/fabric/peers.toml`. A
  default-home `fabric add` then migrated peers to the config dir and removed the
  in-`--home` copy, silently leaving the daemon with **zero peers**: `ping`/
  `status`/`shell` still worked (in-memory allow-list) but every **dial** failed
  with `unknown peer` — taking down consumers like `st sync` (endless
  `fabric pull failed … re-dialing`) with a real lockout risk on peers with no
  fallback access. An explicit `--home`/`FABRIC_HOME` equal to the default state
  root now resolves peers/config exactly like the no-argument default; a
  genuinely different `--home` keeps the isolated config-under-root layout. See
  the new [Troubleshooting](README.md#troubleshooting) note.

## [0.2.0] - 2026-07-20

### Added — `fabric sync` (generic file-sync primitive)

fabric now owns file sync: a declarative, daemon-managed, fs-watched primitive
that keeps a folder converged with trusted peers over iroh.

- **`syncs.toml`** — a new authoritative, hand-editable, reload-able config file
  (sibling of `peers.toml`). Each `[[sync]]` entry is
  `{name, folder, peers, policy, include?}`. `name` is the shared logical key
  (same name on two machines = the same sync); `peers = "*"` follows the
  `peers.toml` allow-list; `policy` is a preset (`catalog` or `bus`); optional
  `include` globs scope which files sync.
- **Reconciliation core** — versioned per-file state with newer-wins (Lamport
  version + deterministic author tie-break, never a wall clock). Merge is a
  semilattice join (commutative/associative/idempotent), so convergence and
  echo/loop-freedom are structural, proven by property tests.
- **Policies** — `catalog` = union + newer-wins + never-delete-on-peer + no-sweep
  (a local delete is restored; decommission is expressed as an edit). `bus` =
  union + newer-wins + tombstone deletes (modelled; sweep TTL not yet wired).
- **Swappable transport** — the sync semantics sit above a transport seam. The
  on-wire backend runs over a reserved `fabric/sync/1` ALPN on fabric's own
  connections and is verified to reach the exact same state as the pure
  reference reconcile.
- **Engine** — the daemon watches each folder (near-instant, not a poll), scans
  changes, reconciles with peers, and materializes results to disk. Manifests are
  persisted per entry so logical versions survive daemon restarts.
- **CLI** — `fabric sync add/ls/rm/reload`. `add` is a convenience writer over
  `syncs.toml` (like `fabric add` → `peers.toml`); `reload` applies the file to a
  running daemon (like `reload-peers`).
- **Control ops** — `SyncReload` and `SyncStatus`.

## [0.1.x] — iroh socket-facade foundation

The shipped foundation before `fabric sync`, released as tags `v0.1.0`–`v0.1.7`.
Reconstructed from git history; not a per-patch breakdown.

### Added
- Local daemon that hides iroh behind local Unix sockets: `expose`
  (`--socket`/`--tcp`/`--exec`) and `dial` with resumable, offset+ACK framed
  tunnels that survive a transport reconnect without reopening the local service.
- Mutual allow-list trust via `peers.toml` (authorized-keys file), enforced at
  the iroh `after_handshake` hook; `add`/`remove`/`peers`/`reload-peers`.
- Built-in ACL-gated `ping` echo and reachability in `status` (direct/relay/mixed
  transport path, round-trip latency); build version in `--version` and `status`.
- Opt-in remote `shell` for trusted peers (`--allow-shell`, off by default).
- Lockout-safe `restart` via a detached helper (safe to run over `fabric shell`).
- `fabric service install` — managed systemd/launchd user service with
  restart-on-failure and a configurable memory backstop.
- Provision-and-go: pre-generate an identity (`key gen`) and deploy a complete
  `peers.toml`; `reload-peers` applies it without an interactive step.
- Release installer (`install.sh`) with prebuilt-binary and `--from-source`
  paths.

### Changed
- Renamed/transferred to the `compoundingtech` GitHub org
  (`github.com/compoundingtech/fabric`; launchd label `com.compoundingtech.fabric`).
- Roaming reliability + Hetzner RSS mitigation: in-process iroh endpoint recycle,
  health poller, network-change debounce, bounded server tunnel sessions, and an
  RSS-triggered recycle with a raised (1 GiB) managed-service memory ceiling.
