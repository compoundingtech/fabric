# Changelog

All notable changes to fabric are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); fabric is pre-1.0 and
EXPERIMENTAL, so on-disk formats and the CLI may change without notice.

## [Unreleased]

### Added

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

### Changed

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
