# Changelog

All notable changes to fabric are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); fabric is pre-1.0 and
EXPERIMENTAL, so on-disk formats and the CLI may change without notice.

## [Unreleased]

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
