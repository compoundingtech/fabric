---
name: fabric
description: Install, configure, operate, verify, and troubleshoot the Fabric Rust CLI and daemon for trusted cross-machine socket transport over iroh. Use when connecting machines with Fabric, managing peer trust, exposing or dialing Unix/TCP/exec services, syncing folders between trusted peers (fabric sync), configuring the managed user service, diagnosing reachability, or developing and validating the compoundingtech/fabric repository.
---

# Fabric

Use `fabric` as a local socket facade for trusted cross-machine services. Keep
iroh addresses, QUIC, relays, and peer authorization inside Fabric; consumers
should only connect to the local Unix socket or TCP listener returned by
`fabric dial`.

## Install

Install a release on macOS or Linux:

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh
fabric --version
```

Expect the binary at `~/.local/bin/fabric` unless `FABRIC_BIN_DIR` or `BIN_DIR`
overrides it. Add that directory to `PATH` when necessary.

For a source checkout, run `./install.sh`, `make install`, or
`cargo install --path .`. Use `cargo run -- <command>` without installing during
development.

## Connect Two Machines

Use the same Fabric release on both machines.

1. Run `fabric id` on each machine.
2. Exchange the two stable NodeIDs over a trusted channel.
3. Write each remote NodeID to the local authoritative `peers.toml` as an
   `[[peers]]` entry with required `id` and optional unique `name`.
4. Run `fabric up` on both machines. Use `fabric reload-peers` instead when the
   daemon is already running.
5. Run `fabric status` and `fabric ping <peer-name>` on each machine.

Require mutual allow-list entries. A remote daemon rejects a NodeID that is not
in its own peer config even when the dialing machine trusts it.

Treat `fabric add <nodeid> [name]` as a convenience writer, not a provisioning
requirement. Automated provisioning should install the complete peer file.

Treat a successful `pong` as the basic connection check. It reports round-trip
latency and, when available, the `direct`, `relay`, or `mixed` transport path.
Do not require a direct path when a healthy relay path is available.

## Expose And Dial Services

Expose exactly one backend per protocol:

```sh
fabric expose <protocol> --socket <unix-socket>
fabric expose <protocol> --tcp <host:port>
fabric expose <protocol> --exec [--max-children N] -- <command> [args...]
```

Use argv after `--` for exec exposures; do not pass a shell command string.
Exposures persist by default. Add `--ephemeral` only for a short-lived
exposure. Remove a persisted exposure with `fabric unexpose <protocol>`.

Create a local Unix socket on the dialing machine:

```sh
fabric dial <peer> <protocol>
```

Or create a local TCP listener:

```sh
fabric dial <peer> <protocol> --tcp <local-host:port>
```

Give the printed socket path or TCP address to the consumer. Do not make the
consumer import iroh types or implement Fabric's peer checks.

## Sync Folders

Use `fabric sync` to keep a folder converged with trusted peers. Declare sync
entries in the authoritative `~/.config/fabric/syncs.toml` (a sibling of
`peers.toml`, hand-editable and provisionable); the running daemon watches each
folder and syncs changes over the reserved `fabric/sync/1` ALPN, gated by the
same peer allow-list.

Each `[[sync]]` entry is `{name, folder, peers, policy}` plus optional `include`
globs. Use the SAME `name` on every machine that shares a sync; local `folder`
paths may differ. `peers = "*"` means every peer in `peers.toml`. Choose
`policy = "catalog"` (union, newer-wins, never deletes on a peer — a deletion is
restored, decommission via an edit) or `policy = "bus"` (adds tombstone deletes).

```sh
fabric sync add <folder> --name <name> --peers "*" --policy catalog
fabric sync ls
fabric sync rm <name-or-folder>
fabric sync reload
```

`fabric sync add` writes `syncs.toml` and reloads; `fabric sync reload` applies a
hand-edited or provisioned file without a restart. Prefer newer-wins on the
logical version, not filesystem mtime. A fresh box is declare-then-reload, no
interactive step.

## Operate The Daemon

Use `fabric up` for the background daemon and `fabric down` to stop it. Use
`fabric up --foreground` when another supervisor owns restarts.

Install a native per-user service when persistent OS supervision is wanted:

```sh
fabric down
fabric service install
fabric service status
```

Use `fabric service uninstall` to remove the service artifact without deleting
identity, peer, config, or log data.

Do not migrate a service-manager identity or replace a live managed daemon
through that daemon's own `fabric shell`. Build and verify the replacement,
preserve a rollback binary, and use a planned window with an independent
recovery path. The service-manager swap restarts Fabric and severs its current
remote shell.

Upgrade the binary lockout-safe: install atomically (`install.sh` uses a temp
file plus rename, so it swaps under a running daemon without `ETXTBSY`) at the
exact path the daemon runs from, keep a `fabric.rollback` copy, then restart
through the SUPERVISOR so the restart survives the shell dropping —
`systemctl --user restart <unit>` for a systemd user service,
`launchctl kickstart -k gui/$UID/<label>` for a macOS LaunchAgent, or
`fabric restart` only for a plain `fabric up` daemon. Never use `fabric restart`
under systemd/launchd (it orphans the daemon from the supervisor), and never a
naked `fabric down` then `fabric up` over a remote shell (a dropped shell mid-swap
is a lockout). Verify from a fresh shell: `fabric --version`, `fabric status`,
`fabric sync ls`. See the README "Upgrading Fabric Safely" section for the full
procedure.

Keep remote shell disabled unless the user explicitly opts in. Enable it with
`fabric up --allow-shell` or `fabric service install --allow-shell`. Every
trusted peer can obtain a shell while it is enabled. Disable it with
`fabric restart --no-allow-shell`.

## Respect State Boundaries

Use `~/.local/share/fabric` as the default runtime home and
`~/.config/fabric/peers.toml` as the default peer file. When `--home <dir>` or
`FABRIC_HOME=<dir>` is set, keep identity, config, peer, control socket, logs,
and command invocations on that same home.

Treat `identity.toml` as a secret because it contains the persisted private
key. Treat `peers.toml` as the authoritative authorization file and
`config.toml` as daemon policy and exposure configuration. Reject duplicate
NodeIDs, duplicate or empty names, and address hints whose ID differs from the
peer ID.

## Troubleshoot

Check these surfaces in order:

1. Run `fabric --version` on both machines and align versions.
2. Run `fabric peers` to validate and inspect the peer file.
3. Run `fabric reload-peers` after changing that file on a running daemon.
4. Run `fabric status` to confirm the daemon loaded the expected trust entries
   and peer reachability.
5. Run `fabric ping <peer>` before debugging an exposed application protocol.
6. Inspect `<home>/logs/daemon.log` for a daemon started by `fabric up`, or
   `<home>/logs/service.out.log` and `service.err.log` for a managed service.
7. Confirm that expose and dial commands use the same protocol string.
8. Run `fabric restart` after a persistent transport failure; use
   `fabric restart --no-allow-shell` when shell must remain disabled.

Use `fabric addr` and `fabric add --addr-json` only for deterministic
same-machine tests or when an explicit address hint is intentionally required.
Normal cross-machine setup should use stable NodeIDs and iroh discovery.

## Develop The Repository

Keep the Cargo package and binary name `fabric`. The repository is
`https://github.com/compoundingtech/fabric`.

Before handing off code changes, run:

```sh
cargo fmt --check
cargo check --all-targets
cargo test --all-targets
```

Also scan owned source, docs, metadata, and help text for stale project names.
Do not treat similarly spelled identifiers inside third-party vendored or
minified bundles as Fabric branding.
