---
name: fabric
description: Install, configure, operate, verify, and troubleshoot the Fabric Rust CLI and daemon for trusted cross-machine socket transport over iroh. Use when Codex needs to connect machines with Fabric, manage peer trust, expose or dial Unix/TCP/exec services, configure the managed user service, diagnose reachability, or develop and validate the compoundingtech/fabric repository.
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
3. On machine A, run `fabric add <machine-b-node-id> machine-b`.
4. On machine B, run `fabric add <machine-a-node-id> machine-a`.
5. Run `fabric up` on both machines.
6. Run `fabric status` and `fabric ping <peer-name>` on each machine.

Require mutual allow-list entries. A remote daemon rejects a NodeID that is not
in its own peer config even when the dialing machine trusts it.

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
key. Treat `peers.toml` and `config.toml` as declarative authorization and
service configuration.

## Troubleshoot

Check these surfaces in order:

1. Run `fabric --version` on both machines and align versions.
2. Run `fabric status` to confirm the daemon, NodeID, trust entries, and peer
   reachability.
3. Run `fabric peers` and confirm mutual trust.
4. Run `fabric ping <peer>` before debugging an exposed application protocol.
5. Inspect `<home>/logs/daemon.log` for a daemon started by `fabric up`, or
   `<home>/logs/service.out.log` and `service.err.log` for a managed service.
6. Confirm that expose and dial commands use the same protocol string.
7. Run `fabric restart` after a persistent transport failure; use
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
