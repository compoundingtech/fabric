> Status: EXPERIMENTAL. Early spike / prototype -- the CLI, APIs, and on-disk formats will change without notice; no stability or security guarantees yet; not production-ready. Use at your own risk.

# fabric

fabric is a standalone Rust CLI and local daemon that hides iroh behind local Unix
sockets.

Consumer tools do not link to iroh, know NodeAddr formats, or open QUIC streams.
They ask fabric for a local socket connected to a remote service, then speak their
own protocol over that socket.

## Build

```sh
cargo build
cargo test
```

The binary is `target/debug/fabric` during development.

## Install

Fast path for macOS and Linux:

```sh
curl -sSf https://raw.githubusercontent.com/myobie/fabric/main/install.sh | sh
```

The remote installer downloads a matching prebuilt release binary into
`~/.local/bin/fabric`, prints the installed version, and fails if that version
does not match the targeted release. Ensure `~/.local/bin` is on PATH. To
install somewhere else, set `FABRIC_BIN_DIR` or `BIN_DIR`.

The remote installer does not silently fall back to source builds. If no
prebuilt binary matches your machine, run an explicit source install:

```sh
curl -sSf https://raw.githubusercontent.com/myobie/fabric/main/install.sh | sh -s -- --from-source
```

To pin a release:

```sh
curl -sSf https://raw.githubusercontent.com/myobie/fabric/main/install.sh | sh -s -- --version v0.1.7
```

From a cloned repo:

```sh
./install.sh
```

or:

```sh
make install
```

The cloned-repo installer builds the current checkout and copies the release
binary to `~/.local/bin/fabric`. It prints the actual installed version; this
path is intentionally for local checkout installs, not remote release installs.

Rust users can also install through Cargo:

```sh
cargo install --path .
```

This installs `fabric` to `~/.cargo/bin`, which rustup normally adds to PATH via
`~/.cargo/env`. Re-run the install command after local changes to update the
installed binary.

For quick development without installing:

```sh
cargo run -- <command>
./target/debug/fabric <command>
```

## State

By default fabric stores local runtime state in:

```text
~/.local/share/fabric
```

Use `--home <dir>` or `FABRIC_HOME=<dir>` to run multiple local nodes on one
machine.

The identity file contains the persisted iroh secret key. The public key is the
node's stable NodeID.

Trusted peers are declarative config. With the default home, fabric reads and
writes:

```text
~/.config/fabric/peers.toml
```

If `--home <dir>` or `FABRIC_HOME=<dir>` is set, fabric reads and writes
`<dir>/peers.toml` instead. The daemon loads this file on `fabric up`; it is the
endpoint allow-list.

## Commands

```sh
fabric --version
```

Print the installed build version as `<semver>+<short-git-sha>`.

```sh
fabric key gen --out <path>
```

Generate an identity file without a running daemon and print its public NodeID.
The output file is in the same format as `<home>/identity.toml`, so it can be
pre-installed onto another machine before that machine ever starts fabric.

```sh
fabric id
```

Print this node's stable NodeID, generating and persisting it on first use.

```sh
fabric peers
```

List trusted peers. The peer list is the daemon's endpoint allow-list.

```sh
fabric status
```

Show the running daemon's local state and echo-ping every trusted peer. Each
peer is reported as reachable or unreachable with round-trip latency and, when
iroh exposes it, the active transport path: `direct`, `relay`, or `mixed`.
Status also prints the daemon build version.

```sh
fabric add <nodeid> [name] [--addr-json JSON]
```

Trust a peer NodeID and optionally assign a human name. `--addr-json` is an
optional local/direct address hint for deterministic same-machine testing; normal
key-only dialing relies on iroh address lookup.

```sh
fabric remove <nodeid-or-name>
```

Remove a trusted peer.

```sh
fabric up [--foreground] [--allow-shell]
```

Start the local fabric daemon. Without `--foreground`, this spawns a background
daemon and logs to `<home>/logs/daemon.log`. After the daemon is ready, `fabric
up` runs the same echo-ping reachability check used by `fabric status` and
prints one line per trusted peer.

`--allow-shell` opts this daemon into serving remote shells for trusted peers.
It is off by default.

```sh
fabric down
```

Stop the local daemon.

```sh
fabric restart [--allow-shell | --no-allow-shell]
```

Schedule a lockout-safe daemon restart through a detached helper and return
before the running daemon goes down. This is safe to run over `fabric shell`: the
helper writes progress to `<home>/logs/restart.log`, stops the old daemon, and
starts a fresh one even if the invoking shell connection drops.

Plain `fabric restart` preserves the running daemon's flags, including
`--allow-shell`. Use `--allow-shell` or `--no-allow-shell` to force the restarted
daemon's shell policy.

```sh
fabric addr
```

Print the running daemon's current iroh `EndpointAddr` as JSON. This is mostly a
local-test aid for `--addr-json`; it is not part of the consumer contract.

```sh
fabric expose <protocol> --socket <local-unix-sock>
```

Expose a local Unix socket service to trusted peers under the protocol's ALPN.
Only allow-listed remote NodeIDs are accepted before the local socket is opened.

```sh
fabric dial <peer> <protocol>
```

Create and print a local Unix socket path. Connections to that socket are
tunneled to the peer's exposed protocol over iroh.

```sh
fabric ping <peer>
```

Connectivity and trust test. `fabric ping` dials the peer's built-in
ACL-gated echo protocol, sends a random nonce, verifies the same bytes come
back, and prints the round-trip latency. When available, it also reports whether
iroh used a direct, relay, or mixed path. Use this first when bringing up a new
machine.

```sh
fabric shell <peer>
```

Open an interactive remote shell on a trusted peer over fabric. The server side
must have been started with `fabric up --allow-shell`; a default `fabric up`
refuses shell requests. The shell runs as the remote daemon's user and uses the
remote user's `$SHELL`.

Enabling shell is a security-sensitive opt-in: every trusted peer in
`peers.toml` can obtain a remote shell while `--allow-shell` is active. Keep the
allow-list tight, enable shell only on machines where that access is intended,
and use `fabric restart --no-allow-shell` to turn it back off without risking a
remote lockout.

### Debug Transport Test Commands

These commands are hidden from normal help output and exist to validate the
resumable transport in live deployments.

```sh
fabric debug echo --socket /tmp/fabric-wan-echo.sock
```

Run a foreground Unix-socket echo service. Use this as the service behind a
generic `fabric expose` when the remote machine does not have `socat` or another
Unix-socket echo tool installed.

```sh
fabric debug unix-cat --socket <local-dial-sock>
```

Connect stdin/stdout to a Unix socket and keep that one local socket open. This
is useful for proving bytes resume over the same local connection after an iroh
attach drop.

```sh
fabric debug block-tunnels
fabric debug drop-tunnels
fabric debug unblock-tunnels
```

Reject new generic tunnel attaches, close active generic tunnel attaches, and
then allow attaches again. This is intentionally non-destructive: it does not
stop the daemon, and it does not affect the built-in `fabric shell` ALPN.

## Declarative Peer Config

`peers.toml` is intentionally human-editable. The minimal form is:

```toml
[[peers]]
id = "peer-node-id-hex"
name = "workstation"
```

`name` is optional. Address hints are optional too; normal key-only dialing uses
iroh address lookup. Local deterministic tests can include the address-hint TOML
that `fabric add <nodeid> <name> --addr-json "$(fabric addr)"` writes.

## Provision And Go

Pre-generate a box identity on a trusted machine:

```sh
BOX_ID=$(fabric key gen --out box-identity.toml)
printf '%s\n' "$BOX_ID"
```

Write the new box's `peers.toml` with the peers it should trust:

```toml
[[peers]]
id = "existing-machine-node-id"
name = "workstation"
```

Pre-add the new box NodeID to the existing machines, either with `fabric add` or
by editing their `peers.toml`:

```sh
fabric add "$BOX_ID" new-box
```

Install the generated identity and peer config on the new box before first boot.
For the default paths:

```sh
mkdir -p ~/.local/share/fabric ~/.config/fabric
install -m 600 box-identity.toml ~/.local/share/fabric/identity.toml
install -m 644 peers.toml ~/.config/fabric/peers.toml
fabric up
fabric ping workstation
```

If provisioning with `FABRIC_HOME=/path/to/fabric`, put both files in that
directory as `identity.toml` and `peers.toml`.

## Local Two-Node Test

The automated integration test is the canonical local walkthrough:

```sh
cargo test --test local_slice
```

It starts three fabric nodes on one Mac:

- node A exposes a dummy Unix-socket echo service under `pty-view`
- node B trusts node A, dials `pty-view`, and round-trips bytes through fabric
- node C has node A's address but is not trusted by node A, and is rejected before
  node A's local service sees a connection

For a manual run, use separate homes:

```sh
FABRIC_A=/tmp/fabric-a
FABRIC_B=/tmp/fabric-b

target/debug/fabric --home "$FABRIC_A" up
target/debug/fabric --home "$FABRIC_B" up

A_ID=$(target/debug/fabric --home "$FABRIC_A" id)
B_ID=$(target/debug/fabric --home "$FABRIC_B" id)
A_ADDR=$(target/debug/fabric --home "$FABRIC_A" addr)
B_ADDR=$(target/debug/fabric --home "$FABRIC_B" addr)

target/debug/fabric --home "$FABRIC_A" add "$B_ID" node-b --addr-json "$B_ADDR"
target/debug/fabric --home "$FABRIC_B" add "$A_ID" node-a --addr-json "$A_ADDR"
```

Start any Unix-socket echo service at `/tmp/fabric-a-echo.sock`, then:

```sh
target/debug/fabric --home "$FABRIC_A" expose pty-view --socket /tmp/fabric-a-echo.sock
target/debug/fabric --home "$FABRIC_B" dial node-a pty-view
```

The printed socket on node B is the local pipe a consumer connects to.

## Live WAN Reconnect Test

Use this procedure to validate Layer 1 over a real Mac-to-Hetzner link without
restarting either daemon. Restarting the accept-side daemon is intentionally not
part of this test because it would lose the server-side in-memory tunnel session.

The standalone systemd-per-daemon Hetzner plan is parked; convoy is the chosen
supervisor. For the retained daemon run surfaces, see
[docs/hetzner-supervisor-plan.md](docs/hetzner-supervisor-plan.md).

On Hetzner, start a generic Unix echo service in one shell:

```sh
fabric debug echo --socket /tmp/fabric-wan-echo.sock
```

In another Hetzner shell, expose it:

```sh
fabric expose wan-echo --socket /tmp/fabric-wan-echo.sock
```

On the Mac, dial the service and connect one long-lived local socket:

```sh
SOCK=$(fabric dial hetzner wan-echo)
fabric debug unix-cat --socket "$SOCK"
```

Type `before` and press Enter; it should echo immediately. Then, from Hetzner,
force a clean generic-tunnel drop and temporarily reject reconnects:

```sh
fabric debug block-tunnels
fabric debug drop-tunnels
```

Back in the Mac `unix-cat` process, type `during-drop` and press Enter. It should
not echo while blocked, but the process and local socket should stay open. Then
unblock Hetzner:

```sh
fabric debug unblock-tunnels
```

The `during-drop` bytes should arrive on the Mac over the same `unix-cat`
process. Type `after` and press Enter to confirm the reattached tunnel continues
to carry new bytes.

## Consumer Contract

A consumer such as `pty` should treat fabric as a local socket provider:

```text
pty ls --remote node-a
  -> asks fabric: dial node-a pty-view
  -> fabric prints /.../dials/<peer>-pty-view.sock
  -> pty connects to that Unix socket
  -> pty speaks its own pty-view protocol bytes
```

The consumer never imports iroh types, parses relay addresses, opens QUIC
streams, or implements allow-list checks. Only fabric owns those details.

## Architecture

The daemon owns one persisted iroh endpoint per fabric home. `fabric expose`
registers an ALPN and local Unix socket in the running daemon; the daemon updates
the endpoint's accepted ALPN list dynamically. Incoming iroh connections pass
through an `EndpointHooks::after_handshake` allow-list check before the daemon
connects to any exposed local service.

`fabric dial` registers a local Unix listener under `<home>/dials`. Each local
connection gets a random tunnel session id bound to the remote peer id. Generic
dials use a small framed byte protocol with offsets and ACKs, so unacked bytes
can be replayed after a real iroh attach loss while the local Unix socket stays
open. Built-in `fabric shell` remains on its raw one-shot stream protocol; a
resilient shell should be built above generic fabric transport, for example by
running a long-lived `pty` session over `fabric dial`.
