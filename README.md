> Status: EXPERIMENTAL. Early spike / prototype -- the CLI, APIs, and on-disk formats will change without notice; no stability or security guarantees yet; not production-ready. Use at your own risk.

# fabric

fabric is a standalone Rust CLI and local daemon that hides iroh behind local Unix
sockets.

Consumer tools do not link to iroh, know NodeAddr formats, or open QUIC streams.
They ask fabric for a local socket connected to a remote service, then speak their
own protocol over that socket.

## Adopt Fabric On Two Machines

Install the same fabric release on both macOS or Linux machines:

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh
fabric --version
```

The installer puts `fabric` in `~/.local/bin` by default. Add that directory to
`PATH` if `fabric --version` is not found.

On each machine, print its stable NodeID:

```sh
fabric id
```

Exchange those NodeIDs over a trusted channel. Trust machine B on machine A:

```sh
fabric add <machine-b-node-id> machine-b
fabric up
```

Trust machine A on machine B:

```sh
fabric add <machine-a-node-id> machine-a
fabric up
```

Trust is deliberately mutual: each daemon accepts connections only from NodeIDs
in its own allow-list. Verify the connection from either machine:

```sh
fabric status
fabric ping machine-b
```

Run `fabric ping machine-a` on machine B. A successful check prints `pong`, the
round-trip latency, and the active transport path when iroh reports it. The two
daemons are now connected and ready for `fabric expose`, `fabric dial`, or the
explicitly enabled remote shell.

For a daemon managed by systemd or launchd instead of the background process
started by `fabric up`, run `fabric down` and then:

```sh
fabric service install
fabric service status
```

See [Expose And Dial A Service](#expose-and-dial-a-service) for the next step.

## Build

```sh
cargo build
cargo test
```

The binary is `target/debug/fabric` during development.

## Install

Fast path for macOS and Linux:

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh
```

The remote installer downloads a matching prebuilt release binary into
`~/.local/bin/fabric`, prints the installed version, and fails if that version
does not match the targeted release. Ensure `~/.local/bin` is on PATH. To
install somewhere else, set `FABRIC_BIN_DIR` or `BIN_DIR`.

The remote installer does not silently fall back to source builds. If no
prebuilt binary matches your machine, run an explicit source install:

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh -s -- --from-source
```

To pin a release:

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh -s -- --version v0.1.7
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

To install fabric as a user-managed service with OS restart supervision:

```sh
fabric service install
```

On Linux this writes and starts a systemd user unit. On macOS this writes and
starts a per-user LaunchAgent. The service runs the foreground daemon directly as
`fabric --home <home> daemon`; it does not run `fabric up`. The default memory
ceiling is 1 GiB and can be changed with `--memory-max-mb`.

The default intentionally leaves headroom above fabric's in-process RSS recycle
trigger. During endpoint recycle, the replacement endpoint can briefly overlap
with the old endpoint in memory; setting the service cap too close to the
300 MiB recycle threshold can let the OS kill fabric during a successful
self-heal.

Remote shell serving remains off unless you opt in:

```sh
fabric service install --allow-shell
```

To force it off in the persisted config:

```sh
fabric service install --no-allow-shell
```

The service uses the same fabric home, identity, persisted exposes, and trusted
peer allow-list. It does not install SSH keys or change fabric's authorization
model.

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
fabric expose <protocol> --tcp <host:port>
fabric expose <protocol> --exec [--max-children N] -- <cmd> [args...]
fabric expose <protocol> --ephemeral ...
```

Expose a local service to trusted peers under the protocol's ALPN. `--socket`
connects each fabric tunnel session to an existing Unix socket service. `--tcp`
connects each tunnel session to an existing local TCP service. `--exec` spawns
the configured command with piped stdin/stdout for each fabric tunnel session;
pass the command as argv after `--`, not as a shell string. Child stderr is
written to the fabric daemon log with the tunnel session id. Exec exposures
default to at most 32 active children per exposure; use `--max-children` to set
a different per-exposure cap.

Exposes are persisted by default to `<home>/config.toml` and are restored when
the daemon starts. That same file also stores shell policy and trusted peers
written by `fabric add`. Use `--ephemeral` for short-lived test exposes that
should not survive a daemon restart.

Only allow-listed remote NodeIDs are accepted before the local socket is opened
or the local TCP connection / exec command is started.

```sh
fabric unexpose <protocol>
```

Stop accepting a protocol and remove its persisted config entry.

```sh
fabric dial <peer> <protocol>
fabric dial <peer> <protocol> --tcp <local-host:port>
```

Create and print a local Unix socket path. Connections to that socket are
tunneled to the peer's exposed protocol over iroh. With `--tcp`, fabric listens
on the local TCP address and forwards each accepted connection to the peer's
exposed protocol.

### Expose And Dial A Service

For example, expose a service listening on TCP port 8080 on machine B:

```sh
fabric expose demo-http --tcp 127.0.0.1:8080
```

On machine A, create a local listener that forwards to it:

```sh
fabric dial machine-b demo-http --tcp 127.0.0.1:9080
```

Clients on machine A can now use `127.0.0.1:9080`. The exposure persists in
fabric's config and returns when the daemon restarts; recreate the dial listener
after restarting machine A's daemon. Run `fabric unexpose demo-http` on machine
B when the exposure is no longer wanted.

### Check Connectivity Or Open A Shell

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
lockout.

```sh
fabric service install [--allow-shell | --no-allow-shell] [--memory-max-mb N]
fabric service status
fabric service uninstall
```

Install, inspect, or remove the OS user service. `install` starts/restarts the
native service manager entry and enables it for future user sessions. `status`
delegates to `systemctl --user status fabric.service --no-pager` on Linux and
`launchctl print gui/$UID/tech.compounding.fabric` on macOS. `uninstall` stops the
managed service and removes only the systemd/launchd artifact; it leaves the
fabric home, identity, peers, logs, and config in place. The default service
memory ceiling is 1 GiB; use a lower `--memory-max-mb` only after validating
that endpoint recycle can complete below that cap on the target machine.

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

The Hetzner supervisor model is undecided and the standalone systemd-per-daemon
plan is parked. For the retained daemon run surfaces, see
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

```text
client machine                                      server machine

+------------------+                               +------------------+
| consumer process |                               | local service    |
| pty / app / tool  |                               | socket/tcp/exec  |
+--------+---------+                               +---------+--------+
         |                                                   ^
         | local Unix socket or TCP                          |
         v                                                   |
+--------+---------+       iroh direct or relay      +-------+--------+
| fabric daemon    |<===============================>| fabric daemon   |
| dial listener    |          QUIC + ALPN            | expose handler  |
| peer allow-list  |                                 | peer allow-list |
+--------+---------+                                 +-------+--------+
         |                                                   ^
         v                                                   |
+--------+---------+                                 +-------+--------+
| identity.toml    |                                 | identity.toml   |
| peers.toml       |                                 | peers.toml      |
| config.toml      |                                 | config.toml     |
+------------------+                                 +----------------+
```

The daemon owns one persisted iroh endpoint per fabric home. `<home>/config.toml`
stores shell policy, trusted peers, and persisted exposes. `fabric expose`
registers an ALPN and a local Unix socket, TCP, or exec target in the running
daemon and, by default, writes it to that config. On startup, the daemon restores
those exposes before binding its accepted ALPN list. Incoming iroh connections
pass through an `EndpointHooks::after_handshake` allow-list check before the
daemon connects to a socket/TCP target or spawns an exec target.

`fabric dial` registers a local Unix listener under `<home>/dials`. Each local
connection gets a random tunnel session id bound to the remote peer id. Generic
dials use a small framed byte protocol with offsets and ACKs, so unacked bytes
can be replayed after a real iroh attach loss while the local Unix socket stays
open. On the expose side, the Unix socket connection or exec child is bound to
that tunnel session, not to each transient iroh attach, so a reconnect resumes
the same local endpoint. If a detached session exceeds the server TTL, fabric
removes the session and kills/reaps its exec child. Built-in `fabric shell`
remains on its raw one-shot stream protocol; a resilient shell should be built
above generic fabric transport, for example by running a long-lived `pty` session
over `fabric dial`.
