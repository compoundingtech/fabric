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
`~/.local/bin/fabric` when one exists. If no prebuilt binary matches your
machine, it falls back to cloning this repo and building with Cargo. Ensure
`~/.local/bin` is on PATH. To install somewhere else, set `FABRIC_BIN_DIR` or
`BIN_DIR`.

From a cloned repo:

```sh
./install.sh
```

or:

```sh
make install
```

The cloned-repo installer builds the current checkout and copies the release
binary to `~/.local/bin/fabric`.

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

By default fabric stores local state in:

```text
~/.local/share/fabric
```

Use `--home <dir>` or `FABRIC_HOME=<dir>` to run multiple local nodes on one
machine.

The identity file contains the persisted iroh secret key. The public key is the
node's stable NodeID.

## Commands

```sh
fabric id
```

Print this node's stable NodeID, generating and persisting it on first use.

```sh
fabric peers
```

List trusted peers. The peer list is the daemon's endpoint allow-list.

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
fabric up [--foreground]
```

Start the local fabric daemon. Without `--foreground`, this spawns a background
daemon and logs to `<home>/logs/daemon.log`.

```sh
fabric down
```

Stop the local daemon.

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
back, and prints the round-trip latency. Use this first when bringing up a new
machine.

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
connection opens one iroh bidirectional stream to the peer and copies bytes in
both directions. This keeps iroh isolated inside fabric while preserving the
ordinary Unix-socket interface expected by local tools.
