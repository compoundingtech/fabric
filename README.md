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

`fabric add` is only a convenience writer. Automated or image-based installs
can deploy the complete authorized-keys file instead; see
[Declarative Peer Config](#declarative-peer-config).

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

## Join An Existing Mesh (add a fresh machine)

Adding a new machine — say a travel laptop — to a mesh that already has peers
(say your desktop and a server) is the same mutual-trust step as above, done once
per existing peer. There is **no auto-pairing/discovery of trust**: you exchange
**NodeIDs by hand**. Trust is symmetric, so the new machine AND each existing peer
must each `fabric add` the other. Nothing is copied between machines except NodeID
strings — they are public keys, safe to paste anywhere. The new machine generates
its own identity on first start; you do **not** copy any file (identity, peers,
config) from an existing machine.

Peers are found by NodeID over iroh discovery (relays), so **no address hints are
needed** and a roaming laptop reconnects on its own as its network changes.

### 1. On the NEW machine — install, start the managed daemon, print its NodeID

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh
fabric --version
fabric service install --allow-shell   # launchd/systemd daemon; survives reboot + sleep/wake
fabric id                               # copy this — the NEW machine's NodeID
```

### 2. On the NEW machine — trust each existing peer

Get each existing peer's NodeID by running `fabric id` on it (or ask whoever runs
it). Then, on the new machine:

```sh
fabric add <DESKTOP_NODEID> desktop
fabric add <SERVER_NODEID>  server
fabric reload-peers
```

### 3. On EACH existing peer — trust the NEW machine

Using the new machine's NodeID from step 1, run this **on the desktop and on every
other existing peer**:

```sh
fabric add <NEW_NODEID> laptop
fabric reload-peers
```

For a peer you can already reach through the mesh (e.g. a server), you can add the
new machine to it **without SSH**, from any machine that already has exec access
to it:

```sh
fabric exec server -- ~/.local/bin/fabric add <NEW_NODEID> laptop
fabric exec server -- ~/.local/bin/fabric reload-peers
```

(`fabric exec` requires the server to have opted in with `--allow-exec`; otherwise
`ssh` in and run the same two commands.)

### 4. Verify from the new machine

```sh
fabric status                     # each peer should show as reachable
fabric ping server                # prints pong + latency + transport (direct/relay)
fabric exec server -- echo ok     # if that peer enabled --allow-exec; prints ok, exit 0
```

If `ping`/`status` shows a peer unreachable, the usual cause is one-sided trust —
confirm BOTH the new machine and that peer ran `fabric add` **and**
`fabric reload-peers`. Reachability may start on `relay` and upgrade to `direct`
within a few seconds as iroh hole-punches.

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

**Prebuilt vs. from source.** The plain `curl … | sh` path above downloads a
prebuilt release binary and needs **no toolchain** — the fast path for a fresh
machine. Every *source* path (`--from-source`, a cloned-repo `./install.sh`,
`make install`, `cargo install --path .`, or `cargo build`) instead compiles
fabric locally and therefore requires a **Rust toolchain**; install one first via
[rustup](https://rustup.rs):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # then: source ~/.cargo/env
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

### Enabling remote shell and exec

Both remote shell (`fabric shell <peer>`) and non-interactive remote exec
(`fabric exec <peer> -- <cmd>`) are **default-deny** — a peer serves them only if
it has opted in. They are independent (allowing one does not allow the other):
`fabric shell` needs `allow_shell`, `fabric exec` needs `allow_exec`.

**Check what a daemon serves** by running `fabric status` on it — it prints
`shell allowed` / `disabled` and `exec allowed` / `disabled`.

These flags are **daemon-global**: enabling `allow_shell` / `allow_exec` opens
that capability to **every** trusted peer, not a chosen subset. Restricting shell
or exec to specific peers is not supported today — it is all-or-nothing per
capability, gated only by the peer allow-list (who is trusted at all). If you need
per-peer scoping, keep the capability off and reach for it deliberately.

Enable them with flags on `fabric service install`:

```sh
fabric service install --allow-shell                # serve remote shell
fabric service install --allow-shell --allow-exec   # serve both
fabric service install --no-allow-shell             # persist shell OFF (also --no-allow-exec)
```

**On an already-installed daemon, enable the same way** — just re-run
`fabric service install` with the flags you want:

```sh
fabric service install --allow-shell --allow-exec
```

The allow flags are read when the daemon **starts**, so re-installing restarts the
managed daemon to apply them (a brief interruption to any connections currently
going through it). Run it **locally or from an independent shell — not over a
`fabric shell` to that same machine**, which would sever the connection driving
the restart. Confirm afterward with `fabric status`, which prints `shell allowed`
and `exec allowed`. (For an unmanaged `fabric up` daemon, use
`fabric restart --allow-shell` instead of re-installing.)

If `fabric shell <peer>` or `fabric exec <peer> -- …` fails with
`unknown peer <peer>`, the problem is on the **calling** side, not the target: the
caller has no trusted peer under that name. Names are per-machine — each machine
can call the same NodeID whatever it likes — so a peer that resolves on one box
may be missing, or named differently, on another. Fix it on the caller:
`fabric add <nodeid> <peer>` then `fabric reload-peers`
(see [Join An Existing Mesh](#join-an-existing-mesh-add-a-fresh-machine)). This is
independent of whether the target serves shell/exec.

`fabric shell` gives a **real interactive TTY** (it allocates a PTY on the remote,
like `ssh -t`), so job control, `stty`, and full-screen programs work.
`fabric exec` is non-interactive — it runs the command with piped stdio and **no
TTY**, so `fabric exec <peer> -- bash -i` reports "no job control" / "stdin isn't a
terminal". That is expected: use `fabric shell` when you want an interactive shell.

**Gotcha — the remote shell runs in the daemon's session, not your login
session.** `fabric shell` spawns the shell as a child of the remote fabric daemon.
On a managed install that daemon runs under launchd/systemd — a **non-GUI** session
with no Keychain/`login` context. A shell-startup (`.bashrc`/`.zshrc`/prompt) that
depends on the GUI login session — e.g. macOS `security show-keychain-info` on the
login keychain, `ssh-add`, or anything that blocks waiting for a GUI unlock prompt
— can **hang** the interactive shell (the connection is fine; the dotfiles are
stuck). To make this easy to guard, fabric sets **marker env vars** in the remote
session: `FABRIC_SHELL=1` inside a `fabric shell`, `FABRIC_EXEC=1` under
`fabric exec`, and `FABRIC_PEER=<nodeid>` (the connecting peer — who opened the
session) in both. Gate fragile startup on them rather than running it
unconditionally, e.g. near the top of your `.bashrc`/`.zshrc`:

```sh
# Skip GUI/login-session-dependent startup when invoked over fabric.
if [ -n "$FABRIC_SHELL" ]; then
  # keychain unlock, ssh-add, slow xcode/brew probes, etc. — skip or make non-blocking
  return 2>/dev/null || true
fi
```

The service uses the same fabric home, identity, persisted exposes, and trusted
peer allow-list. It does not install SSH keys or change fabric's authorization
model.

Migrating an already-installed service across a launchd or systemd identity
change restarts the daemon. Do not perform that switchover through the daemon's
own `fabric shell`: it severs the only recovery path if the replacement fails.
Build and verify the replacement first, keep a rollback binary, and schedule the
swap for a window with independent machine access.

## Upgrading Fabric Safely

Upgrading the fabric binary under a running daemon — especially on a remote
machine reached only over `fabric shell` — must be done lockout-safe: a botched
restart can sever the only path back to the box. Follow this order.

1. **Install the new binary atomically, at the path the daemon runs from.**
   `install.sh` installs via a temp file plus a rename, so it can replace the
   binary while the daemon is running: the daemon keeps executing the old inode
   while the path points at the new one. Never `cp` over a running binary in
   place (Linux fails with `ETXTBSY`, "text file busy"). Confirm the daemon's
   binary path first (from its service unit or `ps`) and install there. Keep the
   previous binary as a rollback before installing, e.g.
   `cp ~/.local/bin/fabric ~/.local/bin/fabric.rollback`.

2. **Restart through the daemon's supervisor** so the restart survives your
   shell disconnecting. The command depends on how the daemon is supervised:
   - **systemd user service** (`fabric service install`, or a custom unit such
     as a keepalive unit): `systemctl --user restart <unit>`. systemd re-execs
     the new binary at the unit's `ExecStart` path.
   - **launchd (macOS LaunchAgent)**: `launchctl kickstart -k gui/$UID/<label>`.
     launchd re-execs the new binary at the plist's program path.
   - **Plain `fabric up` background daemon** (no OS supervisor): `fabric restart`.
     A detached helper stops the old daemon and starts a fresh one and survives
     the invoking shell disconnecting.

   Do **not** use `fabric restart` under systemd or launchd supervision: a clean
   exit can leave the supervised job stopped while an unmanaged daemon runs.
   And **never** run a naked `fabric down` then `fabric up` over a remote shell —
   if the shell drops in between, the daemon is down with no supervisor and you
   are locked out.

3. **Verify from a fresh shell** (not the one that drove the restart):
   `fabric --version` matches the new release, `fabric status` shows trusted
   peers still reachable, and `fabric sync ls` if sync is configured. If anything
   is wrong, restore the rollback binary and restart again.

For a coordinated multi-machine upgrade, stage the new binary on every machine
first (install, then hold the restart), then restart them in one planned window
to keep the transport blip to a single interval.

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

If `--home <dir>` or `FABRIC_HOME=<dir>` points at a **non-default** directory,
fabric reads and writes `<dir>/peers.toml` instead — an isolated node with its
own allow-list. As a deliberate exception, an explicit `--home`/`FABRIC_HOME`
equal to the **default** state root (`~/.local/share/fabric`) still uses
`~/.config/fabric/peers.toml`, so the managed service — which always launches the
daemon as `--home <default-root>` — and the interactive CLI never disagree about
where peers live. The daemon loads this authoritative allow-list on startup and
when `fabric reload-peers` is run.

Older default-home installs may have peer entries in
`~/.local/share/fabric/config.toml` or `~/.local/share/fabric/peers.toml`.
Fabric migrates those entries to `~/.config/fabric/peers.toml`; an existing
canonical `peers.toml` wins.

## Developing Fabric (dev vs prod)

A production fabric daemon is often load-bearing (it may be your only path to a
remote machine), so **hack on fabric without touching the prod daemon** by
running a **dev instance on its own home**. Because a home owns its control
socket, identity/NodeID, config, and (ephemerally) its UDP port, a dev instance
on a distinct home is structurally unable to collide with prod.

Set `FABRIC_HOME` once for your dev shell and every `fabric` command targets the
dev instance — nothing to forget:

```sh
export FABRIC_HOME=~/.local/share/fabric-dev   # or a repo-local ./.fabric-dev
fabric up                                       # a manual dev daemon on its own home
fabric status                                   # talks to the dev daemon, not prod
fabric down                                     # stops only the dev daemon
```

Rules that keep dev and prod from fighting:

- **Prod is the only OS-managed service.** `fabric service install` **refuses a
  non-default home** — a second managed service would share the one global
  service label and fight the prod daemon. Dev instances run manually via
  `fabric up`, never as an installed service.
- **Mutating commands warn on a home mismatch.** If `fabric down`/`restart`
  can't reach a daemon at the target home but one *is* running on the default
  home, fabric warns you (you probably forgot `--home`/`FABRIC_HOME`, or your dev
  daemon is down).
- **The default home is prod.** A bare `fabric …` with no `--home`/`FABRIC_HOME`
  targets `~/.local/share/fabric` — that's the prod daemon. Keep `FABRIC_HOME`
  set while developing.

The same pattern applies to any per-instance daemon: per-instance
home/socket/identity, prod is the one service, dev is a manual run on a distinct
home.

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

Read and list the entries in the authoritative `peers.toml`.

```sh
fabric reload-peers
```

Validate `peers.toml` and apply it to the running daemon without restarting.
The daemon keeps its previously loaded allow-list if parsing or validation
fails.

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
the daemon starts. That same file also stores shell policy; `fabric add` writes
the separate authoritative `peers.toml`. Use `--ephemeral` for short-lived test
exposes that should not survive a daemon restart.

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
`launchctl print gui/$UID/com.compoundingtech.fabric` on macOS. `uninstall` stops the
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

## File Sync

`fabric sync` keeps a folder converged with trusted peers. A declarative config
file lists sync *entries*; the running daemon watches each folder and syncs
changes to peers near-instantly over fabric's own transport. A tool or a human
just adds an entry and drops files in the folder.

Entries live in an authoritative, hand-editable `syncs.toml` next to `peers.toml`
(`~/.config/fabric/syncs.toml` for the default home, `<home>/syncs.toml` with
`--home` or `FABRIC_HOME`):

```toml
[[sync]]
name   = "catalog"                # shared logical key: the SAME name on every machine
folder = "/abs/path/to/catalog"   # machine-local; may differ per machine
peers  = "*"                      # "*" = every peer in peers.toml, or ["workstation", "server"]
policy = "catalog"                # catalog | bus
# include = ["*.toml"]            # optional: only matching files sync (default: all)
```

Two machines are the *same* sync when they use the same `name`; their local
`folder` paths may differ. `peers = "*"` follows the `peers.toml` allow-list, so
sync only ever touches already-trusted peers — it adds no new trust surface.

### Policies

- `catalog` — union, newer-wins, and **never deletes on a peer**: a file present
  on any peer is present on all peers, and a local deletion is restored.
  Decommission a file by editing it (for example `retired = true`), never by
  deleting it. Safe for a job catalog.
- `bus` — union, newer-wins, and propagates deletes via tombstones. (Tombstone
  sweeping is not yet implemented.)

Conflicts resolve newer-wins by a logical version with a deterministic tie-break,
not by filesystem mtime (which is unreliable across machines).

### Sync Commands

```sh
fabric sync add <folder> --name <name> [--peers "*"|a,b] [--policy catalog|bus] [--include "*.toml"]
fabric sync ls
fabric sync rm <name-or-folder>
fabric sync reload
```

`fabric sync add` is a convenience writer for `syncs.toml`; the file can also be
hand-edited or provisioned before the daemon runs. `fabric sync reload` applies
the file to a running daemon, mirroring `reload-peers`. The daemon serves and
dials sync over the reserved `fabric/sync/1` ALPN, gated by the same peer
allow-list as every other fabric protocol.

## Declarative Peer Config

`peers.toml` is Fabric's authorized-keys file. It is intentionally
human-editable and can be provisioned before Fabric ever runs. Each
`[[peers]]` entry accepts:

- `id` (required): the peer's 64-character hexadecimal iroh NodeID.
- `name` (optional): a non-empty, unique local alias for commands such as
  `fabric ping workstation`.
- `addr` (optional): an iroh `EndpointAddr` hint whose `id` must match the
  peer's `id`.

NodeIDs and names must be unique. Normal cross-machine setup should omit
`addr`; NodeID-based iroh discovery supplies the current addresses.

The usual file contains only NodeIDs and optional names:

```toml
[[peers]]
id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
name = "workstation"

[[peers]]
id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
name = "server"
```

An explicit address hint, mainly useful for deterministic tests, has this exact
TOML shape:

```toml
[[peers]]
id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
name = "workstation"

[peers.addr]
id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[[peers.addr.addrs]]
Relay = "https://relay.example.com/"

[[peers.addr.addrs]]
Ip = "203.0.113.10:11204"
```

Prefer generating hint data with
`fabric add <nodeid> <name> --addr-json "$(fabric addr)"` instead of writing it
by hand.

For the default home, install a prepared file and apply it without any
interactive command:

```sh
FABRIC_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/fabric"
install -d -m 755 "$FABRIC_CONFIG_DIR"
install -m 644 ./peers.toml "$FABRIC_CONFIG_DIR/peers.toml"
fabric reload-peers
fabric peers
fabric status
```

If the daemon is not running yet, omit `fabric reload-peers`; `fabric up`,
`fabric up --foreground`, and the managed service all read the file at startup.
With `FABRIC_HOME=/srv/fabric`, install it as `/srv/fabric/peers.toml` and use
that same environment for every Fabric command.

Removing an entry and reloading prevents new connections from that NodeID.
Reloading does not forcibly close an already active tunnel or shell; restart in
a safe maintenance window when immediate disconnection is required.

## Troubleshooting

### Sync stalls or "unknown peer" after a daemon restart

**Symptom.** `fabric ping`, `fabric status`, and `fabric shell` all report the
peer as reachable, yet anything that goes through a **dial** — `fabric dial`, or
a consumer like `st sync` — fails on a loop. `st sync` shows
`fabric pull failed: <peer>::… — re-dialing` forever, and
`~/.local/share/fabric/logs/service.err.log` shows
`dial socket connection failed: unknown peer "<peer>"`.

**Cause.** A peer-config **split** between the daemon and the CLI. `ping`/`status`
answer from the daemon's in-memory allow-list, but the dial/tunnel path
re-resolves the peer from `peers.toml` on disk each connection. If the daemon was
launched with a `--home` whose `peers.toml` is missing or empty while the CLI
writes to a different `peers.toml`, the dial path resolves nothing → the tunnel
never opens → the consumer's socket gets zero bytes and times out. This is a
fabric transport issue, not a consumer bug (e.g. `st sync`'s
`SyncFailedError`-on-`rsync --timeout` is that consumer behaving correctly). A
default-home `fabric add`/`remove` can trigger the split by migrating peers to
`~/.config/fabric/peers.toml` and removing the legacy in-`--home` copy.

**Fix.** Make sure the peers file the daemon actually reads contains the peer,
then reload:

```sh
# Confirm the running daemon's --home (e.g. from `ps` or the service plist),
# then point every command at that same home so the CLI and daemon agree:
fabric --home <daemon-home> add <nodeid> <name>
fabric --home <daemon-home> reload-peers
fabric --home <daemon-home> status      # peer should now be reachable AND dialable
```

On current fabric an explicit `--home` equal to the default state root resolves
peers from `~/.config/fabric/peers.toml` (see [State](#state)), so a
service-launched daemon and the interactive CLI can no longer diverge this way.

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

On every existing machine, add the new box to its canonical `peers.toml`:

```toml
[[peers]]
id = "<new-box-node-id>"
name = "new-box"
```

Replace `<new-box-node-id>` with the value printed in `BOX_ID`, deploy the file
with the machine's normal configuration-management or file-copy mechanism, and
run `fabric reload-peers` on a daemon that is already running.

Install the generated identity and prepared peer config on the new box before
first boot. For the default paths:

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
stores shell policy and persisted exposes; `peers.toml` stores the peer
allow-list. `fabric expose` registers an ALPN and a local Unix socket, TCP, or
exec target in the running daemon and, by default, writes it to `config.toml`.
On startup, the daemon restores those exposes before binding its accepted ALPN
list. Incoming iroh connections pass through an
`EndpointHooks::after_handshake` allow-list check before the daemon connects to
a socket/TCP target or spawns an exec target.

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
