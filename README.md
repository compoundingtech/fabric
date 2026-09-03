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

Installing Fabric creates a new local identity but trusts no remote machine.
There is no account, central trust service, or automatic pairing: exchange the
public NodeIDs over a channel you trust, and add each side to the other side's
allow-list. Adding a peer authorizes that NodeID at Fabric's transport boundary;
remote shell and exec remain separate, default-deny target-side capabilities.

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

### Presence, partitions, and local ownership

Fabric reports connection facts about each trusted peer by canonical NodeID.
It reports a normal peer as reachable or unreachable. Set `roaming = true` for
a peer that is expected to disconnect, such as a laptop. Fabric reports that
peer as away while it is offline. Its absence does not add failures or cause
endpoint recovery. Fabric logs one away event and one return event instead of
logging each failed probe. A durable `last_seen` status surface remains a desired gap.

The architecture MUST isolate network partitions from unrelated local work:
each machine and its local processes continue from their last instructions
while peers are unreachable, and failure of one remote path MUST NOT cascade
into other local work. Existing sessions, routes, and sync state remain owned by
the runtime that created them. Replicas may resume synchronization after
reconnect, while each consumer remains responsible for safe and atomic
application of received state.

Whether an unavailable peer matters is decided by the active session, route,
sync, or consuming workload—not by a fleet-wide Fabric health classification.
Remote shell continuity across all detach cases and daemon sleep/wake
self-healing remain open work in [issue #21](https://github.com/compoundingtech/fabric/issues/21)
and [PR #22](https://github.com/compoundingtech/fabric/pull/22). A future remote
st2 PTY attachment composes with this boundary: Fabric transports the stream,
while st2/PTY owns the PTY child, terminal policy, and lifecycle/expiry.

### 1. On the NEW machine — install, start the managed daemon, print its NodeID

```sh
curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh
fabric --version
fabric service install                 # launchd/systemd daemon; survives reboot + sleep/wake
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

(`fabric exec` requires `allow_exec = true` in the server's `peers.toml`.
Otherwise, use SSH to run the same two commands.)

### 4. Verify from the new machine

```sh
fabric status                     # each peer should show as reachable
fabric ping server                # prints pong + latency + transport (direct/relay)
fabric exec server -- echo ok     # if that peer enables exec; prints ok, exit 0
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
`fabric --home <home> daemon`; it does not run `fabric up`. No memory ceiling
is set unless you pass `--memory-max-mb`. Fabric declares none by default,
because a healthy working set has not been measured, and a ceiling nobody chose
kills the daemon at a number nobody chose. (This README used to say the default
was 1 GiB. It was not.)

If you do set one, leave headroom above fabric's in-process RSS recycle trigger.
During endpoint recycle, the replacement endpoint can briefly overlap with the
old endpoint in memory; a service cap too close to the 300 MiB recycle threshold
lets the OS kill fabric during a successful self-heal.

### Enabling remote shell and exec

Both remote shell (`fabric shell <peer>`) and non-interactive remote exec
(`fabric exec <peer> -- <cmd>`) are **default-deny**. The top-level
`allow_shell` and `allow_exec` settings in `peers.toml` control what this
machine serves. A missing setting is false.

**Check what a daemon serves** by running `fabric status` on it — it prints
`shell allowed` / `disabled` and `exec allowed` / `disabled`.

Each peer also needs an `allow` list that contains `shell` or `exec`. A peer
with no `allow` field has no grants. Both the machine setting and the peer grant
must permit the service.

Set the machine policy at the top of `peers.toml`:

```toml
allow_shell = true
allow_exec = true

[[peers]]
id = "<peer-node-id>"
name = "desktop"
allow = ["echo", "shell", "exec"]
```

Reload the file after each policy change:

```sh
fabric reload-peers
```

The daemon applies both machine settings during `reload-peers`. Confirm the
result with `fabric status`. The old command flags remain accepted for
compatibility, but they do not decide the policy.

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

Download a release asset directly, verify it against that asset's published
`.sha256`, and stage both the old and new binaries with same-directory
renames. A release archive contains exactly one member named literal `fabric`
(not `./fabric`); verify that shape before extracting:

```sh
set -eu

tag='v0.2.0+a0478a6'
case "$(uname -s):$(uname -m)" in
  Darwin:arm64) target='aarch64-apple-darwin' ;;
  Linux:x86_64) target='x86_64-unknown-linux-gnu' ;;
  Linux:aarch64|Linux:arm64) target='aarch64-unknown-linux-gnu' ;;
  *) echo "unsupported release target: $(uname -s):$(uname -m)" >&2; exit 1 ;;
esac

asset="fabric-$target.tar.gz"
release_url="https://github.com/compoundingtech/fabric/releases/download/$tag"
download_dir="$(mktemp -d)"
trap 'rm -rf "$download_dir"' EXIT

curl --fail --location "$release_url/$asset" --output "$download_dir/$asset"
curl --fail --location "$release_url/$asset.sha256" --output "$download_dir/$asset.sha256"

# The release publishes ONE .sha256 per asset, not a combined manifest. The file
# holds "<hash>  dist/<archive>", with the builder's path still in it, so
# `shasum -c` fails here. Take field one and compare it ourselves.
expected="$(awk 'NR == 1 { print $1 }' "$download_dir/$asset.sha256")"
test -n "$expected"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$download_dir/$asset" | awk '{ print $1 }')"
else
  actual="$(shasum -a 256 "$download_dir/$asset" | awk '{ print $1 }')"
fi
test "$actual" = "$expected"

members="$(tar -tzf "$download_dir/$asset")"
if [ "$members" != 'fabric' ]; then
  printf 'unexpected release archive members:\n%s\n' "$members" >&2
  exit 1
fi
tar -xzf "$download_dir/$asset" -C "$download_dir"
test "$("$download_dir/fabric" --version)" = "${tag#v}"

# Confirm this is the exact path in launchd/systemd ExecStart before replacing it.
fabric_path="${FABRIC_BIN_PATH:-$(command -v fabric)}"
test -x "$fabric_path"
rollback="$fabric_path.rollback-$(date -u +%Y%m%dT%H%M%SZ)"
install -m 755 "$fabric_path" "$rollback.new.$$"
mv -f "$rollback.new.$$" "$rollback"
install -m 755 "$download_dir/fabric" "$fabric_path.new.$$"
mv -f "$fabric_path.new.$$" "$fabric_path"
echo "installed $("$fabric_path" --version); rollback: $rollback"
```

Set `tag` to the release being installed. `FABRIC_BIN_PATH` is only needed when
the service's `ExecStart` binary is not the `fabric` found on the interactive
shell's `PATH`. Do not compile a release through `fabric exec` on a managed
target: the compiler inherits the daemon's service cgroup and, if a memory
ceiling is set, can exhaust it and restart the daemon that provides the
connection.

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

Connection counters live in `<home>/telemetry.json`. They are counters, not
state the product depends on, so an unreadable file starts the counts over and
says so rather than stopping the daemon. Delete the file to reset the counts.

### Did my session actually come back?

`fabric status` answers this from counters that survive a daemon restart:

```text
sessions	since 2026-08-24T14:12:09Z
  hetz	lost=4 resumed=4 failed=0 attempts=7 reconnect_p50=2.0s reconnect_max=4.5s
    lost_on=direct=3,relay=1 resumed_on=direct=2,relay=2
  droppy [not in peers.toml]	lost=3 resumed=0 failed=1 attempts=10885 reconnect_p50=- reconnect_max=-
```

The heading gives the start of the cumulative counter window. A daemon restart
does not reset it. A reset caused by an unreadable or incompatible snapshot says
why on the same line. A snapshot from before window tracking says that its start
is unknown instead of inventing a time.

Read the pair, not either number alone: 4 resumed out of 4 lost and 4 out of 40
are very different systems. `attempts` counts every retry, so it exceeds `lost`
whenever a break needed more than one try. `reconnect_p50` is the measured time
from the loss to the resume, which is not the retry backoff delay the log line
reports. The percentiles come from bounded histogram buckets, so they are
approximate and never exceed the largest sample seen; `reconnect_max` is exact.

The path breakdown answers "came back how". A session lost on `direct` that
resumes on `relay` is fabric falling back correctly. If that becomes the normal
outcome for a peer, the direct path to it is unhealthy even though the peer
still reports reachable.

A peer with no recorded loss is omitted. A separate `no losses recorded` line
means nothing has dropped during the displayed window. A telemetry peer that is
absent from the current allow-list is marked `[not in peers.toml]`. Its totals
are history, not evidence that Fabric still dials the removed peer.

### Which path is this peer actually using?

The peer table above shows one instantaneous ping. `fabric status` also reports
what every probe during the displayed telemetry window measured, split by path:

```text
paths
  droppy	reachable 252/252
    relay 	78%	n=196	mean=83.0ms	max=316.0ms
    direct	22%	n=56	mean=84.7ms	max=680.8ms
  hetz	reachable 252/252
    direct	99%	n=250	mean=64.8ms	max=335.3ms
    relay 	1%	n=2	mean=74.6ms	max=76.6ms
```

The busiest path is listed first, because which path a peer spends its time on
is usually the finding. Compare the two rows for one peer, not one peer against
another.

Read the example: `hetz` holds a direct path 99% of the time at 64.8ms — a
stable address. `droppy` sits on the **relay** 78% of the time, and when it does
get a direct path that path is no better on average and far worse at the tail,
680.8ms against 316.0ms. That is what a peer behind a moving address looks like.

Unlike the reconnect percentiles above, these are **exact**: `mean` and `max`
are stored precisely rather than bucketed. Percentiles are deliberately not
reported here — the latency buckets double in width, so around 50–200ms two
genuinely different paths fall into the same bucket and print identical
percentiles, hiding the difference this table exists to show.

Fabric also uses this evidence conservatively. Three consecutive samples must
exceed both one second and eight times that transport class's baseline before
Fabric closes the shared peer connection. The next stream creates a fresh
multipath connection, so iroh can select direct or relay again. A 60-second
per-peer cooldown prevents a redial storm. An endpoint generation change clears
the old evidence.

Git, sync, shell, exec, send-file, echo, and exposed services share this one
connection as separate streams. Simultaneous cross-dials converge on one
connection. Recent application traffic proves liveness, so Fabric skips the
next redundant health probe for that peer.

A peer is listed on probe evidence alone, so a healthy peer that has never
dropped a session still shows its paths.

### Fabric deletes its own old logs

The daemon writes one validation log per day to `<home>/logs/` and **keeps the
most recent 45, deleting older ones**. Say this out loud rather than let someone
find it out: a log from more than 45 days ago is gone, and an empty directory is
a bad way to learn that.

Forty-five days is chosen from the job the logs do, not rounded for looks. It has
to outlast a month away from the machine, so a fault in the first week is still
readable on return, with margin for the gap before anyone looks. At the observed
rate of 8.8–10.3 MB per day that is roughly 420 MB.

`FABRIC_LOG_RETENTION_DAYS` overrides the count. **`0` disables deletion
entirely** and restores unbounded growth, which is the right trade only if you
would rather spend disk than lose history. An unparseable value falls back to the
default rather than to unbounded, because failing open on an unattended machine
is the worst outcome. The daemon records the value it resolved in the
`diagnostic_logging_init` line, so the running config is checkable.

This bounds the **number of files**, which is what stops indefinite growth. It
does not bound bytes: one noisy day has reached 587 MB, so a bad run still costs
far more than the daily average suggests. Capping that is a question about log
volume, not retention.

Fabric does not reclaim logs written before this bound existed. Deleting those is
an operator's call.

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

Three branches on `origin` are neither merged into `main` nor known to be
obsolete, so `git branch --merged` will never retire them. Before you tidy them
away, read [docs/unresolved-branches.md](docs/unresolved-branches.md) — it says
what is known, what is not, and why deleting them on a guess is the wrong trade.

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
fabric git install-helper
fabric git share <remote> <repository>
fabric git grant <remote> <peer> --read|--write|--read-write
fabric git revoke <remote> <peer> --read|--write|--all
fabric git unshare <remote>
fabric git ls
fabric git status

git clone fabric://<peer>/<remote>
git remote add <name> fabric://<peer>/<remote>
```

Store a local Git directory and its exact peer grants in `peers.toml`. A share
starts with no access. Read and write are separate grants. A read grant serves
`git upload-pack`. A write grant serves `git receive-pack`, can update every ref
that Git accepts, and can run the repository's receive hooks.

Git finds `git-remote-fabric` on `PATH` for each `fabric://` URL. The installer
and updater place that relative helper link beside the `fabric` binary. Run
`fabric git install-helper` to repair a missing link. The command refuses to
replace an unrelated file.

The URL contains only a peer name and a declared remote name. It never contains
a host filesystem path. Fabric authenticates the peer, reads the requested
remote and operation, and checks the exact `git/<remote>/read` or
`git/<remote>/write` grant before it starts a Git process. Fabric runs Git with
direct arguments and no shell.

```sh
fabric reload-peers
```

Validate `peers.toml` and apply it to the running daemon without restarting.
The daemon keeps its previously loaded allow-list if parsing or validation
fails.

```sh
fabric status
```

Show the running daemon's local state and echo-ping every trusted peer. A normal
peer is reachable or unreachable. An offline peer with `roaming = true` is
away. Reachable peers include latency and the `direct`, `relay`, or `mixed`
transport path when iroh supplies it. Status also prints the daemon build.

```sh
fabric add <nodeid> [name] [--addr-json JSON] [--allow service,...]
```

Trust a peer NodeID and optionally assign a human name. `--addr-json` is an
optional local/direct address hint for deterministic same-machine testing; normal
key-only dialing relies on iroh address lookup.
An omitted `--allow` grants no service.

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

`--allow-shell` remains accepted for compatibility. It does not change the
policy. Set the top-level `allow_shell` field in `peers.toml` instead.

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

The `--allow-shell` and `--no-allow-shell` options remain accepted for
compatibility. They do not change the policy in `peers.toml`.

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

Only permitted remote NodeIDs are accepted before the local socket is opened or
the local TCP connection or exec command starts. If no trusted peer can reach a
new exposure, `fabric expose` warns once and names the peers that need the new
service in their `allow` lists.

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

**8080 and 9080 above are examples, not recommendations.** Pick a port you have
checked, and remember that **availability is not permission**: a port being free
right now does not make it yours to take. On a machine you share, `lsof -i :<port>`
tells you what is listening at this instant and nothing about whose port it is.
If a port belongs to somebody's habitual workflow, take a different one — a
dial listener is a local choice and costs nothing to move.

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

Open an interactive remote shell on a trusted peer over fabric. The server needs
top-level `allow_shell = true` and a `shell` grant for the caller in
`peers.toml`. The shell runs as the remote daemon's user and uses the remote
user's `$SHELL`. Current peers negotiate resumable `fabric/shell/1`, so the
same remote PTY survives a transient transport drop. A new client
automatically falls back to the byte-compatible one-shot `fabric/shell/0`
protocol when the peer is running an older Fabric release.

Enabling shell is a security-sensitive opt-in. Keep each peer's `allow` list
tight. Set `allow_shell = false` and reload the file to turn shell off.

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
fabric home, identity, peers, logs, and config in place. No memory ceiling is
set unless `--memory-max-mb` is passed, and `--no-memory-max-mb` removes one
that was. The shell and exec flags remain accepted for compatibility, but they
do not decide policy. Set one memory limit only after validating that endpoint
recycle can complete below that cap on the target machine.

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

### What fabric syncs, and where it differs from git

A catalog should be carriable by either fabric or git, so the two must agree
about what a catalog *is*. Fabric syncs the attributes git tracks:

| | git | fabric |
| --- | --- | --- |
| file content | yes | yes |
| executable bit | yes | yes |
| symlinks | yes | **no** — skipped, and logged when skipped |
| modification time | no | recorded but never applied |
| other permission bits | no | no |

Two differences are worth knowing before you rely on either transport.

**A `chmod` on an already-synced file does not propagate.** Git propagates one:
a mode change rewrites the tree object and is a real commit. Fabric does not,
because a chmod alters no bytes — see the limitation below. A **new** file
carries its mode correctly.

**A same-content rewrite does not propagate at all.** Rewriting a file with
identical bytes and a new timestamp changes nothing fabric will send, so a
replica keeps its older mtime. **Do not read a replica's mtime as an activity
signal** — if you need a heartbeat, put the time in the file's bytes.

Both are the same limitation: fabric cannot propagate a metadata-only change,
because a change that alters no bytes never advances a logical version, and that
early return is what keeps applying a peer's content free of echo loops.



`fabric sync` keeps a folder converged with trusted peers. A declarative config
file lists sync *entries*; the running daemon watches each folder and syncs
changes to peers near-instantly over fabric's own transport. A tool or a human
just adds an entry and drops files in the folder.

The watcher reacts only to filesystem mutations (create, modify, and remove);
opening or reading files does not schedule sync work. Write bursts settle for
150 ms, while a continuously changing tree is coalesced into at most one
watcher-driven sync per two-second window. Inbound no-op sessions already queued
for the same durable folder generation reuse its pre-merge scan, and routine
sync-accept path snapshots are sampled in the default validation log.

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

- `catalog` — union, newer-wins, and **a delete propagates**: a file present on
  any peer is present on all peers, and a file deleted on any peer is deleted on
  all peers. Tombstones are retained and never swept, so a delete cannot
  un-stick. Fabric does NOT restore a local deletion. It did until August 2026,
  and this README said so; that behaviour is gone, and git is the safety net for
  a file somebody still wanted.
- `bus` — union, newer-wins, and a delete propagates as a versioned tombstone.
  Tombstones are retained by default. Sweeping them is opt-in: set
  `FABRIC_TOMBSTONE_SWEEP_DAYS` in the daemon's environment, and only an entry
  with an explicit `peers` list sweeps, once every named peer has acknowledged
  the tombstone. `fabric sync ls` reports the sweep state per entry as `sweep=`.

Both policies propagate a delete, so the difference today is only whether a
tombstone may ever be swept. A tombstone inherited from an older build stays
authoritative under both policies: the stale bytes are removed, never revived.
If no node retains a copy, Fabric cannot reconstruct deleted content; recreate
the file deliberately.

Publication tools must treat every watcher-visible, included path as a durable
logical sync key. Stage temporary, backup, and partial files **outside the
configured sync folder** (on the same filesystem when an atomic rename is
required), then move only canonical final paths into the folder. Do not use a
sibling temporary name inside the synced subtree: either policy will propagate
that name as a real key, and deleting it later propagates that delete to every
peer.
Include globs scope which paths are keys; they do not make matching temporary
paths ephemeral.

Conflicts use logical versions, never filesystem mtime (which is unreliable
across machines). A higher logical version always wins. At the same version, a
Present update wins over a Tombstone delete, followed by deterministic
author/content-hash tie-breaks. For example, if two peers start from v1 while
offline, then one edits the file and the other deletes it, both operations are
v2 and the updated file intentionally reappears everywhere. If a peer deletes
that winning update afterward, the delete advances to v3 and removes it
everywhere under `bus`.

Fabric stores each entry's internal recovery data under
`<home>/sync/<sanitized-name>/`. `state.json` is the authoritative atomic record
of both the converged manifest—including bus tombstones—and the files last
observed on local disk; `manifest.json` is only a compatibility and inspection
projection. Do not edit, delete, or restore either file independently: losing
tombstones can resurrect deleted paths, while losing the observed-disk receipt
can make old physical bytes look like a new local update. A deliberate rollback
or downgrade must stop Fabric and restore the entire per-entry state directory
together with the matching Fabric binary and config, not only `manifest.json`.
A connected peer that still has a newer logical state can supersede that
rollback on the next reconcile.

### Sync Commands

```sh
fabric sync add <folder> --name <name> [--peers "*"|a,b] [--policy catalog|bus] [--include "*.toml"]
fabric sync ls
fabric sync ls --json
fabric sync rm <name-or-folder>
fabric sync reload
```

`fabric sync add` is a convenience writer for `syncs.toml`; the file can also be
hand-edited or provisioned before the daemon runs. `fabric sync reload` applies
the file to a running daemon, mirroring `reload-peers`. The daemon serves and
dials sync over the reserved `fabric/sync/1` ALPN, gated by the same peer
allow-list as every other fabric protocol.

`fabric sync ls` reports `present` (logical files in the manifest),
`tombstones` (retained logical deletions), and `observed` (included paths in the
last durable local-disk receipt). `drift=clean` means the logical Present paths
and observed bytes agree. A `drift=WARNING` names `missing` Present paths,
`unexpected` observed paths whose manifest is tombstoned or absent, and
`mismatched` paths whose observed content hash differs from the logical Present.
`scan_issues` names existing paths that the last scan could not read as syncable
regular files.

A delete propagates only when a complete parent directory listing proves the
path is absent. An unreadable path remains present with an unknown state. A file
over 512 MiB is also present but not syncable: fabric does not read, hash,
overwrite, or send it. Reduce it to 512 MiB or less, or exclude it from the sync
entry. If it is later deleted, the next complete scan propagates that delete.
The per-entry `full_scans`, `inbound_noop_transactions`, and
`inbound_guarded_transactions` counters are monotonic while that name remains
continuously configured in the same daemon process. They let operators measure
whether inbound reconciliation selected the exact-manifest no-scan path or the
guarded scan/materialize path. Ordinary reloads preserve the counters; a daemon
restart or removing and later re-adding the name starts a new counter epoch.
`fabric sync ls --json` emits a stable array with all of those fields plus a
Boolean `drift` for automation.

### Sync an st2 catalog safely

An st2 catalog mixes declarative fleet data, durable bus data, and strictly
machine-local runtime state. Do not make the catalog one broad sync entry.
Instead, configure these two positive allow-lists on **every** host:

```sh
ST2_CATALOG="${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog"

fabric sync add "$ST2_CATALOG" --name st2-declarations-default --peers "*" --policy catalog --include "_templates/**,agents/**/agent.kdl,plans/**"
fabric sync add "$ST2_CATALOG/agents" --name st2-bus-default --peers "*" --policy bus --include "**/resources/**,**/status"
```

The shell resolves `ST2_CATALOG` to an absolute, machine-local path. The path may
differ between hosts, but the logical names `st2-declarations-default` and
`st2-bus-default` must be identical everywhere. `--peers "*"` means every peer
already trusted by that host's `peers.toml`; to pin membership, replace it with
a comma-separated local selector such as `--peers "workstation,server"` on each
host. Peer aliases may differ between hosts even though the two sync names do
not.

The entries deliberately have different semantics:

- `st2-declarations-default` syncs only templates, `agent.kdl` declarations, and
  plans. It uses `catalog` policy because these files declare desired fleet
  membership. Retire an agent by editing its declaration (for example, setting
  its retirement field); do not express retirement by deleting the file.
- `st2-bus-default` syncs agent status plus everything below
  `resources/**`. That includes normal `resources/inbox`,
  `resources/archive`, `resources/context`, and `resources/links` paths. It uses
  `bus` policy so moves and deletions become tombstones and propagate instead
  of stale inbox or resource files reappearing.

> [!WARNING]
> PTY registries and process state are strictly machine-local and **MUST NEVER
> sync**. Never sync the entire st2 catalog. In particular, never include
> `$ST2_CATALOG/pty`, sockets, PIDs, locks, exec runtime state, logs, or
> temporary, backup, and partial files.
> Workspaces and hooks are provisioned separately unless a future explicit
> contract says otherwise. Never add a hidden `_syncproof` fixture; validate
> sync with ordinary agent resources and messages instead.

Positive includes are the safety boundary. When publishing an included file,
stage it outside the synced folder and move only its canonical final path into
place. A broader root, a catch-all include, or a watcher-visible sibling temp
file turns machine-local or partial state into a durable logical key.

After configuring every host, apply and check the live daemon:

```sh
fabric sync reload
fabric sync ls
fabric status
fabric ping <peer-name>
```

`fabric sync ls` must show the same two logical names and the same logical
Present/Tombstone counts on every host, with the local catalog paths and
intended peer selectors. In steady state `observed` equals `present` and
`drift=clean`. `fabric status` and `fabric ping` must show the selected peers
reachable.

For the default Fabric home, inspect the effective include lists and fail if
machine-local paths were added:

```sh
FABRIC_SYNCS="${XDG_CONFIG_HOME:-$HOME/.config}/fabric/syncs.toml"
FABRIC_STATE="${FABRIC_HOME:-$HOME/.local/share/fabric}/sync"

sed -n '1,200p' "$FABRIC_SYNCS"
if grep -Eq '_syncproof|catalog/pty|\.sock|\.pid|events\.jsonl|/logs?/' "$FABRIC_SYNCS"; then
  echo "unsafe st2 sync include in $FABRIC_SYNCS" >&2
  exit 1
fi

for state in \
  "$FABRIC_STATE/st2-bus-default/state.json" \
  "$FABRIC_STATE/st2-bus-default/manifest.json" \
  "$FABRIC_STATE/st2-declarations-default/state.json" \
  "$FABRIC_STATE/st2-declarations-default/manifest.json"
do
  test -f "$state"
  if grep -Eq '^[[:space:]]+"(pty|exec|run|logs?)/|\.sock"[[:space:]]*:|\.pid"[[:space:]]*:|events\.jsonl"[[:space:]]*:' "$state"; then
    echo "machine-local runtime key found in $state" >&2
    exit 1
  fi
done
```

No output from either failure branch is success. The state check intentionally
matches a root `pty/` key; an agent identity such as
`workstation/pty/resources/...` is ordinary allow-listed bus data, not the
sibling `catalog/pty` registry.

Use normal st2 operations for a harmless end-to-end check. On one host:

```sh
PROOF_IDENTITY="${ST_AGENT:?set ST_AGENT to the agent running this check}"
resource_ref="$(st2 resource add "https://github.com/compoundingtech/fabric" --title "st2 sync check" --tag "sync-check" --relation verification)"
message_file="$(st2 message send "$PROOF_IDENTITY" --subject "st2 sync check" -m "ordinary inbox item; verify on every host, then archive")"
printf 'identity=%s resource=%s message=%s\n' "$PROOF_IDENTITY" "$resource_ref" "$message_file"
```

On every host, including the origin, verify the same resource, inbox item, and
status:

```sh
st2 resource read "$PROOF_IDENTITY" "$resource_ref"
st2 message ls "$PROOF_IDENTITY" | grep -F "$message_file"
st2 status "$PROOF_IDENTITY"
```

Then archive the message once on the origin:

```sh
st2 message archive "$PROOF_IDENTITY" "$message_file"
```

On every host, the message must disappear from the inbox and appear once in the
archive:

```sh
if st2 message ls "$PROOF_IDENTITY" | grep -F "$message_file"; then
  echo "message still present in inbox" >&2
  exit 1
fi
st2 message ls "$PROOF_IDENTITY" --archive | grep -F "$message_file"
```

If a check has not converged yet, confirm `fabric status`/`fabric ping` first
and retry; do not weaken the include lists to make the proof pass.

## Declarative Peer Config

`peers.toml` is Fabric's allow-list file. It is intentionally
human-editable and can be provisioned before Fabric ever runs. The top-level
`allow_shell` and `allow_exec` fields control the services this machine offers.
Each field defaults to false. Each `[[peers]]` entry accepts:

- `id` (required): the peer's 64-character hexadecimal iroh NodeID.
- `name` (optional): a non-empty, unique local alias for commands such as
  `fabric ping workstation`.
- `addr` (optional): an iroh `EndpointAddr` hint whose `id` must match the
  peer's `id`.
- `roaming` (optional): whether this peer is expected to disconnect and return.
  It defaults to false.
- `allow` (optional): the service names this peer may reach. An omitted or
  empty list permits no service.

NodeIDs and names must be unique. Normal cross-machine setup should omit
`addr`; NodeID-based iroh discovery supplies the current addresses.

The same file accepts top-level `[[git_remotes]]` declarations. Each declaration
maps a logical remote name to an absolute host-local Git directory.

Trust is local and based on NodeID, not alias: `name` is only a command-line
label. Each machine must independently list the other NodeID. The `allow` list
grants named services such as `sync`, `shell`, `exec`, or an exposure name.
Anything unlisted is refused. The machine-level shell and exec settings also
apply and cannot be overridden by a peer entry.

A file can grant no services to one peer and exact services to another:

```toml
[[peers]]
id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
name = "laptop"
roaming = true
allow = []

[[peers]]
id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
name = "server"
allow = ["echo", "exec", "git/mandat/read", "send-file", "shell", "sync", "web"]

[[git_remotes]]
name = "mandat"
path = "/srv/git/mandat.git"
```

Git grants use `git/<remote>/read` and `git/<remote>/write`. A shell, exec, or
pty grant gives no Git access. The `git/` namespace is reserved and cannot be
used by `fabric expose`.

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
removes the session and kills/reaps its exec child.

Built-in `fabric shell` rides this resumable transport. It negotiates
`fabric/shell/1` and falls back to the legacy one-shot `fabric/shell/0` when the
peer does not offer it, so a mixed-version pair still works and only the newer
pair gets resumption. A shell that loses its transport briefly — a network blip,
a roaming peer, an endpoint recycle — reconnects to the *same* remote PTY and
replays unacked bytes, and the client prints a status line for the loss, each
reconnect attempt, and the resume.

The limit worth knowing before relying on it: resumption only survives an
outage shorter than the server's detached-session TTL, which defaults to **15
minutes** (`--server-session-detached-ttl-secs`). A closed lid over lunch keeps
its shell; a laptop left overnight does not. Past that window the server reaps
the PTY.

The window is 15 minutes because that is what the cost measures out to, not as a
round guess. An idle detached shell buffers nothing — 0 bytes across a full
detached window — so holding one costs a session struct and a PTY process and
nothing that grows with time.

A session still producing output is the expensive case, and it is bounded too.
The replay buffer stops at 4 MiB. Nothing acknowledges a detached session, so
the reader waits for buffer space that never frees and the remote process then
blocks writing to its own PTY. Measured directly, a runaway producer pins at
exactly 4 MiB and stays there. Retention is therefore bounded per session
regardless of how long this window is, and in aggregate by the server session
cap — 256 MiB at the default of 64 sessions.

Backpressure, not this TTL, is what bounds a runaway remote process. The TTL
bounds how long a session lives, not how much it holds.

> An earlier version of this section said the replay buffer had no cap, that a
> runaway shell would reach roughly 17 MB across this window, and that the TTL
> was the only backstop. All three were wrong. The "no cap" claim was also the
> stated reason not to raise the TTL further, so that reason no longer applies.

Past the window, the client does not retry a session the server has already
refused: it reports `remote shell could not resume`, names the expired session,
and exits non-zero, so a dead session ends promptly instead of hanging.
Restarting the remote daemon has the same effect, because the session store is
in memory. What is lost in both cases is the PTY and its scrollback, not just
the connection; only outages shorter than the window resume in place.
