# Stability

What fabric promises from 1.0.0 on, and what each kind of release may
change. Every part of fabric is in one of three levels:

- **Stable**: changes only in ways that keep working for whatever used it,
  until 2.0.
- **Provisional**: shipped and supported, but its interface may still
  change in a minor release, with a CHANGELOG entry that says so.
- **Internal**: how fabric works inside. It may change in any release. Do
  not build on it.

Anything this page does not name is internal.

## What is stable

| Area | What exactly |
| --- | --- |
| Identity | A NodeID is the public half of the machine's ed25519 key. The `identity.toml` format. `fabric key gen` and `fabric id`. |
| Trust and grants | `peers.toml`: each peer's `id`, `name`, `allow`, `dial_only` and `inbound_only`. A grant is the full service name, matched exactly, and an unlisted service is refused. An untrusted NodeID is refused. An inbound-only peer is never dialled or probed by this machine. `fabric reload-peers` ends every session the new file no longer allows. `fabric add` (with `--allow`, `--dial-only` and `--inbound-only`), `fabric remove`, `fabric peers`. |
| Exposures and dials | `fabric expose` (a Unix socket, a TCP address or `--exec` command), `fabric unexpose`, `fabric dial` (a Unix socket, or `--tcp`), `fabric undial`. A service name is its ALPN. Persisted exposures and dials come back after a restart. |
| The direct exposure wire | Everything in [docs/tunnel-wire.md](tunnel-wire.md): admission, the frame layout, Hello, Data, Ack, Close and Error, resume, the limits and their defaults, and the 403 refusals with their reasons. |
| Git over fabric | `fabric://<peer>/<remote>` URLs, the grants `git/<remote>/read` and `git/<remote>/write`, and `fabric git share`, `unshare`, `grant` and `revoke`. |
| Error codes | Everything in [docs/errors.md](errors.md): the codes, their classes, exit statuses, the `--json` error object and the `fabric:<code> ` token on the wire. |
| The commands programs drive | `id`, `addr`, `peers`, `add`, `remove`, `reload-peers`, `expose`, `unexpose`, `dial`, `undial`, `probe`, `peer-events --watch`, `send-file`, `exec`, `up` and `down`. This covers their arguments and flags, their exit statuses, and the `--json` output of each command that has it. Human-readable output is not part of it, so parse `--json`. |
| Talking across versions | Any two 1.x daemons can trust, dial, expose to and run exec on each other, whatever their minor versions. |

## What is provisional

| Area | Why |
| --- | --- |
| File sync (`fabric sync`, `syncs.toml`, the `fabric-sync` companion, `fabric/sync/1`) | Still settling: open performance and catalog-removal work. It will get its own 1.0. |
| `fabric-stdio` | The way to write an exec service. New, and its API may still change. |
| `fabric shell` | Interactive use is supported. The resumption window and status lines may change. |
| `fabric join`, `fabric ca`, `fabric cert`, `fabric service`, `fabric restart` | Supported conveniences whose flags and output may still change. |
| `fabric doctor` and `fabric status` exit statuses | Their meanings may still be refined. |

## What is internal

- `fabric/mux/2`, the framing between daemons, and the built-in services
  carried over it (`fabric/exec/0`, `fabric/shell/1`,
  `fabric/send-file/0`). Their compatibility across 1.x is promised above;
  their bytes are not.
- The control socket and its JSON schema. Use the commands.
- `telemetry.json`, the logs and the names of validation-log events
  (`runtime_stalled`, `access_revoked` and the rest).
- The human output of every command, `doctor` and `status` included.
- How `fabric update` and the restart supervisor work: their helper
  processes, generation records and jobs.
- `fabric debug` and the hidden commands (`daemon`, `restart-helper`,
  `restart-detacher`, `supervise-restart`, `install-helper`).
- Files under the state directory other than `identity.toml`, and the
  `config.toml` layout. What it records is stable through the commands that
  write it.
- The Rust crates (`fabric`, `fabric-config`, `fabric-sync`,
  `fabric-service-api`) as libraries. `fabric-stdio` is the exception and is
  provisional.

## The 1.x rules

Releases follow semantic versioning: MAJOR.MINOR.PATCH.

**A patch release** (1.2.3 to 1.2.4) fixes bugs. It adds nothing and changes no
interface, provisional ones included.

**A minor release** (1.2 to 1.3) may:

- add commands, flags, `--json` fields, error codes, `peers.toml` keys and
  services;
- change a provisional part, with a CHANGELOG entry under **Changed** that
  names it as provisional and says what to do;
- deprecate a stable part (see below);
- change anything internal.

**Only 2.0** may:

- change the wire: the direct exposure wire, the 403 refusals and their
  tokens, or what a 1.x daemon accepts from another 1.x daemon;
- change what a grant means, what trust means or how `peers.toml` is read;
- remove or rename a stable command, flag, `--json` field or error code, or
  change its meaning, type or exit status;
- stop reading a file a 1.x release wrote.

Additions are not breaking. A program that reads `--json` must ignore keys
it does not know. A program that reads error codes must act on the class of
a code it does not know (docs/errors.md).

**Deprecation.** A stable part that will go away is marked deprecated in a
minor release. That release adds a CHANGELOG entry under **Deprecated** and
documents the replacement. When the part is used, it prints a warning on
stderr. It keeps working until 2.0 at the earliest.

**Security.** A fix for a vulnerability may break stable behaviour when the
old behaviour is the vulnerability. The CHANGELOG says so under
**Security**, in any kind of release.

**Mixed versions.** A daemon and its sync companion must be the same build;
the companion refuses to attach to any other. Two daemons only need to share
the major version.

## The CHANGELOG

[CHANGELOG.md](../CHANGELOG.md) follows
[Keep a Changelog](https://keepachangelog.com/). Every change lands with its
entry under **[Unreleased]**, in one of Added, Changed, Deprecated, Removed,
Fixed or Security, and the release moves them under its version. From 1.0.0:

- An entry that changes something stable says so, and is only ever under
  Added, Deprecated, Fixed or Security in a 1.x release.
- An entry that changes something provisional starts with **Provisional:**
  and names the part.
- Internal changes are recorded when someone could notice them: a log event,
  a file, a measured cost.
