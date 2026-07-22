# Dev-vs-prod isolation for fabric (and the general pattern)

_Design for review, 2026-07-22 by fabric-claude. For Nathan + cos. Bring the
design before big changes; this is that design._

## The problem (concrete, from this morning)

We must **develop fabric** — build, run, restart, crash it — **without ever
touching the production daemon** that is cos's *only path to hetz*, and without
the launchd service fighting a dev instance. This morning's status-5 outage was
exactly a **service-vs-manual race**: a managed launchd daemon and a manual
`fabric up` both wanting the same daemon, on the same home, socket, identity, and
service label. Nathan: *having fabric as a launchd/.service makes fabric
development hard.* Fix the isolation, structurally.

## What already isolates, and the two traps that don't

fabric keys almost everything off `--home` / `FABRIC_HOME`. A distinct home
already gives a distinct:

- **control socket** — `<home>/run/control.sock` (so `down`/`restart`/`status`
  target only that instance),
- **identity / NodeID** — `<home>/identity.toml` (dev gets its own node),
- **config / peers / dial sockets** — all under `<home>`,
- **iroh UDP port** — bound ephemerally (auto), so no fixed-port collision.

So a dev instance on its own home is **already structurally unable to collide**
with prod on socket, identity, or port. Two things still let a dev foot-gun into
prod:

1. **The launchd service label is global and fixed** (`com.compoundingtech.fabric`).
   A dev `fabric service install` — even on a dev home — installs the **same
   label**, colliding with prod's managed service. The service is a *prod-only*
   concept; nothing stops a dev from installing it.
2. **The CLI defaults to the prod home.** A `fabric` command with no
   `--home`/`FABRIC_HOME` targets `~/.local/share/fabric` = **prod**. A dev who
   forgets the flag runs `fabric down`/`restart`/`service install` against the
   *production* daemon. (This is the home-mismatch trap cos hit; the `9f5391b` fix
   aligned an explicit `--home <default-root>` with the CLI default, but the
   forgot-the-flag case remains.)

## Recommended model

**PROD is the only OS-managed service. DEV is always a manual run on a distinct
home. The tooling makes the wrong thing hard.**

1. **A dev-home convention via env.** Standardize `FABRIC_HOME` for dev, e.g.
   `~/.local/share/fabric-dev` (or a repo-local `./.fabric-dev/`). Set it once in
   the dev shell / a `direnv`/`.envrc`, and **every** `fabric` command auto-targets
   the dev instance — no per-command `--home`, so there's nothing to forget.
2. **`fabric service install` refuses a non-default home.** The managed service is
   prod-only; installing it against a `--home`/`FABRIC_HOME` other than the
   default state root should **error** ("the managed service is for the default
   prod home; dev instances run manually via `fabric up`") — or at minimum warn
   loudly. This makes trap #1 structurally impossible.
3. **Guard mutating CLI ops against a home/daemon mismatch.** For `down` /
   `restart` / `service *`, if the target home has no running daemon but a daemon
   *is* running on a different home, warn instead of silently no-op'ing or hitting
   the wrong one. Closes trap #2's blast radius.
4. **Optional ergonomic sugar — a `fabric dev` subcommand.** Runs a foreground
   daemon on the conventional dev home with a visible `DEV (home=…)` banner. Makes
   "start a throwaway instance to hack on" one obvious command that *cannot*
   resolve to the prod home. (Env convention #1 covers most of this; the
   subcommand is nicety, not required.)

### Acceptance

Build + run + restart + crash a **dev** fabric (on `FABRIC_HOME=…-dev`) and the
**prod** daemon (default home, launchd) never blips; the two can't collide on
socket, identity, or service label. Verified by: two instances up at once, kill
the dev one repeatedly, `fabric status` on prod stays reachable throughout.

## The general pattern (for pty, pty-rust, st2)

State it once, carry it to each owner:

> **Per-instance home = per-instance {socket, identity, config, ephemeral port}.
> PROD is the ONE OS-service (launchd/systemd). DEV is a manual run on a distinct
> home. The service-install command refuses a non-default home, and mutating
> commands default to an env-selected home so a dev never targets prod by
> forgetting a flag.**

Each tool exposes its own `*_HOME` env (`FABRIC_HOME`, `PTY_HOME`, …) and keeps
all per-instance state under it. fabric is the acute case because it's a live
network dependency; the pattern generalizes directly.

## Defense-in-depth: service `KillMode=process`

Real incident, 2026-07-22 deploy: restarting hetz's fabric service SIGTERM'd the
**entire agent fleet**, because a prior manual recovery had launched `convoy up`
(and all agents) from inside a `fabric shell` — so those processes inherited
fabric's systemd cgroup, and the unit's default `KillMode=control-group` kills the
whole cgroup on stop/restart. (Primary fix was operational: the fleet was
relaunched under its own `convoy-up.service` unit = isolated cgroup. And during
the deploy we avoided the same class by having Nathan fire the hetz restart from
ssh rather than from a `fabric shell` child.)

Belt-and-suspenders for fabric itself: set the fabric service unit to
**`KillMode=process`** so a restart kills only the daemon, never a neighbor that
mistakenly landed in its cgroup.

Owner assessment (recommend `KillMode=process`): fabric does **not** rely on
cgroup-reaping to clean up its own children — every child it spawns
self-terminates when the daemon closes its stdio: exec-expose children exit on
stdin EOF (the locked `--exec` contract), `shell` PTY children get SIGHUP when the
master closes, `exec` command children hit SIGPIPE/EOF. So the cgroup-kill buys
little tidy-up and risks catastrophic collateral kills. The only downside of
`process` is that a misbehaving child ignoring stdio-close would linger as an
orphan (a minor leak) instead of being force-killed — an acceptable trade against
killing unrelated neighbors. (`mixed` is not enough: it still SIGKILLs the whole
cgroup at the end, so a stuck neighbor still dies.)

Two places to apply it (fold into the isolation follow-up, currently doc-only):

- fabric's **generated** unit — `render_systemd_user_unit` in `src/service.rs`
  currently sets no `KillMode` (defaults to `control-group`); add
  `KillMode=process` there (+ a unit-render test).
- **hand-written** units like hetz's `fabric-keepalive.service` — an ops change,
  not fabric code; flag to add `KillMode=process` when convenient.

## Scope note

This is design only — no big change yet, per the gate. If approved I'll implement
the small, safe pieces first (service-install refuses non-default home; the
mutating-op mismatch guard) and document the `FABRIC_HOME`-for-dev convention in
the README; the `fabric dev` subcommand is optional follow-up.
