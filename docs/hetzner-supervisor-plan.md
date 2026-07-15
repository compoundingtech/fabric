# Hetzner Supervisor Plan

Status: parked. The Hetzner supervision model is undecided. Do not deploy the
standalone systemd-per-daemon units or a convoy-owned replacement from this
document until Nathan makes the architecture call.

The open architecture question is whether Hetzner should be supervised as:

```text
systemd -> convoy up -> fabric + pty remote-serve + st-sync + agents
```

or as standalone systemd-owned daemon units such as:

```text
systemd -> fabric
systemd -> pty remote-serve
systemd -> fabric pty-view exposure
```

This document is retained as a run-surface and diagnostic reference while that
choice is pending.

## Run Surfaces For Convoy

These are the process commands and readiness checks any supervisor model needs
to preserve.

### Fabric

Run surface:

```sh
~/.local/bin/fabric --home ~/.local/share/fabric daemon --allow-shell
```

Required readiness checks:

```sh
~/.local/bin/fabric status
```

The status output must include:

```text
shell	allowed
```

This is a lockout check. A live daemon with `shell	disabled` is not a healthy
Hetzner recovery state until pty remote attach fully replaces shell recovery.

### pty Remote-Serve

Run surface:

```sh
PTY_ROOT=~/.local/state/convoy/pty \
  ~/.local/bin/pty remote-serve --socket "$XDG_RUNTIME_DIR/pty-remote.sock"
```

Important constraints:

- `PTY_ROOT` must point at Hetzner's real session registry:
  `~/.local/state/convoy/pty`.
- The remote socket must stay outside `PTY_ROOT`; otherwise pty can mis-scan it
  as a phantom session.
- pty is adding `pty remote-serve --print-systemd-unit` as an authoritative
  source for the exact service surface. When that lands, prefer the generated
  command details over hand-maintained copies.

### Fabric Exposure For pty

Run after both fabric and pty remote-serve are ready, and after the socket
exists:

```sh
~/.local/bin/fabric expose pty-view --socket "$XDG_RUNTIME_DIR/pty-remote.sock"
```

Required readiness check:

```sh
~/.local/bin/fabric status
```

The status output must include `pty-view` in `exposed`.

## Historical Standalone Systemd Draft

The remaining sections are the pre-decision standalone systemd draft. They are
useful for command surfaces and acceptance checks, but they are not active
deploy instructions until Nathan chooses the supervisor model.

## Goals

- Keep the Hetzner `fabric` daemon running under systemd with automatic restart.
- Keep the pty remote-control server running under systemd with automatic
  restart.
- Preserve `fabric --allow-shell` until pty remote attach fully replaces it.
- Make deploys diagnosable: verify binary version before restart and verify
  local health after restart.
- Log to both journald and the existing app logs where practical.
- Avoid using an interactive shell, `nohup`, detached helpers, or pty sessions
  as the long-term owner of either daemon.

## Non-Goals

- Do not deploy this while the fabric bus is down.
- Do not run the WAN drop test as part of supervisor installation.
- Do not replace pty's session-level persistence. systemd owns only the
  per-machine service processes; pty still owns PTY session state.

## Service Ownership

Use systemd user units for the Hetzner account that owns the fabric and pty
state. Enable lingering once so the user manager starts at boot and survives
logout:

```sh
sudo loginctl enable-linger "$USER"
loginctl show-user "$USER" -p Linger
```

Expected:

```text
Linger=yes
```

The units live in:

```text
~/.config/systemd/user/fabric.service
~/.config/systemd/user/pty-remote-serve.service
```

## Fabric Unit

Use `fabric daemon` directly so systemd owns the foreground daemon process. Do
not use `fabric up`, because `up` is a convenience command that can spawn a
background daemon and then exit.

Draft:

```ini
[Unit]
Description=Fabric daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/fabric --home %h/.local/share/fabric daemon --allow-shell
Restart=on-failure
RestartSec=2s
StartLimitIntervalSec=60
StartLimitBurst=10
KillSignal=SIGTERM
TimeoutStopSec=10s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
```

Notes:

- `--allow-shell` is intentionally explicit for the current Hetzner workflow.
  Long-term, move this policy into config so restarts do not depend on command
  memory.
- The daemon still writes its existing file log under
  `~/.local/share/fabric/logs/daemon.log` for app-level diagnostics.
- `Restart=on-failure` restarts crashes and non-zero exits. If we decide manual
  `fabric down` should also be healed immediately, change to `Restart=always`
  and use `systemctl --user stop fabric` for intentional stops.

## pty Remote-Serve Unit

The pty remote-control server should be owned the same way. pty confirmed that
`pty remote-serve --socket <SOCK>` is the long-running process systemd should
own directly with `Type=simple`.

Draft:

```ini
[Unit]
Description=pty remote control server
After=fabric.service
Wants=fabric.service

[Service]
Type=simple
Environment=PTY_ROOT=%h/.local/state/convoy/pty
ExecStart=%h/.local/bin/pty remote-serve --socket %t/pty-remote.sock
Restart=on-failure
RestartSec=2s
StartLimitIntervalSec=60
StartLimitBurst=10
KillSignal=SIGTERM
TimeoutStopSec=10s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
```

Notes:

- `PTY_ROOT` is critical. On Hetzner, pty sessions live under
  `%h/.local/state/convoy/pty`. If this environment variable is wrong,
  remote-serve will read the wrong registry and remote `ls`/`attach` will look
  empty even though the service is running.
- `%t/pty-remote.sock` expands to `$XDG_RUNTIME_DIR/pty-remote.sock`, such as
  `/run/user/<uid>/pty-remote.sock`. Keep this socket outside `PTY_ROOT`; a
  socket under `PTY_ROOT` can be mis-scanned as a phantom pty session.
- Use the absolute pty binary path actually installed on Hetzner. systemd does
  not use an interactive shell `PATH`.
- Leave `PTY_REMOTE_SERVE_DEBUG` unset in production unless diagnosing this
  service.
- `Type=simple` is intentional. Do not background, `nohup`, `setsid`, or
  double-fork the process.

## pty Fabric Exposure Unit

pty stays transport-agnostic, so fabric exposes the pty remote-control socket in
a separate one-shot unit on older fabric builds. Current fabric persists exposes
to `<home>/config.toml`, so re-running `fabric expose pty-view ...` once is
enough for fabric restarts after the socket path is stable. pty confirmed the
fabric protocol/ALPN is `pty-view`.

Draft:

```ini
[Unit]
Description=Expose pty remote control over fabric
After=fabric.service pty-remote-serve.service
Requires=fabric.service pty-remote-serve.service
PartOf=fabric.service pty-remote-serve.service

[Service]
Type=oneshot
ExecStartPre=/bin/sh -lc 'for i in $(seq 1 50); do test -S %t/pty-remote.sock && exit 0; sleep 0.1; done; echo "pty remote socket not ready: %t/pty-remote.sock" >&2; exit 1'
ExecStart=%h/.local/bin/fabric expose pty-view --socket %t/pty-remote.sock
RemainAfterExit=yes

[Install]
WantedBy=default.target
```

Notes:

- The `ExecStartPre` wait avoids a startup race where systemd has started
  remote-serve but the Unix socket has not appeared yet.
- Current fabric persists exposes by default. After restarting `fabric.service`,
  verify `fabric status` still lists `pty-view`; no manual re-expose should be
  needed unless the target socket path changes. Use `fabric unexpose pty-view`
  to retire the durable mapping.
- `<home>/config.toml` is the durable daemon config for shell policy, trusted
  peers, and exposes.
- The companion unit is only needed for older fabric builds without
  `<home>/config.toml` persisted-expose support.

## Deployment Flow

Run this only after Hetzner is reachable by SSH or a known-good control path.

1. Diagnose before changing anything:

```sh
pgrep -af fabric || true
tail -200 ~/.local/share/fabric/logs/daemon.log || true
tail -200 ~/.local/share/fabric/logs/restart.log || true
uptime
df -h
```

2. Install the intended binaries.

For fabric, use the existing release installer or copy the built binary to:

```text
~/.local/bin/fabric
```

For pty, install the matching pty build to:

```text
~/.local/bin/pty
```

3. Verify versions before restart:

```sh
~/.local/bin/fabric --version
~/.local/bin/pty --version
```

The fabric version must include the expected short git SHA.

4. Install or update unit files:

```sh
mkdir -p ~/.config/systemd/user
$EDITOR ~/.config/systemd/user/fabric.service
$EDITOR ~/.config/systemd/user/pty-remote-serve.service
$EDITOR ~/.config/systemd/user/fabric-pty-view-expose.service
systemctl --user daemon-reload
```

5. Enable lingering and services:

```sh
sudo loginctl enable-linger "$USER"
systemctl --user enable fabric.service
systemctl --user enable pty-remote-serve.service
systemctl --user enable fabric-pty-view-expose.service
```

6. Restart fabric first and verify locally before declaring the machine healthy:

```sh
systemctl --user restart fabric.service
systemctl --user status fabric.service --no-pager
journalctl --user -u fabric.service --no-pager -n 100
~/.local/bin/fabric status
```

The `fabric status` output must include:

```text
shell	allowed
```

This is a lockout check, not cosmetic output. If shell shows `disabled`, the
daemon is alive but remote shell recovery is broken; fix the unit command or
the future shell policy config before declaring the deploy healthy.

7. Restart pty remote-serve, expose it through fabric, and verify locally:

```sh
systemctl --user restart pty-remote-serve.service
systemctl --user status pty-remote-serve.service --no-pager
journalctl --user -u pty-remote-serve.service --no-pager -n 100
systemctl --user restart fabric-pty-view-expose.service
systemctl --user status fabric-pty-view-expose.service --no-pager
~/.local/bin/fabric status
```

The `fabric status` output must include `pty-view` in `exposed`.

8. From the Mac, verify remote reachability:

```sh
fabric ping hetzner
fabric status
```

Do not proceed to WAN drop testing until these checks are green.

## Recovery Commands

When Hetzner is sick and SSH is available, capture cause before restarting:

```sh
date -Is
hostname
uptime
pgrep -af fabric || true
pgrep -af pty || true
tail -200 ~/.local/share/fabric/logs/daemon.log || true
tail -200 ~/.local/share/fabric/logs/restart.log || true
systemctl --user status fabric.service --no-pager || true
systemctl --user status pty-remote-serve.service --no-pager || true
systemctl --user status fabric-pty-view-expose.service --no-pager || true
journalctl --user -u fabric.service --no-pager -n 200 || true
journalctl --user -u pty-remote-serve.service --no-pager -n 200 || true
journalctl --user -u fabric-pty-view-expose.service --no-pager -n 200 || true
df -h
free -h || true
```

Then restart:

```sh
systemctl --user restart fabric.service
systemctl --user restart pty-remote-serve.service
systemctl --user restart fabric-pty-view-expose.service
~/.local/bin/fabric status
```

## Acceptance Criteria

- `systemctl --user is-enabled fabric.service` prints `enabled`.
- `systemctl --user is-active fabric.service` prints `active`.
- `systemctl --user is-active pty-remote-serve.service` prints `active`.
- `fabric status` works locally on Hetzner after a service restart.
- `fabric status` prints `shell	allowed` after a service restart.
- `fabric status` lists `pty-view` under `exposed` after restarting
  `fabric-pty-view-expose.service`.
- `fabric ping hetzner` works from the Mac after a service restart.
- Killing the fabric process causes systemd to restart it:

```sh
pkill -f 'fabric .* daemon'
sleep 3
systemctl --user is-active fabric.service
~/.local/bin/fabric status
```

- Rebooting Hetzner brings the fabric service back without SSH login, assuming
  lingering is enabled.
- Rebooting Hetzner brings pty remote-serve and the `pty-view` fabric exposure
  back without SSH login, assuming lingering is enabled.

## Later Hardening

- Simplify the Hetzner keepalive unit after rollout so config, not
  `daemon --allow-shell`, is the source of shell policy.
- Add `fabric doctor` to collect process, log, version, and reachability facts.
- Add `fabric status` fields for generic tunnel reconnecting state, attempts,
  last error, and buffered bytes before pty attach becomes user-facing over WAN.
- Add richer `fabric expose` status output that distinguishes socket, exec, and
  ephemeral targets.
