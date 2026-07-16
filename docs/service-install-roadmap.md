# Fabric Managed Service Roadmap

This is the queued roadmap for making `fabric daemon` a managed service across
the machines Nathan actually uses. Roaming reconnect remains the higher priority;
this work supports reliability by replacing hand-launched daemons, the watchdog
hack, and one-off keepalive units with OS-native supervision.

## Reference

`n0-computer/pigeons` has the useful shape to borrow:

- CLI surface: `service install`, `service uninstall`, `service restart`,
  `service status`, and `service log`.
- Linux/macOS: generate and run installer scripts from Rust.
- Windows: stage the binary and configure SCM through the `windows-service`
  crate.
- Runtime: supervisor starts the long-running foreground server command, not a
  helper that daemonizes and exits.

Checked reference commit: `n0-computer/pigeons@9dcf720`.

Key files in that implementation:

- `src/service/mod.rs`
- `src/service/linux.rs`
- `src/service/macos.rs`
- `src/service/windows.rs`
- `service/install_linux.sh`
- `service/install_macos.sh`
- `src/tunnel.rs`

## Fabric Shape

Add a first-class command group:

```sh
fabric service install [--home <dir>] [--allow-shell | --no-allow-shell]
fabric service status
fabric service logs
fabric service restart
fabric service uninstall
```

The installed service should run the foreground daemon entrypoint directly:

```sh
fabric --home <home> daemon [--allow-shell]
```

Do not run `fabric up` from a service manager. `fabric up` starts a background
daemon and exits; service managers need to own the foreground process.

## Platform Plan

Linux:

- Prefer a systemd user unit by default, because fabric identity, peer config,
  dials, and logs are user-owned.
- Support a documented system unit mode later for dedicated boxes if needed.
- Enable restart-on-failure with bounded restart delay.
- On servers that must survive logout/reboot, document `loginctl enable-linger`.
- Expose `journalctl --user -u fabric.service` through `fabric service logs`.

macOS:

- Prefer a per-user LaunchAgent by default for the same state ownership reason.
- Use `KeepAlive` and `RunAtLoad`.
- Write stdout/stderr to the fabric home log directory, not a system-wide root
  path.
- Keep a LaunchDaemon/system mode as a later option only if Nathan wants a
  machine-wide service account.

Windows:

- Use SCM through `windows-service`.
- Stage the service binary in a stable install location.
- Decide explicitly whether the service runs as the current user, LocalService,
  or a dedicated service SID before implementation, because fabric state and peer
  config must be readable by the account running the daemon.
- Add Windows firewall rules only if they prove necessary for iroh UDP/relay
  traffic.

## Acceptance Criteria

- `fabric service install` leaves the daemon managed and running.
- The service restarts automatically after a daemon crash.
- A reboot brings the daemon back without an interactive shell.
- `fabric status` works after service install and after service restart.
- Existing identity and `peers.toml` continue to be used; NodeID does not change.
- Persisted exposes in `<home>/config.toml` survive service restart.
- `fabric service uninstall` stops the service and removes only service-manager
  artifacts, not identity, peers, or fabric home state.
- No SSH-key auth model is adopted. Fabric continues to use its own peer
  allow-list and config.

## Non-Goals For The First Slice

- No replacement for the current roaming reconnect work.
- No multi-daemon stack supervisor for pty or convoy.
- No switch to SSH user keys as fabric authorization.
- No system-wide service account unless the user explicitly selects that mode.
