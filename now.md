# now — fabric working state

The living handoff for whoever owns fabric next (there was none before; keep this
current). This records what is DONE, what is IN FLIGHT, and what is NEXT — the
things the repo history alone does not carry.

_Last updated: 2026-07-21 by fabric-claude (opus). fabric 0.2.0 on origin main._

## Hotfix DEPLOYED — peer-config split (resolved 2026-07-21)

On 2026-07-21 the Mac↔hetz stopgap sync (rsync over `fabric dial`) failed in a
loop and blocked cos from reaching the hetz fleet. Root cause: the launchd
service launched the daemon as `--home ~/.local/share/fabric`, which (pre-fix)
made the dial path read `peers.toml` from under `--home` while the CLI writes
`~/.config/fabric/peers.toml`; a default-home `fabric add` migrated + deleted the
in-`--home` copy, leaving the daemon with zero peers on the dial path
(`unknown peer` in `service.err.log`) while `ping`/`status`/`shell` kept working.

- **Durable fix (commit `9f5391b`):** `FabricHome::resolve` treats an explicit
  `--home`/`FABRIC_HOME` equal to the default state root like the no-arg default
  (peers from `~/.config/fabric/`); a genuinely different `--home` stays isolated.
  Unit-tested (`resolve_from` + 6 regression tests).
- **DEPLOYED:** as of ~17:33 the Mac daemon runs the fixed binary
  (`0.2.0+9f5391b`, pid rotates under launchd) — swapped via stop-old →
  `fabric service install`. Verified: hetzner reachable, dial probe returns
  bytes, sync log clean bidirectional cycles, no fresh `unknown peer`.
- **launchd label changed:** the service is now `com.compoundingtech.fabric`
  (was `com.myobie.fabric`, org rename). The stale `com.myobie.fabric` job was
  booted out and its plist moved aside to `*.stale-501`. Use the new label.
- **Holds LIFTED:** default-home `fabric add`/`remove` and `fabric restart` are
  safe again — the daemon reads the same `~/.config/fabric/peers.toml` as the CLI.

## What fabric is

A standalone Rust CLI + local daemon that hides iroh behind local Unix sockets.
Consumer tools ask fabric for a local socket wired to a trusted remote peer and
speak their own protocol over it; only fabric touches iroh/QUIC/relays/NodeAddr
and the peer allow-list. See `README.md` and `SKILL.md`.

## In flight — `fabric sync` (generic file-sync primitive)

fabric now owns file sync. A config file (`syncs.toml`) lists sync entries; the
running daemon watches each folder and keeps it converged with its peers.

Status:
**Fully landed + pushed to origin main (fabric 0.2.0):** the config surface
(`syncs.toml`) and the property-tested reconciliation core
(`src/sync/{config,manifest,node,glob}`), the on-wire backend
(`src/sync/wire.rs`), the async `SyncEngine` (`src/sync/engine.rs`, fs-watch +
scan/materialize), the daemon wiring (`fabric/sync/1` ALPN, `IrohSyncTransport`,
engine started at boot, `SyncReload`/`SyncStatus` control ops), and the
`fabric sync add/ls/rm/reload` CLI. Proven end-to-end over real iroh by
`tests/sync_slice.rs`. All tests green; CLI smoke-tested; zero warnings.

Design decisions (why, so they are not re-litigated):
- **Backend-agnostic semantics.** The merge/conflict/delete/echo/convergence
  rules live in fabric (`sync::manifest` + `sync::node`), above a swappable
  transport. `Manifest::merge` is a semilattice join (commutative/associative/
  idempotent) → convergence and echo-freedom are structural, proven by property
  tests. Newer-wins = Lamport version + deterministic author tie-break (never a
  wall clock). The on-wire backend is verified to reach the exact same state as
  the pure reference reconcile — that is the swappable-backend guarantee.
- **Policies.** `catalog` = union + newer-wins + never-delete-on-peer + no-sweep
  (a local delete is restored; decommission is an edit, e.g. `retired = true`).
  `bus` = + tombstone deletes (modelled; sweep TTL not yet wired).
- **Config.** `~/.config/fabric/syncs.toml`, hand-editable, `fabric sync reload`
  applies live (mirrors `peers.toml` / `reload-peers`). Entry =
  `{name, folder, peers, policy, include?}`. `name` is the shared logical key
  (same name on two boxes = same sync); `peers = "*"` follows the `peers.toml`
  allow-list.

## Next

0. **ROAMING GAP — the shareability blocker (Nathan 2026-07-22). BUILT + STAGED,
   awaiting the deploy window.** A peer that changes network/public IP went
   unreachable both ways until its daemon was manually restarted. Root cause:
   fabric only reacted to LOCAL netmon changes and never probed peer reachability.
   - **Fix landed (commit `298a593`):** `run_peer_health_loop` echo-probes each peer
     every `FABRIC_PEER_HEALTH_SECS` (default 20s) and, on N consecutive failures,
     drives recovery (drop tunnels + iroh `network_change()` → re-discover/relay;
     recycle only after repeated nudges fail) — no local-change dependency. Pure
     `PeerHealthTracker` decision core with escalating backoff, fault-injection
     unit-tested. Emits per-probe latency + direct/relay telemetry. `=0` disables.
   - **Staged, NOT deployed:** the running daemon is still `9f5391b`. Deploy =
     `./install.sh` + restart, which blips cos's only path to hetz → **Nathan-gated
     window** with him present. Rollback ready: `~/.local/bin/fabric.prev` = the
     known-good `9f5391b` binary (restore + restart to revert). Live validation:
     move a laptop between networks, confirm self-heal without a manual restart.
   - Analysis + reliability data + nix design: `docs/multi-machine-reliability-2026-07-22.md`.

0b. **`fabric exec` — non-interactive remote command execution (Nathan 2026-07-22).
   BUILT + STAGED, same deploy window as the roaming fix.** `fabric exec <peer> --
   <cmd>` = scriptable counterpart to `shell` (stream stdout/stderr, propagate
   exit code). DEFAULT-DENY per machine via `allow_exec` (`--allow-exec` on
   up/daemon/service install), separate ALPN `fabric/exec/0`. Validated with a
   local two-node e2e test (allow + deny + stderr split + exit codes); that test
   caught a real bug — `dial_alpn` routed exec through the mux tunnel whose Hello
   frame corrupted the argv; exec now uses the raw dial path like shell.
   **Combined deploy bundle — LOCKED, ONE Nathan-gated window = main HEAD:**
   roaming self-heal (`298a593`) + `fabric exec` (`4d02a01`/`4fe8557`) +
   service-install idempotency (`83a3614`) + dev/prod isolation (`a9b336e`:
   service-install refuses a non-default home, down/restart home-mismatch guard,
   README FABRIC_HOME-for-dev convention). Fold-in is FINAL (cos's superseding
   "LOCKED" decision — earlier decouple/tag approach retired; no tag). Build the
   Mac binary from **main HEAD**. New binary on Mac AND hetz + `--allow-exec` on
   hetz so cos can `fabric exec hetzner -- <cmd>`. Rollback:
   `~/.local/bin/fabric.prev` = 9f5391b.

   **Deploy runbook verify (Nathan present, fabric.prev ready):** (a) REGRESSION —
   default-home `fabric service install` still succeeds + the managed daemon comes
   up (`fabric status` reachable, shell allowed) — the one risk the isolation could
   add; (b) FEATURE — `fabric --home /tmp/x service install` refused,
   `fabric --home /tmp/empty down` warns about the running default daemon; (c)
   exec — `fabric status` shows `exec allowed` on hetz + `fabric exec hetzner --
   echo ok`; (d) roaming — `peer_health_probe` telemetry + a laptop-moves-networks
   self-heal test.

   **Deferred:** the `fabric dev` subcommand (env convention covers it), the
   Erlang idle-skip probe refinement (low urgency), and `fabric cp`/discovery
   (await Nathan's product prioritization — see
   `docs/liveness-and-product-gaps-2026-07-22.md`).

1. **Fleet redeploy before the hetz proof** (CoS-coordinated). The *installed*
   fabric binaries are stale/pre-sync (`~/.local/bin/fabric` was 0.1.7+940afd1).
   A machine only serves/dials sync after its running daemon is **restarted**
   onto the 0.2.0 binary — a fresh file on disk is not enough. fabric-claude owns
   build + `./install.sh` + `fabric restart`; the Mac daemon restart blips the
   live network so the CoS sequences the window; the CoS drives the hetz
   pull+build+restart.
2. Run the **real hetz proof** (Mac → Hetzner): convoy declares the catalog on
   both (per-network name `convoy-catalog-<network>`), drop a `host=hetz` job in
   the Mac catalog, it appears on hetz, hetz's `convoy up` launches it. This is
   the done-bar.
3. Evaluate **iroh-docs 0.101.0** (iroh `^1`, range-based set reconciliation,
   LWW, iroh-blobs content) as backend #2 and green the SAME conformance suite
   against it. If healthy it likely becomes the production backend; `fast_rsync`
   stays a large-file delta optimization only.

## Open items / known gaps

- `bus` tombstone-sweep TTL is not implemented yet (deletes propagate; sweep is a
  no-op). Fine — bus is a later consumer (smalltalk).
- mtime is carried in the manifest but not restored to disk (ordering is logical,
  not mtime; disk mtime preservation is a future nicety).
- Changing an existing entry's `folder` needs a daemon restart to re-point its
  watcher; add/remove of entries is picked up live by `fabric sync reload`.
- The folder scan uses blocking `std::fs` on the async task; fine for small
  catalogs, revisit (spawn_blocking) if large trees appear.

## Coordination

- **convoy-claude** is the first consumer (its network catalog), wire-ready and
  standing by for the "green on main" ping.
- **cos-claude** is the supervisor; direct-on-main landing, push to origin.
