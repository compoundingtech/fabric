# Path Quality Reconnect Roadmap

Status: implemented in the 2026-09-02 degraded-path recovery slice. The live WAN
proof and its 24-hour cost window remain.

## Live Repro

Source: `/tmp/cos-fabric-watchdog.log`, reported 2026-07-17 23:29 CEST.

Observed behavior:

- `fabric ping hetzner` held a direct path with roughly 5 second RTT for more
  than 30 minutes.
- Both daemons were otherwise healthy:
  - Mac: roughly 5 percent CPU and 98 MiB RSS.
  - Hetzner: roughly 2 percent CPU, 180 MiB RSS, and idle load around 0.06.
- Adjacent reachability checks were healthy:
  - ICMP Mac to Hetzner public IP: 18 ms, 0 percent packet loss.
  - ICMP Mac to Hetzner Tailscale IP: 18 ms, 0 percent packet loss.
  - Tailscale ping: 18 ms.
- A fabric restart recovered service, but only to roughly 46 ms over relay. It
  did not immediately establish a fresh direct iroh path.

This is a degraded-but-connected path. It is not a dead endpoint, not a memory
threshold breach, and not a host network outage.

The ICMP data does not prove that iroh's UDP hole-punched direct flow could have
achieved 18 ms. The direct UDP flow may have degraded for causes ICMP would not
show, such as stale NAT state, UDP rate limiting, or an asymmetric UDP path. The
precise claim is that iroh's direct flow went bad, iroh did not detect or
reselect away from that bad path, and restart fell back to a usable relay path.

## Former Blind Spot

Current in-process recovery paths cover:

- Debounced network-change events, followed by `endpoint.network_change()` and a
  health check.
- Periodic endpoint health checks that recycle when `Endpoint::online()` does
  not recover.
- RSS threshold recycle plus allocator trim on Linux.

Those signals missed a path that remained connected but became unusably slow. In
the repro, direct-path RTT ballooned while `Endpoint::online()` remained true
and daemon CPU/RSS stayed normal.

## Implemented Trigger

The per-peer health monitor reuses the built-in echo path on the shared
connection.

The implementation uses this policy:

- Poll trusted peers periodically with the existing echo ping implementation.
- Track an RTT baseline per peer and per observed transport class
  (`direct`, `relay`, or `mixed`) when iroh exposes it.
- Mark a peer degraded after three successful pings exceed both eight times the
  baseline and one second.
- Ignore the first samples after endpoint generation changes so cold-path
  setup and relay/direct warmup do not look like degradation.
- Close only the shared peer connection with reason `path quality degraded`.
  The next stream redials and lets iroh select direct or relay again.
- Permit one quality redial per peer per 60 seconds.
- Skip a liveness probe when recent application traffic already proved the peer
  reachable during the same probe interval.

The first slice should prefer correctness over aggressiveness. A functional
relay path at 40-150 ms is acceptable; a persistent 5 second iroh path is not,
even when the process is otherwise healthy and non-iroh reachability checks look
good.

## Validation

Unit-level acceptance:

- A classifier test keeps a stable baseline during normal 40-150 ms jitter.
- A classifier test marks 5 second RTT as degraded after the configured
  consecutive sample count.
- A generation-change test resets or suppresses degradation state.
- A cooldown test ensures one degraded peer cannot cause a redial storm.

Integration-level acceptance:

- A two-daemon test proves that simultaneous traffic selects one shared
  connection per peer pair.
- The Git transport test proves that echo and Git use the same connection.
- The tunnel recovery tests prove that closing the shared connection resumes
  the existing local sessions.
- The remaining WAN eval must assert that a degraded-but-connected path
  self-recovers without manual process restart.

## Non-Goals

- Do not try to force iroh to use direct paths. Select the path that is actually
  healthy, even if that is relay.
- Do not replace network-change or RSS recovery. This is an additional trigger.
- Do not make service supervision responsible for this class. A managed service
  can restart crashes, but this failure happened inside a healthy process.

## Decided Direction (2026-07-20)

Investigation (log evidence + iroh 1.0.2 source) established two composing facts:

- iroh 1.0 connections are **multipath**: one connection carries several paths
  (direct v4/v6, relay), each with its own RTT and a *selected* path, exposed via
  `Connection::paths()` / per-path `rtt()` / `is_selected()` / `path_events()`.
- Fabric previously opened **one iroh connection per tunnel/socket**. That made
  N independent path states per peer and matched the connection-handle leak.

iroh exposes **no fine-grained per-path evict/re-select** API: only
`Endpoint::network_change()` (coarse re-probe) or a fresh `connect()` / restart.
So a degraded-but-*validated* selected path cannot be individually dropped; the
lever is re-dialing the connection to force fresh path selection.

Decided build (Nathan, 2026-07-20):

1. **Consolidate to exactly one multipath QUIC connection per machine-pair.**
   Multiplex every logical socket/tunnel as a QUIC stream on that shared
   connection. Result: one path-state per peer to health-check and one place to
   apply a selection fix, and far fewer connection handles. Keyed by peer so it
   works for an N-peer mesh, not just one pair.
2. **Selection-stickiness fix: re-dial + reselect, do not pin relay.** A
   path-quality monitor watches the selected path's RTT; on persistent
   degradation (multiplier over baseline AND an absolute floor, over consecutive
   samples, suppressed across generation changes) it re-dials the peer connection
   so iroh re-selects a healthy path — which may be relay, but is not *pinned* to
   relay.
3. **Heavy, durable instrumentation** (per-path RTT, selected path, UDP 4-tuple,
   path events, reconnects, degradation windows) so reliability is proven with
   evidence. See `src/pathwatch.rs`.

## Future Experiment: Dual-Path Bonding (parked)

Not to be built now — captured here at Nathan's request as a named future
experiment. Once the single-multipath-connection-per-peer design is reliable and
instrumented:

- Deliberately keep **two paths active at once** (e.g. a direct path and a relay
  path, or two direct paths across different interfaces) instead of only the one
  iroh selects.
- **Bond / multiplex across both** for (a) higher reliability — traffic survives
  one path degrading without waiting for re-selection — and (b) higher aggregate
  bandwidth by striping across paths.
- Open questions to evaluate when it is picked up: does iroh expose enough to pin
  and drive two paths simultaneously (today path selection is automatic and
  single-selected); scheduling/reordering across bonded paths; and whether the
  reliability win justifies the complexity over fast re-selection on one
  connection.
