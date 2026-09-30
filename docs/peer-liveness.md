# Peer liveness

How the daemon decides that a trusted peer is still reachable, and why the
design stays simple.

## The gap a peer probe closes

The daemon reacts to local network changes: a debounced notice from the local
network monitor asks iroh to re-probe its paths and checks the connections it
holds. That covers this machine moving. It does not cover the other machine
moving.

When a remote peer roams to a new network and this machine's network is
unchanged, no local notice fires. The established direct QUIC path to the
peer's old address silently stops answering, and `Endpoint::online()` stays
true because it only says that this endpoint reached a relay. Without an active
check, nothing re-resolves the peer or fails over to the relay, and the peer
stays unreachable in both directions until a daemon restarts.

The daemon retains a cheap presence cache and probes idle trusted peers on an
interval (`PEER_HEALTH_PROBE_INTERVAL`, 20 s; `FABRIC_PEER_HEALTH_SECS` overrides
and `0` disables periodic probing). Recent application progress skips the idle
probe. An authenticated mux connection is evidence of life even without an echo
grant. The echo probe also measures latency and direct/relay path selection.

Any machine can be offline. Once absence is cached, new local dial requests
close immediately. Actual connection attempts use exponential backoff with
jitter, growing from seconds through minutes to about an hour. Repeated failed
probes do not add error counters or trigger endpoint rebuilds. Permission and
configuration errors remain separate. An explicit `fabric probe` requests one
fresh measurement under its own deadline.

A starting or returning daemon announces to its trusted peers by connecting;
that authenticated connection can carry traffic in both directions. Interface,
address, wake and relay-return notifications refresh paths and announce again.
Identical interface notifications are ignored. Healthy sessions stay up.

Simultaneous connects use a deterministic canonical direction. When a fresh
connection would lose to an older cached path, Fabric checks that old path's
admission response within two seconds. A refusal still proves life; silence
allows the fresh connection to replace it before the QUIC idle timeout. The
check holds no global peer-map lock. This handles a peer returning while the
other machine still holds its stale address/path state.

`fabric peer-events --watch` reads a bounded local event journal without an idle
network probe or disk write. New authenticated connections and offline
transitions carry canonical peer IDs, observed timestamps, path/cause, sequence
and a daemon instance token. A new connection signals life even when the old
handle had not timed out yet. Consumers reset their affected retry immediately.
A stale cursor or daemon restart returns a current snapshot with `reset: true`.

## Prior art

Two well-known designs bracket the option space.

**Erlang distribution (`net_ticktime`).** Every pair of nodes keeps one
connection, and liveness is a periodic tick on each connection. A tick is sent
only when no other data crossed the connection during the interval, so a busy
link spends nothing on heartbeats. A peer is declared down when nothing arrives
for a full tick time. The cost is a full mesh: each node ticks every other node,
so per-node cost grows with the cluster, and the practical ceiling is on the
order of 50 to 100 nodes.

**SWIM gossip (as in Serf and Consul).** Each period, a node probes one random
peer. If that peer does not answer, the node asks a few other peers to probe it
indirectly, which separates "my path to it is down" from "it is down".
Membership changes spread by gossip piggybacked on probes. Per-node cost is
constant regardless of cluster size, so it runs to thousands of nodes, at the
price of weak consistency, probabilistic detection time, and a gossip layer.

## The choice

A fabric network is a handful of machines, far inside full-mesh territory. So
fabric takes the simple end:

- **A plain periodic probe per peer.** The cost is linear in the number of
  peers, which is negligible at this size.
- **The Erlang idle-only rule.** A probe is skipped when recent application
  traffic on that peer's connection already proved it reachable during the same
  interval. Real traffic is the heartbeat; the probe exists for idle links.
- **Cached absence with jittered backoff.** A returning machine connects to its
  peers, so recovery does not wait out the quiet retry schedule.
- **No indirect probes and no gossip.** Those are the scale-out step if fabric
  ever needs tens of nodes, not something a small network should pay for.

The interval balances how quickly a roamed peer is noticed against how chatty
an idle machine is. A held connection that stops answering after a local
network change is handled faster by the network-change check described in
[failure-modes.md](failure-modes.md); the probe is the backstop for changes this
machine cannot see.
