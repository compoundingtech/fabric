# Liveness prior art + fabric product-gap readiness

_For Nathan, 2026-07-22 by fabric-claude. Grounds the roaming-fix heartbeat and
assesses fabric as a standalone dev product. Steer: keep liveness dumb-simple._

## Part A — Liveness prior art (grounding the per-peer heartbeat)

Two well-worn designs bracket the option space. fabric's roaming fix sits at the
simple end, deliberately.

### Erlang distribution — `net_ticktime` (the simple end)

- Erlang clusters keep a **persistent TCP connection between every pair of nodes**
  (full mesh). Liveness is a periodic **tick** on each connection.
- **The idle-only trick (the part we want):** a tick is sent **only if no other
  data crossed that connection** in the interval. Any real traffic resets the
  tick timer, so **a busy link sends zero extra heartbeat bytes** — the heartbeat
  exists purely to keep an *idle* link proven-alive. Detection: a peer is declared
  down if nothing (tick or data) arrives for a full `net_ticktime` (default 60s,
  sent as ~4 sub-interval ticks).
- **Why it caps ~50–100 nodes:** full mesh is O(n²) connections, and each node
  ticks all n−1 peers — per-node liveness cost grows O(n). Past ~100 nodes the
  all-to-all tick + connection overhead dominates. (Erlang's escape hatch is
  hidden nodes / `-connect_all false`, i.e. stop being a full mesh.)

### Consul / Serf — SWIM gossip (the scalable end)

- **SWIM** = Scalable Weakly-consistent Infection-style Membership.
- Each node, each ~1s period, **probes ONE random peer**. If no ack, it asks **K
  other random peers to indirectly probe** the suspect (covers "my direct path to
  it is down but it's alive"). Membership changes are **gossiped**, piggybacked on
  probes, spreading in O(log n) rounds.
- **Why it scales:** per-node load is **O(1)** — one direct probe + a little
  indirect/gossip per period, *independent of cluster size* — so it runs to
  thousands of nodes. The cost is complexity, weak consistency (eventual
  membership), probabilistic detection time, and a gossip layer.

### Recommendation for fabric NOW

fabric has a **handful of nodes** (Mac + hetz, growing to a few) — deep in
full-mesh territory, nowhere near the ~100 cap. So:

1. **Dumb-simple periodic per-peer heartbeat** — which is exactly the peer-health
   probe the roaming fix already ships (`run_peer_health_loop`, every
   `FABRIC_PEER_HEALTH_SECS`, default 20s, recover after 3 consecutive misses).
   O(n) per node is fine for n ≪ 50.
2. **Add the Erlang idle-only skip** — skip a peer's probe when its link saw real
   traffic within the interval (the traffic already proves liveness). Honors
   Nathan's "never spam the pipe." Concretely: gate the probe on iroh
   `RemoteInfo` last-activity for that peer (skip if `last_received < interval`),
   or on a fabric-recorded last-tunnel-activity timestamp. **Low urgency at
   today's scale** (a 20s probe to 1–2 peers is negligible), but it's the right
   principle and cheap to add — folding it into the probe next.
3. **Park SWIM** until fabric genuinely needs 50+ nodes. At that point add
   indirect-probe + gossip for O(1)/node; note it as the scale-out migration, not
   today's work.

**Mapping:** interval 20s (Erlang 60s / SWIM ~1s — 20s balances roam-detection vs
chattiness), threshold 3 misses (≈ Erlang's ~4-tick rule), no indirect-probe or
gossip (SWIM-only, not needed at this scale). The probe doubles as latency +
direct/relay telemetry.

## Part B — Product-gap readiness (what a dev needs from fabric)

Lens: fabric as a standalone multi-machine dev tool. Solid / thin / missing.

### Solid — a dev can rely on these today

| Need | fabric | State |
| --- | --- | --- |
| Reach a remote port/socket | `fabric dial` (unix + `--tcp`) | solid |
| Interactive remote shell | `fabric shell` (default-deny) | solid |
| **Non-interactive remote command** | **`fabric exec` (default-deny)** | **NEW — was the #1 gap, now built + e2e-validated** |
| Keep a folder synced | `fabric sync` (declarative) + rsync stopgap | solid; declarative engine landing |
| Reachability / latency | `fabric ping`, `status` | solid |
| Expose a local service | `fabric expose` (socket/tcp/exec) | solid |
| Trust management | `fabric add/remove/peers` | solid |

### Thin — works but has rough edges (mostly addressed this cycle)

- **Roaming resilience** — a peer that changed networks stayed dead until manual
  restart. Fixed by the peer-health self-heal (this deploy).
- **Service ergonomics** — the launchd bootout→bootstrap race (fixed) and
  dev-vs-prod isolation (design in `dev-prod-isolation.md`).
- **Observability** — per-path latency/transport wasn't recorded (pathwatch gated
  off); the new probe now emits it.

### Missing — prioritized

1. **One-off file copy (scp-like) — HIGH.** Continuous `sync` is the wrong tool
   for "copy this one file/dir now." A dev reaches for `scp` constantly. Propose
   `fabric cp <local> <peer>:<path>` / `fabric cp <peer>:<path> <local>` — can be
   built quickly on top of `exec` (tar/cat stream) or as a small dedicated copy
   protocol. Biggest remaining everyday-ergonomics gap after exec.
2. **Peer discovery — MEDIUM.** Onboarding is manual NodeID exchange
   (`fabric add <nodeid>`). A LAN discovery (mDNS) or a rendezvous/short-code flow
   would make "trust this machine" a one-liner. iroh already has discovery; fabric
   could surface it. Matters most for shareability (a new user's first 5 minutes).
3. **Reverse tunneling — MEDIUM.** `dial` is forward (local socket → remote
   service). The `ssh -R` direction (expose a *local* dev server *to* a remote
   peer) is a common dev need (share a local server, webhook testing). Propose
   `fabric forward`/reverse-expose.
4. **`fabric exec` stdin — LOW.** exec v1 has no stdin (fine for capture-output
   scripting). Forwarding local stdin would let you pipe *into* a remote command;
   add if a real need shows up.
5. **Multi-hop / relay-through-a-peer — LOW.** Reaching C via B. Niche until the
   topology grows.

**Net:** with `exec` landed, the everyday CLI surface (dial, shell, exec, sync,
ping, expose) is solid. The next highest-leverage additions for a shareable dev
product are **one-off `cp`** and **discovery/onboarding** — both about the first-
five-minutes and daily-driver ergonomics, not raw capability.
