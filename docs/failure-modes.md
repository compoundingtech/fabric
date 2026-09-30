# What happens when the network breaks

fabric connects two machines that are not on the same network. Networks fail, so
this page is about what fabric does when they do.

**Every number here was measured.** Most measurements use one machine running
two fabric daemons, with the test named in the last column. A fleet measurement
names its machine and window. Where there is no test, the row says `NOT PROVEN`
and stays in the table. A page that lists only the failures we happened to test
would read as a complete list of what can go wrong, and it would not be one.

## The two questions

When a connection is interrupted, there are two different questions and they can
have different answers:

1. **Does a new request work again?** This is a person reloading a page.
2. **Does a connection that was already open survive?** This is a live-reload
   websocket, an SSH-like session, anything long-lived.

The second matters more than it looks. **A page that appears fine and has
quietly stopped updating is worse than one that visibly failed**, because
nothing tells you to look.

## The table

| What breaks | What fabric does | How long | Proven by |
| --- | --- | --- | --- |
| A brief interruption, a few seconds | The tunnel resumes on its own. The connection that was open keeps working, and the service on the far side is never reopened. | Under a second | `tcp_expose_dial_listener_round_trips_and_reconnects` |
| A long outage, ninety seconds | Nothing gives up. A new request works immediately. A connection that was already open waits out the retry schedule before it resumes. | New request 11 ms, open connection 12.4 s | `a_long_outage_does_not_time_out_permanently` |
| One direction fails while the other still works | Both recover. This is the case that usually breaks retry logic, because each side sees something different and only one of them knows anything is wrong. | Open connection 23 ms, new request 6 ms | `a_tunnel_recovers_from_an_asymmetric_partition` |
| The network flaps: repeated brief interruptions | A valid attach or temporary admission reply resets the retry delay. | Open connection 4.69 ms, new request 6.89 ms after five 200 ms drops in an 8.52 s focused run on 2026-09-30 | `flapping_does_not_make_recovery_slower_than_the_outage` |
| **The far machine restarts** — you restart your dev server while a browser is connected | The open connection does not survive because the process that owned it is gone. A new request during the outage fails within Fabric's three-second initial-connect bound. A client can then retry. A new request works when the peer returns. See "Whose problem is a page that stops updating" below. | During the outage 3.006 s; after restart 91.681 ms; one 9.87 s focused run on 2026-09-02 | `a_peer_restarting_mid_session_restores_service_without_intervention` |
| **The far machine's fabric daemon restarts while you hold a `fabric shell`** — an update, or a `fabric restart` | The PTY dies with the daemon that owned it, so the session cannot resume. The client reports the refused resume, starts a new shell to the same peer in the same terminal, and says when it is ready. Input typed in between is discarded and counted, not replayed. It gives up after 5 minutes without an answer. | Fresh prompt 1.98 to 2.02 s after the restart (3 runs, 2026-09-16); before the change the command exited 1 after 1.19 to 1.32 s | `shell_starts_a_new_session_after_the_remote_daemon_restarts` |
| A laptop sleeps with a `fabric shell` open, then wakes | Shorter than the 15 minute detached window, the same PTY resumes and unacknowledged bytes are replayed. Longer, the server has reaped the PTY and the wake looks like the row above: a new shell in the same terminal. | Same-PTY resume 0.53 s after a forced drop (3 runs, 2026-09-16). **`NOT PROVEN` on a real sleep/wake:** two daemons on one always-on machine cannot sleep, so the laptop measurement needs a person. | `resumable_shell_one_survives_transport_drop`, `shell_starts_a_new_session_after_the_remote_daemon_restarts` |
| The direct path between the machines dies while a relay is available | `NOT PROVEN.` Two daemons on one machine cannot lose a direct path they never had, so this cannot be forced in a test here. It is not hypothetical: on a real multi-machine network both direct and relay paths are in constant use. Proving the switch needs two real machines. | Unmeasured | `NOT PROVEN` |
| A machine's address changes mid-session, as a laptop moving between networks does | The session survives without restarting the process, and the machine keeps its identity. Proven for one kind of tunnel. | Not separately measured | `generic_tunnel_survives_client_endpoint_recycle_without_process_restart`. **`NOT PROVEN` for TCP tunnels specifically.** |
| A local network change (a VPN coming up, a Wi-Fi switch, an interface change) leaves the connection to a peer on a path that no longer answers | After the debounced notice the daemon asks each peer connection it still holds to answer one echo within 3 s and resets only the one that does not; the next request redials with fresh path selection. A connection that still answers is kept, and the endpoint is not rebuilt. One reset per peer per minute. | Reset 3.2 s after the change. Before this check: 64.6 s (the QUIC path-idle timeout, measured 2026-09-11) or the 60 s peer-probe backstop, whichever came first. | `a_network_change_resets_a_held_connection_that_stopped_answering_without_a_recycle`, `a_vpn_coming_up_keeps_a_held_connection_that_still_answers` |
| A consumer opens one short TCP connection per request through a dial listener, so every request is a tunnel session that ends within a second | Each clean end is the session finishing, not the transport failing, so it no longer counts toward replacing the shared peer connection. Before the fix every third one replaced the connection. | On a real two-machine pair before the fix, the shared connection was replaced every few seconds and never lived longer than that; after the fix, one connection held on both sides with no counted failures. | `one_request_tcp_sessions_leave_the_shared_connection_alone` |
| A configured peer stays offline | Its failed connection attempt stays isolated. Healthy peer streams still open. Failed probes retain no connection. | Under 250 ms in the regression test. A matched measurement on a real machine found no attributable cost (below). | `offline_peer_cost_is_bounded_and_healthy_peer_stays_fast` |
| Five minutes or four hours offline | Cached requests fail locally; the returning peer announces from its new address. | 0.64 s / 0.48 s after startup, virtual absence | `a_peer_returns_after_minutes_and_hours_without_errors_or_a_helper` |
| Both daemons restart | Persisted exposure and Unix/TCP dial listeners return automatically. | Same Unix path and allocated TCP port | `registered_dials_and_default_exposure_survive_both_daemon_restarts` |
| Both sides connect concurrently | Deterministic admission preserves one shared connection. | Bounded convergence; subsequent traffic keeps its identity | `simultaneous_peer_traffic_converges_to_one_shared_connection`, `duplicate_incoming_keeps_an_admitted_live_connection` |
| Interface/address/wake notifications | Refresh paths and announce; preserve healthy traffic and declarations. | Scripted isolated matrix | `interface_address_and_wake_changes_preserve_live_sessions_and_declarations` |

## What you see while it is broken

An offline peer is normal on any machine. A known-offline dial closes locally
within the test's 200 ms budget. The first unknown connection attempt is bounded
at two seconds before absence is cached. Offline retries do not add failed
connection counters or repeated error logs. A real loss of an attached session
is recorded once, and permission or service errors still report their cause.

The `peer-events` local stream exposes online/offline transitions without an
idle network probe or disk write. A returning daemon announces to trusted peers,
so an established incoming connection wakes waiting retries even when the
waiting machine only has an old address hint.

## Whose problem is a page that stops updating

A dev server with live reload holds a websocket open. **Restart the server and
that socket dies — with or without fabric.** The process that owned it is gone.
What makes it a non-event when you work locally is that the CLIENT reconnects,
which vite and webpack both do within a second or two.

So the question is what happens to that reconnect, and fabric's part is
measured. A new request during the outage fails in 3.006 seconds on one 9.87
second focused run on 2026-09-02. A new request succeeds 91.681 milliseconds
after the peer returns. A client can retry instead of waiting for its own longer
timeout.

**So if your page stops updating after you restart your dev server, the
application must retry after the failed request.** Fabric accepts the next
request after the peer returns.

**The limit of this evidence:** it was measured with a TCP client through the
tunnel, not with a browser driving a real websocket. The transport property —
Fabric bounds a new request while the peer is down and accepts another request
after the peer returns. Whether a particular client retries remains that
client's behavior.

## Known rough edges

Retry delays now grow to about an hour with jitter during an extended absence,
and a peer-online event wakes the affected session without waiting out that
schedule. An application must retry a new request that already closed while the
peer was offline.

The isolated outage test advances a virtual clock by five minutes and four
hours, keeps real endpoints and local TCP listeners, and restarts the remote
peer on a new UDP address. Eight new requests close within 200 ms during each
absence without adding failure counters. New tunnel traffic resumes 0.64 s and
0.48 s after peer startup in one 10.22 s run on 2026-09-30. This verifies retry
and announcement behavior; it is not four hours of physical network testing.

The interface matrix scripts Wi-Fi changes, Ethernet, VPN up/down, IPv6
rotation, NAT rebinding, captive portal, sleep/wake and server uplink return
notifications through the production rehome path. A live TCP stream, Unix/TCP
dial declarations and endpoint identity remain intact. Both Linux and macOS CI
run it. Physical router/NAT migration and a real laptop sleep/wake remain
separate evidence, as do direct-to-relay switching measurements.

**A restart on the far side ends open connections.** See the table. Whether the
application notices is up to the application; fabric restores the tunnel but
cannot resurrect a socket the far process no longer has. `fabric shell` is the
one consumer fabric owns end to end, so there it does the retry itself: a new
shell in the same terminal, announced, with typed-while-disconnected input
discarded rather than replayed.

**Offline probes are due only after their backoff deadline.** The regular health
loop skips absent peers until that deadline. A successful connection or a local
network/relay return resets it. Repeated identical interface notifications do
not reset it. A missing relay and non-answering peers are insufficient evidence
to rebuild the endpoint; the daemon waits for the uplink to return.

Remove a truly retired peer from `peers.toml` on every machine. This file is a
local allow list, so removal on one machine does not remove trust elsewhere.
`fabric doctor` can report an unreachable peer, but it cannot know that the peer
was retired. The fleet has no authoritative peer set today. An operator must
compare every machine's `peers.toml` to find this drift.

## Why `send-file` is not shaped like scp

`scp` lets the sender choose where a file lands on the far machine. **fabric
does not, and that is deliberate rather than unfinished.**

Sending a file to another machine is a remote write. If the sender chooses the
path, it can write anywhere the receiving fabric can, and
`../../.ssh/authorized_keys` is where that ends. So **the receiver decides**:
every file arrives under an inbox belonging to the peer that sent it, and the
sender may only name a relative path inside it.

This is the same rule as per-peer permissions — **the side being acted on
decides what is allowed** — and it has a second benefit: you always know where
things arrive, without reading whatever command the sender typed.

The name a sender asks for is checked on both machines. The sending side checks
so that a mistake is reported to the person who made it. The receiving side
checks because it cannot trust the sender, and that is the check that would
still be there if the peer were hostile.

## How this page stays true

Each row names the test that proves it. Each of those tests carries a comment
naming this page. **If you change what a test proves, change the row; if you add
a failure mode, add the row even when there is no test yet.**

Every test here was checked by breaking the code it covers and confirming the
test fails. A test that passes whether or not the feature works is worse than no
test, because it reads as coverage.


## Resolved finding: three failures did not share one deadline cause

Three daemon-slice tests were classified as flaky after CI failures passed on a
rerun. The classification guessed that CI load made their deadlines too short.
The guess was not proved, and 300 attempts of each test on unchanged main did
not reproduce it under parallel process load.

The two transport failures preceded later tunnel and mux recovery repairs. The
ledger failure remained current, but its cause was different. One file write
started both a watcher pass and an explicit reload. Both were valid inbound
transactions, while the test required exactly one. The test now uses one
trigger for each exact expected count. See [known-flaky-tests.md](known-flaky-tests.md)
for the measured retirement record.
