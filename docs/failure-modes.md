# What happens when the network breaks

fabric connects two machines that are not on the same network. Networks fail, so
this page is about what fabric does when they do.

**Every number here was measured**, on one machine running two fabric daemons
against each other, by the test named in the last column. Where there is no
test, the row says `NOT PROVEN` and stays in the table. A page that lists only
the failures we happened to test would read as a complete list of what can go
wrong, and it would not be one.

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
| The network flaps: repeated brief interruptions | It recovers, but **more slowly than the interruptions themselves**. Five 200 ms flaps cost seconds, not milliseconds. See "Known rough edges" below. | Open connection 1.2 to 2.0 s, new request 9 ms | `flapping_does_not_make_recovery_slower_than_the_outage` |
| **The far machine restarts** — you restart your dev server while a browser is connected | The connection that was open does not survive, and **that part is not fabric's doing**: the process that owned it is gone, and the same socket dies with no fabric involved. What matters is the reconnect, and fabric accepts one straight away. A reconnect succeeds 76 ms after the restart, and **a reconnect attempted while the server is still down also succeeds, in 111 ms — it never hangs.** So a client that retries gets through. See "Whose problem is a page that stops updating" below. | Reconnect 76 ms; during the outage 111 ms; never hangs | `a_peer_restarting_mid_session_restores_service_without_intervention` |
| The direct path between the machines dies while a relay is available | `NOT PROVEN.` Two daemons on one machine cannot lose a direct path they never had, so this cannot be forced in a test here. It is not hypothetical: on the three-machine fleet today, 1,569 connections used a direct path and 1,463 used a relay, so both are in constant use. Proving the switch needs two real machines. | Unmeasured | `NOT PROVEN` |
| A machine's address changes mid-session, as a laptop moving between networks does | The session survives without restarting the process, and the machine keeps its identity. Proven for one kind of tunnel. | Not separately measured | `generic_tunnel_survives_client_endpoint_recycle_without_process_restart`. **`NOT PROVEN` for TCP tunnels specifically.** |

## What you see while it is broken

fabric knows it is retrying and says so: it reports the attempt number, how long
until the next try, and the error. **A dial that hangs silently would be worse
than one that says the peer is unreachable**, so the information exists.

## Whose problem is a page that stops updating

A dev server with live reload holds a websocket open. **Restart the server and
that socket dies — with or without fabric.** The process that owned it is gone.
What makes it a non-event when you work locally is that the CLIENT reconnects,
which vite and webpack both do within a second or two.

So the question is what happens to that reconnect, and fabric's part is
measured: **a reconnect succeeds 76 ms after the restart, and one attempted
while the server is still down succeeds in 111 ms. It never hangs.** A client
that retries at all gets through, and it is not punished for retrying.

**So if your page stops updating after you restart your dev server, the
application is not reconnecting.** That is worth knowing because it is fixable
in the application and not in fabric, and because a page that looks fine and has
stopped updating is the kind of thing that costs an hour before anyone suspects
it.

**The limit of this evidence:** it was measured with a TCP client through the
tunnel, not with a browser driving a real websocket. The transport property —
fabric accepts a reconnect immediately and never hangs — is proven. Whether a
particular dev server's client retries is that client's business, and fabric
cannot make it.

## Known rough edges

**Flapping costs more than the outage.** The retry delay grows on each failure —
100 ms, 250 ms, and so on up to 15 seconds — and only resets once a connection
has been stable for two seconds. A network that fails faster than that never
resets it, so a series of 200 ms interruptions can cost several seconds of
silence, and in the worst case up to fifteen. **A brief problem should cost a
brief recovery, and today it does not always.**

**A long outage delays an open connection by up to fifteen seconds** after the
network is back, for the same reason, while a new request is immediate.

**A restart on the far side ends open connections.** See the table. Whether the
application notices is up to the application; fabric restores the tunnel but
cannot resurrect a socket the far process no longer has.

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


## Candidate finding: daemon-slice deadlines are tuned to a fast machine

Three tests in the daemon slice fail under CI load and pass on a rerun of the
same commit: see [known-flaky-tests.md](known-flaky-tests.md). They are timing
failures, not logic failures, and they cluster on one cause. A deadline that
holds locally and elapses under CI load is a property of the deadline in the
code, not a defect in the test. The two round-trip timeouts
(`a_peer_not_permitted_for_a_service_cannot_reach_it`,
`exec_expose_reconnect_keeps_child_bound_to_tunnel_session`) and the ledger
count race (`production_status_exposes_exact_inbound_scan_ledger`) are worth
retuning or making load-tolerant, so a red `deterministic` always means a real
regression. NOT PROVEN as a single root cause; recorded here so the next reader
starts from the pattern rather than one instance.
