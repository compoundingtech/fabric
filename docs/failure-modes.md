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
| **The far machine restarts** — you restart your dev server while a browser is connected | A new request works almost immediately. **The connection that was already open does NOT survive.** So the page still loads, but a live-reload socket is gone and the page will sit there looking fine and stop updating. **You have to reload.** | New request 85 ms; open connection does not return | `a_peer_restarting_mid_session_restores_service_without_intervention` |
| The direct path between the machines dies while a relay is available | `NOT PROVEN.` Two daemons on one machine cannot lose a direct path they never had, so this cannot be forced in a test here. It is not hypothetical: on the three-machine fleet today, 1,569 connections used a direct path and 1,463 used a relay, so both are in constant use. Proving the switch needs two real machines. | Unmeasured | `NOT PROVEN` |
| A machine's address changes mid-session, as a laptop moving between networks does | The session survives without restarting the process, and the machine keeps its identity. Proven for one kind of tunnel. | Not separately measured | `generic_tunnel_survives_client_endpoint_recycle_without_process_restart`. **`NOT PROVEN` for TCP tunnels specifically.** |

## What you see while it is broken

fabric knows it is retrying and says so: it reports the attempt number, how long
until the next try, and the error. **A dial that hangs silently would be worse
than one that says the peer is unreachable**, so the information exists.

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

## How this page stays true

Each row names the test that proves it. Each of those tests carries a comment
naming this page. **If you change what a test proves, change the row; if you add
a failure mode, add the row even when there is no test yet.**

Every test here was checked by breaking the code it covers and confirming the
test fails. A test that passes whether or not the feature works is worse than no
test, because it reads as coverage.
