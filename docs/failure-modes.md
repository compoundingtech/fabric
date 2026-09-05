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
| The network flaps: repeated brief interruptions | It recovers, but **more slowly than the interruptions themselves**. Five 200 ms flaps cost seconds, not milliseconds. See "Known rough edges" below. | Open connection 1.2 to 2.0 s, new request 9 ms | `flapping_does_not_make_recovery_slower_than_the_outage` |
| **The far machine restarts** — you restart your dev server while a browser is connected | The open connection does not survive because the process that owned it is gone. A new request during the outage fails within Fabric's three-second initial-connect bound. A client can then retry. A new request works when the peer returns. See "Whose problem is a page that stops updating" below. | During the outage 3.006 s; after restart 91.681 ms; one 9.87 s focused run on 2026-09-02 | `a_peer_restarting_mid_session_restores_service_without_intervention` |
| The direct path between the machines dies while a relay is available | `NOT PROVEN.` Two daemons on one machine cannot lose a direct path they never had, so this cannot be forced in a test here. It is not hypothetical: on the three-machine fleet today, 1,569 connections used a direct path and 1,463 used a relay, so both are in constant use. Proving the switch needs two real machines. | Unmeasured | `NOT PROVEN` |
| A machine's address changes mid-session, as a laptop moving between networks does | The session survives without restarting the process, and the machine keeps its identity. Proven for one kind of tunnel. | Not separately measured | `generic_tunnel_survives_client_endpoint_recycle_without_process_restart`. **`NOT PROVEN` for TCP tunnels specifically.** |
| A configured peer stays offline | Its failed connection attempt stays isolated. Healthy peer streams still open. Failed probes retain no connection. | Under 250 ms in the regression test. On hetz, 300 of 300 healthy pings passed over 91.663 seconds. | `offline_peer_cost_is_bounded_and_healthy_peer_stays_fast` |

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

**An offline peer still gets a health probe every 20 seconds.** This is extra
work, but its fleet cost was not detectable on 2026-09-05. A 91.663-second
treatment had six failed probes. All 300 healthy-peer pings passed, with no ping
above one second.

Two matched resource traces each used 380 one-second samples over 379.095
seconds. The offline-peer treatment used 5.925% of one core. The no-offline-peer
control used 11.942%, because unrelated work made the control busier. Treatment
RSS spanned 155,824 KiB. Control RSS spanned 196,048 KiB. Both traces crossed the
daemon's 128 MiB allocator sawtooth. These results show no attributable cost at
this fleet size. They do not show that a failed probe costs nothing.

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
