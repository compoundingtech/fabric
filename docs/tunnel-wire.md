# The direct exposure wire

A client that is not fabric can reach a service exposed with `fabric expose`
using nothing but an iroh endpoint. This page describes the bytes it speaks.
They are a compatibility surface. Fabric does not change them in place. A
change to anything on this page is recorded in CHANGELOG as a compatibility
change. The test `daemon::tests::the_direct_exposure_wire_is_frozen` is a
client written from this page with every byte spelled out by hand. It covers
a granted client's admission, the frame layout, the opening Hello and Ack, a
half-closed request and reply, teardown that leaves no session behind, and a
refused resume. A change to any of those fails it.

Daemons do not use this path to talk to each other. They multiplex streams over
one shared connection (`fabric/mux/2`), and that framing is internal. A client
outside fabric should not speak it.

## Connecting

1. Bind an iroh endpoint with a stable secret key. The daemon uses
   `presets::N0`. The key's public half is the client's NodeID.
2. On the exposing machine, trust that NodeID and grant it exactly the
   exposure's name: `fabric add <NodeID> <name> --allow <protocol>`, then
   `fabric reload-peers`. A grant is the full protocol string, matched exactly.
   `fabric add` replaces any other peer that has the same name.
3. Connect with the ALPN set to the exposure's name, as given to
   `fabric expose`. A name that is not exposed fails ALPN negotiation.
4. An untrusted NodeID is closed after the handshake with application code
   403 and the reason `node is not in fabric allow-list`. A trusted NodeID
   without the grant is closed with code 403 and a reason that contains
   `not permitted for service`. Neither of these is a network fault, so a
   client should not retry them as one.
5. Open one bidirectional stream and send a Hello at once. The daemon accepts
   one stream per connection and sees it only once bytes arrive. One local
   connection on the client maps to one iroh connection and one session.

## Frames

Every frame is `[u8 kind][u32 big-endian payload length][payload]`. The payload
is at most 1 MiB. All integers are unsigned and big-endian.

| Kind | Payload | Meaning |
| --- | --- | --- |
| 1 Hello | 16-byte session id, u64 `recv_next`, u8 `resume` | Attach a session; `recv_next` counts the peer's bytes this side has delivered |
| 2 Data | u64 `offset`, bytes | Bytes at this direction's absolute offset |
| 3 Ack | u64 `recv_next` | Cumulative: every peer byte below this offset has been delivered |
| 4 Close | u64 `offset` | This direction ends at `offset` |
| 5 Error | UTF-8 text | The daemon refused the Hello |

A Hello is 25 bytes, so its header is `01 00 00 00 19`. The daemon also
accepts an older 24-byte Hello without the resume byte. Offsets start at zero
in each direction.

## A session

- The client sends a Hello with a random session id, `recv_next` 0 and
  `resume` 0. The daemon connects to the exposed socket or TCP address, then
  answers with a Hello carrying the same id and its own `recv_next`, followed
  at once by an Ack. Wait for that Hello before sending Data.
- Each side advances `recv_next` only after the bytes are written to its local
  end, and sends an Ack whenever `recv_next` changes. The daemon has no
  delayed-Ack timer. It ignores an Ack below what it already has, and clamps
  an Ack beyond what it sent.
- Acks are the flow control. The daemon stops reading the exposed service once
  4 MiB it sent is unacknowledged. It checks before each read of up to 8 KiB,
  so it can go over by one read.
- Data that starts past `recv_next` is a protocol error and ends the attach. A
  prefix already delivered is discarded.
- When local input ends, send Close at the final offset, after all Data up to
  that offset, and keep reading. The daemon sends its Close the same way. A
  receiver shuts down its local write half once `recv_next` reaches the Close
  offset. A Close that arrives early is held until then.
- A session is complete when both Closes have been exchanged and all of each
  side's data is acknowledged. The client knows this without waiting for
  anything further. The daemon then drops the session and the connection, so
  the client may see the stream end or the connection close. Both mean the
  same at that point. Send the last Ack before you finish or close anything,
  because a QUIC close discards unsent stream data.
- An Error answers a Hello: the service could not be reached, the session
  limit was reached, or a resume named a session the daemon no longer holds.
  The daemon then ends the stream and, within about a second, the connection.
  The text is a diagnostic, not part of this contract, and it can contain a
  path on the exposing machine.

## Detached sessions and limits

A session that ends any other way stays on the exposing machine, detached.
That covers a lost connection, a protocol error, or a close before the final
Ack. A detached session keeps its connection to the exposed service open and
keeps reading from it into its buffer. It lasts until the detached TTL passes
(900 seconds by default) or until it is evicted.

The defaults allow 16 sessions per peer and 64 in all. The 64 are shared with
every other peer and with resumable shell and exec sessions. A new session
that would go over a limit evicts the oldest detached session, the same peer's
first. It is refused with an Error only when every slot is attached. The
limits and the TTL are set with the `fabric up --server-session-*` options.

A client can resume a detached session. It reconnects with the same ALPN and
sends a Hello with the same id, its `recv_next` and `resume` 1. Each side then
resends its unacknowledged bytes from the offset the other side reported.
Only the NodeID that created a session can resume it. A resume after the
session has expired or been evicted is answered with an Error and must not be
replayed as a new session.

## Trust after admission

`fabric reload-peers` takes effect for new connections and resumes. It does
not close a session that is already attached.
