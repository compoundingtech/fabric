# Runtime peers and exact grants

This control API is provisional. Start the daemon with `fabric up --runtime-peers`
(or pass `--runtime-peers` to its supervised `daemon` command). Without the flag,
existing static-file behavior remains in force and runtime requests return
`runtime_peers_disabled`. The presence of a runtime file never enables the mode.
The internal restart helper preserves the selected mode; a service manager must
keep the flag in its own startup command.

The daemon owns private (mode 0600) `runtime-peers.toml` next to `identity.toml`.
An existing runtime file must belong to that account and have no group/other
permissions. Its configured `peers.toml` is a read-only input, including symlinks
and default XDG layouts.
Runtime mode requires current format-2 static input; perform legacy migrations
explicitly before enabling it. Runtime requests do not replace static symlinks
or run the static loader's migrations.

`owner` is attribution, not authentication. Local access to the control socket
is the authority. A process with that access can supply any owner. Each change
edits only that owner's records. Effective trust and exact grants are the union
of static and runtime sources. Removing one source never suppresses another;
replies report the access that remains and every source granting it. A static
source includes the configured file path.

## Pairing permits

The local control request `{"type":"pairing","request":{...}}` supports
`issue`, `query`, and `cancel`. These operations require `--runtime-peers`.
They create no ordinary trust record, grant, or service listener. Runtime mode
also enables the restricted pairing ALPN and opaque join exchange below.
Atomic confirmation is not implemented.

`issue` takes `owner`, `creation_id`, an absolute Unix-seconds `deadline` within
the next 600 seconds, and `policy`: exact `grants` (at most 128), optional `name`,
and `direction` (default `normal`). Wildcard grants and dial-only policies with
inbound grants are refused. The policy is immutable, with a digest in status.
`query` and `cancel` select the creating `owner` and `creation_id`.

Replies use `{"type":"pairing","reply":{"status":...,"error":null}}`.
Only the first successful issue reply also includes `secret`: a 256-bit random
bearer token. The private runtime state stores only its salted hash. Debug
output redacts the secret; status and cancellation never include it.
Keep the first reply private. Repeating an identical issue returns the same
attempt without another secret; a different policy/deadline with the same
creation ID is refused. After a lost creation reply, query by creation ID,
cancel the inaccessible permit, and issue a new creation ID.

Status includes `attempt_id`, `creation_id`, `state` (`issued`, `pending`,
`expired`, `rejected`), `attempt_revision`, original `deadline`, `policy_digest`,
`terminal_at`, `bound_node_id`, and `terminal_reason`. Cancel is idempotent and
never renews a deadline. Expiry is
reconciled durably on startup and permit operations. Live deadline observations
use a monotonic clock that reanchors on forward wall-clock jumps; all outstanding
permits expire on restart. Terminal transitions erase salt and secret hash.

There are at most 64 outstanding permits and 1,024 retained attempts. Issuance
reserves its own terminal receipt slot for 24 hours after termination; the
pairing pool is separate from mutation receipts. Expired lookup is unknown.
Invalid requests use `invalid_argument`; persistence errors use the existing
runtime-state error classes. Uncertain commit outcomes quarantine admissions,
exactly as runtime policy commits do. Owner remains attribution rather than
authentication.

### Restricted presentation and status

Runtime mode advertises `fabric/pair/1`. Only that negotiated ALPN bypasses the
ordinary peer allow-list; no pending identity is added to mux, built-in services,
or direct exposures. Joining clients must use the gateway NodeID from their
invitation as the authenticated destination, not discover or substitute another
NodeID. Iroh authenticates both transport identities.

One bidirectional stream carries bounded JSON frames, each prefixed by a
four-byte big-endian byte length (maximum encoded request length 266,240 bytes,
allowing JSON byte-array encoding for a 64 KiB payload):

```json
{"operation":"present","attempt_id":"<attempt>","secret":"<creation secret>"}
{"operation":"status","attempt_id":"<attempt>"}
```

Replies are length-prefixed pairing reply objects, without the local-control
`type` envelope and without a secret. The first valid presentation durably binds
the permit to the connection's authenticated NodeID and changes it to `pending`
at attempt revision 2. The identical node/token can replay that attempt without
extending its deadline; another NodeID cannot take its binding. Status requests
are accepted only for the authenticated bound node, including retained terminal
status. Unknown, wrong-node, wrong-token and malformed requests close with a
generic reason and expose no attempt status.

There are at most 32 accepted pairing connections and one active connection per
attempt. Validation is globally token-bucket limited to one request per second
with burst 10, and per NodeID to one per six seconds with burst 3. These limits
apply to valid and invalid requests, including status; identity rotation cannot
bypass the global bucket. The bounded per-node rate book holds at most 1,024
entries. Rate permission precedes body parsing/allocation. Idle framing waits
are limited to 10 seconds; connections last no longer than 600 seconds and
shorten to the permit deadline once the attempt is known.

Pairing has its own 32-handler pool, separate from ordinary service handlers,
and at most two connections per authenticated NodeID. Capacity exhaustion
explicitly closes with application code 503. While runtime mode is enabled,
ordinary connections arriving at full ordinary capacity are refused rather than
waiting after handshake; they cannot accumulate unbounded handshaken waiters or
make pairing wait for an ordinary handler. With runtime mode disabled, ordinary
capacity is reserved before accept and retained for the handler's full lifetime,
unchanged from ordinary admission without pairing.

Transport stream/window limits are reduced immediately after ALPN selection,
but QUIC cannot retract already advertised initial credit. The handler therefore
closes immediately with application code 400 if any unidirectional stream or a
second bidirectional stream appears, including during first-header or runtime
validation waits. The first complete request and its runtime-mutex validation
must fit the same three-second deadline; subsequent validation reads use the
earlier of the attempt deadline and the ten-second idle deadline. Durable bind
publication remains independent of socket cancellation.
Network validation has at most 32 blocking workers. Each worker owns its capacity
permit through runtime-mutex acquisition and read completion, even if its socket
deadline expires first. Later requests wait asynchronously for that capacity
within their existing deadline; repeated socket timeouts cannot accumulate
unbounded blocking threads behind a stalled filesystem commit.

The diagnostic/status `active_incoming_handlers` counter counts ordinary
admission slots only. In runtime mode it excludes in-flight handshakes and pairing
handlers. With runtime mode disabled it also includes the accept loop's one
pre-accept reservation.

Pairing sockets are never routed to an ordinary service. Cancellation and expiry
erase eligibility and hashes while retaining the bound identity's terminal
status. Restart durably rejects pending attempts with `exchange_lost`, before
accepting connections; an unbound issued permit can survive restart.

### Opaque bounded join exchange

Only a pending bound node and the creating owner may exchange bytes. The local
control operations are `pair_send` and `pair_receive`; the restricted network
operations are `send` and `receive`. Each selects `attempt_id`. Local operations
also select `owner`. Send takes `sequence` (starting at 1 in each direction),
`digest` (BLAKE3 lowercase hex of the payload), and `payload` (a JSON byte array).
Receive takes optional `ack`: the sequence of a previously delivered message.
Receives are nonblocking; no message returns `message: null` rather than holding
the policy writer open.

Replies add `exchange` with `sent_sequence`, `message`, `total_bytes`, and
`total_messages`. A message carries `direction` (`node_to_owner` or
`owner_to_node`), `sequence`, `digest`, and opaque `payload`. The surrounding
status binds the message to the attempt and authenticated NodeID. Fabric never
interprets admission signatures or chooses policy from payload bytes.

An exact send retry is budget-neutral; the same sequence with different
bytes/digest or a skipped sequence is refused. Receive replays an unacknowledged
message until its delivered sequence is acknowledged. Duplicate acknowledgments
are idempotent; an unsent or undelivered message cannot be acknowledged.
Acknowledged payload storage is freed, while bounded sequence/digest history
remains to recognize send retries.

Each payload is at most 64 KiB. Both directions share a cumulative 1 MiB byte
budget, including queued data, and 32 messages total (at most 16 per direction).
Reconnect resumes the same memory-only queues and counters, without replenishing
budgets. Exceeding a payload/count/byte bound durably rejects the attempt with
`exchange_limit` and discards its queues. Cancellation, expiry, and other
terminal transitions also erase queues. A one-second maintenance sweep clears
expired idle queues without waiting for a caller; every exchange operation
checks the deadline before accepting bytes. Idle ticks perform no fsync.
Restart rejects pending exchanges with `exchange_lost`; no transcript is written
to runtime state or logs.

## Requests

Use the existing newline-delimited JSON control connection: one request and one
reply. The outer request is `{"type":"runtime_peers","request":{...}}`.
In runtime mode the encoded control line is limited to 1 MiB, including its
newline. A batch contains at most 128 operations. The Rust request types live in
[`runtime_peers`](../src/runtime_peers.rs) and can be sent through the existing
`send_control`; the separate typed client crate is subsequent work.

All mutation selectors are exact NodeIDs. Names are labels and never transfer
permissions. An operation cannot remove or change another owner's record.
Labels for one NodeID must agree across sources and must be unique across
NodeIDs. All sources for a NodeID must also agree on direction.

The `operation` field selects one of:

| Operation | Fields |
| --- | --- |
| `list` | Optional `cursor` from the preceding page. |
| `add_peer` | `owner`, `request_id`, `expected_revision`, `node_id`; optional `name`, `direction`, `addr`. |
| `set_grants` | `owner`, `request_id`, `expected_revision`, `node_id`, `grants`. |
| `remove_peer` | `owner`, `request_id`, `expected_revision`, `node_id`. |
| `batch` | `owner`, `request_id`, `expected_revision`, `operations`. |
| `cancellation` | `request_id` of a retained mutation receipt. |

An owner and request ID each contain 1–128 bytes without control characters.
A new record starts with no grants and normal direction. An omitted name,
direction, or address preserves that owner's existing value. `set_grants`
replaces the entire owner's grant set; `[]` withdraws all services while
retaining trust. Grants name exact services, without wildcards or port syntax;
there is no implicit echo, shell, exec, prefix, or future-service grant.

Directions are `normal`, `inbound_only`, and `dial_only`. An inbound-only node is
never dialled, probed, announced to, or retried by this daemon. A dial-only node
cannot carry inbound grants. A conflicting source is refused explicitly.
Switching between the two opposite directions requires removing and re-adding
the owner's record in one batch; the replacement starts with no grants.
Optional `addr` is the existing EndpointAddr JSON address hint and must identify
the same NodeID. Mutations do not interpret membership signatures.

Batch operations use `op` as their discriminator. A re-key or replacement is one
batch, so validation sees its final candidate and a reused label cannot publish
an intermediate identity or unintended dual access:

```json
{
  "type": "runtime_peers",
  "request": {
    "operation": "batch",
    "owner": "controller-a",
    "request_id": "replace-17",
    "expected_revision": 12,
    "operations": [
      {"op": "remove_peer", "node_id": "<OLD_NODE_ID>"},
      {"op": "add_peer", "node_id": "<NEW_NODE_ID>", "name": "laptop"},
      {"op": "set_grants", "node_id": "<NEW_NODE_ID>", "grants": ["work/0"]}
    ]
  }
}
```

Static and other-owner records on the old NodeID remain visible and effective;
the caller must inspect the result before claiming complete retirement.

## Revisions, persistence, and lost replies

Shutdown allows up to three seconds for in-flight policy/config writes to drain,
then continues endpoint and child cleanup even if storage remains stalled. A
drain failure is reported after cleanup.

A mutation is serialized with accepted static reloads. Its expected revision
must match the current policy revision. A committed record or static-input
change advances that revision; a rejected transaction does not. An unchanged
owner record is a no-op and returns the current revision without storing a
receipt. A no-op request ID has no retention or replay guarantee; read current
state and revision before issuing a later mutation.

The daemon validates the final candidate, including sync selectors, then holds
the shared admission-policy generation through the durable commit and
publication. Persistence uses a private same-directory temporary file, file
fsync, rename, and directory fsync on the blocking pool. A success means both
state and effective policy are installed. A pre-rename failure leaves the old
policy and receipt history intact for runtime transactions. Any failed static
reload closes new admissions, preserving existing work when no valid candidate
exists but cancelling exactly the candidate's withdrawals when only its
persistence fails before rename. A successful repair/reload then
revokes work no longer permitted. If rename succeeds but durability is
uncertain, `runtime_commit_indeterminate` closes network admissions and cancels
old work. Repair the storage problem, let quarantined work end, and explicitly
reload to reconcile the file. A malformed or unreadable file also keeps the
control socket available with network policy closed; the daemon never silently
falls back to an older applied policy.

Check an existing request receipt before comparing revisions. Repeating the
same request ID and contents returns its original committed revision plus the
**current** effective-access view. It does not apply the mutation again, even
if access was withdrawn later. Different contents under a retained ID return
`request_id_conflict`. On a lost reply, query `cancellation` or repeat the exact
request; do not invent a new request ID for an uncertain outcome.

The store permits at most 1,024 owner/NodeID records in total. Receipts are
retained for at least 24 hours in a pool of 1,024 slots. Ordinary mutations may
use 960 slots; the remaining 64 are reserved for batches containing only
`remove_peer` or `set_grants` that remove services without adding any. Pure
no-ops consume no slot, even when the pool is full. Expired receipts are pruned
by a subsequent changed mutation. When the applicable capacity is full, that
mutation is refused before commit. After retention, reconcile current state
and revision before deciding on a new operation.

## Replies and cancellation completion

The response is `{"type":"runtime_peers","reply":{...}}`. Its `snapshot`,
`mutation`, `affected`, and `error` fields are explicit. A failed mutation
includes its coded `error` and the current affected-node access reports when
policy is available; a caller need not infer residual rights from an error
message. Error fields are `code`, `class`, and `message` (see
[errors](errors.md)). A faulted snapshot names its fault and reports no effective
network access.

A list snapshot contains `instance`, `policy_revision`, `static_digest`,
`peers`, `next_cursor`, and `fault`. Pages contain at most 256 nodes, ordered by
NodeID. A cursor binds instance, revision, and the last NodeID; changes or
restart invalidate it. Fetch a fresh list rather than combining generations.
Each peer reports name, direction, trust and direction sources, and a service
map whose values list **all** granting sources.

A mutation receipt contains `request_id`, `committed_revision`,
`current_revision`, `affected`, `removals`, and `cancellation`. Each removal
distinguishes `owner_record_removed` from `effective_trust_removed`.

At publication, newly unauthorized admissions are refused and selected existing
work is signalled to stop. Detached sessions are removed before success and
cannot resume. Partial withdrawal preserves still-permitted work, including
access supplied by another owner or static input. Cancellation survives a
control caller disconnecting.

The cancellation object lists selected admission and session IDs, the remaining
IDs, and `state` (`pending`, `complete`, or `timed_out`). Counts are the lengths
of these lists. Completion is distinct from signalling: the daemon waits for
its selected handlers, tunnel cleanup, and owned children to end. A separate
monitor targets completion within **two seconds** of the success reply, without
holding the policy writer behind an uncooperative service. A missed bound
returns `cancellation_timeout` on receipt queries together with the committed
receipt and remaining IDs; it never rolls back policy. A recorded timeout stays
visible after work ends or the daemon restarts; its remaining-ID lists become
empty when completion is confirmed. Completion persistence failure retains the
previous durable receipt and is logged. Restart resolves unconfirmed receipts
as `timed_out` / `cancellation_timeout`, preserving their remaining IDs and
`restart_evidence: {daemon_instance_changed: true, child_exit_observed: false}`.
A later reload cannot turn that missing child-exit observation into completion.
Receipts that already observed actual completion retain it. Cross-crash child
cleanup is separate work in [#325](https://github.com/compoundingtech/fabric/issues/325).

The synchronous narrow-grant interface (for example Git repository grants)
fails closed with a transient `runtime_state_unavailable` while the durable
policy writer holds its barrier. It records the exact grant used so withdrawal
can end that operation without cancelling unrelated services.

Rotation expiry, policy watches, pairing permits and join exchanges are later
slices of [the approved design](runtime-peers.md). They are not implemented by
these requests.

Runtime mode accepts exact grant strings only: unlike the normal static loader,
it rejects `:` aliases, `*`, whitespace in grants, and grants on `dial_only`
records. Migrate those inputs before enabling the mode. A shell that ignores HUP
may use the existing two-second grace before SIGKILL; its cancellation can report
`cancellation_timeout` before subsequent observation proves late completion.
