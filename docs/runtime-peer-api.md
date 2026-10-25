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
`issue`, `query`, `cancel`, `confirm`, and `reject`, plus the exchange operations
below. These operations require `--runtime-peers`. Issue/presentation/exchange
create no ordinary trust record, grant, or service listener. Only atomic
confirmation installs the immutable permit policy.

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
`confirmed`, `expired`, `rejected`), `attempt_revision`, original `deadline`,
`policy_digest`, `terminal_at`, `bound_node_id`, `terminal_reason`, and optional
durable `completion` on owner control only. Cancel is idempotent and never renews
a deadline. Reads project expiry without persistence or the admission barrier.
Live deadline observations use a monotonic clock that survives reconciliation;
all outstanding permits expire on restart. Terminal transitions erase salt and
secret hash.

There are at most 64 outstanding permits, 1,024 retained attempts globally and
128 retained attempts per owner. Issuance reserves its own terminal receipt slot
for 24 hours after termination; this pool is separate from mutation receipts.
Expired lookup is unknown.
Invalid requests use `invalid_argument`; persistence errors use the existing
runtime-state error classes. Uncertain commit outcomes quarantine admissions,
exactly as runtime policy commits do. Owner remains attribution rather than
authentication.

### Joining-side daemon bridge

A joining consumer uses its running runtime-enabled daemon, rather than creating
a second identity or temporarily trusting the gateway:

```json
{"type":"pairing_remote","gateway":"<invitation-pinned NodeID>","addr":null,"request":{"operation":"present","attempt_id":"<attempt>","secret":"<creation secret>"}}
```

`gateway` is mandatory. Optional `addr` uses the existing EndpointAddr JSON hint;
its `id` must equal `gateway`. The endpoint authenticates the pinned NodeID even
when addresses are supplied. No allow-list entry is required or installed.
`request` is an existing restricted network request: `present`, `status`, `send`,
or `receive`. The response is `{"type":"pairing","reply":{...}}`. Unlike owner
control, restricted/bridge replies omit `access` and private completion metadata.
Only a reply bound to the requested attempt and this daemon's NodeID is returned.

Each call makes one restricted connection using the daemon's current endpoint,
with at most 32 outbound operations and one 30-second deadline covering connect,
request, and completion. On exchange reconnect, a small status frame first
authenticates the bound attempt on the same stream before the larger send/receive
frame. Pre-auth present/status frames remain capped at 4 KiB; replies are bounded
to 1 MiB before allocation. The stream is half-closed after the final request;
the gateway releases its attempt lease before closing, so a subsequent call can
reconnect safely. Serialize calls for the same
attempt. No retry is implicit: on `timeout` or `unreachable`, reconcile using
`status` or the exchange's sequence/digest/ack replay semantics. An uncertain
transport outcome is never admission evidence. Disabled runtime mode returns
`runtime_peers_disabled`, and a mismatched address ID returns `invalid_argument`.
Network refusal or malformed/unbound replies return `unreachable` without
reflecting untrusted reply content or permit secrets into error text.

### Restricted presentation and status

Runtime mode advertises `fabric/pair/1`. Only that negotiated ALPN bypasses the
ordinary peer allow-list; no pending identity is added to mux, built-in services,
or direct exposures. Joining clients must use the gateway NodeID from their
invitation as the authenticated destination, not discover or substitute another
NodeID. Iroh authenticates both transport identities.

One bidirectional stream carries bounded JSON frames, each prefixed by a
four-byte big-endian byte length (4 KiB before bound-attempt authentication;
maximum encoded exchange request length 266,240 bytes after authentication):

```json
{"operation":"present","attempt_id":"<attempt>","secret":"<creation secret>"}
{"operation":"status","attempt_id":"<attempt>"}
```

Replies are length-prefixed pairing reply objects, without the local-control
`type` envelope, secret, access provenance, or private completion metadata.
The first valid presentation durably binds
the permit to the connection's authenticated NodeID and changes it to `pending`
at attempt revision 2. The identical node/token can replay that attempt without
extending its deadline; another NodeID cannot take its binding. Status requests
are accepted only for the authenticated bound node, including retained terminal
status. Unknown, wrong-node, wrong-token and malformed requests close with a
generic reason and expose no attempt status.

There are at most 32 accepted pairing connections and one active connection per
attempt. Unknown identities need global credit before parsing or runtime work:
one request per second, burst 10, refunded after successful validation. Previously
validated bound identities use an independent bucket (burst 8, refill one/second)
selected by a bounded expiring hint that never authorizes an operation. Ordinary
traffic uses neither pool nor bucket. Pairing availability is best effort under
attack. First-header waits are limited to three seconds; subsequent idle framing
waits are ten seconds, and connection lifetime is bounded by the attempt deadline.
Post-ALPN transport settings allow one bidi stream, zero uni streams and an 8 KiB
receive window; already-advertised QUIC credits cannot be retracted.

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
accepting connections; unbound issued permits expire on restart.

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

### Atomic confirmation and completion receipts

The creating owner verifies its own admission protocol and bound NodeID before
calling local control `confirm`. Fabric does not validate membership signatures.
The request contains `owner`, `attempt_id`, `expected_node_id`,
`expected_attempt_revision`, `expected_policy_revision`, and `request_id`.
All selectors are checked in the serialized transaction that installs the
immutable exact peer/grants and confirmed receipt. There is no separate
validate/install call. Existing creating-owner records and conflicting source
labels/directions are refused rather than overwritten.

Preparation, including sync configuration validation, precedes the admission
barrier. Commit rechecks binding, revisions, pending state, and the original
deadline after preparation under that barrier. Starting before expiry does not
reserve success: a commit reaching the barrier at or after expiry is refused
without installing grants. A commit accepted before expiry may finish fsync
and reply later. The receipt and peer/grants share one file commit and policy
publication completes before a success reply or any new-policy admission.

After a lost confirmation reply, resend the identical request body, including
the original `expected_policy_revision`. Refreshing that revision changes the
request digest: the same retained `request_id` then returns
`request_id_conflict`, not a replay.

Local `reject` takes `owner`, `attempt_id`, `expected_node_id`,
`expected_attempt_revision`, and `request_id`. It atomically records rejection
and erases pending eligibility, hashes, and transcript queues, installing no
access. Its exact retry returns the retained receipt. Reused request IDs with
different contents are refused.

Completion records contain `request_id`, `attempt_id`, `node_id`,
`policy_digest`, `policy_revision`, `committed_at`, and `outcome`. Confirmation
advances the policy revision and attempt revision; rejection never installs a
policy generation. The pairing receipt pool is reserved on permit creation,
independently of ordinary mutation receipts.

Owner control replies can report current effective `access` with source provenance.
Retry/query returns the historical completion plus current access, never
reinstalls historical grants. In particular, retrying confirmation after a
later owner removal or daemon restart cannot restore withdrawn access. Bound
nodes can read confirmed/rejected status on the restricted path, but terminal
attempts cannot exchange more bytes. Confirmed receipts and their installed
policy survive restart; pending attempts instead become `exchange_lost`.
Unknown or retention-expired attempts cannot be confirmed as new permits.
Restricted network status omits `creation_id`, completion metadata, secrets,
and effective-access provenance; the creating owner retains the full report.

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
| `set_expiry` | `owner`, `request_id`, `expected_revision`, `node_id`, `expires_at`. |
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

### Bounded overlap expiry

To publish a prepared replacement over the still-working old identity, use one
batch with `add_peer` for the next NodeID, `set_grants` for its **exact** grants,
and `set_expiry` for the old owner's record. For example, replace the immediate
removal in the example above with:

```json
{"op":"set_expiry","node_id":"<OLD_NODE_ID>","expires_at":1792887000}
```

The integer is illustrative: use an absolute UTC Unix-seconds deadline strictly
after the daemon's current observation and no more than 600 seconds ahead.
The signed 64-bit wire/storage representation rejects overflow; zero, negative,
past and over-limit deadlines return `invalid_argument` without committing any
part of the batch. The old and next NodeIDs must use distinct labels during
overlap. This grants overlap on the receiving peer only, not dual-identity
serving by the rotating daemon.

The owner's record starts with no expiry. Once set, `expires_at` cannot be
cleared, shortened or extended, including by updating grants, re-adding the
same existing record, or replaying an old request. An identical setter is a
no-op. Explicit removal and re-addition creates a new record; it is not an
implicit renewal. Static and other-owner access is never modified.

Runtime entries in `trust_sources`, `direction_sources`, and service provenance
include `expires_at` and `expiry_request_id` when set. Save `expiry_request_id`
from the scheduling reply/list before expiry; query it with `cancellation` to
observe automatic removal, residual access, selected work and completion. The
reserved ID belongs to that scheduling transaction, so a separately re-created
record cannot replay an earlier expiry receipt. Until cleanup commits, its
cancellation query returns an unknown receipt, never a premature completion.
Reservation conflicts with retained receipt IDs return `request_id_conflict`
before any part of the scheduling batch commits.

Authorization and attach select a prevalidated effective generation at the
deadline, without disk I/O or waiting for the mutation sequencer. A separate
50 ms deadline sweep uses the same admission/session cancellation and connection
retirement paths as removal. The serialized 100 ms cleanup sweep writes the
owner-scoped removal and advances the policy revision; idle ticks do not fsync.
Expiry withdrawals are not blocked by stale sync selectors. A blocked writer
cannot extend expired authorization or live work; residual grants preserve
still-permitted work. Cancellation monitoring retains its two-second deadline
and missed-bound evidence even when the automatic receipt is persisted later.
Sweep selection and signaling share an in-memory publication-generation guard;
they cannot revoke against a stale snapshot after newer residual grants publish.
This guard never covers filesystem work.

Running deadline observations combine wall and monotonic time and never move
backward, including reload reconciliation. Each forward wall observation anchors
a progressing monotonic floor, so rollback after scheduling cannot extend the
600-second elapsed bound. Restart removes elapsed records durably before
accepting traffic. A clock below the persisted last expiry
observation conservatively expires outstanding records. Arbitrary rollback
across power loss is not assumed detectable. Cleanup write failures cannot
restore expired access; uncertain commits use ordinary policy quarantine.

Homes without expiry keep the old representation. Once expiry is used, its
record fields and retained clock anchor make runtime state incompatible with
older strict readers lacking this feature.

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
While quarantined, every runtime-state writer, including pairing mutations and
cancellation completion updates, refuses to write until that reload reconciles
the durable generation. This holds even if recording the in-memory fault fails.

Check an existing request receipt before comparing revisions. Repeating the
same request ID and contents returns its original committed revision plus the
**current** effective-access view. It does not apply the mutation again, even
if access was withdrawn later. Different contents under a retained ID return
`request_id_conflict`. On a lost reply, query `cancellation` or repeat the exact
request; do not invent a new request ID for an uncertain outcome.

The store permits at most 1,024 owner/NodeID records in total. Receipts are
retained for at least 24 hours in a pool of 1,024 slots. Ordinary mutations may
use 960 slots; the remaining 64 are reserved for batches containing only
`remove_peer`, `set_expiry`, or `set_grants` that remove services without adding
any. Installing expiry also reserves a future automatic receipt slot; all
mutations account for those reservations before committing. Pure no-ops consume
no slot, even when the pool is full. Expired receipts are pruned by a subsequent
changed mutation. When the applicable capacity is full, that
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
