# Runtime peers, grants, and the control client

Status: approved design. Runtime mode, direct peer/grant transactions, revisions,
and revocation receipts are implemented as a provisional API; see the
[runtime peer API](runtime-peer-api.md). Immutable rotation expiry and pairing
are implemented; policy watches and the typed client remain follow-up slices.
The surface remains provisional through at
least one minor release of real use, with an explicit review before promotion.
This note covers [#312](https://github.com/compoundingtech/fabric/issues/312)
and its typed control client, [#310](https://github.com/compoundingtech/fabric/issues/310).

## Existing behavior and the implementation boundary

At the design baseline `e5126b9`, [`ControlRequest`](../src/control.rs)
had `ReloadPeers` and no direct peer/grant mutations. The first runtime slice
adds those requests without a second endpoint. A control connection carries
one newline-delimited JSON request and one response, then closes.
[`daemon_control`](../crates/fabric-config/src/daemon_control.rs) already has a
client for a subset of that schema. The new client must share extracted
message types rather than introduce a third copy.

[`PeerBook`](../src/config.rs) provides exact grants and direction flags.
Its loader can migrate files, rewrite rollback mirrors, and remove legacy
inputs. It is not a read-only loader. Runtime mode needs a separate read-only
static-input path; calling the existing loader and merely avoiding `save()`
is insufficient.

[`reload_peers` and `revoke_withdrawn`](../src/daemon.rs) install accepted
policy and cancel withdrawn direct admissions and attached or detached server
sessions. A peer-load failure clears the book for new admissions without
ending existing work; sync validation failure leaves the old book installed.
At that baseline the book and connection allow-list used separate locks, and
cancellation counts were not completion acknowledgments. The first runtime
slice shares one policy generation and tracks completion explicitly, while
preserving those existing failed-reload distinctions without the flag.

## State and ownership

Runtime mode is an explicit startup choice (proposed `--runtime-peers`),
not a side effect of the first client request. Without it, existing file/CLI
behavior remains in force. In runtime mode the daemon owns
`<STATE>/runtime-peers.toml`, next to `identity.toml`. It contains a format
version, policy revision, runtime records, expiring records, pairing state,
and bounded transaction/completion receipts. It is private to the daemon
account and written through a same-directory temporary file, file fsync,
atomic rename, and directory fsync.

Pairing fields are omitted when the attempt list is empty and the persisted
pairing clock anchor is zero. A runtime state file that has never used pairing
therefore remains readable by 0.9.2, including after unrelated runtime-policy
writes. Once pairing has been used, the file is not downgrade-compatible with
0.9.2: its strict reader rejects the new fields. Receipt cleanup does not make
that downgrade safe; the nonzero pairing clock anchor remains.

The configured `peers.toml` is a read-only input, including when it is a
symlink. Runtime operations never rewrite it, replace the symlink, change
its target, or run write-on-load migrations. Existing format interpretation
and exact-grant semantics remain authoritative; any needed migration must
happen explicitly before runtime mode is enabled. Default XDG layouts and
custom homes use their existing resolved config path, not an assumed
`<STATE>/peers.toml`. CLI file edits continue to address static policy; runtime
clients use the daemon API.

A runtime record is keyed by `(owner, NodeID)` and contains a label, exact
grant set, direction, and optional expiry. Labels never identify a mutation
subject. Effective trust and grants are the union of static and runtime
records; trust with an empty grant set grants no service, including echo.
Grants use the existing exact service names (`exec`, `shell`, `echo`, or an
exposed service name, for example), without wildcards, prefixes, ports, or
implicit future services.

**Owner is attribution, not authentication.** Authority is local access to
the control socket. A process with that access can supply any owner string.
Owner scoping protects cooperating tools from accidental interference; it
provides no security separation against another process with that authority.
The runtime API neither verifies membership signatures nor decides membership.

Each operation edits only records under the supplied owner. Removing an
owner's record cannot remove static or other-owner access. Removing a node
with only a static record returns the proposed coded refusal
`fabric:peer_managed_statically`. A node belonging only to another runtime
owner returns an ownership refusal and its effective-access report. Removing
an entirely absent node is an idempotent no-op.

All sources that specify a label for the same NodeID must agree. Labels must
also be unique across effective NodeIDs. All sources for a NodeID must agree
on direction: normal, `inbound_only`, or `dial_only`. A conflicting change is
refused with the conflicting source named; no source wins silently. An omitted
field preserves an existing owner's value; a new record defaults to normal
direction and zero grants. `dial_only` cannot carry inbound grants.
`inbound_only` continues to exclude every dial, probe, announcement, and
background recovery attempt towards that node. A change between opposite
directions requires explicit replacement in a batch, with all sources agreeing.

Every list entry and mutation reply reports affected NodeIDs, labels, trust
sources, direction sources, and each effective service with **all** its sources:

```text
NodeID: <node-a>
effective services:
  exec:  [{kind: runtime, owner: controller-a}]
  echo:  [{kind: static, path: <CONFIG>/peers.toml},
          {kind: runtime, owner: controller-b}]
```

The configured static path is always present; its resolved target and content
digest can also be reported. Residual access is reported, never edited. A
reply distinguishes `owner_record_removed` from `effective_trust_removed` and
lists any services still effective. A caller must inspect that result before
claiming revocation. An unconfirmed pairing adds no rights; any pre-existing
static or other-owner access to that NodeID is reported by the same rule.

## Transactions, replacement, and expiry

The typed operations are `add_peer`, `set_grants`, `set_expiry`, `remove_peer`,
`list`, and `batch`. `set_grants` replaces that owner's entire set; `[]`
withdraws it. `add_peer` for a new ID starts with zero grants. Renaming an
existing ID preserves its grants. A batch has one owner, a fresh request ID,
an expected policy revision, and at most 128 operations within a 1 MiB encoded
request. Validation applies to the final candidate, including sync selectors,
label uniqueness, direction constraints, and source ownership.

Re-key and machine replacement use one batch: remove the old owner's record,
add the new NodeID, and set the new exact grants. Reusing the old label is
valid only in that final candidate. There is one durable write and one policy
publication, with no intermediate dual authorization or label-based transfer.
Other-owner/static records on the old ID survive and are explicitly reported.
The caller must resolve them separately if it requires complete retirement.

A single mutation sequencer covers runtime changes, accepted static reloads,
pairing confirmation, and automatic expiry. Prepare and validate the candidate
before entering the final commit section. In that section, an admission barrier
covers the final revision/deadline/single-use checks, durable commit, and
publication of one immutable effective-policy generation. New admissions and
policy reads cannot observe a half-installed generation. All inbound paths,
including connection admission and detached-session attach, check that same
generation. Registering an admission and checking it must preserve the current
record-before-check race protection across publication and revocation.

A success reply means durable state and effective policy are both installed.
For runtime mutations, validation or pre-rename write failures leave the old
state unchanged and
return no success. A rename/fsync failure whose outcome is uncertain is a
fault with an indeterminate outcome, not a claim that nothing changed. Block
new admissions until the committed file can be reconciled; resolve the request
by its durable ID before retrying. Crash after durable commit but before
publication/reply loads the committed generation at restart. Never acknowledge
an in-memory-only mutation or silently fall back to an older runtime file.
Malformed/unreadable runtime state leaves control diagnostics available with
network admissions closed until recovery, rather than silently restoring
access from an earlier snapshot.

Each mutation carries `expected_revision` and a unique request ID. Check an
existing receipt before the revision comparison. A retry with the same ID and
same request digest returns its recorded outcome and committed revision,
plus the **current** effective-access view; it never applies the mutation a
second time. Reusing an ID with different contents is refused. Receipts retain
bounded metadata for 24 hours in 1,024 slots, reserving 64 for withdrawal-only
mutations; refuse mutations at their applicable capacity rather than evict
unexpired receipts. There are at most 1,024 owner/NodeID records in total.
After retention, an old expected revision prevents a stale mutation from
reinstalling access; the client must reconcile instead of inventing a new ID
for an uncertain request. No-op mutations return the current revision without
storing a receipt.

Rotation defaults to immediate replacement. Explicit overlap uses the existing
atomic batch to add the new NodeID, set its exact grants, and `set_expiry` on
the old owner's record. This changes peer policy only: the rotating machine
prepares its key, publishes that batch over its still-working old identity, and
then activates its new identity. Fabric never needs to serve both identities.
The old ID retains a distinct label during overlap.

`expires_at` is an absolute UTC Unix-seconds signed 64-bit integer, strictly
in the future and at most 600 seconds away when first set. Negative, zero,
out-of-range and later deadlines are refused atomically. The deadline is
immutable for that owner/NodeID record, including through `add_peer` and
`set_grants`; an identical setter is a no-op and a historical batch retry
cannot renew or reinstall access. Removing and explicitly re-adding a record
creates a new record. Runtime source provenance reports `expires_at` and the
reserved `expiry_request_id` used to query automatic cancellation.

Policy generations precompute the effective union at each distinct deadline.
Their shared wall/monotonic observation never moves backward during one daemon
lifetime, including reconciliation. Admission, service authorization, outbound
policy lookup and session attach use the applicable generation without waiting
for a timer, mutation sequencer or disk writer. The deadline sweep independently
signals the existing admission/session revocation path, including connection
retirement, while the serialized writer records owner-scoped removal and
advances policy revision. Still-permitted static and other-owner work survives.
The two-second cancellation target is measured from expiry, not a delayed
cleanup commit; timeout evidence survives through the automatic receipt.

Setting expiry reserves an automatic receipt slot in the existing bounded pool.
An idle sweep performs no disk writes. Failed cleanup persistence cannot
reopen expired access; indeterminate outcomes use ordinary quarantine and reload
reconciliation. Cleanup withdrawals cannot be blocked by stale sync selectors.
Restart filters and durably removes elapsed records before admitting traffic,
with complete empty cancellation receipts because no work from the new daemon
was admitted. The persisted expiry clock detects rollback below its last
observation and conservatively expires outstanding records. This does not claim
that arbitrary clock rollback across a power loss can be measured.

Expiry fields and the clock anchor are omitted until expiry is used. Existing
state without expiry remains readable by older strict readers; state that has
used expiry is not downgrade-compatible with releases lacking this slice.

## Revision and watch contract

A persistent `policy_revision` advances for each committed record/provenance
change, accepted static-input change, confirmation, or expiry. It is not a
network-health counter. Rejected transactions do not advance it. A daemon
instance ID changes on restart; policy revision alone does not imply that a
watcher's in-memory event history survived.

`list()` returns `{instance, policy_revision, static_digest, peers}` from one
generation. Large lists use pages of at most 256 nodes, with a cursor bound to
that revision. A concurrent change invalidates the cursor; the client restarts
the snapshot rather than joining pages from different revisions.

`watch_policy(instance, after_revision, timeout)` uses repeated control
long-polls, like the existing peer-events interface, with a maximum wait of
30 seconds and a separate 256-event journal. It returns the instance, current
revision, `reset`, and ordered changes. Each change identifies its revision,
reason, affected IDs, and transaction/cancellation receipt IDs. It carries no
pairing secret or transcript. An idle timeout returns an empty batch at the
current revision. Restart, an unknown instance, a future cursor, or a journal
gap returns `reset=true`; the client fetches a fresh list and resumes from its
revision. No new unsolicited messages are multiplexed onto an old request.
The typed client exposes resets explicitly and never treats a gap as no change.

## Revocation acknowledgment and completion

At publication, close newly unauthorized admissions and signal cancellation
of existing unauthorized streams, raw service work, and attached or detached
server sessions. A detached session must already be unable to resume when the
mutation replies. Partial grant withdrawal preserves still-permitted work,
including work allowed by a residual source.

The mutation reply reports streams and sessions selected for cancellation,
completion already observed, and a cancellation receipt. These are separate
counts, not a claim that a signalled task has exited. Completion is measured
from the success reply, with a target bound of **2 seconds** for every selected
stream/session and its owned exec child to end. Dedicated completion tracking
and bounded child termination are required; an arbitrary service must not make
the policy writer wait forever. Failure to meet the bound is reported as a
cancellation fault with remaining IDs, not a rollback of the committed policy.
A receipt/status query and watch event distinguish complete from timed out.
Disconnecting the caller never cancels revocation. Restart resolves unconfirmed
cancellation receipts as `cancellation_timeout`, retaining remaining IDs and
explicit restart evidence that child exit was not observed; only observed
completion stays complete, and cross-crash child cleanup is tracked separately
in [#325](https://github.com/compoundingtech/fabric/issues/325).

## Pairing protocol and bounded join exchange

Pairing is opt-in and uses a new additive ALPN, proposed `fabric/pair/1`, on the
same daemon. The handshake hook must allow an untrusted authenticated NodeID
**only for this negotiated ALPN**; it remains refused by the ordinary service,
mux, and direct-exposure paths. Pending IDs are never inserted into the ordinary
connection allow-list. Both sides authenticate transport keys, and the joining
node pins the gateway NodeID from its invitation. The gateway serves the join
exchange without dialing an inbound-only node.

The owner creates a permit through the control client with immutable exact
grants, label/direction options, and an expiry no more than 10 minutes away.
The permit has a random attempt ID and a 256-bit random secret. Only a salted
hash of the secret is stored; plaintext appears only in the creation reply
and invitation, never in logs, list, watch, or completion receipts. A create
request also has an idempotency ID so its lost reply can be reconciled.

Pairing queries and read-only validation never write or fsync runtime state,
take the admission barrier, or acquire the peer-book write lock. They check
expiry against the in-memory monotonic clock floor; even a poll that observes
expiry does not durably commit it. Clock observations remain in memory and are
persisted only with pairing state transitions (issue, bind, terminal, or
expiry), not as a side effect of polling.

Recovery deliberately uses an issue-time anchor rather than periodic durable
uptime checkpoints. All outstanding permits expire conservatively on daemon
restart, even if the wall clock appears to be before their original deadlines.
The issue-time anchor cannot establish how long a permit was live before a
crash or how far the clock rolled back during downtime; preserving its
remaining lifetime would risk extending the 10-minute cap. Issue a fresh
permit after restart; queries cannot renew the old one.

First valid presentation on the authenticated pairing connection reserves the
permit to that connection's NodeID and durably creates `pending`. It installs
no standing trust or grant. A repeat by that same NodeID returns the existing
attempt; another NodeID cannot take the binding or start a second exchange.
Invalid tokens, exhausted limits, and expired permits do not create pending
access records. Pairing has its own capacity and rate limits: 64 outstanding
permits, 32 active connections, at most two per NodeID, and one active connection
per attempt. As soon as the ALPN is known, the connection is configured for one
bidirectional stream, zero unidirectional streams, and an 8 KiB receive window.
QUIC cannot retract credits already advertised before ALPN selection; these
limits constrain subsequent credit replenishment. The first frame header must
arrive within three seconds. Pairing-only handshake and framing deadlines do not
change ordinary handshake behaviour or blocking handler-slot acquisition.

Credential-less requests consume the global invalid-request bucket (one per
second, burst ten); successful validation refunds that token. The bound node's
authenticated traffic uses a separate small per-node bucket and does not depend
on global tokens left by rotating attacker keys. Pairing failures share one fixed
diagnostic/backoff key, rather than growing a map per untrusted identity.
Pairing availability remains best effort under attack: credential-less connections
can occupy the dedicated pool. Ordinary traffic does not use that pool or bucket.
Read-only validation and status never enter the policy commit barrier; only a
real binding or terminal transition persists. Commit replies are bounded by a
three-second wait, while an already-started durable commit finishes its policy
quarantine handling even if the caller disconnects or times out.

A pending NodeID status is insufficient to complete a signed admission. The
pairing connection therefore carries one logical bidirectional join exchange,
forwarded to the owner by `pair_receive` / `pair_send` control operations:

| Limit | Proposed value |
| --- | --- |
| One message | 64 KiB payload |
| Both directions combined | 1 MiB payload per attempt |
| Message count | 16 messages per direction, 32 total |
| Queued data | Included in the same 1 MiB budget |
| Session lifetime | Original permit deadline, at most 10 minutes |
| Concurrent logical exchanges | One per attempt and bound NodeID |

Messages carry direction, monotonically increasing sequence, and content
digest. Retrying the same sequence and digest is idempotent; different bytes
at the same sequence are refused. Reads can replay an unacknowledged message,
and sending counts each logical message once. Reconnection by the bound NodeID
may resume that same exchange before expiry; it does not reset budgets or open
another exchange. Exchange receive is nonblocking: an empty queue returns an
empty result immediately; callers poll explicitly rather than holding a control
long-poll open. The monotonic attempt deadline still bounds all exchange work.
Exceeding a byte/message bound rejects the attempt and discards its queues.

Sending, receiving and acknowledging exchange messages mutate only memory, never
fsync runtime policy or take the admission barrier. Only a real binding or
terminal transition is persisted. Bound-node exchange traffic uses its own small
rate bucket, independent of rotating credential-less keys' global bucket.
The expiry timer skips pairing work when no issued or pending attempts exist and
enters the durable writer only when expiry is due; idle ticks perform no fsync.
Worst-case payload memory is 64 active attempts × 1 MiB = 64 MiB, plus bounded
message metadata and request/reply framing.

Before a connection authenticates its bound attempt, present/status framing is
capped at 4 KiB. A bound node may use the larger encoded exchange-message cap;
reconnection must first authenticate with a small present/status request. An
over-cap frame on that authenticated connection rejects only its associated
attempt. An unauthenticated oversized frame is closed without allocating its
claimed body or guessing an attempt to reject. Sending to an issued-only or
terminal attempt returns a distinct coded error, not an empty success.

Fabric transports opaque bytes, such as a handshake or member-key transcript.
It binds them to the authenticated NodeID, attempt, direction, and sequence;
it does not parse membership, validate signatures, choose grants from the
payload, execute commands, or make this a general service. The owner verifies
its admission protocol and compares the bound NodeID before confirming. The
exchange uses neither frozen exposure framing nor mux/tunnel payloads.

Transcript buffers are memory-only and are never logged. A daemon restart
terminates an unfinished exchange and durably rejects that pending attempt
with reason `exchange_lost`; a new permit is needed. Confirmed receipts survive.
A transport reconnect without daemon restart can resume the existing buffers.
Confirm, rejection, or expiry closes the exchange and discards its bytes;
terminal status remains readable by the owner and bound node on the restricted
pairing path. It grants no further byte exchange or service access.

### Atomic confirmation and durable completion

Attempt status returns the attempt ID, state (`issued`, `pending`, `confirmed`,
`expired`, or `rejected`), attempt revision, bound NodeID if any, original
deadline, immutable permit-policy digest, and terminal receipt when present.
The permit-policy digest has an explicit format version and hashes a fixed
encoding of the policy fields, not the serialization layout of the `Policy`
struct. Adding a struct field must not silently change existing digests.
The attempt revision advances on binding/state changes; transcript sequences
are separate. A pending record grants no ordinary protocol, while the bound
node can read its own status through the restricted pairing protocol. Other
attempts are not enumerable from that path.

`confirm(owner, attempt_id, expected_node_id, expected_attempt_revision,
expected_policy_revision, request_id)` checks the creating owner attribution,
immutable binding, pending state, deadline, and single-use condition in the
**same serialized transaction** that installs the peer/grants and writes the
confirmed completion receipt. There is no separate validate-then-install RPC.
Confirmation conservatively refuses any NodeID already present in effective
static or runtime trust, even under another owner or without a label. Pairing
never augments or renames pre-existing trust; use an explicit runtime policy
transaction to change an existing peer. Competing confirmations cannot both
install policy. Completion request IDs are scoped by creating owner, so another
owner's retained ID neither conflicts with nor reveals its completion.

A newly confirmed normal-direction NodeID immediately participates in existing
wildcard sync selectors (`peers = "*"`), including outbound sync targeting.
Inbound-only direction still excludes outbound work. This is the existing sync
selector policy, not a grant inferred from exchange payloads.

The restricted network reply contains no `access` field or full provenance, and
no completion request ID or policy revision. The owner control path alone
returns complete source paths, owner labels and effective-access provenance.
Historical confirm replay returns the retained outcome without republishing the
peer book or retrying presence; in particular, replay cannot reopen quarantined
admissions. An explicit successful recovery/reload, not replay, clears quarantine.

Preparation does not reserve success. The final deadline check occurs at the
commit linearization point under the admission barrier, after preparation;
expiry and rejection use the same sequencer. The receipt stores that commit
time, NodeID, immutable permit-policy digest, and policy revision. A commit
accepted before expiry can finish its fsync/reply later; starting a request
before expiry is insufficient if preparation reaches commit after the deadline.
Fault-injection tests must cover that distinction. Durable commit and policy
publication finish before a success reply or any new-policy admission.

Repeating a confirmed attempt returns its durable receipt and current effective
access without installing anything again. In particular, a retry after a later
revocation must not restore the peer. Rejection and expiry delete the pending
eligibility record and secret hash, retain a terminal completion receipt, and
install no access. Pairing receipts last 24 hours and use their own bounded
1,024-slot pool, with at most 128 retained attempts per owner. Both limits count
outstanding permits and retained terminal attempts, including cancelled ones;
capacity is reserved when issuing permits, so completion never depends on
freeing another attempt's receipt. Issuing is refused at either limit rather
than evicting unexpired history. Owner attribution is not authentication: the
per-owner cap limits one owner string, not a hostile process able to choose
other strings. Expired receipt lookup returns unknown; an unknown attempt can
never be confirmed as if it were a new permit.

| Lost response or crash boundary | State left behind and recovery |
| --- | --- |
| Create fails before durable commit | No permit. Retry the same creation ID. |
| Permit committed, creation reply lost | An unbound, expiring permit; no access. Query by creation ID. The hash cannot recover the secret: cancel it and issue a fresh permit if the caller lost the secret. |
| Daemon restarts with an outstanding issued permit | Durably expire the permit regardless of the apparent remaining UTC lifetime; issue-time anchoring cannot rule out rollback across downtime. No access is installed. |
| Presentation fails before durable pending commit | Permit remains unbound; no access. The same node may present again before expiry. |
| Pending committed, presentation reply lost | One pending NodeID binding; no access. Presenting the same permit from the same NodeID returns that attempt without consuming another slot or extending expiry. |
| Transcript send/read reply lost | Replay by sequence/digest or acknowledge the same queued message; no extra bytes counted and no access installed. Restart instead rejects the unfinished exchange. |
| Owner confirms after expiry, rejection, or with a different NodeID | Refused atomically; terminal receipt and effective-access report show no pairing-installed access. |
| Confirm fails before durable commit | Pending remains eligible only until its original deadline; no partial peer installation. Query before retrying if the transport outcome was uncertain. |
| Confirm committed, before publication/reply, or reply lost | Durable confirmed receipt and peer policy are one commit. Finish publication or recover it at startup. Retry/query returns the receipt, never a second install. |
| Rename/fsync outcome uncertain | Fault, not success. Close admissions until recovery resolves the file and request ID; the owner must reconcile before reporting admission. |
| Reject/expire reply lost | Terminal durable receipt; no pending eligibility or transcript remains. Retrying cannot restore either. |
| Confirm receipt later outlives a peer removal | Historical confirmation plus current residual-access view; no regrant on lookup or retry. |

## Typed client, compatibility, and implementation proof

One supported provisional Rust client wraps the existing host daemon; it starts
no endpoint and launches no CLI per operation. Build it with the #303 extraction
and a shared internal control schema, rather than exposing today's JSON as a
new stable wire contract. It provides typed runtime operations, pairing/status
and policy watches, plus the dial/status/probe/expose and peer-events surfaces
from #310. The implementation PR follows approval of this note.

Capability/version discovery distinguishes an older daemon from a transient
transport fault. Unsupported runtime/pairing operations return a typed
unsupported result; they never fall back to editing static files or launching a
second daemon. Unknown error codes retain their refusal/fault class. Proposed
new refusal/fault codes must be added with the error documentation and JSON
contract tests in their implementation PR; this note adds no released codes.

`ensure_dial` means a local listener exists, not that remote admission succeeded.
Typed `open_stream` errors depend on the original refusal plumbing from #302;
EOF cannot supply the missing classification, and a follow-up probe is not
substituted for that failure. Selected route/session evidence depends on #301.
Keep status/probe evidence scoped to its own sample and preserve additive
status fields as those changes land. This client does not fix session limits
or promise a latency improvement without measurement.

Implementation gates, after the tag and design approval:

- Two real daemons, read-only static file and symlink target, writable runtime
  state: exact grant succeeds, unrelated service and echo are denied, rename
  preserves access, restart preserves policy and input bytes/symlink identity.
- Static plus two owners on one ID: removal/expiry touches only the requested
  owner, replies list all residual services and source paths, and static-only
  removal returns the coded refusal. Conflicting labels/directions are refused.
- Atomic re-key/replace, including reused labels, concurrent mutations, stale
  revisions, lost replies, failed fsync/rename, and crashes at every commit
  boundary. No snapshot admits the wrong identity or accidental dual access.
- Rotation maximum and timer/writer stalls: expiry rejects fresh admissions and
  resume immediately; clock changes/restart do not silently extend overlap.
- Revocation: new admission denied at reply, live stream ends, detached session
  cannot resume, owned children exit within the separate 2-second bound, and
  still-authorized work survives a partial reduction. Report a missed bound.
- Watch snapshot/pagination races, gaps, journal overflow, duplicate events,
  restart/instance reset, expiry changes, and idle long-poll timeout.
- Pairing: all lost-response rows above; two nodes racing one permit; duplicate
  presentation/confirmation; post-revocation confirm retry; preparation delayed
  beyond expiry; rejection; restart; no ordinary service reachable through a
  pending record. Exercise secret redaction and permit capacity/rate limits.
- A pending node and owner complete an opaque signed-admission-shaped exchange
  within the bounds, including lost message replies. Wrong ID/attempt/sequence,
  oversized payload, exhausted message count, expiry, and terminal exchange
  closure are refused. Fabric never interprets membership signatures.
- Real control framing/schema parity, Unix/TCP reuse, concurrent first use,
  stale listeners after restart, duplex half-close, typed error classes,
  capability negotiation, and explicit event reset handling in the client.

These are acceptance requirements, not a claim that every gate has passed.
The implemented opt-in runtime policy and pairing control/transport contract is
documented in [runtime-peer-api.md](runtime-peer-api.md). Policy watches,
rotation overlap, and the separate typed client remain design work rather than
released interfaces.
