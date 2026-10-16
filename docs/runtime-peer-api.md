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

A mutation is serialized with accepted static reloads. Its expected revision
must match the current policy revision. A committed record or static-input
change advances that revision; a rejected transaction does not. An unchanged
owner record is a no-op and retains the current revision, with a durable
request receipt.

The daemon validates the final candidate, including sync selectors, then holds
the shared admission-policy generation through the durable commit and
publication. Persistence uses a private same-directory temporary file, file
fsync, rename, and directory fsync on the blocking pool. A success means both
state and effective policy are installed. A pre-rename failure leaves the old
policy and receipt history intact. If rename succeeds but durability is
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

Receipts have 1,024 reserved slots and are retained for at least 24 hours.
Expired receipts are pruned by a subsequent mutation; new mutations are refused
while all slots remain unexpired. After retention, reconcile current state and
revision before deciding on a new operation.

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
receipt and remaining IDs; it never rolls back policy. Completion persistence
failure retains the durable pending receipt and is logged. Restart ends old
work and resolves pending receipts with `restart_ended_work=true`.

The synchronous narrow-grant interface (for example Git repository grants)
fails closed with a transient `runtime_state_unavailable` while the durable
policy writer holds its barrier. It records the exact grant used so withdrawal
can end that operation without cancelling unrelated services.

Rotation expiry, policy watches, pairing permits and join exchanges are later
slices of [the approved design](runtime-peers.md). They are not implemented by
these requests.
