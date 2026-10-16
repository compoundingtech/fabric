# `--json` output

With `--json`, a command in the table below prints exactly one JSON object on
stdout and nothing else, so a program can read the stream without parsing text.
`--json` is a global flag: it works before or after the command.

On a failure, any command prints one error object on stdout instead (see
`docs/errors.md`), nothing else is printed there, and the exit status is the
error code's. A command that is not in the table has no result to print as JSON
and prints what it always did, and so does any command without `--json`.

The keys below are the contract. A key is never renamed or retyped inside 1.x;
a new key may be added, so read objects by key and ignore keys you do not know.
`tests/json_output.rs` compares every shape below with a file in
`tests/golden/json/`, so a change to one fails a test in the same pull request.

| Command | Object |
| --- | --- |
| `id` | `{"id": "<NodeID>"}` |
| `addr` | the daemon's `EndpointAddr` object (`{"id": ..., "addrs": [...]}`), with or without `--json`; it was JSON before the flag existed |
| `peers` | `{"peers": [{"node", "name" (or null), "allow": [...], "dial_only", "inbound_only", "roaming"}]}` |
| `probe PEER PROTOCOL` | `{"peer", "peer_id", "protocol", "outcome", "round_trip_micros", "transport", "error", "code", "class"}`; `outcome` is `supported`, `unsupported`, `unreachable` or `timeout`, and the exit status is unchanged |
| `status` | `{"version", "node_id", "endpoint_addr", "exposed_protocols", "dial_sockets", "audit": {"dropped_records", "queued_records", "since"} (or null), "dials": [{"peer", "protocol", "socket" or "tcp", "last_error": null or {"code", "class", "at"}}], "dial_restore_failures": [{"peer", "protocol", "tcp", "code", "class", "error"}], "dial_handlers": {"active", "max"}, "sync_runtime": {"owner", "companion"}, "peers": [{"id", "name", "reachable", "echo_denied", "inbound_only", "roaming", "round_trip_micros", "transport", "error"}]}`; connection telemetry is not part of it. A peer that trusts this machine but has not granted it `echo` is `reachable: true` with `echo_denied: true`, `error: null` and no round trip or transport |
| `audit` | `{"records": [...], "audit": {"dropped_records", "queued_records", "since"} (or null), "malformed_records": N, "truncated": bool, "limit": N}`; records and limitations are described in [audit.md](audit.md) |
| `dial PEER PROTOCOL` | `{"socket": "<path>"}` |
| `dial PEER PROTOCOL --tcp ADDR` | `{"address": "<host:port>"}` |
| `expose`, `unexpose` | `{"ok": true, "protocol": "<name>"}` |
| `send-file` | `{"ok": true, "peer", "name", "bytes"}` |
| `reload-peers` | `{"ok": true}` |

`peer-events` already prints one JSON object per line and is unchanged. `exec`
streams the remote command's own output and returns its exit status, so
`--json` changes only how a failure of `exec` itself is reported.

`dial_restore_failures` lists saved listeners that the daemon could not restore
at startup (for example, a Unix socket path that is too long or a TCP port now
held by another process). The daemon keeps running and restores its other
listeners. The declaration stays in `config.toml` for a later restart; repairing
and successfully dialing it, or `undial`, clears its failure from status.

`dials` describes active local Unix and TCP listeners. `peer` is the stable peer
identity; the endpoint is `socket` for Unix or `tcp` for TCP. For generic byte
tunnels, `last_error` is the latest session or admission failure in this daemon
run, even without `--error-preface`. `at` is its RFC 3339 UTC timestamp. It is
`null` before a failure, resets when the listener is replaced or the daemon
restarts, and remains after successful sessions. Concurrent sessions share
this history: it is not a request-specific result or proof of current service
authorization. Registered service sockets use their own framing and do not
populate this generic tunnel history. Older daemon replies default `dials` to
an empty array. `probe --json` remains the older-daemon fallback.

The peer latency and transport in `status` measure the built-in echo protocol.
When `echo_denied` is true, their null values say nothing about another service's
availability, latency, or transport. No additional echo grant is needed to
measure a permitted service: use `fabric probe PEER SERVICE --json` and read its
`protocol`, `outcome`, `transport`, and `round_trip_micros`. That probe describes
one protocol sample, not the route of every application session.

## Provisional runtime-peer control replies

`--runtime-peers` adds the `runtime_peers` request/reply family on the daemon's
existing JSON-lines control socket. It does not change `fabric peers --json`,
which continues to read static file policy. Runtime clients use the effective
snapshot and per-source mutation results described in
[runtime-peer-api.md](runtime-peer-api.md). Replies expose explicit `snapshot`,
`mutation`, `affected`, and coded `error` fields. A cancellation fault can
accompany a committed mutation receipt; clients must inspect both. Protocol
fixtures are pinned in `tests/golden/json/runtime_peer_control.json`.
Unconfirmed receipts after restart retain remaining IDs and return
`cancellation_timeout` with `restart_evidence` naming the changed daemon instance
and the absence of observed child exit. Restart alone is not completion proof.
A pure no-op returns the current revision in `mutation` without retaining a
receipt; its request ID cannot be queried later through `cancellation`.

## Audit records

The additive `audit` field in `status --json` and `audit --json` counts dropped
records and queued records for the current daemon lifetime, starting at the
RFC3339 `since` time. An older reply or unavailable daemon yields `audit: null`,
which means unknown rather than zero loss. Retained `audit_loss` records describe
older gaps even after the live counter restarts.

Audit call records have `event`, `time`, `peer`, nullable `name`, nullable
`service`, nullable `argv`, `session_id`, nullable `outcome`, nullable `code`,
nullable `exit_status`, nullable `duration_millis`, `bytes_in`, and `bytes_out`.
Events are `exec_start`, `exec_end`, `shell_start`, `shell_end`, or `refused`.
Outcomes are `ok` (including a nonzero command exit), `refused`, `spawn_failed`,
or `cancelled`; start events have no outcome yet. Codes use `fabric:<code>`;
legacy uncoded diagnostic rejections have a null code. `audit_loss` records
instead contain `event`, `time`, `count`, `first_time`, and `last_time`.

The audit reader defaults to the newest 10,000 matching records (`--limit N`,
1–100,000) and at most 16 MiB of retained encoded lines. `truncated` indicates
omitted matches under either bound, and `limit` is the requested record cap.
Call records with shortened argv add `truncated: true` and `original_argc`;
pre-auth refusal coalescing adds nonzero `suppressed_refusals` for the pair and
`other_refusals_suppressed` for uncached/expired pairs. These optional fields
default to false, null or zero for older records. Unicode separators, DEL/C1
and bidi controls are escaped in file and CLI JSON as well as human output.
