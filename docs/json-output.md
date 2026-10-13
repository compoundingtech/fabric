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
| `status` | `{"version", "node_id", "endpoint_addr", "exposed_protocols", "dial_sockets", "dial_handlers": {"active", "max"}, "sync_runtime": {"owner", "companion"}, "peers": [{"id", "name", "reachable", "echo_denied", "inbound_only", "roaming", "round_trip_micros", "transport", "error"}]}`; connection telemetry is not part of it. A peer that trusts this machine but has not granted it `echo` is `reachable: true` with `echo_denied: true`, `error: null` and no round trip or transport |
| `dial PEER PROTOCOL` | `{"socket": "<path>"}` |
| `dial PEER PROTOCOL --tcp ADDR` | `{"address": "<host:port>"}` |
| `expose`, `unexpose` | `{"ok": true, "protocol": "<name>"}` |
| `send-file` | `{"ok": true, "peer", "name", "bytes"}` |
| `reload-peers` | `{"ok": true}` |

`peer-events` already prints one JSON object per line and is unchanged. `exec`
streams the remote command's own output and returns its exit status, so
`--json` changes only how a failure of `exec` itself is reported.
