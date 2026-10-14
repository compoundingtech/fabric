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
| `status` | `{"version", "node_id", "endpoint_addr", "exposed_protocols", "dial_sockets", "relays": {"source", "urls": [...]}, "dials": [{"peer", "protocol", "socket" or "tcp", "last_error": null or {"code", "class", "at"}}], "dial_restore_failures": [{"peer", "protocol", "tcp", "code", "class", "error"}], "dial_handlers": {"active", "max"}, "sync_runtime": {"owner", "companion"}, "peers": [{"id", "name", "reachable", "echo_denied", "inbound_only", "roaming", "round_trip_micros", "transport", "error"}]}`; connection telemetry is not part of it. A peer that trusts this machine but has not granted it `echo` is `reachable: true` with `echo_denied: true`, `error: null` and no round trip or transport |
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

`relays` reports the startup relay candidates used by the running daemon.
`source` is `default` when config omitted the list, `custom` for a nonempty list,
or `disabled` for an explicit empty list. `urls` contains the effective,
normalized candidates in sorted order. This is configuration, not connected
relay health or an outgoing relay allow-list: iroh may contact a peer's
advertised relay outside these candidates. Older daemon replies produce
`source: "unknown"` with an empty URL list. Editing config or reloading peers
does not change this snapshot; restart to use a different selection.

The peer latency and transport in `status` measure the built-in echo protocol.
When `echo_denied` is true, their null values say nothing about another service's
availability, latency, or transport. No additional echo grant is needed to
measure a permitted service: use `fabric probe PEER SERVICE --json` and read its
`protocol`, `outcome`, `transport`, and `round_trip_micros`. That probe describes
one protocol sample, not the route of every application session.
