# Error codes

Every failure fabric reports carries a stable code. A program reads the code;
the text beside it is for people and may change. Fabric 0.2.41 and later
carry codes. This page is the contract, and the test
`fabric_config::error::tests::the_documented_table_is_the_code_list` keeps the table below
identical to the code.

## Codes

Each code has a class, and the class says what a caller should do:

- `refusal`: trust or a grant said no. Retrying, or trying another route to
  the same machine, will not change that; only a change to trust will. Show
  it to a person as a refusal.
- `fault`: it did not work this time. Another attempt, later or by another
  route, may.
- `usage`: fabric rejected the arguments. Fix the call.

<!-- codes -->
| Code | Class | Exit | Meaning |
| --- | --- | --- | --- |
| `peer_not_in_allow_list` | refusal | 77 | The other machine does not trust this NodeID, or stopped trusting it. |
| `peer_not_permitted_for_service` | refusal | 77 | The other machine trusts this NodeID but does not grant it this service, or removed the grant. |
| `unknown_peer` | fault | 69 | This machine has no trusted peer by that name or NodeID. |
| `peer_offline` | fault | 69 | The peer was recently found absent, so fabric did not try again yet. |
| `unreachable` | fault | 69 | Fabric tried and could not reach the peer. |
| `timeout` | fault | 69 | The caller's deadline passed before an answer. |
| `daemon_not_running` | fault | 69 | The local daemon is not running (`fabric up` starts it). |
| `daemon_stop_timeout` | fault | 69 | The daemon is still stopping after the bounded wait; its lease is not free yet. |
| `service_not_exposed` | fault | 69 | The peer is reachable but exposes no service by that name. |
| `service_unavailable` | fault | 69 | The service is exposed, but its socket or address could not be reached. |
| `session_limit_reached` | fault | 69 | The peer holds as many sessions as its limits allow, all attached. |
| `session_expired` | fault | 69 | A resume named a session the peer no longer holds. Start a new session; do not replay the old one. |
| `spawn_failed` | fault | 69 | The command behind an exec service or `fabric exec` could not be started. |
| `internal` | fault | 1 | Any failure not yet given a more specific code. |
| `invalid_argument` | usage | 2 | Fabric rejected an argument's value. |
<!-- /codes -->

### Rules for the 1.x line

- Codes may be added in a minor release. None is removed or renamed, and no
  code moves to another class, before 2.0.
- A reader that meets a code it does not know acts on its class. The class is
  authoritative.
- A failure that gains a more specific code in a later release moves out of
  `internal`, which is a fault either way.

## The command line

A failing command exits with the status in the table: `77` for a refusal,
`69` for a fault, `2` for a usage error (the status clap already uses for an
unknown flag or a missing argument) and `1` for `internal`, which is the status
every failure had before codes.

These commands keep the exit statuses they already documented, and report codes
in their output instead:

- `fabric exec` exits with the remote command's status, so a status alone
  cannot tell a fabric failure from the command's own. Read the stderr line.
- `fabric probe` exits 0 supported, 1 unsupported, 2 unreachable, 3 timeout,
  and 64 when it cannot answer. These statuses are a legacy contract, kept
  for scripts that already read them; a grant refusal, for one, has always
  exited 2. The code is authoritative: `probe --json` prints its answer
  object, in which `error` is a string or `null`, and `code` and `class` name
  the code behind any answer other than `supported` (both `null` for
  `supported`). When it cannot answer, `--json` prints the error object
  instead. See [`fabric probe --json`](#fabric-probe---json) below.
- `fabric doctor` exits 0 to 3 as its help describes.
- `fabric update --check` exits 0 current, 1 available, 2 error.
- `fabric dial` does not connect: it resolves the peer and opens a local
  listener, so it fails only for what it can tell locally (`unknown_peer`,
  `daemon_not_running`, `invalid_argument`). A refusal or a fault met later
  surfaces as a closed local connection, with no text. To learn why, run
  `fabric probe <peer> <service> --json` right after and read its `code`.
- `fabric sync` subcommands other than `add`, `rm` and `reload` are run by the
  sync companion, which is provisional and does not report codes yet. Their
  own `--json` goes after the subcommand: `fabric sync ls --json`.

The last line fabric writes to stderr for a failure is:

```text
Error: fabric:<code> <text>
```

The text is for people. It keeps the words fabric printed before codes, so a
script that looks for them still finds them.

### `--json`

With `--json`, a failing command also prints exactly one line on stdout, a JSON
object, and exits with the status above:

```json
{"error":{"code":"peer_offline","class":"fault","message":"dial web on box: peer is offline","peer":"box","service":"web"}}
```

- `code` and `class` are as in the table.
- `message` is the text for people.
- `peer` is the peer as the caller named it, and `service` the service, or
  `null` when the failure concerns no single one. Both keys are always present.
- Nothing else is printed on stdout for a failure. Stderr still carries the
  human line; do not parse it when you have the JSON.

### `fabric probe --json`

Every probe prints one line. When the probe was answered, it is the answer
object, which has the same keys every time:

```json
{"peer":"server","peer_id":"<node id>","protocol":"web","outcome":"supported","round_trip_micros":812,"transport":"direct","error":null,"code":null,"class":null}
```

| Case | Exit | `outcome` | `code` | `class` |
| --- | --- | --- | --- | --- |
| The service answered | 0 | `supported` | `null` | `null` |
| The peer exposes no such service | 1 | `unsupported` | `service_not_exposed` | `fault` |
| The peer does not grant the service to this machine | 2 | `unreachable` | `peer_not_permitted_for_service` | `refusal` |
| The peer does not trust this machine | 2 | `unreachable` | `peer_not_in_allow_list` | `refusal` |
| No answer, or no path to the peer | 2 | `unreachable` | `unreachable` or `peer_offline` | `fault` |
| The `--timeout` passed | 3 | `timeout` | `timeout` | `fault` |

`error` is the far side's text, or `null`. When the probe could not be asked
at all (no daemon, an unknown peer name, an invalid `--timeout`), it exits 64
and prints the error object instead, for example
`{"error":{"code":"unknown_peer","class":"fault",...,"peer":"ghost","service":null}}`.

A build before 0.2.41 prints the same answer object without `code` and
`class`. When it cannot answer, it prints nothing on stdout and exits 64.

A clap usage error (an unknown flag, a missing argument) is found before
fabric knows `--json` was asked for. It exits 2 with usage text on stderr and
prints nothing on stdout.

A build older than 0.2.41 does not know `--json` on most commands, and answers
the same way: exit 2, usage text on stderr, nothing on stdout. A reader can
treat "no JSON object on stdout" as an older build and fall back to its text.
`fabric probe --json` and `fabric sync ls --json` existed before codes. On
`probe`, an error object is one whose `error` is an object (a probe answer's
`error` is a string or `null`); or compare `fabric --version`.

## On the wire

A machine-to-machine refusal or failure carries the same token, followed by
one ASCII space and the text fabric sent before codes:

```text
fabric:<code> <text>
```

`<code>` is `[a-z][a-z0-9_]*`. It appears in:

- The reason of an application close with code 403, on the direct exposure
  wire ([docs/tunnel-wire.md](tunnel-wire.md)) and between daemons. For
  example `fabric:peer_not_in_allow_list node is not in fabric allow-list`
  and `fabric:peer_not_permitted_for_service peer not permitted for service
  "web"`. The close code stays 403 for both refusals, so a client that reads
  only the code is unaffected.
- The text of an Error frame (kind 5) on the direct exposure wire, for example
  `fabric:session_expired ...`.
- The denial a daemon sends another daemon for a stream it will not serve, so
  a command run on one machine reports the code the other machine chose.

A reader finds the token by its `fabric:` prefix followed by a code and a
space; the old text after it is unchanged, so a reader that matched the old
text with a substring search keeps working. A reader that compared the whole
reason for equality must look for the text after the token instead.

A daemon older than 0.2.41 sends the old text without a token. Fabric reads
`not permitted for service` as `peer_not_permitted_for_service` and
`node is not in fabric allow-list` as `peer_not_in_allow_list`, and other
clients can do the same.
