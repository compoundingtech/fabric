# Exec and shell audit history

Fabric records incoming built-in exec and shell calls and commands started by
exec exposures in `<home>/audit.jsonl`. One dedicated thread serializes and
appends JSON lines; runtime workers enqueue metadata without waiting for disk.
Completed appends are synced by that writer thread; new-file creation and
rotation sync their directory entry too. Audit failure does not refuse a call
or change its transport behavior.

```sh
fabric audit
fabric audit --since 1h --peer laptop
fabric audit --peer <NodeID> --json
```

The reader works with the daemon stopped. `--since` accepts a nonnegative
number with `ms`, `s`, `m`, `h`, or `d`; a bare number means seconds. `--peer`
matches the authenticated NodeID or the local label captured for each call.
Labels are historical snapshots: use the NodeID to follow a peer across
renames. Global loss records remain visible under a peer filter because a lost
record cannot reliably be attributed to a peer. Malformed and partial lines
are skipped and counted in `malformed_records`; a concurrent append may leave
a temporary partial tail. The reader holds file descriptors across rotation
and rescans for newly named segments, without stopping the writer.

## Recorded facts

Exec records a start before process spawn and one end after completion; a
spawn failure has its own end outcome. Shell records one start and one end for
the process/session, including across transport reattachments. Refused calls
are recorded before reading denied argv: `argv` is null and the existing
refusal code is retained. A handshake refusal can precede the mux target, so
its service and session identity are unavailable; the service stays null and
the record uses a fresh audit correlation ID.

Fields include UTC time, authenticated caller NodeID, local label, service
(`exec`, `shell`, or the exposure name), exec argv, session ID, exit status,
duration and byte counts. An admitted durable session uses its tunnel session
ID. Exec and shell start/end events share the same ID. `ok` means the command
ran, including a nonzero exit; `spawn_failed` means it could not start, and
`cancelled` covers a call ended by disconnect, revocation or shutdown. A status
that was not observed remains null. Exec signal exits use 128 plus the signal. Shell signal exits have null status
because the PTY API supplies a signal description without its number; the
existing shell wire reply still uses that API's fallback code.

Counters measure process payload observed or accepted by the daemon, excluding
argv, protocol frames, shell resize frames and tunnel retransmissions. A
cancelled call records bytes observed up to its end event; later pipe data is
excluded. Exec
output includes stdout and stderr; built-in exec has null stdin and counts
zero input bytes. Completion includes output processing. A descendant holding
an exec-exposure pipe open can delay the end until it closes or the session is
cancelled; built-in exec retains its existing bounded post-exit drain.

No environment map, shell keystrokes, stdin content or stdout/stderr content is
stored. Exec argv is recorded verbatim, so keep secrets out of command arguments.
The exact JSON shapes are in [json-output.md](json-output.md).

## Rotation and visible gaps

New files are 0600; opening an existing active file enforces that mode. Active
files rotate at 10 MiB or 24 hours from file creation (from open time on a
filesystem without creation times). At most ten rotated segments are retained.
Segments whose last write is older than 30 days are pruned when the writer
opens or rotates. Inactive files can remain until the next audit activity.

The audit queue allows 4,096 waiting records and one in progress, with a 16 MiB
record-storage budget including the in-progress record. Encoded records are
limited to 8 MiB. Overflow, oversized records and failed writes count as lost
records. On recovery the writer appends an `audit_loss` record with the count
and first/last drop times. It retries a failed summary without counting the
summary itself as a new loss.

`status --json` and `audit --json` expose the current daemon's `audit` object:
`dropped_records`, `queued_records` and the counter window's RFC3339 `since`
time. Counters restart with the daemon. Null means that a live counter could
not be obtained, not that no records were lost. Retained loss summaries cover
older gaps.

A start enqueue is cheap, not a durability barrier before spawn. A stalled
writer or abrupt daemon death can lose any queued or in-progress record: starts,
ends, refusals and pending loss summaries. An I/O failure can also leave a
record written but not confirmed synced; its loss count reports that uncertainty.
A command can therefore leave no durable trace. Shutdown attempts a
bounded flush; it does not wait indefinitely for disk or claim an fsync before
running a command. Rotation and retention intentionally remove history too.

This log detects misuse; it does not prevent it. A process running as the same
account can alter or delete it. Ship the file to a journal or another collector
under separate control when the history must survive that account's actions.
