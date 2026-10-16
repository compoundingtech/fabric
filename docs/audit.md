# Exec and shell audit history

Fabric records incoming built-in exec and shell calls and commands started by
exec exposures in `<home>/audit.jsonl`. Runtime workers bound and encode metadata, then enqueue the encoded bytes
without waiting for disk. One dedicated thread appends and syncs JSON lines.
Completed appends are synced by that writer thread; new-file creation and
rotation sync their directory entry too. Audit failure does not refuse a call
or change its transport behavior.

```sh
fabric audit
fabric audit --since 1h --peer laptop
fabric audit --peer <NodeID> --limit 1000 --json
```

The reader works with the daemon stopped. By default it returns the newest
10,000 matching records, ordered by event time; `--limit N` accepts 1 through
100,000. Retained matching lines are also bounded to 16 MiB of encoded data.
When either bound omits matching history, JSON reports `truncated: true` and
`limit` gives the selected record cap; human output warns on stderr. Filtering
happens before the bounds. The JSON response borrows the retained records
without cloning them into a second JSON tree. `--since` accepts a nonnegative
number with `ms`, `s`, `m`, `h`, or `d`; a bare number means seconds. `--peer`
matches the authenticated NodeID or the local label captured for each call.
Labels are historical snapshots: use the NodeID to follow a peer across
renames. Global loss records remain visible under a peer filter because a lost
record cannot reliably be attributed to a peer. Malformed and partial lines
are skipped and counted in `malformed_records`; a concurrent append may leave
a temporary partial tail. After reopening, the writer separates a partial old
line from the next event so that the new event remains readable. The reader
holds file descriptors across rotation
and rescans for newly named segments, without stopping the writer.

## Recorded facts

Exec records a start before process spawn and one end after completion; a
spawn failure has its own end outcome. Shell records one start and one end for
the process/session, including across transport reattachments. Refused calls
are recorded before reading denied argv: `argv` is null and the existing
refusal code is retained. Handshake refusals are recorded only when the ALPN
identifies built-in exec or shell, with a fresh audit correlation ID. Unknown
mux targets and unrelated ALPNs are omitted without reading denied input.
Pre-auth refusals are coalesced to at most one record per (NodeID, service) per
second. The next emitted record for that pair includes `suppressed_refusals`.
To bound changing-key floods, at most 64 pre-auth records are admitted per
one-second window and at most 64 can be queued or in progress. The coalescer
keeps at most 128 pairs, expiring inactive pairs after two seconds; suppression
counts for uncached or expired pairs appear as `other_refusals_suppressed` on
the next emitted pre-auth record, independently of its caller. These optional
fields are omitted when zero. Pending suppression counts can be lost on crash;
coalescing is distinct from writer loss in `audit.dropped_records`.
An exec/shell or exec-exposure session takeover attempt is recorded as refused
before any additional input is read.
A dial waiting for capacity can retry before attachment; each server-side
refusal is an attempt and remains visible under the same tunnel session ID,
even if a later attempt starts the command.

Fields include UTC time, authenticated caller NodeID, local label, service
(`exec`, `shell`, or the exposure name), exec argv, session ID, exit status,
duration and byte counts. An admitted durable session uses its tunnel session
ID. Exec and shell start/end events share the same ID. `ok` means the command
ran, including a nonzero exit; `spawn_failed` means it could not start, and
`cancelled` covers a call ended by disconnect, revocation or shutdown. A status
that was not observed remains null. A cancelled end can appear without a start
when input is invalid or the client disconnects during setup; event-pairing
readers must tolerate missing starts as well as missing ends. Exec signal exits use 128 plus the signal. Shell signal exits have null status
because the PTY API supplies a signal description without its number; the
existing shell wire reply still uses that API's fallback code.

Counters measure process payload observed or accepted by the daemon, excluding
argv, protocol frames, shell resize frames and tunnel retransmissions. A
cancelled call records bytes observed up to its end event; later pipe data is
excluded. Exec
output includes stdout and stderr; built-in exec has null stdin and counts
zero input bytes. Completion includes output processing. A descendant holding
an exec-exposure pipe open can delay the end until it closes or the session is
cancelled. The exec-exposure `cleanup_done` guard stays held until output
pipes drain, so a descendant retaining stdout can delay cleanup completion
until the session closes; cleanup waiters close the session before waiting.
Built-in exec retains its existing bounded post-exit drain. A
cancelled exposure gets up to 200 ms for buffered pipe observations in a
separate audit task, without delaying child cleanup or retaining its cap.

No environment map, shell keystrokes, stdin content or stdout/stderr content is
stored. Exec argv is capped at 64 KiB of encoded JSON, retaining a prefix
of elements and, if necessary, a prefix of the final element. A shortened
snapshot has `truncated: true` and `original_argc` with the original element
count; the actual executed argv is untouched. Newline and JSON controls are
escaped, as are DEL, C1, Unicode line separators and bidi controls in both file
and CLI output. Decoding JSON recovers the retained argv. Keep secrets out of
command arguments even when the snapshot might be truncated.
The exact JSON shapes are in [json-output.md](json-output.md).

## Rotation and visible gaps

New files are 0600; opening an existing active file enforces that mode. Active
files rotate at 10 MiB or 24 hours from file creation (from open time on a
filesystem without creation times). Normally ten rotated segments are retained.
Segments whose last write is older than 30 days are pruned when the writer
opens or rotates. Pruning is best effort: one cleanup warning per open does not
stop appends, and unremovable segments may exceed retention. Inactive files
can remain until the next audit activity.

The audit queue allows 4,096 waiting records and one in progress, with a 16 MiB
encoded-byte budget including the in-progress record; queued data is stored
as encoded bytes, without per-argv-element queue amplification. Encoded records are
limited to 8 MiB. Overflow, oversized records and failed writes count as lost
records. On recovery the writer appends an `audit_loss` record with the count
and first/last drop times. It retries a failed summary without counting the
summary itself as a new loss.

`status --json` and `audit --json` expose the current daemon's `audit` object:
`dropped_records`, `queued_records` and the counter window's RFC3339 `since`
time. `queued_records` includes a record currently being written as well as
waiting records. Counters restart with the daemon. Null means that a live counter could
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
