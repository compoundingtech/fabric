# Changelog

All notable changes to fabric are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/); fabric is pre-1.0 and
EXPERIMENTAL, so on-disk formats and the CLI may change without notice.

## [Unreleased]

### Fixed

- **A disconnected `fabric exec` caller cannot leave its remote child alive.**
  The server now detects client EOF while the child runs or waits. It kills the
  child when the session ends, even when the child produces no output.

- **`fabric restart` cannot replace a supervised daemon with an unmanaged
  process.** If the selected default home has an installed launchd or systemd
  service, the command refuses before it schedules a helper and names the
  correct native restart command. If service ownership cannot be read, it also
  names the native status command that diagnoses the check. An unsupervised
  daemon remains restartable.
  Shell and exec policy comes back from `peers.toml`, and the restart test proves
  both capabilities in both traffic directions.

## [0.2.1] - 2026-09-03

### Added

- **A roaming peer can stay away without disrupting the rest of the mesh.** Set
  `roaming = true` on a peer in `peers.toml`. The default stays `false`. Fabric
  still probes and syncs with an away roaming peer, so it detects the peer's
  return. Failed attempts do not start recovery or increase failure counters.
  Status, doctor, and sync output report the peer as away. The daemon logs only
  the away and returned transitions.

  A peer can have a different local name on each machine. The same NodeID is
  `bluey` on Silber and `air` on hetz. The roaming setting follows the NodeID's
  peer entry, not its local name.

- **Peer traffic shares one multipath connection.** Fabric carries each Git,
  sync, shell, exec, send-file, echo, and exposed-service session as a stream on
  one authenticated connection per peer pair. Simultaneous cross-dials select
  one connection and close the duplicate. Mux version 2 exchanges endpoint
  generations before it registers the connection. A newer generation replaces
  stale cached state without a daemon restart. A peer that does not support
  `fabric/mux/2` uses the existing direct service protocol until both builds
  support mux version 2. Other connection failures do not cause a downgrade.
  New servers continue to accept old direct-protocol clients.

- **Persistently slow paths recover without a daemon restart.** The peer health
  loop records every selected path and its RTT. It redials a shared connection
  after three samples exceed both one second and eight times that path class's
  baseline. A generation change resets the evidence. A 60-second per-peer
  cooldown prevents redial storms. Recent application traffic skips a redundant
  liveness probe.

- **Git remotes work through Fabric.** A `fabric://<peer>/<remote>` URL uses
  Git's smart protocol over an authenticated Fabric connection. The host checks
  an exact per-repository read or write grant before it starts `git
  upload-pack` or `git receive-pack`. The installer and updater install the
  `git-remote-fabric` helper link. Session limits, a ten-second first-answer
  deadline, separate stderr framing, and child cleanup bound the service.

- **The executable bit is replicated.** Fabric now syncs the attributes git
  syncs. A file arrives executable if it was executable at its origin, applied
  before the atomic rename so it is never briefly visible with the wrong mode.

  The live case: the synced catalog holds fabric binaries, and they arrived
  without the bit and could not be run until somebody chmod-ed them by hand.

  **Divergence from git:** a `chmod` on an ALREADY SYNCED file does not
  propagate. Git propagates one, because a mode change rewrites the tree object
  and is a real commit. Fabric does not, for the reason below. A NEW file
  carries its mode correctly, which is the case that actually bites.

- **Symlinks are skipped out loud.** A skipped symlink now says so, and says
  that git tracks symlinks and fabric does not yet. Previously it was skipped in
  silence. Fabric still does not sync them: a symlink is a different kind of
  manifest entry rather than a file with a flag.

### Known limitation

- **Fabric cannot propagate a metadata-only change. One cause, two symptoms.**
  `SyncNode::local_write` returns early when the content hash is unchanged, and
  that early return is what makes applying a peer's content echo-free. So any
  change that alters no bytes never advances a logical version, and a change
  that does not advance a version never crosses the wire.

  1. *A heartbeat is invisible.* Rewriting the same bytes with a new mtime does
     not propagate, so a replica keeps the older timestamp. A consumer must not
     read a replica's mtime as an activity signal.
  2. *A chmod is invisible.* `chmod +x` on an already-synced file changes no
     bytes, so the new mode does not propagate.

  Both are the same defect. Fixing either symptom alone does not touch the
  cause. Closing it needs a local metadata-only change to advance a version
  while a received one stays inert, which puts an asymmetry into the exact
  mechanism that prevents infinite echo — a core engine change.

### Changed

- **Peer permissions use a strict allow list.** An omitted or empty `allow`
  field in `peers.toml` denies every service. The machine-level shell and exec
  gates still apply. The daemon starts with an empty list so an operator can
  inspect and migrate the configuration with `fabric peers make-explicit`.

- **The updater refuses an older or unrelated release by default.** It names
  both versions and leaves the installed binary unchanged. An operator must
  pass `--allow-downgrade` to replace it explicitly.

- **Durable connection totals now include their window and roster context.**
  `fabric status` prints when the cumulative telemetry window started. It keeps
  that start across daemon restarts. An unreadable or incompatible snapshot
  prints its reset reason. A legacy snapshot with no recorded start says that
  the start is unknown instead of inventing one. Session and path rows mark a
  retained telemetry peer as `[not in peers.toml]` when the current allow-list
  no longer contains it. A retired peer's historical retry total therefore no
  longer looks like current dial activity.

- **Tunnel writers cannot miss a session-state wake.** The writer and buffer
  wait loops create their notification futures before they inspect session
  state. Data or acknowledgements that arrive between the state check and the
  wait now wake the loop. A recovered live tunnel no longer stalls after it
  carries its first replayed payload.

- **The scan cache is keyed on local disk facts, not on the replicated
  manifest.** The cache reused a recorded hash when size and mtime matched what
  the *manifest* held — but the manifest crosses the wire, so a local caching
  decision was made from a value another machine chose. Two contending entries
  of equal size could collide on size plus mtime, and the cache then reported
  content the file did not hold. That was the permanent three-node divergence.

  A separate, never-transmitted `scan_cache` now records what this machine
  observed on its own disk. It is deliberately not merged into the `observed`
  receipt: that receipt decides whether a missing path becomes a tombstone, and
  a tombstone crosses the wire, so loading a performance concern onto it would
  repeat the original mistake mirrored. The cache is filled only from real disk
  reads, never from a requested value, so a filesystem that truncates cannot
  make it miss forever. Absent in an older state file, where it warms on the
  first scan.

- **`FileMeta.mtime` is documented as informational and is never applied.**
  Fabric records a modification time, sends it, and does not write it to a
  materialized file, because git does not track mtime. The field is retained
  rather than removed: it has no serde default, so removing it would break
  parsing on any peer that has not upgraded.

- **`fabric status` reports per-path probe latency.** The daemon had measured a
  round trip and a path class on every liveness probe since the connection
  telemetry landed, and there was no way to read it but to parse
  `telemetry.json` by hand — the exact grepping those counters exist to end.

  A new `paths` block reports, per peer and per path, the share of probes, the
  sample count, and the exact mean and maximum. The busiest path is listed
  first, because which path a peer spends its time on is usually the finding.
  On a real mesh this makes the roaming signature legible at a glance: a peer
  with a stable address holds a direct path 99% of the time, while a peer behind
  a moving address sits on the relay 78% of the time and its direct path is no
  better on average and more than twice as bad at the tail.

  A peer is listed on probe evidence alone, so a healthy peer that has never
  dropped a session still shows its paths — unlike the `sessions` block, which
  is keyed off losses.

  `mean` and `max` are exact rather than bucketed, and percentiles are
  deliberately not reported: the latency buckets double in width, so around
  50–200ms two genuinely different paths fall in the same bucket and print
  identical percentiles, hiding the difference the table exists to show.

  This reports facts and reaches no verdict. Nothing labels a path degraded and
  nothing changes routing.

- **Fabric deletes its own old logs.** The daemon wrote one validation log per
  day and never removed any of them, so the directory grew without limit for as
  long as the daemon ran. One machine had accumulated **2.4 GB across 20 daily
  files**, with a single noisy day reaching 587 MB. Nothing in the tree
  implemented retention, pruning, or a maximum age.

  The most recent **45** daily logs are now kept and older ones are deleted.
  Forty-five is derived from the job rather than rounded: retention has to
  outlast a month away from the machine so a fault in the first week is still
  readable on return, plus margin for the gap before anyone looks. At the
  observed 8.8–10.3 MB per day that is roughly 420 MB.

  `FABRIC_LOG_RETENTION_DAYS` overrides the count; `0` disables deletion and
  restores the old unbounded behaviour. An unparseable value falls back to the
  default rather than to unbounded, because failing open on an unattended
  machine is the worst outcome. The resolved value is recorded in the
  `diagnostic_logging_init` line.

  This bounds the file count, which is what stops indefinite growth. It does not
  bound bytes — a single day has reached 587 MB — and capping that is a question
  about log volume, not retention. Logs written before this bound are not
  reclaimed; that is an operator's call.

### Fixed documentation

- **Two README statements that decided operator actions were false.** The
  policies section said catalog "never deletes on a peer" and that "a local
  deletion is restored"; catalog has propagated deletes since August 2026 and
  `tests/sync_slice.rs` pins it. Two operational choices on 2026-08-29 were made
  the wrong way on the strength of that sentence. The service section said "the
  default memory ceiling is 1 GiB"; no ceiling is set unless `--memory-max-mb`
  is passed, and `src/service.rs` pins that too. Both now say what the code
  does, and say what they used to say so a reader with the old sentence in
  their head knows it changed. Finding 15 of the 2026-08-29 review.

- **The detached replay buffer is capped, and the retention docs said it was
  not.** They claimed the buffer had no cap of its own, that a runaway remote
  shell would retain roughly 17 MB across a 15 minute window, and that the
  detached TTL was therefore the only backstop against such a process. Measured
  directly, all three were wrong.

  A detached session is never acknowledged, so its reader waits for buffer space
  that never frees and stops at `MAX_BUFFERED_BYTES`, currently 4 MiB. A runaway
  producer pins at exactly 4 MiB and stays there while the remote process blocks
  writing to its own PTY. Retention is bounded per session no matter how long
  the window is, and in aggregate by the server session cap — 256 MiB at the
  default of 64 sessions.

  Backpressure is the backstop against a runaway remote process; the TTL bounds
  how long a session lives, not how much it holds. This matters beyond accuracy:
  the "no cap" claim was the stated reason not to raise the detached TTL
  further, and that reason does not hold. A test now pins the bound so the claim
  cannot drift back.

### Added

- **Legacy peer permissions can become explicit without narrowing access now.**
  `fabric peers make-explicit` reads the running daemon's live exposures, adds
  the five built-in service names, and writes that list to every peer whose
  `allow` field is absent. Persisted and ephemeral exposures are both included.
  Existing explicit lists stay unchanged. Access available now stays available;
  a service exposed later becomes opt-in instead of being granted silently.

- **A new unreachable exposure says so at creation time.** After `fabric expose`
  succeeds, it warns when every trusted peer's explicit list denies the service.
  The one-line warning names the peers that need the service added. The warning
  does not refuse the exposure.

- **Durable connection telemetry.** `fabric status` now reports, per peer, how
  many times a session lost its transport, how many came back, how many gave
  up, and how long the reconnect took. The counters persist in
  `<state>/telemetry.json`, so they survive a daemon restart.

  Fabric already logged a line for each loss, reconnect attempt, and resume. A
  line reconstructs one incident by hand and cannot answer whether resumption
  works in daily use, because the answer needed a grep over megabytes of log
  and died at the next rotation. Three things the log could not report are now
  recorded: a durable count, the measured total reconnect time (the log carried
  the backoff delay before the next attempt, which is a different number), and
  the path in use beside each loss and each resume. "It came back" and "it came
  back on the relay" are different outcomes.

  The liveness probe also keeps what it measures. It computed a round trip time
  and a path on every probe and discarded both, so comparing the direct path
  against the relay meant parsing days of log text. Latency is now summarized
  per peer and per path in bounded histogram buckets, which keeps the cost flat
  on a daemon that runs for weeks. Nothing reads these numbers to make a
  routing decision yet; this change only stops throwing them away.

- **Production sync scan ledger.** `fabric sync ls` and its JSON form now expose
  per-entry monotonic full-scan, exact-noop inbound, and guarded-inbound
  transaction counters. Operators can prove scan-free convergence and the
  expected guarded scan transaction without relying on test-only instrumentation
  or CPU inference.

- **Truthful sync status.** `fabric sync ls` now reports logical Present,
  Tombstone, and observed-on-disk counts plus explicit missing, unexpected, and
  content-mismatch drift. `fabric sync ls --json` provides the same state as a
  stable machine-readable array.

- **Safe st2 catalog sync recipe.** The README now provides a copy-pasteable,
  two-entry positive allow-list for declarations and bus data, with explicit
  machine-local PTY/runtime exclusions and an ordinary resource/message
  propagation check. A provisioning test executes and locks the documented CLI
  and resulting policy/include semantics.

- **Deterministic CI stability gate.** Pull requests and main pushes now run
  library/model tests, explicit two- and three-machine temp-folder simulation,
  provisioning, real-iroh sync/restart, and the serial local multi-node daemon
  slice under bounded timeouts.

- **`fabric exec <peer> -- <cmd...>`** — non-interactive remote command
  execution: run a command on a trusted peer with no tty, stream its stdout and
  stderr back on separate streams, and exit with the remote command's exit code.
  The scriptable counterpart to `shell`. Security mirrors `shell` and is
  **default-deny per machine** (`allow_exec`, opt-in via `--allow-exec` on
  `up`/`daemon`/`service install`), under a separate ALPN (`fabric/exec/0`) so
  allowing exec never implies shell. `fabric status` now reports `exec allowed`.

### Removed

- **The fixed 300 MiB endpoint-recycle limit.** The daemon used to recycle its
  iroh endpoint whenever RSS crossed 300 MiB. The memory was never in the
  endpoint, so recycling did not reclaim it and simply ran again: one machine
  logged 1,206 threshold crossings, 645 recycles, and 599 that its own follow-up
  sample recorded as ineffective. Every recycle tore down live shell and tunnel
  sessions, which is how a held remote shell died mid-session. RSS is now
  observed and reported on each new peak, and nothing interrupts the daemon for
  memory alone. A fixed number also cannot know a healthy working set for a
  given network size; if memory grows, that is now an operator's call.

- **The default memory ceiling in generated services.** `--memory-max-mb` is
  unset by default, so a generated systemd unit carries no `MemoryMax` and a
  generated launchd plist carries no resident-set limits. On Linux `MemoryMax`
  is a hard kill and on macOS `ResidentSetSize` biases the kernel to reclaim
  from Fabric first, and neither was justified while a healthy working set is
  unmeasured. An operator who has measured one can still declare it.

### Changed

- **Detached shells are retained for 15 minutes, not 60 seconds.** A held remote
  shell now survives a closed lid over lunch; previously it did not survive a
  coffee break. The number comes from measured cost rather than taste: an idle
  detached session buffers 0 bytes across a full window, so holding one costs a
  session struct and a PTY process and nothing that grows with time. A session
  still producing output is bounded too: the replay buffer stops at 4 MiB, so
  retention does not grow with the length of the window. Past the window the
  behaviour is unchanged and already proven: the client reports `remote shell
  could not resume`, names the expired session, and exits non-zero.

- **Release archives have an enforced one-file contract.** Each platform tarball
  must contain exactly one member named literal `fabric` (not `./fabric`), and
  the lockout-safe upgrade runbook verifies that shape before extraction.

- **Sync publishers must stage outside the synced folder.** The file-sync
  documentation now makes explicit that every watcher-visible path matched by
  an entry is a durable logical key. Temporary, backup, and partial files must
  be assembled outside the configured sync folder before only canonical final
  paths are moved into place; a matching sibling temp name under `catalog`
  policy is propagated and restored like any other catalog key.

- **Equal-version sync conflicts preserve updates.** Higher logical versions
  still always win, but at the same version a Present update now wins over a
  Tombstone delete before deterministic author/content-hash tie-breaking. A
  later delete advances its logical version and wins normally.

- **Roaming self-heal.** The daemon now actively echo-probes each trusted peer
  (`FABRIC_PEER_HEALTH_SECS`, default 20s) and, on repeated failures, drives
  recovery (drop tunnels → iroh `network_change()` re-discovery → endpoint
  recycle) — so a peer that roams to a new network no longer stays unreachable
  both ways until a manual daemon restart. Previously the daemon only reacted to
  *local* network changes and never checked per-peer reachability. Each probe
  also emits latency + direct/relay telemetry.

### Fixed

- **Repeated short tunnel drops no longer retain a 15-second retry delay.** A
  valid tunnel Hello resets the old outage backoff. Five 200-millisecond drops
  now resume within a fresh three-step retry budget after the path heals.
  Temporary tunnel blocks remain retryable, while ACL denials remain permanent.

- **A delete now requires affirmative absence.** A scan distinguishes a present
  file, a path absent from a completely read parent directory, and a path whose
  state is unknown. Only the second state becomes a tombstone. An unreadable
  file or directory no longer stops the whole entry, and skipping it cannot
  turn it into a delete.

  A file over 512 MiB is present but not syncable. Fabric does not read or hash
  it, does not overwrite it during materialization, and reports its path under
  `scan_issues`. If the file is later deleted, a complete parent scan still
  proves that delete and propagates it normally.

  `fabric doctor` also distinguishes a missing remote sync entry and residual
  size-limit errors from an unreachable peer. These states need a configuration
  or file change; waiting for the network cannot fix them.

- **Include globs are now a receive-side boundary, not only a scan-side one.** A
  node adopted every winning entry a peer sent, whatever its own include said, so
  a host with a broad include (or a mistaken `["**"]`) had its machine-local
  files taken into every peer's manifest and relayed onward across the mesh. The
  README always said includes were the boundary; the code enforced it only on the
  scan. `adopt_from_peer` now refuses a path outside the node's include, so it
  never enters the manifest and never crosses the wire to a third peer. This is
  the receive half of the same defect whose delete half was finding 2. Finding 8
  of the 2026-08-29 review.

  Kept distinct from loading a node's OWN durable state, which still adopts every
  path it already held even outside a narrowed include, because that record is
  the node's, not a peer's. **Behaviour change:** two peers whose includes differ
  no longer converge on the excluded paths (which is the point), and widening a
  receiver's include now re-adopts the newly-included paths from a peer on the
  next reconcile rather than materialising them from a manifest it had already
  taken.

- **`send-file` streams instead of holding the whole file in memory on both
  daemons.** The sender read the file whole with `std::fs::read` and the receiver
  allocated `header.len` bytes up front, so a 1.5 GiB transfer cost about 1.5 GiB
  of resident memory on each daemon at once; on a host with a memory ceiling that
  is enough to kill the daemon mid-transfer. Both sides now stream the body in
  bounded chunks (`tokio::io::copy`), and the receiver writes straight to its
  temp file, so neither allocates against the file size. The wire format is
  unchanged, so a streaming build and an old build interoperate. A transfer that
  ends short of its declared length is refused and leaves no file. Finding 7 of
  the 2026-08-29 review.

- **`fabric doctor` reports whether the service is ENABLED, not just that its
  unit file exists.** It read `service_installed` from the unit file's presence,
  so a service disabled during an incident with its unit left in place said
  "installed and managed by the OS" — and after a reboot no daemon started. The
  CA trust check was fixed for exactly this mistake; the service check was not.
  Doctor now queries the manager (`systemctl --user is-enabled`, or whether
  launchd has the label loaded) and reads three states: enabled, present but not
  enabled (a problem, because a reboot leaves no daemon), and not installed.
  Finding 10 of the 2026-08-29 review.

- **`fabric doctor` on a non-default home asks the right daemon which build a
  peer runs.** Doctor shells out to `fabric exec <peer> -- fabric --version`
  without `--home`, so `fabric --home X doctor` asked the DEFAULT daemon about a
  peer only the X daemon knows and reported `unknown peer`. The flag a person
  typed does not reach the child unless doctor carries it. It now passes
  `--home` before the `exec` subcommand. Finding 12 of the 2026-08-29 review.

- **A lost network monitor no longer shuts the daemon down.** `serve()` runs
  every background loop in one `select!` and ends when the first returns, so a
  loop returning `Ok` shuts the daemon down. When the OS interface watcher
  disconnected (a netlink or route-socket error, or a sleep/wake), the roaming
  rehome loop printed "network monitor stopped; roaming rehome disabled" and
  returned `Ok` — so the daemon exited with code 0, which neither supervisor
  restarts (launchd `KeepAlive.SuccessfulExit=false`, systemd
  `Restart=on-failure`). The daemon stayed down until a person noticed, and the
  one log line blamed roaming rather than the exit. The loop now parks until the
  daemon is cancelled, exactly like the monitor-unavailable-at-startup branch
  already did; only roaming rehome is lost, and shell, exec and sync keep
  serving. Finding 9 of the 2026-08-29 review.

- **`fabric update` no longer rolls a good build back when systemd fires the
  restart late.** The update schedules the restart at +3s and a verifier at +12s
  that waits 45s for the new version and reinstalls the previous binary if it
  does not appear. The verifier's timer set `AccuracySec=1s`; the restart's did
  not, so systemd (default `AccuracySec=1min`) could batch the restart up to a
  minute late while the verifier fired on time, saw the old daemon for its whole
  window, and rolled the update back — cleanly, with the only record in the
  journal. The restart timer now sets `AccuracySec=1s` too, so the two delays
  keep the order they encode. Finding 6 of the 2026-08-29 review. Linux only.

- **The tombstone sweep no longer forgets a tombstone in the pass it arrived.**
  The sweep stamps a tombstone the first time it is seen expired and demands an
  ack from every peer after that stamp. Stamps are whole seconds, and one pass
  can reconcile a peer, adopt an already-expired tombstone, and sweep inside the
  same second, so `acked >= held_since` read `T >= T` as proof and forgot the
  tombstone before that peer was sent it. The peer still held the file, handed
  it back on the next pass, and the cycle repeated silently. The gate is now
  strictly later, which with whole seconds means a later pass. Finding 5 of the
  2026-08-29 review. The sweep is opt-in (`FABRIC_TOMBSTONE_SWEEP_DAYS`) and off
  on the fleet, so nothing live was affected.

- **A sync entry that names a peer not in `peers.toml` says so instead of
  reporting healthy.** A selector that matched no peer was dropped without a
  record. The engine looped over the peers that did resolve, recorded nothing for
  the one that did not, and `fabric sync ls` and `fabric doctor` both called the
  entry clean and syncing with every peer while two machines silently stopped
  converging. A typo in `syncs.toml` did it from day one; renaming a peer with
  `fabric add` did it the moment after. Finding 3 of the 2026-08-29 review.

  The entry now reports the selector under `stopped=` with the reason `unknown`,
  next to `denied` and `unreachable`, and doctor names the file to fix. A
  wildcard entry on a machine that trusts no peer reports `*:unknown`. A stopped
  state also no longer outlives its peer: a peer removed from `peers.toml` drops
  out of `stopped=` on the next pass instead of keeping its last verdict.
- **The daemon no longer holds every version of every synced file in memory
  until restart.** The content store only grew: `local_write` and the wire
  receive path inserted, and nothing removed. Every superseded version of every
  file in every entry stayed resident. Finding 4 of the 2026-08-29 review, and a
  sufficient cause for the 2.52 GB resident size recorded on Silber on 19 August.

  The store is now bounded by the manifest: a blob stays while some Present
  entry names its hash and goes when none does, after a local write, a local
  delete, an adopt, or a reconcile. A peer can only ask for a hash it adopted
  from this manifest, so nothing a peer can request is dropped.

  Measured on two release-build daemons on one machine, one 5 MB file rewritten
  40 times over 82 s: before, the writer grew by 206 MB and its peer by 203 MB;
  after, by 25 MB and 15 MB, and both report `content_bytes=5000005`, the live
  file. `fabric sync ls` now prints `content_bytes`, which is the number that
  would have said so on 19 August.

- **A dial to a peer that cannot be reached no longer holds its permit after the
  consumer leaves.** Every local connection to a dial socket, and every `shell`
  and `exec`, holds one of 32 dial permits for the life of its session. A session
  whose peer never answered had no remote output, so the only thing that could
  discover its consumer had gone (a failed local write, issue 51) never fired. It
  retried for ever with the permit held. Thirty-two such connections, which a
  roaming peer that is asleep produces for free, made every new `shell`, `exec`
  and dial on the machine wait with no error while `status` and `ping` stayed
  green. Only a restart cleared it. Finding 1 of the 2026-08-29 review.

  A session whose local input has ended now asks the kernel once a second whether
  anybody still holds the other end, with a zero-length write. A consumer that
  closed both directions fails it and the session ends at once, in whichever
  state it was in: waiting to retry, mid-connect, or attached to a silent peer.
  A consumer that half-closed and is waiting for output passes it and is served.

  `fabric status` now prints `dial handlers N/32 in use`, which is the number
  that would have said what was wrong.

- **A path outside an entry's include is no longer deleted on every peer when
  it is deleted locally.** Narrowing an include was already safe on its own:
  the scan refuses to treat a path that left the include as a delete. But
  materialization still recorded every manifest path it found on disk as
  observed, include or not, so the excluded path stayed protected. The pass
  after an operator deleted it locally saw "protected and not on disk", wrote a
  tombstone at a higher version, and every peer deleted its copy. This is
  finding 2 of the 2026-08-29 review, and the same class as the 2026-08-25
  loss, one function later. `materialize_tracked` now consults the include
  globs the way the scan does.

  **Behaviour change:** a machine no longer WRITES a path its own include does
  not select, and it no longer applies a peer's tombstone for one. Such a path
  is left exactly as the operator left it. It stays in the manifest, so
  widening the include later materializes it on the next pass without a resend.
  Previously the receiver wrote the file and never scanned it, which is exactly
  the protected-but-excluded shape above.

- **Mixed-version shells no longer fail on the first frame.** `fabric/shell/0`
  is a wire contract with every released Fabric and carries one-shot raw
  framing only; the resumable shell moved to its own `fabric/shell/1` ALPN.
  Previously the resumable path reused the legacy ALPN, so an older peer met
  tunnel framing it could not parse, rejected the first `SERVER_OUTPUT` frame as
  an unknown tunnel frame, and reconnected forever. A client that cannot
  negotiate shell/1 now falls back to shell/0, so upgrading one host at a time
  works. Signal exits restore the caller's exact terminal settings.

- **One absent peer no longer recycles the shared endpoint.** Peer health probes
  the whole round before recovering anything, so a roaming peer that is simply
  away neither drops everyone's tunnels nor escalates to a global endpoint
  recycle while other peers are answering.

- **Endpoint recycles no longer kill live sessions.** A recycle is refused while
  shell or tunnel sessions are attached; a session dying mid-command is worse
  than whatever prompted the recycle. Detached-but-resumable sessions do not
  pin the endpoint.

- **Shell recovery is visible in the daemon log.** Connection loss, reconnect
  attempt with its delay, and resume or failure are logged with the peer and
  endpoint generation. The log previously showed the drop and nothing about
  getting the session back.
- **Converged periodic syncs no longer hash the full tree twice.** An inbound
  peer whose manifest exactly matches the local node can bypass both folder
  scans when the local content store is complete. Differing manifests, missing
  content, and every potentially mutating reconcile retain the guarded
  pre-merge and completion scans that protect local archive/delete intent.


- **Inherited catalog tombstones no longer leave folders divergent.** Under
  catalog policy, any surviving physical copy now advances an inherited
  Tombstone to a higher Present version and supplies its content over the wire,
  converging every materialized folder and persisted restart. Bus policy keeps
  its existing delete semantics and removes the same stale bytes.

- **macOS startup no longer blocks forever in CoreWLAN.** The optional Wi-Fi
  transmit-rate lookup is now bounded to 250 ms and cached per interface; a
  wedged synchronous CoreWLAN XPC request falls back to unknown speed without
  preventing iroh or Fabric's control socket from binding. Repeated network
  refreshes cannot spawn duplicate blocked workers. The launchd install path
  also uses non-destructive `kickstart`, avoiding a readiness race that killed
  the PID created moments earlier by `bootstrap`.

- **Linux file reads could self-trigger an unbounded sync storm.** Linux
  inotify reports file opens as access events; treating every watcher event as
  a mutation meant Fabric's own scans could wake all peers again. Watchers now
  forward only create/modify/remove events, continuous write streams are
  coalesced into bounded two-second windows, queued inbound no-op sessions
  reuse a durable pre-merge scan, and sync-accept path snapshots are sampled
  1-in-128 in the default validation log. A Linux read-vs-write regression and
  a three-node, 2,000-file continuous-mutation stress gate cover the recovery.

- **Repeated `fabric exec`/`shell` calls exhausted daemon file descriptors.**
  Replacing a command's deterministic local dial socket removed its pathname
  but left the old listener task and file descriptor alive. The daemon now owns,
  cancels, and joins each replaced accept loop while allowing already-accepted
  sessions to finish, and deterministically closes all dial listeners on
  shutdown.

- **Bus archive/delete intent lost to an inbound reconcile.** An atomic local
  rename could remove an inbox file while Fabric's filesystem watcher was still
  inside its 150 ms debounce window; an inbound sync in that gap could
  materialize the stale Present entry and undo the archive. Inbound sessions now
  carry an explicit last-observed disk receipt across reconciliation, durably
  record local changes before merging, and guard final materialization. This
  preserves deletes in both pre-merge and post-scan races while still accepting
  genuinely new remote files, without deadlocking simultaneous peer syncs. The
  manifest and observed-disk receipt are committed together in one authoritative
  state file, so the same guarantee survives daemon restarts and time spent with
  a sync entry disabled.

- **Peer-config split when the daemon runs with `--home <default-root>`.** The
  managed service always launches the daemon as
  `--home ~/.local/share/fabric`, which made it read `peers.toml` from under that
  `--home` while the interactive CLI reads `~/.config/fabric/peers.toml`. A
  default-home `fabric add` then migrated peers to the config dir and removed the
  in-`--home` copy, silently leaving the daemon with **zero peers**: `ping`/
  `status`/`shell` still worked (in-memory allow-list) but every **dial** failed
  with `unknown peer` — taking down consumers like `st sync` (endless
  `fabric pull failed … re-dialing`) with a real lockout risk on peers with no
  fallback access. An explicit `--home`/`FABRIC_HOME` equal to the default state
  root now resolves peers/config exactly like the no-argument default; a
  genuinely different `--home` keeps the isolated config-under-root layout. See
  the new [Troubleshooting](README.md#troubleshooting) note.

## [0.2.0] - 2026-07-20

### Added — `fabric sync` (generic file-sync primitive)

fabric now owns file sync: a declarative, daemon-managed, fs-watched primitive
that keeps a folder converged with trusted peers over iroh.

- **`syncs.toml`** — a new authoritative, hand-editable, reload-able config file
  (sibling of `peers.toml`). Each `[[sync]]` entry is
  `{name, folder, peers, policy, include?}`. `name` is the shared logical key
  (same name on two machines = the same sync); `peers = "*"` follows the
  `peers.toml` allow-list; `policy` is a preset (`catalog` or `bus`); optional
  `include` globs scope which files sync.
- **Reconciliation core** — versioned per-file state with newer-wins (Lamport
  version + deterministic author tie-break, never a wall clock). Merge is a
  semilattice join (commutative/associative/idempotent), so convergence and
  echo/loop-freedom are structural, proven by property tests.
- **Policies** — `catalog` = union + newer-wins + never-delete-on-peer + no-sweep
  (a local delete is restored; decommission is expressed as an edit). `bus` =
  union + newer-wins + tombstone deletes (modelled; sweep TTL not yet wired).
- **Swappable transport** — the sync semantics sit above a transport seam. The
  on-wire backend runs over a reserved `fabric/sync/1` ALPN on fabric's own
  connections and is verified to reach the exact same state as the pure
  reference reconcile.
- **Engine** — the daemon watches each folder (near-instant, not a poll), scans
  changes, reconciles with peers, and materializes results to disk. Manifests are
  persisted per entry so logical versions survive daemon restarts.
- **CLI** — `fabric sync add/ls/rm/reload`. `add` is a convenience writer over
  `syncs.toml` (like `fabric add` → `peers.toml`); `reload` applies the file to a
  running daemon (like `reload-peers`).
- **Control ops** — `SyncReload` and `SyncStatus`.

## [0.1.x] — iroh socket-facade foundation

The shipped foundation before `fabric sync`, released as tags `v0.1.0`–`v0.1.7`.
Reconstructed from git history; not a per-patch breakdown.

### Added
- Local daemon that hides iroh behind local Unix sockets: `expose`
  (`--socket`/`--tcp`/`--exec`) and `dial` with resumable, offset+ACK framed
  tunnels that survive a transport reconnect without reopening the local service.
- Mutual allow-list trust via `peers.toml` (authorized-keys file), enforced at
  the iroh `after_handshake` hook; `add`/`remove`/`peers`/`reload-peers`.
- Built-in ACL-gated `ping` echo and reachability in `status` (direct/relay/mixed
  transport path, round-trip latency); build version in `--version` and `status`.
- Opt-in remote `shell` for trusted peers (`--allow-shell`, off by default).
- Lockout-safe `restart` via a detached helper (safe to run over `fabric shell`).
- `fabric service install` — managed systemd/launchd user service with
  restart-on-failure and a configurable memory backstop.
- Provision-and-go: pre-generate an identity (`key gen`) and deploy a complete
  `peers.toml`; `reload-peers` applies it without an interactive step.
- Release installer (`install.sh`) with prebuilt-binary and `--from-source`
  paths.

### Changed
- Renamed/transferred to the `compoundingtech` GitHub org
  (`github.com/compoundingtech/fabric`; launchd label `com.compoundingtech.fabric`).
- Roaming reliability + Hetzner RSS mitigation: in-process iroh endpoint recycle,
  health poller, network-change debounce, bounded server tunnel sessions, and an
  RSS-triggered recycle with a raised (1 GiB) managed-service memory ceiling.
