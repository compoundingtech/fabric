# now — fabric working state

The living handoff for whoever owns fabric next (there was none before; keep this
current). This records what is DONE, what is IN FLIGHT, and what is NEXT — the
things the repo history alone does not carry.

_Last updated: 2026-09-04 by Silber.fabric-codex. Main's last code commit is
deployed as `0.2.1+48208e4` on Silber and hetz._

## Current release — 2026-09-04

PR #152 removed the final full-state clones from a sync pass. The engine still
clones the required pre-peer manifest and baseline. It compares the final
manifest and observed map by reference under their existing locks. It merged as
`48208e4`.

The red contract counted `Manifest::clone` calls during one clean sync pass. It
failed before the fix with two calls, where one call was required. It passes
after the fix with only the required baseline clone. The manual test counter is
available only in test builds.

The comparison does not increase the lock window. A temporary release-mode
benchmark used 29,337 realistic entries. A full clone took 1,207,037 ns per
operation. Equality took 310,312 ns per operation, or 3.89 times less time.
The temporary benchmark was removed before the merge.

The final library suite passed 456 active tests, with three measurements
ignored. The serial folder-sync suite passed 18 tests. Exact-main test run
`33908307690` passed on macOS and Linux. Exact-main Nix run `33908307736`
passed.

Release `v0.2.1+48208e4` passed all four jobs in run `33909616308`. The archive
SHA-256 values are `fefb1bb8ab468b2d3c11a02f9e2f1d2a6963ee6a28ce5c82cfa21613ee4cfdc9`
for Apple arm64, `4371fa60c8d25cf87ecc3832c000e4bbf32d95329e4ca4682503e137db9dedff`
for Linux arm64, and `628c4a1dbe9af18372c7a0d24d97f80dad99767115be85d7233db3ee8eed98d7`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+48208e4` before
deployment.

The rollout updated hetz first and Silber second. The mixed-version fleet passed
two-way ping and exec through direct paths. Both final binaries and daemons
report `0.2.1+48208e4`. Doctor passes on both hosts. Both native services are
active and enabled. A two-way send-file round trip matched SHA-256
`5ac13b6c2f9ebb908bd82784d8cd9fd62edfcb2d9f2453e614c556280a53009d`.

The final snapshots match at 29,369 present bus paths and 17,809 tombstones.
Their bus digest is
`9ac561f8c2bed946367146a8436e0069f31a62d684a332de9942cea6e28b2b05`.
Both declaration entries have 119 present paths, 41 tombstones, and digest
`a2a70ac8a21be7a49c9654f055a9440a5e165a857217fea39873f8b4247b5d51`.
Both entries are clean, with no missing, unexpected, or mismatched paths.

Five live hetz windows each measured 30 seconds. They used malloc, calloc,
realloc, and posix_memalign probes against PID 940540. The windows requested
446,536,561, 387,734,904, 784,988,157, 449,767,374, and 1,621,589,144 bytes.
Their rates were 49.90, 43.33, 87.73, 50.27, and 181.23 GiB/hour.

The new median is 50.27 GiB/hour. The earlier five-window median was 56.17
GiB/hour, so the median fell 10.51 percent. The new aggregate is 3,690,616,140
bytes during 150 sampled seconds, or 82.49 GiB/hour. The earlier aggregate was
105.29 GiB/hour, so the aggregate fell 21.65 percent. The busy fifth window
confirms that more whole-tree allocation remains.

The five bus windows added 1, 2, 3, 2, and 5 passes. They added 4, 4, 8, 4,
and 16 scans. Their guarded transaction counts increased by 1, 0, 1, 0, and 3.
The fourth window also included one declaration pass and two declaration scans.

A separate clean 10-second clone window added one bus pass, three scans, and
one inbound no-op transaction. It added no guarded transaction. The full
`Manifest::clone` scope requested 13,504,896 bytes. This is one full bus
manifest clone. The former final full clone did not occur.

The updater rollback binaries both reported `0.2.1+863dc5b`. The exact files
were `/Users/myobie/.local/bin/fabric.rollback-1788549527` and
`/home/myobie/.local/bin/fabric.rollback-1788549476`. Both rollback files,
release files, transfer files, and measurement files were permanently deleted.
No bpftrace or perf process remains. The old `fabric_alloc_863` probes and all
known `863dc5b` trace paths are absent.

PR #146 releases allocator-retained Linux pages without recycling an endpoint.
Each new 128 MiB RSS growth step calls `malloc_trim(0)` on glibc Linux. Other
platforms keep the RSS report without attempting the trim. The action cannot
free live allocations and does not touch an endpoint or session. PR #146 merged
as `aab58e5`.

The live fault was not bounded near 1 GiB. One hetz process grew from
445,186,048 bytes to 1,499,697,152 bytes during 17 hours and 26 minutes. The
next process reached 977,544 kB after 795 seconds and 1,120,140 kB after 2,824
seconds.

Twenty live anonymous mappings were at least 60 MiB. Thirteen held at least 60
MiB resident. Those large mappings contained 845,748 kB. The 16-worker daemon
used the glibc per-thread arena shape.

Existing live records supplied the direct control. Endpoint close changed RSS
from 994,246,656 bytes to 994,344,960 bytes. The following allocator trim
reduced it to 238,071,808 bytes in 65 milliseconds. A second trim reduced
1,094,070,272 bytes to 257,318,912 bytes in 77 milliseconds. Close returned
nothing. The two trims returned 721.2 MiB and 798.0 MiB.

Two test proxies did not reproduce the live retention. A single-thread Linux
probe stayed between 30 MiB and 48 MiB after 14.0 GB of rereads. A 16-thread
proxy grew from 34 MiB to 235 MiB, then trim returned only 10 MiB. Both proxies
were removed after their negative results were recorded. The live control is
the evidence for the fix.

The focused red contract recorded zero trim requests. The fixed contract
records exactly one request and proves that the endpoint generation stays
unchanged. It passed 20 of 20 runs. The complete library suite passed 443
active tests, with three measurements ignored.

Final pull-request test run `33870695087` passed on macOS and Linux. Final
pull-request Nix run `33870695210` passed. Exact-main test run `33871785503`
passed on macOS and Linux. Exact-main Nix run `33871785448` passed.

Release `v0.2.1+aab58e5` passed all four release jobs in run `33872935201`.
The release contains three archives and three checksum files. Every archive
matched its recorded checksum and contained only `fabric`. The Apple and Linux
x86_64 binaries both reported `0.2.1+aab58e5` before deployment.

The archive SHA-256 values are `22653dd55fe8b11e34a38622326cfb40a6f20734106b8a0c3e307ff1e380f66e`
for Apple arm64, `8bb2c5debe6ea2c3ee5e06395701367fccf83369888504345f0bb40b9135c542`
for Linux arm64, and `f3e0c8222fa1343adf16cbe75af5a237962767ce8787266e0fb3392ae30258aa`
for Linux x86_64.

The rollout updated hetz first and Silber second. Mixed-version ping and exec
passed in both directions. Both binaries and daemons now report
`0.2.1+aab58e5`. Final two-way ping and exec passed through direct paths.
Doctor passes on both hosts.

The first production trim reduced hetz RSS from 432,263,168 bytes to
166,961,152 bytes in 21 milliseconds. The endpoint stayed at generation 8.
This matched the prediction made before deployment.

The production proof kept hetz PID 3992077 for 60.9 minutes. One-minute RSS
samples stayed between 307,820 kB and 570,820 kB and ended at 375,748 kB. At
the matching 12.9-minute point, the new daemon held 358,036 kB. The old daemon
held 977,544 kB. At the matching 47.9-minute point, the new daemon held
493,620 kB. The old daemon held 1,120,140 kB.

All 115 trim events during the window succeeded and had a follow-up RSS sample.
They returned 25.89 GiB cumulatively and took 2.269 seconds total. The average
trim took 19.7 milliseconds, with a 10-to-35-millisecond range.
Pre-trim RSS ranged from 284.0 MiB to 627.5 MiB. Post-trim RSS ranged from
153.0 MiB to 247.6 MiB.

The daemon allocated and freed at least 25.89 GiB during the 60.9-minute
window. The trim manages the retained-memory symptom. The allocation rate is
measured, but its necessity is not diagnosed.

The deployment produced the predicted sawtooth instead of the old smooth rise.
It also produced 115 warning records during 60.9 minutes. The trim calls used
approximately 0.062 percent of the wall-clock window. The warning
frequency is the next measured quietness cost; do not weaken the memory fix to
remove the notices.

PR #147 keeps every allocator trim and bounds routine success notices. The
first success reports immediately. Later successes form one aggregate summary
every 30 minutes. A failed trim, an unavailable trim, or a missing follow-up
RSS sample still reports immediately. PR #147 merged as `6f90ccb`.

The same 115-event pattern now produces at most three default warnings during
60.9 minutes. This is a 97.4 percent reduction. The summary keeps the success
count, observed RSS reduction, total trim time, and latest RSS values. The full
library suite passed 444 tests, with three measurements ignored. Pull-request
test run `33880401744` and Nix run `33880401558` passed. Exact-main test run
`33881547544` and Nix run `33881547475` passed.

Release `v0.2.1+6f90ccb` passed all four jobs in run `33881590090`. The archive
SHA-256 values are `8ee5ecc852120cb56dd626027b645aba44fc38270dd93c284396cd9ddd22c44f`
for Apple arm64, `fb21a7354351547ab4dbc63aaac29fc79990b57d66d358f90e916a3868fc3676`
for Linux arm64, and `055efbf2a7f0c7baf8b4e82b4fc3e1ad943ea9ffaf65790676ff2c3874dfafe5`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+6f90ccb` before
deployment.

The rollout updated hetz first and Silber second. Mixed-version and final
two-way ping and exec passed through direct paths. Two-way send-file hashes
matched. Doctor passed on both hosts. Both sync entries had matching digests,
clean drift, no scan issues, no stopped or away peer, and no delta fallback.

The first production trim on the new build reduced RSS from 462,651,392 bytes
to 168,275,968 bytes in 26 milliseconds. It wrote one success summary and kept
endpoint generation 9. The rollout rollback binaries contained
`0.2.1+aab58e5`. Both exact files were removed after verification.

Nathan asked what creates the 25.89 GiB/hour allocation rate. The current scan
does not reread unchanged file content. It reuses cached hashes when size and
mtime match. A 30-second live bpftrace window on hetz measured 508 MB requested
through malloc, calloc, realloc, and posix_memalign. This is 56.8 GiB/hour at
that measured workload.

A second 15-second live trace marked exact function boundaries. Ten folder
walks requested 437.5 MB, or 43.75 MB each. Ten post-walk scan phases requested
212.0 MB, or 21.20 MB each. Nine materialize phases requested 206.4 MB, or
22.93 MB each. The repeated work creates and clones path strings, path sets,
cache maps, observed maps, and manifest path vectors for approximately 29,000
present files and 17,700 tombstones.

This class of churn is not new. A repository measurement from 2026-08-19 found
that materialization reread 70,157,702 bytes every 0.51 seconds. A 90-second
test drove 12.5 GB of churn and left 2.2 GB resident and dirty. PR #59 removed
the content reread. The current allocation rate is lower, but the full-tree
metadata allocation remained.

PR #148 removed path copies that a complete scan kept only to prove absence.
It retains directories, excluded regular-file blockers, and uninspectable paths.
These paths still make absence unknown and prevent unsafe descendant deletes.

The red 100-file contract retained 103 auxiliary path copies before the fix.
It retains zero after the fix. A negative control found an excluded regular-file
blocker that the first implementation missed. The corrected control failed on
the approved first head and passed on the final head.

PR #148 merged as `7bde48c`. Pull-request test run `33884605125` and Nix run
`33884605142` passed. Exact-main test run `33885893710` and Nix run
`33885893645` passed.

Release `v0.2.1+7bde48c` passed all four jobs in run `33886459496`. The archive
SHA-256 values are `c63038c65dc2b485e1107fdb428c272e384f4e9321c56b44ebcec1aac12bfe70`
for Apple arm64, `9f197d6425177b814c475304579f6fa0997ac19ac653c126d9de47e77f42e290`
for Linux arm64, and `97732f91129cd611c4666f439d03e0ef5cea90c06bc660ea200e2d34aad1f432`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+7bde48c` before
deployment.

The rollout updated hetz first and Silber second. Mixed-version and final
two-way ping and exec passed through direct paths. Two-way send-file hashes
matched. Doctor passed on both hosts. Both sync entries matched across hosts
with no drift, scan issue, stopped peer, away peer, or delta fallback.

The deployed 15-second trace measured 232,193,264 requested bytes across six
folder walks. This is 38.70 MB per walk, down from 43.75 MB. The change removed
5.05 MB per walk, or 11.5 percent. A second 20-second trace printed one full
bus walk at 38,698,949 bytes. This result confirms the average is not a
scan-mix artifact.

The same full pass requested 22,246,378 bytes after the walk and 25,871,580
bytes during materialization. The full pass still requests 86.82 MB. The next
work must remove repeated post-walk and materialization allocation without
weakening delete evidence or concurrent-edit guards.

Before the rollout, both old daemons had approximately 44 minutes of uptime.
Silber used 207,168 kB and hetz used 417,268 kB. The earlier 29-minute
convergence did not hold. The hetz process was 2.01 times larger at the matched
age, so the cross-host lead remains useful.

The rollout rollback binaries both reported `0.2.1+6f90ccb`. The exact files
were `/Users/myobie/.local/bin/fabric.rollback-1788534220` and
`/home/myobie/.local/bin/fabric.rollback-1788534158`. Both paths and all
release, preflight, and transfer files are absent after verification.

Two-way send-file hashes matched. The Silber-to-hetz hash was
`ee03b206037ca54f4bdb7d56badc929dfabbcf938a20bae75615f4c673336339`.
The hetz-to-Silber hash was
`3e096e8179b958e6d1d13e4eb46c6aaa6beb6e9c372761503936fa7c0b9a4996`.
All transfer and preflight files were deleted after verification.

Both sync entries have matching cross-host digests. The bus digest is
`2e97897f219816333079ce0d40a735fb9866b80438bd5c5d13641f02cf2b7fcd`.
The declarations digest is
`c653141c2291a854ab0c2a30feba733b69bdf0add812bf6b45915811f9fd0f3d`.
Both hosts report zero drift, missing, unexpected, mismatched, scan issues,
stopped peers, away peers, and delta fallbacks. Silber retains one cumulative
reconcile failure from the rollout window. A later successful pass cleared the
stopped state. Hetz reports zero reconcile failures.

PR #149 removed the eager owned snapshot of every present manifest path during
materialization. It borrows the manifest iteration and defers only node writes
and deletes until that borrow ends. It also avoids replacing an observed hash
with an identical clone.

The red 100-file contract counted 100 eager path copies before the fix and zero
after it. Two properties guard the deferral. A local edit and local delete both
still outrank remote state. Content from a deferred local edit can still
materialize a later path during the same pass.

PR #149 merged as `40d7aa6`. Pull-request test run `33889458426` and Nix run
`33889458423` passed. Exact-main test run `33890590443` and Nix run
`33890590425` passed. The final library suite passed 449 active tests, with
three measurements ignored. The serial folder-sync suite passed 18 tests.

Release `v0.2.1+40d7aa6` passed all four jobs in run `33892068141`. The archive
SHA-256 values are `36def117ff5f814609ad1e75b709408ae56908e13ae20a52a48ccbcdf2122256`
for Apple arm64, `35fa2a169e09037c6044d7095b016fd11355e2fb8371ad29e000ecb35c1dae39`
for Linux arm64, and `54835ea7e45a674fcd52a1fd29862f0b14e54a8bb10dd63ca74d21206f6524e6`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+40d7aa6` before
deployment.

The rollout updated hetz first and Silber second. Mixed-version and final
two-way ping and exec passed through direct paths. Doctor passed on both hosts.
Two-way send-file hashes matched. The Silber-to-hetz hash was
`d2c450b11ec2c0c87034569606e3d463e42b92f1cc01aa451fcf862be3bd362c`.
The hetz-to-Silber hash was
`c8f30d8d3113eacff05bd8ce2aabe0217edec1223761eed9c069be17fc33ad8d`.

Both sync entries matched across hosts with clean drift and no scan issue,
stopped peer, away peer, reconcile failure, or delta fallback. The bus digest
prefix was `192bbb283031`. The declarations digest prefix was `7bbb8aa56243`.

The deployed 20-second exact-symbol trace observed four converged
materializations at exactly 14,249,092 requested bytes each. The 25,871,580-byte
baseline fell by 11,622,488 bytes, or 44.9 percent. One changing call requested
12,701,676 bytes. The trace counted five calls and 69,698,044 bytes total.

The rollout rollback binaries both reported `0.2.1+7bde48c`. The exact files
were `/Users/myobie/.local/bin/fabric.rollback-1788537663` and
`/home/myobie/.local/bin/fabric.rollback-1788537618`. Both paths and all exact
release, preflight, and transfer paths are absent after verification. The exact
Trash cleanup directory was permanently deleted and is also absent.

PR #150 made Linux atomic rename events eligible for daemon-write
acknowledgement. A batch must pair a vanished `.fabric-tmp` path with its final
path. The final path must then match the committed daemon fingerprint. Missing
journal entries, unpaired renames, external atomic replacements, changed files,
removed files, and dropped event generations all fail open to a normal scan.

The modeled atomic-write contract failed at the acknowledgement assertion
before the fix and passed after it. A Linux-only test exercised the real kernel
watcher shape. An external atomic replacement with identical bytes remained
dirty because its inode and change time differed.

PR #150 merged as `71f3d06`. Pull-request test run `33894394190` and Nix run
`33894394182` passed. Exact-main test run `33895554477` and Nix run
`33895554472` passed. The local library suite passed 451 active tests, with
three measurements ignored. The serial folder-sync suite passed 18 tests.

Release `v0.2.1+71f3d06` passed all four jobs in run `33896872205`. The archive
SHA-256 values are `88ca6ff57ee021ce6171249a5c8ee15322ef4d36ca5bd2999b3bf17a6d9d6852`
for Apple arm64, `7541f83370bba3432411a33805483d89d7867a5e28a6dc98dd736587a5a98ab9`
for Linux arm64, and `7dc4464a4519c99b7797a8df95a6f9f73e7983c2a92915d41dc96057da1dfff5`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+71f3d06` before
deployment.

The rollout updated hetz first and Silber second. Mixed-version and final
two-way ping and exec passed through direct paths. Doctor passed on both hosts.
Two-way send-file hashes matched. The Silber-to-hetz hash was
`d2c450b11ec2c0c87034569606e3d463e42b92f1cc01aa451fcf862be3bd362c`.
The hetz-to-Silber hash was
`c8f30d8d3113eacff05bd8ce2aabe0217edec1223761eed9c069be17fc33ad8d`.

Both sync entries matched across hosts with clean drift and no scan issue,
stopped peer, away peer, reconcile failure, or delta fallback. The bus digest
prefix was `8414a8fb9fbe`. The declarations digest prefix was `3bc75951920a`.

The predeployment 30-second hetz window fully attributed 12 scans. Two guarded
inbound transactions took four required scans. Their two required forward
passes took four scans. Their two redundant self-watcher passes took four scans.
Every atomic final-write burst was under a Silber path. This proves that hetz
does more guarded work because it receives more changes from Silber.

The recorded prediction was 12 scans falling to 8 in a comparable window. The
first postdeployment window had one guarded inbound transaction, two passes,
and six scans. Four scans were predicted. The redundant pass remained, so the
prediction failed. A second window had no guarded inbound transaction and
attributed its two passes and four scans to real local hetz st2 writes.

The remaining cause is now named. `complete_inbound` commits the daemon-write
fingerprint. `note_inbound_adoption` then increments the same mutation
generation to preserve forward work. The self-write acknowledgement requires
the current generation to equal the watcher batch's last generation, so the
intentional forward generation invalidates the exact receipt. Separate forward
bookkeeping from watcher mutation generations. Preserve the periodic forward
backstop and fail open on every missing or mismatched receipt.

Immediately before the rollout, the old daemons had about 49 minutes of uptime.
Silber used 280,032 kB RSS and had 76 guarded inbound transactions. Hetz used
474,664 kB and had 127 guarded inbound transactions.

The rollout rollback binaries both reported `0.2.1+40d7aa6`. The exact files
were `/Users/myobie/.local/bin/fabric.rollback-1788540671` and
`/home/myobie/.local/bin/fabric.rollback-1788540631`. Both paths and all exact
release, preflight, and transfer paths were permanently deleted after
verification and are absent.

PR #151 separates forward work from watcher mutation receipts. An inbound
adoption advances a forward generation and wakes the entry loop. A successful
pass settles only the forward generation it observed at its start, so an
adoption that arrives during that pass remains pending. The 30-second tick
still schedules missed forward work. A held wake for work another trigger
already completed now costs no pass.

The ordering contract failed before the fix because the forward marker made an
exact daemon atomic-write receipt look stale. It passes after the fix. Separate
contracts cover an adoption during a pass, the periodic backstop, and a stale
held wake. The existing journal-overflow and dropped-generation contracts still
fail open to a normal scan.

PR #151 merged as `863dc5b`. Pull-request test run `33899754269` and Nix run
`33899754314` passed. Exact-main test run `33899950082` and Nix run
`33899950176` passed. The final local library suite passed 455 active tests,
with three measurements ignored. The serial folder-sync suite passed 18 tests.

Release `v0.2.1+863dc5b` passed all four jobs in run `33901173856`. The archive
SHA-256 values are `b590eb68b92b955865948d0d020c02d2eb9ed407d11256d8d34c168f487590b4`
for Apple arm64, `15ac41d336a09e6c409a4f9cb3d6e2a49f81e2025163b051fe89489f7565222d`
for Linux arm64, and `8bdc16fdf56175aed23a2723d2a40615db9d2f2ad291b6977bda5f7070b8db20`
for Linux x86_64. Each archive matched its sidecar and contained only
`fabric`. The Apple and Linux x86_64 binaries reported `0.2.1+863dc5b` before
deployment.

The independent predeployment baseline matched on all three readings. Both
Silber readings and the hetz binary reported `0.2.1+71f3d06`. The rollout then
updated hetz first and Silber second. Mixed-version and final two-way ping and
exec passed through direct paths. Doctor passed on both hosts. Both native
services are active and enabled. Two-way send-file hashes matched. The
Silber-to-hetz hash was
`d2c450b11ec2c0c87034569606e3d463e42b92f1cc01aa451fcf862be3bd362c`.
The hetz-to-Silber hash was
`c8f30d8d3113eacff05bd8ce2aabe0217edec1223761eed9c069be17fc33ad8d`.

The repeated final snapshots converged at 29,314 present bus paths and 17,791
tombstones on both hosts. Their bus digest prefix was `0412294a3827`. Both
declaration entries had 119 present paths, 41 tombstones, and digest prefix
`57d8522f710d`. Drift was clean, with no scan issue, stopped peer, away peer,
reconcile failure, or delta fallback.

The decisive watcher-ready 30-second hetz window started at 34 sync passes, 96
full scans, 26 inbound no-op transactions, and 14 guarded transactions. It
ended at 36 passes, 104 scans, 28 no-ops, and 16 guarded transactions. The
window saw exactly two daemon atomic renames, from `.fabric-tmp` paths to
`Silber/poc-server/status` and `Silber/vrs-study/status`.

Hetz was walking its entire tree because two agents on Silber updated their
status files.

Two guarded transactions therefore produced two forward passes and eight
scans. A receipt mismatch would have scheduled an extra watcher pass, so the
absence of either extra pass proves that suppression fired twice. The two
no-op inbound transactions added no scan. The recorded prediction was six
scans falling to four for one guarded transaction, or twelve falling to eight
for two. The measured two-transaction case matched.

An earlier 30-second window also measured one guarded transaction, one pass,
and four scans. A later window had no guarded transaction, three passes, six
scans, and one local hetz atomic-write burst. The latter cannot test the
prediction; it confirms only that every pass still cost two scans.

Five later live 30-second hetz windows used the same malloc, calloc, realloc,
and posix_memalign probes as the original allocation measurement. They
requested 466,272,149, 502,615,392, 1,589,763,800, 1,703,068,388, and
448,672,305 bytes. Their rates were 52.11, 56.17, 177.67, 190.33, and 50.14
GiB/hour. The median was 56.17 GiB/hour. The aggregate was 4,710,392,034 bytes
during 150 sampled seconds, or 105.29 GiB/hour.

The median is only 1.1 percent below the original 56.8 GiB/hour window. The
busy windows also show that the 75.20 MB full-pass estimate and scan frequency
do not explain all daemon allocation. The improvement is not enough. Even the
lowest deployed window remained above 50 GiB/hour, so Fabric does not yet meet
the low-memory and low-CPU goal.

Six matched host intervals covered 184 seconds without either daemon PID
changing. Silber added 23 passes, 60 scans, 14 inbound no-ops, and seven
guarded transactions. Its RSS stayed between 266,848 and 277,280 kB and ended
48 kB above its first sample. Hetz added 20 passes, 59 scans, 14 no-ops, and
nine guarded transactions. Its trimmed RSS moved between 174,160 and 302,840
kB and ended 284 kB below its first sample.

The proved suppression removes two scans per guarded transaction. Applying
that delta to the matched series gives 74 scans without the fix on Silber and
77 on hetz. The deployed build avoided 14 scans on Silber, or 18.9 percent,
and 18 on hetz, or 23.4 percent. Hetz avoided four more scans because it
received two more guarded transactions. This matches the predicted host
direction, while the short RSS series shows no durable slope on either host.

Allocator return-address probes then attributed the remaining rate. The probes
covered malloc, calloc, realloc, and posix_memalign on the live hetz PID. A
matched high 10-second window requested 406,458,691 bytes. In that window, the
bus entry added two sync passes, four scans, and one inbound no-op transaction.
It added no guarded transaction. The declarations entry did not move.

Direct sync-tree callers requested at least 197,568,968 bytes, or 48.61 percent
of that high window. Manifest `BTreeMap` subtree clones requested 52,970,256
bytes. Scan-cache table reserves requested 51,118,192 bytes. Content-hash map
clones requested 37,355,680 bytes. `opendir` and `Path::_join` requested
56,124,840 bytes. Generic `String::clone` and `RawVec` callers account for much
of the remainder, but the immediate caller cannot assign those bytes safely.

A separate 30-second window requested 465,510,520 bytes and showed the same
shape. Its 60 reported callers named 88.75 percent of all requested bytes.
Conservative sync-tree names accounted for approximately 54 percent. This
longer result confirms that the high-window result is not one callsite sample.

The matched low 10-second window requested 6,918,599 bytes. The bus entry added
no sync pass, two scans, and one guarded transaction. `netdev::recv_multi`
requested 4,194,304 bytes, chiefly for the measurement connection. No manifest,
scan-cache, or folder-walk caller reached the 31,360-byte top-20 cutoff. The
high window requested 58.75 times more bytes than the low window.

No QUIC or mux caller reached the 190,752-byte top-20 cutoff in the high window.
Named QUIC, noq, and rustls callers were small in the low window. The transport
is visible, but it does not create the large bursts. Whole-tree sync work does.

The earlier 75.20 MB full-pass estimate covered the measured scan and
materialize phase boundaries. It did not cover all engine baseline and final
map clones or all wire work. The engine clones complete manifest and observed
maps before and after guarded work and normal passes. The wire path can also
clone or serialize a manifest payload. The direct-caller trace proves that the
missing allocation is sync bookkeeping, but it does not yet split generic
`String` and `RawVec` allocation among those parent operations. Do not reduce a
new candidate until a red measurement isolates one of these operations.

The rollout rollback binaries both reported `0.2.1+71f3d06`. Both exact
rollback files and all release, instrumentation, preflight, and transfer files
were permanently deleted after verification and are absent.

The rollout created two rollback binaries from `0.2.1+9d1c138`. They were
`/Users/myobie/.local/bin/fabric.rollback-1788525446` and
`/home/myobie/.local/bin/fabric.rollback-1788525381`. Both exact files were
deleted after successful verification, and both paths are absent.

PR #141 retired the stale ACL and exec reconnect flake classifications. Each
unchanged test passed 300 of 300 runs. It also fixed two ledger test races
without weakening the exact count checks. The ledger test passed 300 of 300
runs under 12-way load. PR #141 merged as `9231e71`.

PR #142 completed F13. Built-in exec now reads caller EOF and uses
`kill_on_drop`. The red test proved that the server ignored EOF and a quiet
child survived. The fixed server completes immediately, kills the child, and
releases the handler permit. PR #142 merged as `5c96c30`.

PR #143 added an initial five-minute inbound sync deadline for F14. The red
test acquired the resolver guard, stalled before `Push`, and proved the guard
remained held. PR #143 merged as `cddff58`.

PR #144 replaced that total deadline with a 30-second I/O progress timeout. A
valid 512 MiB transfer could need less than 1.71 MiB/s, so the total deadline
could reject valid work. Progressing sessions now have unlimited total time.
Only 30 seconds without bytes causes a timeout. PR #144 merged as `25abce1`.

The final progress test sent one `Push` byte every 80 milliseconds. The
320-millisecond transfer failed under a 200-millisecond total deadline and
passed under the idle deadline. The stall test still reacquired the guard
within 100 milliseconds. The library suite passed 443 tests, with three
ignored.

Exact-main macOS CI found an ordering defect in the symlink-root watcher test.
The test captured its quiet generation before it drained setup events. A
deterministic negative control made the old assertion fail. PR #145 moved the
baseline read after the drain and merged as `9d1c138`.

The corrected watcher test passed 300 of 300 runs under 12-way load. Exact-main
test run `33864311707` passed on macOS and Linux. Exact-main Nix run
`33864311805` also passed.

Release `v0.2.1+9d1c138` passed all four release jobs in run `33865527858`.
The release contains three archives and three checksum files. Every archive
matched its checksum and contained only `fabric`. The Apple binary reported
`0.2.1+9d1c138` before deployment.

The rollout updated hetz first and Silber second. During the mixed-version
window, ping and exec passed in both directions. Silber-to-hetz ping took
38.860 milliseconds. Hetz-to-Silber ping took 40.025 milliseconds. Both paths
were direct.

Both binaries and both daemons now report `0.2.1+9d1c138`. Doctor passes on
both hosts. Final two-way ping and exec also passed. The final pings took
110.445 milliseconds from Silber and 37.012 milliseconds from hetz. Both paths
were direct.

Two-way send-file hashes matched. The Silber-to-hetz hash was
`d2c450b11ec2c0c87034569606e3d463e42b92f1cc01aa451fcf862be3bd362c`.
The hetz-to-Silber hash was
`c8f30d8d3113eacff05bd8ce2aabe0217edec1223761eed9c069be17fc33ad8d`.
The two transfer test files were deleted after verification.

Both sync entries have matching cross-host digests. Both hosts report zero
drift, missing, unexpected, mismatched, scan issues, reconcile failures,
stopped peers, away peers, and delta fallbacks. The bus digest is
`878ee23d35b412d04fe5e69d8e12fa4ac9324ebccd66f8a6eee1cc011412d6aa`.
The declarations digest is
`d4f0b8a0086206f18305f48c034534de1e03cbc7bd25fe41216712716c86e55e`.

The rollout created two rollback binaries from `0.2.1+a49be9c`. They were
`/Users/myobie/.local/bin/fabric.rollback-1788519741` and
`/home/myobie/.local/bin/fabric.rollback-1788519706`. Both exact files were
deleted after successful verification, and both paths are absent.

## Current incident — 2026-09-03

### Resolved mux stream outage and rollout

At 15:14Z, Silber lost every Silber-initiated Fabric operation to hetz. Ping,
exec, and send-file failed after a remote-generation read error and duplicate
mux refusals. Hetz-initiated sync continued in both directions, so the pair was
degraded but not partitioned.

Silber ran endpoint generation 6, and hetz ran generation 5. Hetz correctly
retained its canonical client connection to Silber generation 6. Silber removed
its live shared connection from the local cache without closing it. New
noncanonical connections then received correct duplicate refusals from hetz.
The generation comparison and equal-generation tie-break were not defective.

A Silber-only daemon restart at 15:57Z moved Silber to generation 7. Hetz then
replaced its generation 6 belief, and all control operations recovered. This
restart was the incident workaround, not the fix.

The root cause was `open_mux_stream` cleanup after a logical stream failure.
It called `forget_if`, which removed the shared QUIC connection without closing
it. Closing that connection after every stream failure was also wrong because
one failed logical stream does not make its healthy sibling streams defective.

PR #140 adds three bounded cleanup states. One unknown stream failure keeps the
shared connection. A concrete `ConnectionError` or visible close reason closes
and removes the exact connection. Three consecutive unknown failures on one
stable connection close and remove it, so attempt four opens a replacement.
The unknown-failure count exists only inside one `open_mux_stream` call. A
stable-ID guard prevents late cleanup from closing a newer replacement.

The first red proof reproduced the production duplicate refusal and orphaned
connection with the old cleanup. The second red proof stayed on the wrong
connection without the unknown-failure bound. Both proofs passed 20 of 20 paired
runs after the fix. The library suite passed 440 tests, with three ignored. The
binary suite passed all 18 tests.

PR #140 merged as `a49be9ca65eee694abd70d3e3985b5a157fe72ed`. Main's Nix,
macOS, and complete Linux matrix passed. Release `v0.2.1+a49be9c` published
three binaries and three checksum files. All four release jobs passed. All six
assets downloaded, and each archive matched its checksum and contained only a
`fabric` binary. The Apple binary reported `0.2.1+a49be9c` before deployment.

The rollout updated hetz first and Silber second. Both daemons now report
`0.2.1+a49be9c`. Doctor passes on both hosts. Ping and exec passed in both
directions. The final measured pings took 42.135 milliseconds from Silber to
hetz and 51.568 milliseconds from hetz to Silber, both through the relay.
Bidirectional send-file hashes matched.

Both sync entries have matching cross-host digests. Both hosts report zero
drift, missing, unexpected, mismatched, scan issues, reconcile failures, stopped
peers, away peers, and delta fallbacks. The bus digest is
`6064612b8cbeba371f98ed50c0d4b348d5df3960a8e710fc55f9b92d62f32dae`.
The declarations digest is
`670d67f124156732a44bec0427958950a8944465ec7720dc6c65a839d1907d55`.

The two transfer test files were deleted after their hashes matched. The exact
rollout rollback binaries were also deleted after verification. They were
`/Users/myobie/.local/bin/fabric.rollback-1788454031` and
`/home/myobie/.local/bin/fabric.rollback-1788454008`.

Silber.cos ordered this rollout complete and ordered Silber.fabric to stop for
the night. The three known flakes, F13, and F14 wait for the next work period.

Release `v0.2.0+ebca516` is published with Apple arm64 and both Linux assets.
The required ACL migration and downgrade warnings lead its release notes.

Release `v0.2.1+f806142` is published with Apple arm64 and both Linux assets.
It was the first v0.2.1 fleet build and remains available for rollback.

Release `v0.2.1+f2757f0` is published with Apple arm64 and both Linux assets.
All four release jobs passed. Silber and hetz run that exact build. The rollout
updated Silber first. During the mixed-version window, ping and exec passed in
both directions. Final direct pings took 84.211 milliseconds from Silber to hetz
and 53.214 milliseconds from hetz to Silber.

Both sync entries have matching digests and zero drift, missing, unexpected,
mismatched, scan-issue, reconcile-failure, stopped, away, and delta-fallback
fields on both hosts. The bus digest is
`09e8c17517fd15ee8aedf6240997aaedc81a16dfbc20d0c7b6d841ad5cba0f32`.
It has 27,777 present records, 17,288 tombstones, and 27,777 observed paths.
The declarations digest is
`fb911eaa327d3c2d560da299c25f4f877d1ce07913266a00cefbe6ef11d63ba1`.
It has 118 present records, 41 tombstones, and 118 observed paths.

Doctor passes every local, peer-reachability, version, and sync check on both
hosts. It keeps Bluey's unreadable version visible as `unknown, roaming` and
does not fail the fleet check. Bluey remains reachable from both hosts.

The two updater rollback binaries made during this deployment were deleted
after verification. Both contained `0.2.1+88ab09f`, and both exact paths are
absent. Older builds remain available through their published release assets.

PR #138 makes `fabric restart` refuse when the default home has an enabled or
installed native service. It names the exact launchd or systemd restart command.
An unknown service state also names the exact native status command. The shell
restart test now proves shell and exec in both directions after a restart. It
merged at `88ab09fac284c737bc15e83b6fa0df52fef23496` and is deployed on both
hosts. Live refusal checks kept the launchd PID 30532 and systemd PID 1924193.

PR #139 runs `lifecycle`, `pathwatch_slice`, and `shell` in Linux CI. PR #76
excluded them because they passed locally on macOS but had never run on Linux
CI. It recorded no Linux failure. All three passed unchanged on the first Linux
CI run. The target-accounting check now rejects every excluded integration
target. The PR also corrects the stale CI description in
`docs/known-flaky-tests.md`. It merged at
`f2757f09bad8275d69f0ca11c399cb725f477dc2` and is deployed on both hosts.

The unchanged ledger test passed 20 of 20 local runs after the #139 merge. The
unchanged ACL test passed five of five local runs. Continue the reproduction
pass before changing either test when Silber.cos starts the next work period.

Nathan removed the release hold on 2026-09-03. Daily builds must include the
features on main. A later public 0.9.0 release is a separate act, and unfinished
public features stay behind flags instead of staying off main.

A matched mux pair again retained a stale canonical connection after Silber
replaced its endpoint. Hetz has the lower NodeID, so both tie-break decisions
correctly select the hetz-client to Silber-server connection. Silber closed its
server-side cache and dialed a fresh noncanonical connection. Hetz retained the
old canonical client handle and rejected every fresh connection as a duplicate.
The bounded retry cannot clear a durable stale handle. A Silber daemon restart
fully closed the old endpoint and restored control.

PR #128 quarantines mux in both directions. Production dials use the
existing direct service ALPNs, and production endpoints do not advertise mux.
This keeps all services, including Git, and removes the stale shared state.
It merged at `8c6458146ee50ef57f134ba47e747cbe5990482b`. Silber and
hetz run the exact merge. Two-way exec passed after each service restart.

PR #129 is the full repair. It merged at
`353de6b2cc8755c5c286018eecfddc8f4ef13a7a`. The first red test preloads
Bluey's old recovery state, misses one hetz probe, and observes a shared
endpoint replacement. The isolated recovery fix now closes only the failed
peer's cached connection. It never changes the shared endpoint or drops another
peer's tunnel.

The second red test holds the old canonical connection while a new endpoint
generation dials. The old mux code refuses all eight replacement attempts. Mux
version 2 exchanges durable endpoint generations before it registers a shared
connection. A higher remote generation replaces stale canonical state. An old
peer rejects the new mux ALPN, so the new peer uses the existing direct service
ALPN until both builds support mux version 2.

Both original red tests pass. A full daemon test also replaces the higher
NodeID endpoint and proves two-way ping reconverges without restarting either
daemon. The library suite passed 424 tests, with two measurement tests ignored.
All 15 binary tests passed. A second full local integration run passed all 29
tests in 213.60 seconds. All three CI jobs passed.

Silber deployed first, then hetz. Mixed-version ping and exec passed in both
directions before the hetz update. After both updates, a live test replaced the
Silber endpoint from generation 1 to generation 2. Silber PID 57719 and hetz PID
851943 stayed unchanged. Both directions reconverged direct in 37 milliseconds,
and remote exec passed. Neither log contains a new duplicate refusal.

The matched-fleet soak starts at 2026-09-03 10:32Z. A release is not cut. Bluey
needs no compatibility action because mux/2 falls back to direct service ALPNs.
It needs a future release update only to receive the isolation and generation
fixes.

The mux value measurement is complete. Two fresh processes ran the exact same
test binary. Each process held 16 proven idle logical sessions for 1,800
seconds. Mux used one connection, and direct ALPN used 16 connections.

Mux used 0.604104 CPU seconds. Direct used 1.274088 CPU seconds, so mux used
52.6 percent less CPU during the 1,800-second window. Mux caused 100 package
idle wakeups and 8,336 interrupt wakeups. Direct caused 373 package idle wakeups
and 23,700 interrupt wakeups. Mux reduced those wake counts by 73.2 percent and
64.8 percent during the window.

Mux transferred 327,020 QUIC UDP bytes. Direct transferred 3,101,070 bytes, so
mux reduced idle network bytes by 89.5 percent during the window. Mux RSS moved
from 42.406 MiB to 41.406 MiB. Direct RSS moved from 44.875 MiB to 45.047 MiB.

Direct won the recovery measurement. Across 160 concurrent logical-session
samples, mux p95 was 44.026 milliseconds and direct p95 was 36.476 milliseconds.
Mux recovery was 7.550 milliseconds, or 20.7 percent, slower. The percentage is
large, but the absolute recovery cost is not perceptible.

The main win is battery life. Fabric is idle for most of its life on Nathan's
laptops. With 16 idle sessions, mux gave the machine substantially fewer reasons
to wake and reduced network bytes by 89.5 percent. Mux earns its place because
it substantially reduces every measured idle cost, with a small recovery cost.

PR #131 records the repeatable mux and direct harness and the result above. It
merged at `edd1c80169e7b856407df3c9fb17accc83dfe371`.

PR #132 adds the per-peer `roaming = true` contract. It merged at
`17a7bb53b80d8a79c46ab4f8a8adcd7fe3c2f69c`. An absent roaming peer still gets
health probes and normal sync attempts. The absence does not enter either
failure counter, close a peer connection, or appear as a stopped sync. Health
and sync log only away and return transitions. `fabric status` reports `away`.
`fabric sync ls` reports `stopped=none` and `away=<peer>`. Doctor reports both
the peer and its paused sync as OK.

The Bluey NodeID now has `roaming = true` in both live peer files. Silber names
it `bluey`; hetz names it `air`. Both old daemons reloaded the file, but build
`353de6b` ignores the new field. Bluey was reachable from both machines at
12:15Z, so no real away or return transition has been observed.

PR #135 adds strict cross-file validation for explicit sync selectors. It
rejects an unknown selector during `sync add`, sync reload, and peer reload.
At daemon start, it keeps the transport up, warns once per affected entry,
reconciles selectors that resolve, and reports unresolved selectors as stopped.
A rejected reload keeps the last valid in-memory configuration. The live hetz
migration was complete before deployment. PR #135 merged at
`6e856c4e6b75fb960f8c1df570f001cad85ba31c` and is deployed on both hosts.

PR #136 makes an unreadable roaming peer version visible without failing
doctor. A normal peer with an unreadable version still fails. It merged at
`eecf28d3d9ae90829f9a2d3ed2a547f81763405f` and is included in the deployed
build.

Code, config, test, CI, and mixed changes need a pull request and specific
Silber.cos approval before merge. Changes that touch only `now.md` go directly
to main without a pull request or approval. Do not create peer-file backup
copies. Put non-secret configuration history in git instead.

Six dated peer and sync copies on Silber and ten on hetz were deleted on
2026-09-03. The deletions are not recoverable. The four live files remain
intact. Do not create replacement copies.

The private catalog now owns their version history under
`docs/fabric/<host>/peers.toml` and `docs/fabric/<host>/syncs.toml`.
Silber.catalog committed Silber's files at `7add563` and hetz's files at
`acbd847`. The two hetz blobs match the two live host files by SHA-256.
Silber.catalog is the only writer for that folder. Send it changed bytes, then
review its commit before it pushes.

The tracked file is a snapshot, not a live configuration source. Nothing
checks it against the host yet. Compare it with the live host file before using
it as current state. Never add `~/.local/share/fabric/identity.toml`; it holds
the machine's private identity key.

The first cross-host review found both hetz sync entries selected the unknown
name `mac`. Both entries now select Silber's exact NodeID. Hetz has nonzero
outbound time and wire bytes, and no stopped peers.

The migration recovered no missing data. Present, tombstone, and observed
counts were unchanged across the migration. The first outbound pass found no
missing records. It restored watcher-driven outbound delivery and the symmetric
design, which closes a future gap during a Silber outage.

## Current handoff — 2026-09-02

Nathan ordered the release backlog completed without approval stops. The active
order is strict ACL baseline, Git remotes, then degraded-path recovery. A release
cut is not the current task.

PR #103 added `KillMode=process` to the Linux service. PR #104 bounded initial
tunnel dials. PR #105 made omitted and empty peer grants deny every service. PR
#106 added Git share declarations and exact read and write grants to
`peers.toml`. PR #107 bounded iroh endpoint close waits after deterministic CI
hung for ten minutes in a half-close recycle test.

The strict ACL migration and rollout are complete on Silber and hetz. Each
machine ran `fabric peers make-explicit` with old build `0.2.0+593a3f7` before
its binary changed. The pre-migration, post-migration, and post-swap ALPN
matrices are identical. Two-way exec works. Both sync entries are clean on both
machines. Both services are active and enabled. Hetz has `KillMode=process`.

At 18:42Z, Silber lost ping and exec control to hetz while the st2 bus still
crossed the same peer pair. A dial failed after four mux reopen attempts because
hetz closed a connection as a duplicate. Silber recycled its endpoint from
generation 2 through generation 5. Hetz stayed active on generation 0 during
the incident. One LOCAL Silber daemon restart restored a direct ping and exec
at 18:51Z. If control fails again with `duplicate mux connection`, restart the
local daemon first. If this failure recurs on `0.2.0+ae755c5`, roll Silber back
without asking. Do not repeatedly restart it.

Bluey was not a closed laptop during the incident. Its host answered over
Tailscale in 62 milliseconds, but its Fabric endpoint stopped answering at
18:23Z. This absence matters. When hetz missed a probe, the health loop saw no
reachable peer and let Bluey's old failure count trigger an endpoint recycle.
The same condition caused generation 2 to 3 at 18:40Z and generation 4 to 5 at
18:50Z. This recycle trigger and the mux convergence defect are separate.

PR #122 fixes the mux defect and merged at `7f4da21`. A generation change
ignored the cached old connection but did not close it before the new endpoint
dialed. The peer retained that canonical connection and rejected the replacement
as a duplicate. The fix closes old-generation cached state before redial. It
also gives an explicit duplicate refusal eight attempts with 100-millisecond
delays. The total added wait is bounded at 700 milliseconds.

Both proofs failed before their fixes. The old cached connection survived its
generation change for more than one second. Four immediate duplicate refusals
then escaped as `open mux stream after reconnect`. Both proofs now pass without
a daemon restart. All 413 active library tests and all 15 binary tests pass.
Two measurement tests stay ignored. PR #122 is merged and deployed on Silber
and hetz as `0.2.0+7f4da21`.

The rollout used Silber first and hetz second. Silber's arm64 binary has SHA-256
`59794988e352df076e4ce44c949f334dce508362d3fe48eff6c05ec1d0f71b1e`.
Hetz built exact commit `7f4da21293732e5be914e95cf16dbf54c248a705` natively.
Its x86_64 binary has SHA-256
`c9a890cfefc531f68847393e6cafca6abf0c1b4dbcb89ceb4041a35336137bc5`.
The rollback paths on both machines report `0.2.0+ae755c5` and end with
`fabric.rollback-ae755c5-pre-7f4da21`.

Bluey returned during the final proof after Nathan updated it. One fresh-process
ping hit `duplicate mux connection` during that real topology change. The next
six fresh-process pings all passed direct in 36 to 128 milliseconds. An exec
returned real remote output. Silber PID 26309 and hetz PID 3364283 stayed
unchanged. The pair therefore reconverged without the daemon restart that the
same failure required before PR #122. Do not run the synthetic recycle tonight.

Bluey is temporarily absent from Silber's live `peers.toml`. Hetz still lists it
as `air` because Nathan uses Bluey to reach hetz. Removing `air` locked Nathan
out and was reverted from the fresh config backup before the hetz restart.
Unreachable from Silber did not mean unused by hetz. Silber now has one peer, so
one missed hetz probe can still recycle its endpoint.

The incident `fabric restart` started a healthy daemon outside launchd. The
orphan used the same home and node identity, so starting a second daemon beside
it was unsafe. Silber stopped the orphan first and started the loaded launchd
job once. Launchd owns PID 26309, and ping plus exec still work. This was the
second critical process found without supervision tonight; the port 3080 relay
was the other. Treat an unsupervised critical process as a shared host pattern,
not as an isolated service detail.

Silber permits the five service names `echo`, `exec`, `send-file`, `shell`, and
`sync` for both peers. Hetz permits those five names plus `deskset-vnc`,
`pty-remote`, and `st-sync` for both peers. The strict daemon starts with an
omitted allow list and grants nothing. It does not refuse startup.

Bluey is Nathan's deferred task. It must run the old `make-explicit` helper and
preserve its full ALPN matrix before it receives a strict binary. Do not wait for
Bluey and do not count it as verified.

PR #108 added Git remotes and merged at `4548b1e`. Silber and hetz ran that
build before the degraded-path rollout. Each has the relative
`git-remote-fabric -> fabric` helper and zero Git remotes. Nathan owns the first
live share and grant.

PR #109 added degraded-path recovery and merged at `bbd69bb`. All outbound
services now use `fabric/mux/1` streams on one shared multipath connection per
peer pair. Simultaneous cross-dials converge on one connection. The health loop
skips a redundant probe after recent application traffic. Three samples above
one second and eight times baseline redial the peer connection. The classifier
resets on endpoint generation changes and has a 60-second per-peer cooldown.
The full local proof passed: 406 library tests, 29 daemon-slice tests, 18
folder-sync tests, 12 shell tests, and all smaller integration slices.

The live WAN proof and the 24-hour idle-cost window remain. PR #114 added the
mixed-version compatibility fallback and merged at `36158f6`. A new client uses
an uncached direct ALPN only after an explicit mux ALPN rejection. Old clients
remain compatible with new servers. Silber first deployed `0.2.0+36158f6`, while
hetz stayed on `0.2.0+4548b1e` for the mixed-version soak.

The compatibility candidate passed an actual two-build proof in isolated
homes. Build `0.2.0+4548b1e` and the new candidate exchanged ping and exec
traffic in both directions. The new side wrote one fallback event across both
services. After the old side changed to the new candidate, two pings and one
exec passed, and the fallback count stayed at one. The in-process proof also
shows zero cached peers during fallback and one shared connection after mux
becomes available.

The first Silber soak found that each new stream repeated the rejected mux
handshake. The log stayed at one event, but manual pings periodically took 2.3
to 2.9 seconds. No path-quality redial occurred. PR #115 suppresses mux re-probes
for 60 seconds after an explicit rejection and merged at `ae755c5`. It then
permits one requested-stream re-probe so an upgraded peer cannot stay
downgraded. It reports cumulative fallback uses at powers of two. This makes
repeated use visible without noisy per-stream logging.

Silber and hetz previously ran `0.2.0+ae755c5`. The Silber updater kept
`/Users/myobie/.local/bin/fabric.rollback-1788366350`, which reports
`0.2.0+36158f6`. Roll Silber back without asking if control to hetz fails, the
st2 bus stops crossing machines, or the per-stream cost grows beyond the
measured cost.

The latency sample used a fresh CLI process for each requested stream. Run
`/usr/bin/time -p fabric ping hetz` repeatedly from Silber and record the
`real` value. The first soak ran 28 successful ping and exec samples. Periodic
ping samples cost 2.3 to 2.9 seconds. Use the same command and the same local
Silber-to-hetz direction after the fix. Report the first probe separately from
the later samples in the 60-second negative-capability window.

PR #115 serializes the first capability check and counts direct fallback uses.
The full library proof passes 409 active tests, with two ignored tests. The
isolated proof ran eight rapid new-to-old pings plus an exec. Candidate
`0.2.0+361e581` reported 3.3 to 7.7 milliseconds per ping. The old daemon
recorded one rejected mux handshake. The new validation log recorded one
fallback entry and cumulative use summaries at 2, 4, and 8 uses. The
in-process capability-flip proof expires the window, enables mux on the old
peer, and proves two requested streams converge on one cached mux connection.

The exact merged archive has SHA-256
`4eaf590ab6f559ac36f8e390a2a2196ff0f6c008cdab61c9481026f336e9bfdf`.
After the Silber update, 28 fresh-process pings ran from 16:26:24Z to
16:26:28Z. Real time ranged from 0.04 to 1.09 seconds. The median was 0.05
seconds, and the 95th percentile was 0.47 seconds. The daemon logged one hetz
fallback window, summaries at 2, 4, 8, 16, and 32 uses, and no hetz redial.
Control, ping, and exec work.

Silber.cos authorized the hetz update after that measurement. Hetz built exact
commit `ae755c5` on x86_64 Linux. Its one-member archive has SHA-256
`4222b76bbe50ad3de5dea1c96e2b3501d68c7ad3181387e0ca751d951508772e`.
Before replacement, `/home/myobie/.local/bin/fabric.rollback-4548b1e-pre-ae755c5`
ran and reported `0.2.0+4548b1e`. The updater staged beside the live path and
renamed the new binary into place. Its additional rollback path is
`/home/myobie/.local/bin/fabric.rollback-1788366755`.

The ordered post-update checks passed. Silber-to-hetz ping took 333
milliseconds. Hetz-to-Silber ping took 36 milliseconds. Exec reported
`0.2.0+ae755c5`, and `hetz.root` returned a native bus probe. The pair produced
one `mux_accept` event at 16:32:40Z. The last fallback-use summary was 128 before
the update. All 140 later ping streams passed from 16:34:58Z to 16:35:08Z, and
no use-256 summary appeared. Neither machine logged a post-update fallback for
the other. Hetz is active, enabled, and uses `KillMode=process`.

The first PR #115 deterministic job exposed a lost tunnel notification wake.
`a_tunnel_recovers_from_an_asymmetric_partition` stalled after a live session
resumed. The writer checked state before it created its notification future.
Data could arrive and notify existing waiters in that gap, then leave the
writer asleep with bytes ready. PR #117 creates each future before the state
check and merged at `5c035dc`. Before the fix, two targeted runs passed and the
third reproduced the 63.56-second failure. After the fix, ten consecutive runs
passed in a 54-second window. All 29 daemon-slice tests passed in 210.80 seconds,
and all 409 active library tests passed in 24.38 seconds. The two measurement
tests stayed ignored. This change is not deployed.

The session totals in `fabric status` persist across daemon restarts. They do
not cover only the current daemon uptime. The old output did not state this
window and listed retained telemetry for removed peers without a marker. That
made droppy's historical 10,885 attempts look like current dial activity.
Droppy was already absent from both authoritative `peers.toml` files. Its last
local log entry was August 12, and its last hetz log entry was August 20.

PR #119 adds the missing context and merged at `8eb296d`. New telemetry
snapshots record their exact UTC window start and retain it across restarts. An
unreadable or incompatible snapshot reports its reset reason. An existing
snapshot keeps its valid counters and states that its start is unknown. Session
and path rows mark retained entries as `[not in peers.toml]`. This change is not
deployed, and it must ride with a later release.

PR #109 deterministic CI found two follow-up defects. A temporary debug tunnel
block became a permanent mux denial, which returned early EOF in five recovery
tests. A valid reconnect also retained the old outage backoff. PR #111 fixed
both and merged at `ff03bfc`. The five-flap proof now has a 1.5-second budget and
measured 315.95 ms locally after five 200-millisecond drops.

The sixth CI failure was portable test setup, not transport behavior. Ubuntu
made a bare Git remote whose HEAD named `master`, while the test pushed only
`main`. PR #112 points the bare HEAD at `main` and merged at `6ee46a8`.

The hetz fabric checkout had a stale SSH origin at the old `myobie/fabric`
repository. Hetz has no GitHub SSH key, so fetch had failed for an unknown time.
Its origin now uses `https://github.com/compoundingtech/fabric.git`, matching
the other working hetz checkouts.

The strict ACL, Git transport, shared mux, mixed-version fallback, bounded
fallback, lost-wake, telemetry-context, and recycle-convergence fixes are
complete. The PR #122 rollout and its live convergence proof are complete.
No deployment action remains from this handoff.

## Historical handoff — 2026-08-29 (Fable session; fleet moved to Codex)

**Who wrote this and why.** A Claude Fable session spent 2026-08-29 on the fabric
review (`../cos/notes/fabric/review-2026-08-29.md`, 15 findings). Workers are
moving to Codex, so this externalizes what commits and PRs do not carry. A
running decision log also lives in st2 context under identity `Silber.fabric` —
on the message bus, NOT in any repository, so it has no history and is not
reviewable. IF YOU CAN REACH IT, run `st2 context read` for the fuller log. Treat
THIS file as the reviewable source of truth. The context is accurate as an
append-only log, with one correction appended 2026-08-29 evening: its "morning
plan" entry was written as if the same agent resumes — the actor is now you, and
cos sends the soak-clear signal.

**State of main (verified 2026-08-29).** origin/main at `343b08b`. Merged today:
#86 (finding 2, include-delete), #87 (finding 1, dial-permit leak), #88 (finding
4A, content memory) — these three are in release `v0.2.0+4bc04af`, deployed to
the fleet. Also merged: #89 (finding 3), #90 (finding 15, README).

**Open PRs:**
- #91 F5 sweep-gate · #92 known-flakes doc · #93 F6 update-timer · #94 F9
  monitor-park · #95 F12 doctor-`--home` · #96 F10 doctor service-enablement.
  These are a STACKED CHAIN: each branch is rebased onto the previous, so
  SQUASH-MERGE THEM IN ORDER 91→96. Out of order or one-at-a-time will conflict
  on CHANGELOG. (#89 and #90 already landed this way, cleanly.)
- #97 (finding 7, send-file streaming) and #98 (finding 8, adopt-honours-include)
  — NOT in the chain. After the chain lands, rebase both onto new main (CHANGELOG
  conflicts). #98 changes wire/convergence semantics — merge AFTER the soak.
- Branch `feat/peer-acls-explicit-not-legacy` (`5350765`) — 0.10 ACL groundwork,
  transcription test only so far; helper + expose warning still to write.

### Decisions + reasons (settled with cos / Nathan, not in any file)

1. **Affirmative-absence deletes — item 1 of the NEXT correctness pass.** A delete
   must be inferred ONLY from an affirmative absence: a path missing from a
   directory the scan COMPLETELY and successfully read. Split "absent from scan"
   into three answers — PRESENT / AFFIRMATIVELY-GONE / UNKNOWN — and tombstone
   only affirmatively-gone. Why: today `scan_folder` (src/sync/engine.rs) aborts
   on the first unreadable file (`read_dir?`/`file_type?`/`fs::read?`), so an
   unreadable file STALLS the whole entry rather than false-deleting; the delete
   loop only runs on a fully-successful scan. Making the scan resilient (skip
   what it can't read) would REINTRODUCE the false-delete for skipped paths. The
   three-answer split fixes both at once and retires the guard accretion — three
   guards today are one mechanism: the 2026-08-25 scan guard, #86's
   materialize-time include check, and F4-B's proposed size guard. Filed as the
   FIRST item of the next correctness pass, deliberately NOT under 1.0 (a
   correctness fix under a feature milestone inherits the wrong gate; 1.0 is
   folder sync + relay + DNS). Gate: the soak. Ceiling: the filesystem gives no
   reliable delete event for a delete while the daemon was off, so the scan stays
   the delete source — the change QUALIFIES its absences, it does not replace it.

2. **F4-B and F11 are the first instances of (1), written in that shape, NOT as a
   third guard.** F4-B: a single file over 512 MiB (`MAX_BLOB`, src/sync/wire.rs)
   makes the whole entry bail and every peer read as "unreachable". F11: `no
   local sync entry named X` and a residual oversize error must stop reading as
   "unreachable, clears when it comes back" (classification in src/sync/engine.rs
   → wording in src/doctor.rs). TRAP: the naive F4-B fix (skip oversized files at
   scan) tombstones a file that was synced under the limit and later grew past it
   — it goes absent-from-scan and bus policy reads that as a delete. That is the
   2026-08-25 loss through a new door. Do not ship the naive version.

3. **0.10 ACL groundwork — transcription vs narrowing.** `PeerBook::may(id,
   service)` (src/config.rs) treats `allow = None` as unrestricted: it reaches
   every service INCLUDING ones exposed in the future (pinned by test
   `a_peer_without_an_allow_list_may_reach_everything`). Writing each legacy
   entry's CURRENT effective permissions as an explicit `allow` list changes the
   file but not today's access — a TRANSCRIPTION, which is cos's to do. It
   becomes NARROWING (Nathan's decision) only when a list is smaller than what
   the peer can reach today. Nathan's conscious decision (via cos): ACCEPT that a
   service exposed AFTER the transcription is no longer auto-granted and must be
   added per peer — that IS the cleanup; auto-granting every future service is
   the leak the moment a peer is a friend rather than one of Nathan's own
   machines. Each entry's explicit list = the five built-in service names
   {shell, exec, sync, echo, send-file} (from `service_name_for_alpn`,
   src/daemon.rs) PLUS THAT MACHINE'S `fabric expose` names. LIVE TRAP found on
   hetz: omit an exposure and you narrow by omission and break the fleet — hetz
   exposes `pty-remote` (how cos reaches hetz agents) and `st-sync` (carries the
   bus/catalog). The helper MUST read the machine's expose names, never
   hard-code; take them from `fabric status` on that machine at transcription
   time.

4. **Release cut convention.** Releases are `v0.2.0+<short-sha>` tags at a main
   commit, NO crate-version bump. Push the tag → `.github/workflows/release.yml`
   builds and publishes per-target tarballs each with a `.sha256` (there is no
   combined SHA256SUMS; the README's checksum recipe is wrong about that).
   `fabric --version` reports `0.2.0+<sha>` (build.rs `git rev-parse --short=7`),
   so it matches the tag. Verify the artifact's checksum + `--version` before any
   rollout. The 0.9 MILESTONE shipped as `v0.2.0+4bc04af` — there will never be a
   tag named 0.9.

5. **The soak.** The fleet is on `v0.2.0+4bc04af`. cos defined "soaked"
   observably: seven conditions at every hourly sweep for 24h from the ~12:46 UTC
   (14:46 Europe/Berlin) deploy on 2026-08-29 — so the window closes ~14:46
   Berlin (12:46 UTC) on 2026-08-30: all three machines read the version;
   delta_fallbacks 0; drift clean; digests agreeing; stopped none;
   reconcile_failures 0; include-drift clean. Any one failing BREAKS the soak
   (diagnose, then restart the window); elevated bytes-per-pass with fallbacks 0
   and digests agreeing does NOT (it is the ~24 MB/restart cursor cost). cos
   measures it and SIGNALS; do not watch the clock or ask.

### Gotchas (cost time; don't rediscover the expensive way)

- **`git checkout main` without `git pull` first** gives a stale local main. A
  branch cut from it silently reverts already-merged commits and produces a
  CONFLICTING PR with no CI. This happened (PR 97). Pull local main to origin/main
  before branching, or branch from `origin/main` explicitly.
- **st2 CLI arguments are shell-evaluated** — backticks and `$(...)` in a `-m`
  body or in `context append --decision/--why` EXECUTE. Write the text to a file
  and pass via `"$(cat file)"`; never inline backticks.
- **Daemon-slice tests flake under CI load**: a_peer_not_permitted_for_a_service_cannot_reach_it,
  exec_expose_reconnect_keeps_child_bound_to_tunnel_session, and
  production_status_exposes_exact_inbound_scan_ledger. See `docs/known-flaky-tests.md`
  (PR #92). Rerun the `deterministic` job; a real regression also fails on rerun.
- **The main and pull-request CI job sets are identical.** Runs 33767026944 and
  33767494392 confirmed the same `macos` and `deterministic` jobs. The current
  gap is narrower: Linux excludes `lifecycle`, `pathwatch_slice`, and `shell`,
  and no other CI job runs those integration targets.
- **Stacked-PR chains squash-merge IN ORDER.** Each branch contains its
  predecessors, so a 3-way merge folds the identical changes (it does not rely on
  ancestry, which squash breaks). Merging out of order conflicts.

### Deliberately NOT done (do not read as oversight)

- F4-B/F11, F14, and the affirmative-absence refactor NOT authored — deferred to
  fresh authoring and gated on the soak (they touch the sync scan / wire path the
  fleet is soaking). Designs are above.
- F13 (an exec child outlives a disconnected caller; src/exec.rs — needs
  `kill_on_drop` PLUS a recv-EOF disconnect arm, because a quiet child never
  triggers the write-failure path) NOT touched — cos gated it on the soak
  because exec is how cos reaches two of the three machines to diagnose anything.
- Did NOT edit live `peers.toml` on any machine — that is the
  change-what-peers-can-reach boundary. Transcription is cos's per-machine
  action; narrowing is Nathan's.
- Did NOT bump the crate version at release (convention: +sha only).
- #98 (F8) authored but flagged for POST-soak merge (wire/convergence semantics).

### What I would do next, in order (my judgement)

1. **When the soak clears (~14:46 Berlin 2026-08-30, cos signals):** author the
   affirmative-absence delete refactor (decision 1), with F4-B and F11 as its
   first instances, in the three-answer split shape. First because it is the
   correctness fix the whole delete subsystem has been accreting guards toward.
2. **Finish the 0.10 ACL slice** on `feat/peer-acls-explicit-not-legacy` (NOT
   soak-gated; merge when green): (a) the make-explicit helper that reads THIS
   machine's `fabric expose` names + the five built-ins and writes each legacy
   entry's equivalent list (never narrow by omission); (b) a `fabric expose`
   warning when no trusted peer is permitted to reach a newly-exposed service —
   one line naming the peers that would need it, NOT a refusal (model on
   `warn_if_permissions_would_stop_a_sync`, src/main.rs). The transcription
   property is already pinned by the test in that branch.
3. **After the #91–#96 chain lands:** rebase #97 and #98 onto new main (CHANGELOG
   conflicts); #98 then merges post-soak.
4. **F14** (an inbound sync session holds the entry guard with no timeout;
   src/sync/engine.rs `prepare_inbound_entry` / src/daemon.rs `handle_sync`) —
   soak-gated; add a deadline to the inbound wire session.
5. **F13** — when cos calls it (post-soak).

### Uncertain / unverified (flagged per "don't assert what you didn't verify")

- Whether a Codex successor inherits my st2 context (`Silber.fabric`). If it can,
  the log there has more detail; if not, this file stands alone.
- bluey's exposures were NOT checked (bluey is a peer but not in Silber's sync
  entries; the handbook says it has no remote exec). Check before transcribing
  any bluey entry.
- Whether the fleet is STILL on `v0.2.0+4bc04af` and the soak is intact — this
  session was paused for budget mid-afternoon and did not re-check. Re-verify
  before acting on anything soak-gated.
- #91/#93/#94/#95/#96/#97/#98 each carry a red-before-green test and were green
  locally on macOS at author time; their CI state at this handoff was not
  re-checked.



## Hotfix DEPLOYED — peer-config split (resolved 2026-07-21)

On 2026-07-21 the Mac↔hetz stopgap sync (rsync over `fabric dial`) failed in a
loop and blocked cos from reaching the hetz fleet. Root cause: the launchd
service launched the daemon as `--home ~/.local/share/fabric`, which (pre-fix)
made the dial path read `peers.toml` from under `--home` while the CLI writes
`~/.config/fabric/peers.toml`; a default-home `fabric add` migrated + deleted the
in-`--home` copy, leaving the daemon with zero peers on the dial path
(`unknown peer` in `service.err.log`) while `ping`/`status`/`shell` kept working.

- **Durable fix (commit `9f5391b`):** `FabricHome::resolve` treats an explicit
  `--home`/`FABRIC_HOME` equal to the default state root like the no-arg default
  (peers from `~/.config/fabric/`); a genuinely different `--home` stays isolated.
  Unit-tested (`resolve_from` + 6 regression tests).
- **DEPLOYED:** as of ~17:33 the Mac daemon runs the fixed binary
  (`0.2.0+9f5391b`, pid rotates under launchd) — swapped via stop-old →
  `fabric service install`. Verified: hetzner reachable, dial probe returns
  bytes, sync log clean bidirectional cycles, no fresh `unknown peer`.
- **launchd label changed:** the service is now `com.compoundingtech.fabric`
  (was `com.myobie.fabric`, org rename). The stale `com.myobie.fabric` job was
  booted out and its plist moved aside to `*.stale-501`. Use the new label.
- **Holds LIFTED:** default-home `fabric add`/`remove` and `fabric restart` are
  safe again — the daemon reads the same `~/.config/fabric/peers.toml` as the CLI.

## Recent work (2026-07-24)

- **Marker env vars shipped** (main `93e0f59`): a `fabric shell`/`exec` runs in the
  daemon's session, not the caller's, so the remote session now exports
  `FABRIC_SHELL=1` / `FABRIC_EXEC=1` / `FABRIC_PEER=<connecting NodeID>`. Lets a
  shell rc detect a fabric session and skip session-fragile startup. Unit + two-node
  iroh tests; README has an rc-guard snippet. Not deployed — ships in the next binary.
- **`fabric shell silber` hang — diagnosed, NOT the suspected commands.** Probing
  the LIVE silber daemon's own session (via the harmless `fabric exec hetzner ->
  fabric exec mac -> …` relay) showed the daemon is in an **Aqua (GUI) session** and
  xcrun / xcode-select / `security show-keychain-info` / brew all return rc 0 (no
  hang); xcode is fully usable in a fabric shell. So the hang was a prior daemon
  state, not those four. Deterministic culprit-finder for Nathan: rc-timing
  instrumentation (`set -x; PS4='+ ${EPOCHREALTIME} '`) on a fresh shell; or just
  gate fragile rc on `[ -z "$FABRIC_SHELL" ]`.
- **PR #16 (nix flake) MERGED** to main (`903d70f`) — `flake.nix` + nix CI now live.

## What fabric is

A standalone Rust CLI + local daemon that hides iroh behind local Unix sockets.
Consumer tools ask fabric for a local socket wired to a trusted remote peer and
speak their own protocol over it; only fabric touches iroh/QUIC/relays/NodeAddr
and the peer allow-list. See `README.md` and `SKILL.md`.

## In flight — `fabric sync` (generic file-sync primitive)

fabric now owns file sync. A config file (`syncs.toml`) lists sync entries; the
running daemon watches each folder and keeps it converged with its peers.

Status:
**Fully landed + pushed to origin main (fabric 0.2.0):** the config surface
(`syncs.toml`) and the property-tested reconciliation core
(`src/sync/{config,manifest,node,glob}`), the on-wire backend
(`src/sync/wire.rs`), the async `SyncEngine` (`src/sync/engine.rs`, fs-watch +
scan/materialize), the daemon wiring (`fabric/sync/1` ALPN, `IrohSyncTransport`,
engine started at boot, `SyncReload`/`SyncStatus` control ops), and the
`fabric sync add/ls/rm/reload` CLI. Proven end-to-end over real iroh by
`tests/sync_slice.rs`. All tests green; CLI smoke-tested; zero warnings.

Design decisions (why, so they are not re-litigated):
- **Backend-agnostic semantics.** The merge/conflict/delete/echo/convergence
  rules live in fabric (`sync::manifest` + `sync::node`), above a swappable
  transport. `Manifest::merge` is a semilattice join (commutative/associative/
  idempotent) → convergence and echo-freedom are structural, proven by property
  tests. Newer-wins = Lamport version + deterministic author tie-break (never a
  wall clock). The on-wire backend is verified to reach the exact same state as
  the pure reference reconcile — that is the swappable-backend guarantee.
- **Policies.** `catalog` = union + newer-wins + never-delete-on-peer + no-sweep
  (a local delete is restored; decommission is an edit, e.g. `retired = true`).
  `bus` = + tombstone deletes (modelled; sweep TTL not yet wired).
- **Config.** `~/.config/fabric/syncs.toml`, hand-editable, `fabric sync reload`
  applies live (mirrors `peers.toml` / `reload-peers`). Entry =
  `{name, folder, peers, policy, include?}`. `name` is the shared logical key
  (same name on two boxes = same sync); `peers = "*"` follows the `peers.toml`
  allow-list.

## Next

0. **ROAMING GAP — the shareability blocker (Nathan 2026-07-22). DEPLOYED
   2026-07-22 (see the deploy summary below).** A peer that changes network/public IP went
   unreachable both ways until its daemon was manually restarted. Root cause:
   fabric only reacted to LOCAL netmon changes and never probed peer reachability.
   - **Fix landed (commit `298a593`):** `run_peer_health_loop` echo-probes each peer
     every `FABRIC_PEER_HEALTH_SECS` (default 20s) and, on N consecutive failures,
     drives recovery (drop tunnels + iroh `network_change()` → re-discover/relay;
     recycle only after repeated nudges fail) — no local-change dependency. Pure
     `PeerHealthTracker` decision core with escalating backoff, fault-injection
     unit-tested. Emits per-probe latency + direct/relay telemetry. `=0` disables.
   - **Staged, NOT deployed:** the running daemon is still `9f5391b`. Deploy =
     `./install.sh` + restart, which blips cos's only path to hetz → **Nathan-gated
     window** with him present. Rollback ready: `~/.local/bin/fabric.prev` = the
     known-good `9f5391b` binary (restore + restart to revert). Live validation:
     move a laptop between networks, confirm self-heal without a manual restart.
   - Analysis + reliability data + nix design: `docs/multi-machine-reliability-2026-07-22.md`.

0b. **`fabric exec` — non-interactive remote command execution (Nathan 2026-07-22).
   DEPLOYED 2026-07-22 (see the deploy summary below).** `fabric exec <peer> --
   <cmd>` = scriptable counterpart to `shell` (stream stdout/stderr, propagate
   exit code). DEFAULT-DENY per machine via `allow_exec` (`--allow-exec` on
   up/daemon/service install), separate ALPN `fabric/exec/0`. Validated with a
   local two-node e2e test (allow + deny + stderr split + exit codes); that test
   caught a real bug — `dial_alpn` routed exec through the mux tunnel whose Hello
   frame corrupted the argv; exec now uses the raw dial path like shell.
   **DEPLOYED 2026-07-22 (tag `deploy-roaming-exec-bootout` = `1964c99`):** roaming
   self-heal (`298a593`) + `fabric exec` (`4d02a01`/`4fe8557`) + service-install
   idempotency (`83a3614`), ZERO isolation (isolation was decoupled). BOTH machines
   on `0.2.0+1964c99`: Mac (launchd-managed) + hetz (`fabric-keepalive.service`).
   Verified both ends — link direct both ways, node identities preserved, roaming
   `peer_health_probe` firing both directions (each sees the other reachable),
   `fabric exec hetzner -- echo` exit 0 (cos's test + mine). hetz exec enabled via
   `allow_exec=true` in its config.toml (keepalive unit passes only `--allow-shell`;
   both `[[exposes]]` pty-remote + st-sync preserved). Deploy method that made it
   clean: build-verify-before-swap; Nathan fired the hetz `systemctl --user restart`
   from ssh (outside the daemon cgroup); rollbacks staged (Mac `fabric.prev`=9f5391b,
   hetz `fabric.prev`=0.1.7+940afd1 recovered from `/proc`). Remaining: Nathan's
   laptop-roaming self-heal test.

   **SEPARATE follow-up (on main `a9b336e`, NOT deployed — needs NO Nathan
   window):** dev/prod isolation: service-install refuses a non-default home,
   down/restart home-mismatch guard, README FABRIC_HOME-for-dev convention.
   Standalone validation before it lands live: default/prod-home `service install`
   still succeeds; a non-default home is refused; empty-home `down`/`restart`
   warns. Design: `docs/dev-prod-isolation.md`.

   **Deferred:** the `fabric dev` subcommand (env convention covers it), the
   Erlang idle-skip probe refinement (low urgency), and `fabric cp`/discovery
   (await Nathan's product prioritization — see
   `docs/liveness-and-product-gaps-2026-07-22.md`).

1. **Fleet redeploy before the hetz proof** (CoS-coordinated). The *installed*
   fabric binaries are stale/pre-sync (`~/.local/bin/fabric` was 0.1.7+940afd1).
   A machine only serves/dials sync after its running daemon is **restarted**
   onto the 0.2.0 binary — a fresh file on disk is not enough. fabric-claude owns
   build + `./install.sh` + `fabric restart`; the Mac daemon restart blips the
   live network so the CoS sequences the window; the CoS drives the hetz
   pull+build+restart.
2. Run the **real hetz proof** (Mac → Hetzner): convoy declares the catalog on
   both (per-network name `convoy-catalog-<network>`), drop a `host=hetz` job in
   the Mac catalog, it appears on hetz, hetz's `convoy up` launches it. This is
   the done-bar.
3. Evaluate **iroh-docs 0.101.0** (iroh `^1`, range-based set reconciliation,
   LWW, iroh-blobs content) as backend #2 and green the SAME conformance suite
   against it. If healthy it likely becomes the production backend; `fast_rsync`
   stays a large-file delta optimization only.

## Open items / known gaps

- **Per-peer exec/shell ACL (feature gap, raised 2026-07-24).** `allow_shell` /
  `allow_exec` live on `FabricConfig` (daemon-global) — enabling either opens the
  capability to *every* trusted peer. There is no per-peer allow field on `Peer`
  (peers.toml), so "allow exec from the Air but not from hetz" is not expressible
  today. Real future work: per-peer capability scoping. README now documents the
  current global scope.
- **Backlog (cos, 2026-07-24, do-not-act-yet): unify `peers.toml` + config.toml +
  `syncs.toml` into ONE config file.** Parked pending greenlight.
- `bus` tombstone-sweep TTL is not implemented yet (deletes propagate; sweep is a
  no-op). Fine — bus is a later consumer (smalltalk).
- mtime is carried in the manifest but not restored to disk (ordering is logical,
  not mtime; disk mtime preservation is a future nicety).
- Changing an existing entry's `folder` needs a daemon restart to re-point its
  watcher; add/remove of entries is picked up live by `fabric sync reload`.
- The folder scan uses blocking `std::fs` on the async task; fine for small
  catalogs, revisit (spawn_blocking) if large trees appear.

## Coordination

- **convoy-claude** is the first consumer (its network catalog), wire-ready and
  standing by for the "green on main" ping.
- **cos-claude** is the supervisor; direct-on-main landing, push to origin.
