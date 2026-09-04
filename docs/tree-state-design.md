# Group the sync tree representations

Status: design for review. Do not implement this design before the review.

## Decision

Replace the separate disk-state collections with one immutable disk snapshot
and a small change overlay. Use stable revisions instead of complete manifest
copies for change detection.

This design keeps the manifest and local disk evidence as separate concepts.
It groups their storage and their snapshot mechanism. It does not make local
disk evidence part of the replicated manifest.

One clean pass will build two full tree containers. The current pass builds 13.
The two remaining containers are the two disk scans required around the peer
step.

## Evidence

Build `0.2.1+48208e4` has five literal full-state clone operations in one clean
`sync_once`. It has 11 full map or set collections and 13 tree-wide containers.

The current 29,369-path bus entry gave these live sizes:

- One complete manifest clone requests approximately 13.51 MB.
- One observed raw-table clone requests approximately 5.67 MB.
- Four passes and two guarded inbound transactions made 17 full manifest
  clones during one 30-second window.
- The manifest clones alone requested approximately 230 MB in that window.

Five post-release 30-second allocation windows had a 50.27 GiB/hour median.
A low 10-second window with no sync pass requested 6.9 MB. The low rate is 2.3
GiB/hour.

The low rate proves that the daemon has no expensive general baseline. The
tree operation creates the high rate.

## Current clean-pass count

The count below covers a steady delta peer. A missing peer cursor adds a full
wire manifest.

| Phase | Full tree container |
|---|---|
| Before scan 1 | protected observed-map clone |
| Scan 1 | scanned-file vector |
| Scan 1 | present-path set |
| Scan 1 | replacement scan-cache map |
| Scan 1 | previous observed-map clone |
| Scan 1 | current observed map |
| Before the peer step | baseline observed-map clone |
| Before the peer step | baseline manifest clone |
| Scan 2 | scanned-file vector |
| Scan 2 | present-path set |
| Scan 2 | replacement scan-cache map |
| Scan 2 | previous observed-map clone |
| Scan 2 | current observed map |

The wire client clones its delta into the header. A clean delta is small, so it
is not a full-tree container. The wire client or server clones a full manifest
when its peer cursor is absent.

## Invariants

The new representation must keep these rules:

1. A local delete needs affirmative absence evidence.
2. An unreadable root or ancestor never proves a delete.
3. A remote-only Present is not local delete evidence.
4. A disk edit during a peer step wins over stale remote content.
5. A path outside the local include is not read, written, or tombstoned.
6. The manifest entry and observed receipt reach durable storage together.
7. The scan cache uses only facts read from this machine.
8. A clean pass does not write durable state.
9. No lock spans a peer dial.
10. A failed or cancelled pass leaves the last published disk view valid.

The first four rules have deleted or restored real files when broken. An
allocation reduction cannot weaken them.

## Data model

`EntryState` will replace `observed`, `scan_cache`, and `scan_issues` with one
`DiskState`.

```rust
struct DiskState {
    base: Arc<DiskSnapshot>,
    overlay: Arc<DiskOverlay>,
    revision: u64,
    durable_revision: u64,
    changed: PathChangeBuffer,
}

enum RootState {
    Complete,
    Missing,
    Unreadable,
}

struct DiskSnapshot {
    paths: HashMap<Arc<str>, DiskRecord>,
    root_state: RootState,
}

struct DiskOverlay {
    paths: HashMap<Arc<str>, Option<DiskRecord>>,
}

enum DiskRecord {
    File(FileReceipt),
    Directory,
    BlockingFile,
    Unknown(ScanIssue),
}

struct FileReceipt {
    hash: ContentHash,
    cache: Option<ScanCacheEntry>,
}

struct DiskView {
    base: Arc<DiskSnapshot>,
    overlay: Arc<DiskOverlay>,
    revision: u64,
}
```

The snapshot owns one normalized path allocation. Records and indexes share
that path through `Arc<str>`. The snapshot does not retain an absolute path.
The engine derives an absolute path from the entry root when it needs one.

`Complete` permits affirmative absence. `Missing` and `Unreadable` permit no
delete inference. An unknown path record blocks absence for itself and every
descendant.

The overlay contains only paths changed by materialization after the last
scan. A baseline clones two `Arc` values and one revision. It never clones the
tree.

`DiskView` supplies these operations:

- `file(path)` returns the current observed receipt.
- `cache(path)` returns local size, mtime, mode, and hash facts.
- `absence(path)` returns present, affirmatively gone, or unknown.
- `files()` iterates observed regular files.
- `issues()` iterates paths with unknown state.
- `changed_since(revision)` returns changed paths only.

The methods merge the overlay with the base. Callers do not materialize a
second map.

`EntryState` holds `DiskState` in one standard mutex. No code holds that mutex
across an await point. The operation guard remains the async coordination
boundary.

## Scan flow

The scanner will build `DiskSnapshot` directly.

1. Capture the current `DiskView` by cloning its two `Arc` values.
2. Walk the folder once.
3. Reuse an existing shared path when the normalized path is unchanged.
4. Reuse a content hash only when the old local cache facts match exactly.
5. Store files, directories, blocking files, and unknown paths in one map.
6. Keep changed file bytes in a change-sized temporary list.
7. Compare the new snapshot with the old view.
8. Apply local writes and affirmative deletes to the node.
9. Publish the new snapshot and an empty overlay in one operation.

The scanner does not build a scanned-file vector, a present-path set, a cache
map, a previous observed map, or a current observed map.

The path map still has one full table allocation per scan. Reused `Arc<str>`
keys make unchanged paths increase reference counts instead of allocating new
path strings.

An absent key proves absence only when `root_state` is complete and no unknown
or blocking ancestor exists. This is the current affirmative-absence rule in a
single lookup surface.

## Materialize flow

Materialization receives a stable `DiskView` as its protected baseline. It
iterates the manifest by reference.

A file write or removal adds one path to the overlay. The overlay records the
observed hash after a successful write. A later scan records new cache facts.

The second scan folds the overlay into a new base. A failed second scan leaves
the earlier base and overlay unchanged.

This preserves the concurrent-edit guard. The baseline remains immutable while
an inbound transaction or another local operation publishes a new view.

## Manifest revisions

`SyncNode` will add a `manifest_revision` field. Every logical manifest change
increments it through one private mutation surface.

The engine will carry a revision across a peer step. It will compare revisions
instead of cloning the manifest. A changed revision can cause an extra durable
write, but it cannot hide a change.

The mutation surface covers these operations:

- a local write;
- a local delete;
- a peer adoption;
- a tombstone sweep;
- a durable-state load.

The code will not expose mutable manifest access. A property test will compare
the manifest before and after every generated node operation. A changed
manifest must always have a changed revision.

The revision does not replace the digest. The wire protocol still uses the
digest to compare two machines. The revision answers only whether this local
node changed during one operation.

## Inbound transactions

`PreparedInboundMode::Guarded` will carry a `DiskView` and a manifest revision.
It will not carry a complete observed map or manifest.

The preparation scan publishes a new disk view. The completion scan compares
against the stable baseline. The operation guard keeps the existing lock order
and still spans the wire session.

The exact no-op path remains unchanged. It does not scan or materialize.

## Durable state

The disk format and wire format will not change in this work.

`state.json` will still contain `manifest`, `observed`, `scan_cache`, and
`peer_acks`. The serializer will stream `observed` and `scan_cache` views from
`DiskView`. It will not build temporary maps.

The append log will keep `LoggedChange`. Persistence will take the union of the
manifest change buffer and the disk change buffer. It will read the current
manifest entry and observed receipt for each changed path.

One operation guard protects that read. The append reaches disk before either
durable cursor advances. Thus the manifest entry and observed receipt keep the
existing crash boundary.

`persisted_observed` will be removed. The disk change buffer becomes the exact
source of observed paths that need a log record.

Snapshot compaction will serialize directly from the manifest and disk view.
It may allocate the JSON output buffer because that buffer is the durable
artifact. It must not allocate another tree collection.

An old build can read the new state files. A new build can read old state files
and construct one `DiskSnapshot`. A mixed-version fleet uses the unchanged wire
protocol.

## Lock boundaries

The entry operation guard keeps its current scope. The node lock and disk lock
keep the order `node` then `disk`.

No lock spans an outbound peer dial. A `DiskView` and two revisions cross that
boundary.

Snapshot serialization runs under the operation guard. The implementation must
measure the node-lock duration for 29,337 paths. It must not exceed the current
clone plus equality window without a separate review.

## Implementation boundary

Implement this as one pull request with reviewable commits. A partial rollout
that keeps the old collections beside the new snapshot would increase memory
and create two authorities.

The commits can follow this order:

1. Add the red allocation measurement and the semantic property tests.
2. Add `DiskSnapshot`, `DiskOverlay`, and read-only adapters.
3. Switch scan, materialize, status, and sweep to `DiskView`.
4. Add manifest revisions and remove engine manifest snapshots.
5. Switch persistence to change cursors and streaming serialization.
6. Remove the old collections and temporary adapters.

No commit in the final pull request will retain both representations as live
authorities.

## Red contracts

The first commit must record the current failure before the fix.

The allocation contract will run one clean pass over a deterministic large
tree. It will count complete tree container construction. The current code must
report 13. The fixed code must report two disk snapshots and no complete
manifest clone.

The count is not the only proof. These property tests must pass before and
after the change:

- an unreadable root cannot create tombstones;
- an opaque ancestor cannot create descendant tombstones;
- a remote-only Present cannot become a local delete;
- a local delete before watcher delivery becomes a tombstone;
- a local edit during the peer step is not overwritten;
- excluded paths stay local and untouched;
- a cancelled scan keeps the previous published view;
- snapshot plus log replay restores the same manifest and disk receipts;
- generated manifest changes always advance the manifest revision;
- generated disk changes always enter the disk change buffer;
- three-node generated histories still converge.

The deterministic large-tree measurement will also record requested bytes and
CPU time. Every number will include its measurement window.

## Live acceptance

The rollout will use the same live probes as build `0.2.1+48208e4`.

1. Deploy hetz first after release approval.
2. Prove mixed-version ping, exec, send-file, and sync health.
3. Deploy Silber.
4. Prove equal clean digests and both doctor reports.
5. Count full manifest clones in matched 10-second windows.
6. Run five allocator windows of 30 seconds each.
7. Compare both the median and the 150-second aggregate.
8. Record bus pass, scan, no-op, and guarded counts for every window.

A healthy steady pass must make no full manifest clone. Conditional full wire
payloads must match cursor loss or first contact. The disk snapshot counter must
increase twice per `sync_once` and once per inbound scan.

The 13-to-2 count does not predict an 84.6 percent byte reduction because the
current containers have different element sizes. The live allocator result is
the acceptance result.

The change succeeds only if the total allocation rate moves. A lower container
count without a lower live rate is a failed prediction.

## Deferred work

This design keeps two complete disk walks per outbound pass. The second walk is
the current concurrent-edit proof. Removing it needs a new source of affirmative
disk evidence and a separate design.

This design does not change tombstone policy, include policy, peer selection,
the wire protocol, or the durable schema. It does not add incremental filesystem
trust beyond the existing watcher.

The two remaining snapshots still scale with the tree. After this change, the
live measurement will decide whether the next work targets the walks or whether
Fabric has reached its idle-cost goal.

## Review questions

1. Does one `DiskRecord` preserve every current absence state?
2. Does the overlay keep the concurrent-edit baseline immutable?
3. Does the paired change-cursor write preserve crash consistency?
4. Is a local manifest revision sufficient for final change detection?
5. Is two full disk snapshots per clean pass an acceptable first boundary?
