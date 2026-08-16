# List & watch performance: analysis and roadmap

Findings from profiling the driver against `test/bench` (apiserver-shaped
workloads, 2000 × 1.5 KB objects, single-node FDB 7.3.77, M-series MacBook).
etcd v3.6.4 on the same machine is the reference.

## How a LIST executes today

The store is two subspaces: `by-key-and-revision` (key → per-revision metadata,
the "index") and `by-revision` (revision → record with chunked value, the
"data"). A LIST:

1. range-scans the index across the whole prefix — **every retained revision
   of every key**, newest-per-key selected during the scan
   ([fdb_read.go](../pkg/drivers/fdb/fdb_read.go) `recordCollector`);
2. for each surviving key, issues a **separate point range-read** into
   `by-revision` to fetch the record value (`listCollector`).

## What we measured and fixed

| Stage | list 2000 objs (p50) | Notes |
|---|---|---|
| baseline | 372 ms | secondary reads drained one at a time |
| pipelined secondary reads (256 in flight) | 88 ms | FDB issues range-read futures eagerly, so batching before draining lets the client pipeline them |
| denormalized values, rev-keyed (`byKeyData`) | 38 ms | one contiguous range read per list batch instead of a point read per record |
| **current-value subspace (`byKeyCurrent`, shipped)** | **25.5 ms** | values chunked under `(key, offset)` with no revision: rev=0 lists scan exactly the live bytes, superseded revisions are never transferred, and the subspace needs no compaction |
| tombstone-free writes (shipped, part of the above) | — | naive `ClearRange`-per-write left range tombstones that slowed scans until storage compaction; clears now happen only on delete and value-shrink, so steady-state same-size updates write clean |
| etcd v3.6.4 | 7.7 ms | single contiguous range stream |

Overall: **372 ms → 25.5 ms (14.6×)**; writes unchanged within noise.

### Why not a different proto marshaler

A 20 s CPU profile under pure list load answered this directly:
`RangeResponse.MarshalToSizedBuffer` (etcd's gogo-generated fast marshaler —
already non-reflective) accounts for **~2 % of CPU**; everything above the
driver (kine server + gRPC + proto) is ~3 %. The budget is dominated by cgo
crossings into the FDB client (~20 %), the FDB C network thread (~15 %), and
GC (~15 %). Swapping marshalers or forking kine for the serialization path
would not move the needle; bytes-scanned and allocation churn did.

### Consistency argument for `byKeyCurrent`

The current-value subspace is written in the same transaction as the index,
so within any read transaction (each list batch is one) snapshot isolation
guarantees it matches the newest index entry per key. The scan-side join
therefore only applies to records the index scan proved are the newest
revision of their key (`Record.LatestForKey`); revision-pinned reads of older
versions and records that predate the subspace fall back to pipelined
`byRevision` point reads (`fetchLegacy`) — which is also the lazy-migration
path, covered by `TestListFallsBackWithoutDenormalizedData`.

## Where the remaining time goes (clean-cluster decomposition)

Measured on a freshly initialized single-node FDB against fresh etcd
(the benchmark now compacts before the read phase, like the apiserver would):

| Scenario | list 2000 objs (p50) |
|---|---|
| clean floor: 1 revision/key, no tombstones | **22 ms** (etcd: 5.9 ms) |
| 2 revisions/key, uncompacted | 25.5 ms |
| immediately after compaction | ~39 ms (transient) |

Two useful negative results, both measured on a quiet machine:
`GOGC` tuning and snapshot reads (skipping read-conflict tracking) moved
nothing outside noise — earlier apparent gains were contamination from a
conformance suite running concurrently. Snapshot reads are kept anyway
(read-only scans never commit, so conflict tracking is pure overhead).

A third finding matters operationally: **compaction leaves range-clear
tombstones in the index range that slow scans by ~1.7× until FDB's ssd
engine cleans them lazily** (minutes, not seconds). The same effect made
naive `ClearRange`-per-write slow before we removed those. With the
apiserver compacting periodically, real-world lists sit between the 22 ms
floor and the post-compaction transient.

The 22 ms floor itself (3.7× etcd) is spread across FDB C-client request
processing and cgo conversions (~4000 KV entries + ~3 MB copied C→Go),
per-entry tuple decoding, allocation/GC churn, and pushing ~3 MB through
gRPC — no single dominant term. Ordered next steps if more is needed:

1. **In-shim consistent read cache** (etcd's watch-cache design one level
   down): the poll loop already streams every event; maintaining a current
   state map and serving rev=0 lists from memory after proving freshness
   (GRV ≥ poll cursor) would reach etcd-class latency. Real engineering:
   memory = full dataset, careful freshness proof. This is the only lever
   left with 3×+ potential.
2. **Count without a full scan.** `Count` scans the whole prefix; the
   apiserver calls it per resource every minute for `storage_objects`
   metrics. Options: `GetEstimatedRangeSizeBytes`-based estimate, maintained
   per-prefix counters (atomic adds), or a short-lived cached count.
3. Cheaper entry decoding / fewer allocations (would require forking the
   FDB Go bindings' KV conversion — moderate gain, high maintenance).
4. Split large scans by shard boundaries (`GetRangeSplitPoints`) and read
   sub-ranges in parallel — matters once data outgrows one storage server;
   irrelevant on a single node.

Note the practical mitigations already in place: the apiserver serves most
LISTs from its watch cache, and paginated LISTs (limit=500) bound the per-call
cost. The 2000-object unpaginated list is the worst case, not the common one.

## Watches

Delivery p50 is ~1 ms (was 10–30 ms before the post-commit poll kick — see the
CRD-finalizer stall analysis in the README/git history; FDB's own watch
notification latency was the floor, and local writes now bypass it).

Remaining ideas, in rough order of value:

1. **Write-through event publish**: the write path already knows the committed
   event; handing it to the poll loop directly (with the log scan as the
   ordering source of truth and multi-instance fallback) would shave the
   remaining ~1 ms read. Care needed: cross-writer ordering by versionstamp,
   dedup against the poll read. Value is modest — the apiserver's watch cache
   absorbs ~1 ms happily.
2. **Prefix-trie fan-out**: today every watcher's filter runs over every event
   batch (O(watchers × events) string prefix checks in `innerWatch`). A trie
   keyed by registry prefix would make dispatch O(events × depth). Matters at
   hundreds of watchers (one per resource type per apiserver).
3. **Native FDB changefeeds** would remove polling entirely, but they are not
   exposed in the public API — not actionable today.

## Reference numbers (current-value subspace, tombstone-free writes)

| Workload | kube-foundationdb | etcd v3.6.4 |
|---|---|---|
| create (guarded txn) | 2374 ops/s, p50 8.0 ms | 2401 ops/s, p50 7.3 ms |
| get+update (guarded txn) | 2385 ops/s, p50 8.0 ms | 1332 ops/s, p50 13.9 ms |
| point get | 15.6k ops/s, p50 1.2 ms | 55.7k ops/s, p50 0.3 ms |
| list 2000 objects | 152 ops/s, p50 25.5 ms | 525 ops/s, p50 7.7 ms |
| watch delivery | p50 1.0 ms | p50 <10 µs |
