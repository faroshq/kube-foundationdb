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
| deeper pipeline (1024) | 87 ms | no further gain — drain stalls were gone |
| **denormalized values (`byKeyData` subspace, shipped)** | **38 ms** | values chunked under `(key, rev, offset)` in a parallel subspace; each list batch fetches them with one contiguous WantAll range read merge-joined against the index scan |
| WantAll index scan (shipped, marginal) | 38 ms | round trips weren't the remaining cost |
| etcd v3.6.4 | 7.7 ms | single contiguous range stream |

Overall: **372 ms → 38 ms (9.8×)**. Writes are unchanged within noise (each
write additionally stores its value chunks in `byKeyData`; tombstones are
skipped). Compaction clears all three subspaces. **Migration is lazy**: records
that predate `byKeyData` simply miss the merge-join and fall back to pipelined
`byRevision` point reads (`fetchLegacy`), so mixed-format directories work and
converge as objects are rewritten — covered by
`TestListFallsBackWithoutDenormalizedData`.

## Remaining gap

The residual ~30 ms for 2000 objects is spread across per-entry Go processing
(tuple unpacks for ~4000 index entries and ~4000 chunks), gRPC marshaling of
the ~3 MB response, and scanning not-yet-compacted superseded revisions
(the bench holds 2 revisions/key; steady state after compaction is leaner).
Ordered next steps:

1. **Count without a full scan.** `Count` scans the whole prefix; the
   apiserver calls it per resource every minute for `storage_objects`
   metrics. Options: `GetEstimatedRangeSizeBytes`-based estimate, maintained
   per-prefix counters (atomic adds), or a short-lived cached count.
2. Cheaper entry decoding (avoid full tuple unpack for chunk keys; the rev
   and offset positions are fixed).
3. Split large scans by shard boundaries (`GetRangeSplitPoints`) and read
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

## Reference numbers (after the pipelining fix)

| Workload | kube-foundationdb | etcd v3.6.4 |
|---|---|---|
| create (guarded txn) | 2677 ops/s, p50 7.1 ms | 2401 ops/s, p50 7.3 ms |
| get+update (guarded txn) | 2771 ops/s, p50 6.9 ms | 1332 ops/s, p50 13.9 ms |
| point get | 16.2k ops/s, p50 1.1 ms | 55.7k ops/s, p50 0.3 ms |
| list 2000 objects | 44 ops/s, p50 88 ms | 525 ops/s, p50 7.7 ms |
| watch delivery | p50 1.0 ms | p50 <10 µs |
