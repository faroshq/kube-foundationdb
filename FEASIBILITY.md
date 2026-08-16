# kube-foundationdb — Feasibility: FoundationDB as an etcd replacement for Kubernetes

**Date:** 2026-08-15
**Verdict: Feasible, and largely proven.** A Go implementation already exists ([melgenek/f8n](https://github.com/melgenek/f8n), Apache-2.0) that passes Kubernetes `sig-api-machinery` conformance tests. Clever Cloud runs an etcd-compatible layer on FoundationDB in production for their managed Kubernetes. The decision is less "can it be done" and more "build vs. fork vs. contribute".

---

## 1. Why this is attractive

- etcd's practical ceiling: ~8 GB recommended DB size, single Raft group (all writes serialized through one leader), full-copy replication — a known bottleneck for large clusters and multi-tenant control planes (kcp-style many-logical-cluster setups especially).
- FoundationDB: ordered, transactional, horizontally scalable KV store with ACID across the cluster, proven at Apple/Snowflake scale. One FDB cluster could back many Kubernetes control planes (subspace per cluster / per shard).
- The etcd API surface Kubernetes actually uses is small and well understood — this is exactly what kine exploits.

## 2. What Kubernetes needs from etcd

The apiserver uses a narrow subset of etcd v3 gRPC:

| API | Usage |
|---|---|
| `Range` | Get + paginated List (limit/continue), reads at a specific revision |
| `Txn` | Only the guarded-write pattern: compare `mod_revision` → put/delete (optimistic concurrency) |
| `Watch` | Streaming mutations from a start revision, with prev-KV, progress notify |
| `Lease` | TTLs (events, apiserver master leases) |
| `Compact` | History garbage collection |
| Maintenance/Status | Health checks, DB size |

No general-purpose etcd semantics needed (no arbitrary Txn trees, no auth API, no member management toward the client).

## 3. Mapping onto FoundationDB

### FDB constraints that shape the design
- **Transactions ≤ 5 s, ≤ 10 MB affected data** ([known limitations](https://apple.github.io/foundationdb/known-limitations.html))
- **Values ≤ 100 KB, keys ≤ 10 KB** — Kubernetes objects go up to ~1.5 MB (etcd request limit), so **value chunking across multiple keys is mandatory**
- FDB's own MVCC window is only ~5 s — etcd must serve reads/watches at older revisions, so **the layer must materialize its own revision history** (like kine does in SQL); FDB is the durable store, not the MVCC engine
- FDB watches are single-key change notifications, not mutation streams

### Design (the f8n / forum-consensus shape)
- **Revisions:** use FDB's commit version / versionstamps. The 8-byte commit version is an int64, monotonic, and assigned at commit with no hot key — it maps directly onto etcd's revision without a sequencer bottleneck.
- **Key layout (subspaces):**
  - `data/{key}/{revision}` → chunked value + metadata (create_rev, mod_rev, version, lease) — full history until compaction
  - `log/{versionstamp}` → change log for watch streaming
  - `lease/{id}` and an expiry index for the TTL sweeper
- **Watch:** poll/range-read the `log/` subspace from the watcher's last revision; use an FDB watch on a "latest revision" key as a wakeup to avoid busy-polling. This reproduces etcd's "stream of mutations" exactly — the only correct approach per the [FDB forum discussion](https://forums.foundationdb.org/t/a-foundationdb-layer-for-apiserver-as-an-alternative-to-etcd/2697).
- **Lists:** serve paginated ranges; pin list pages to a stored revision (history is materialized, so no 5 s problem); Record-Layer-style continuations if a single page's read exceeds limits.
- **Txn (guarded write):** read current mod_revision + compare + write in one FDB transaction — FDB's serializable transactions make this trivial and cross-key correct.
- **Leases & compaction:** background goroutines (TTL sweeper, revision GC below the compaction watermark).

### Deployment shape
`apiserver → (etcd gRPC) → shim (stateless, N replicas) → libfdb_c → FDB cluster`

The shim is stateless — all state including watch progress derives from the DB — so it can run as multiple replicas behind a service; apiserver's `--etcd-servers` points at it. HA story is arguably *simpler* than etcd (no Raft quorum in the shim; FDB handles replication).

## 4. Prior art

| Project | Language | Approach | Status |
|---|---|---|---|
| [f8n](https://github.com/melgenek/f8n) | **Go** | kine backend for FDB; records up to 2 MiB; lists, watches, TTL, compaction | v0.5.0 (Sep 2025), pushed Dec 2025; passes sig-api-machinery for K8s v1.33; subset of etcd robustness tests; Apache-2.0 |
| [fdb-etcd](https://github.com/PierreZ/fdb-etcd) | Java (Record Layer + Vert.x) | Standalone etcd layer | Experimental, useful for design reference |
| [Clever Cloud Materia KV](https://www.clever-cloud.com/blog/company/2025/06/27/why-we-finally-built-our-own-managed-kubernetes-etcd/) | proprietary | etcd-compatible KV on FDB | **In production** backing their managed K8s |
| [kine](https://github.com/k3s-io/kine) | Go | etcd shim over SQL/NATS; pluggable driver registry (`pkg/drivers`) | Mature, used by k3s/k0s/Rancher — the scaffolding to reuse |

## 5. Risks

1. **cgo / libfdb_c** — FDB's Go bindings require CGO and the C client lib pinned to the cluster's major.minor version. Complicates static builds and container images; manageable (multi-version client libs exist), but it's the main DX wart.
2. **Watch latency & fan-out** — polling-based watch adds latency vs etcd's in-memory notify; needs a shared watch-cache in the shim so 1000 watchers don't mean 1000 pollers. (Apiserver's own watch cache absorbs most of this.)
3. **Correctness long tail** — etcd semantics are subtle (compaction errors, progress notify, watch ordering guarantees, `WithPrevKV`). Mitigation: etcd's own robustness test suite + kine's test matrix; f8n already runs a subset.
4. **Operational cost of FDB itself** — running FDB well (roles, coordinators, tuning) is real work; the [fdb-kubernetes-operator](https://github.com/FoundationDB/fdb-kubernetes-operator) helps. Only worth it if you're beyond etcd's comfort zone or already run FDB.
5. **Performance unknowns for this workload** — per-object write latency will likely be fine (FDB commits are fast); large LIST-heavy workloads and compaction churn need benchmarking (kube-burner vs. etcd baseline).

## 6. Recommended path

**Don't start from scratch.** Two viable routes:

- **Route A (fastest): build on kine + f8n.** Evaluate f8n as-is; fork or contribute if it's 80% there. All etcd-gRPC plumbing, watch bookkeeping, and the K8s-facing test matrix come for free from kine. Weeks to a working PoC.
- **Route B (most control): standalone shim** implementing the etcd gRPC subset directly on FDB (fdb-etcd's shape, in Go). Justified only if kine's driver abstraction blocks FDB-native wins (versionstamp revisions, multi-tenancy subspaces, watch efficiency) — worth deciding after reading f8n's driver code.

### Milestones
1. **Spike (days):** run f8n against a local FDB + kind cluster (`--etcd-servers` → shim); confirm conformance claims; read its kine-driver code for gaps (multi-tenancy, chunking limits, watch scaling).
2. **PoC:** own driver or fork; pass kine's test suite + sig-api-machinery against a real apiserver.
3. **Correctness:** etcd robustness tests (linearizability/watch guarantees), fault injection (kill shim replicas, FDB recoveries).
4. **Performance:** kube-burner / etcd-benchmark vs. etcd baseline; measure watch latency, list throughput, 5k-node-scale object counts.
5. **Multi-tenancy (the differentiator):** subspace-per-control-plane so one FDB cluster backs N apiservers/kcp shards — this is the thing nobody has polished yet.
6. **Ops:** container images bundling libfdb_c, TLS, metrics, FDB-operator-based deploy example.

## Sources
- https://github.com/melgenek/f8n
- https://github.com/PierreZ/fdb-etcd
- https://github.com/k3s-io/kine
- https://forums.foundationdb.org/t/a-foundationdb-layer-for-apiserver-as-an-alternative-to-etcd/2697
- https://apple.github.io/foundationdb/known-limitations.html
- https://www.clever-cloud.com/blog/company/2025/06/27/why-we-finally-built-our-own-managed-kubernetes-etcd/
- https://pkg.go.dev/github.com/apple/foundationdb/bindings/go
- https://medium.com/@PlanB./goodbye-etcd-why-one-developer-rebuilt-kubernetes-on-foundationdb-09c7568c31a8
