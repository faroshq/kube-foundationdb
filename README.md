# kube-foundationdb

An etcd shim backed by [FoundationDB](https://www.foundationdb.org/), letting
Kubernetes-style API servers — including [kcp](https://kcp.io) — use a
FoundationDB cluster as their storage backend instead of etcd.

```
kcp / kube-apiserver ── etcd v3 gRPC ──> kube-foundationdb ── libfdb_c ──> FoundationDB
```

The shim is [kine](https://github.com/k3s-io/kine) with a FoundationDB driver.
The driver (`pkg/drivers/fdb`) is derived from the Apache-2.0
[f8n](https://github.com/melgenek/f8n) project (see `NOTICE`): etcd revisions
map to FDB versionstamps (one transaction per commit batch makes the 8-byte
commit version a unique, monotonic int64), full revision history is materialized
in FDB subspaces (`by-revision`, `by-key-and-revision`, watch log, compaction
watermark), values are split across keys to stay under FDB's 100 KB value
limit, and watches stream from the revision log. Background loops handle TTL
(leases) and compaction. See `FEASIBILITY.md` for the design research.

## Layout

- `cmd/kube-foundationdb` — the shim binary (kine app + FDB driver)
- `pkg/drivers/fdb` — the FoundationDB kine driver
- `test/smoke` — etcd clientv3 round-trip tests against a running shim
- `test/e2e` — full e2e: boots the shim + a real `kcp` binary on top of it
- `.fdb/` (gitignored) — local FoundationDB install: client lib, headers,
  `fdbserver`/`fdbcli`, cluster file, data

## Local FoundationDB (macOS, no sudo)

The official `FoundationDB-<ver>_arm64.pkg` from
[apple/foundationdb releases](https://github.com/apple/foundationdb/releases)
was expanded with `pkgutil --expand-full` (no installation, no sudo) and the
needed pieces copied into `.fdb/`:

```
.fdb/bin/{fdbserver,fdbcli,fdbmonitor}
.fdb/lib/libfdb_c.dylib          # install name @rpath/libfdb_c.dylib
.fdb/include/foundationdb/*.h
.fdb/fdb.cluster                 # kube:fdb@127.0.0.1:4500
```

Binaries link against it via `CGO_LDFLAGS="-L.fdb/lib -Wl,-rpath,.fdb/lib"` —
the Makefile sets this up.

## Usage

```sh
make fdb-start     # single-node FoundationDB on 127.0.0.1:4500 (idempotent)
make build         # builds bin/kube-foundationdb
make run           # shim on 127.0.0.1:2379

# elsewhere:
kcp start --etcd-servers http://127.0.0.1:2379
# or
kube-apiserver --etcd-servers http://127.0.0.1:2379 ...
```

## Tests

```sh
make test-driver   # driver suite against real FDB (~30 s)
make test-smoke    # etcd clientv3 put/get/txn/list/watch (shim must be running: make run)
make test-e2e      # boots shim + kcp binary, CRUD + watch + Workspace scheduling (~30 s)
```

### Upstream Kubernetes conformance

The official Kubernetes conformance suite (sig-api-machinery focus) can be run
against a k3s cluster whose datastore is this shim — the same harness the f8n
project uses:

```sh
make conformance-shim   # terminal 1: shim on 0.0.0.0:2380
make conformance-k3s    # k3s in Docker, datastore -> shim
make conformance        # registry.k8s.io/conformance, sig-api-machinery focus
```

The skip list mirrors f8n's (features orthogonal to storage). Note the
apiserver runs with `etcd-compaction-interval=10s` to stress the shim's
compaction path.

`test-e2e` expects a kcp binary; override with `KCP_BINARY=/path/to/kcp`
(default: `../../kcp-dev/kcp/bin/kcp` in the classic GOPATH layout). The e2e:

1. starts `kube-foundationdb` on a random port with a per-run FDB directory,
2. starts `kcp start --etcd-servers http://127.0.0.1:<port>` (embedded etcd disabled),
3. waits for `/readyz`,
4. namespace + configmap create/get/update/list/delete,
5. verifies watch events are delivered,
6. creates a `tenancy.kcp.io/v1alpha1` Workspace and waits for phase `Ready` —
   kcp's workspace scheduling exercises controller watch loops end to end.

## Benchmarks

`test/bench` measures apiserver-shaped workloads (guarded-txn create/update,
point gets, prefix lists, watch delivery latency) against any etcd-compatible
endpoint — the general-purpose etcd benchmark tool can't be used because kine
shims only serve the apiserver's API subset. Run `make bench` against a running
shim, or `.github/workflows/bench.yml` for the shim-vs-etcd comparison.

Indicative numbers (M-series MacBook, single-node FDB 7.3.77 ssd engine vs
etcd v3.6.4, 20 workers, 1.5 KB values):

| Workload | kube-foundationdb | etcd v3.6.4 |
|---|---|---|
| create (guarded txn) | 2677 ops/s, p50 7.1 ms | 2401 ops/s, p50 7.3 ms |
| get+update (guarded txn) | 2771 ops/s, p50 6.9 ms | 1332 ops/s, p50 13.9 ms |
| point get | 16.2k ops/s, p50 1.1 ms | 55.7k ops/s, p50 0.3 ms |
| list 2000 objects | 44 ops/s, p50 88 ms | 525 ops/s, p50 7.7 ms |
| watch delivery | p50 1.0 ms | p50 <10 µs |

Writes are competitive; reads pay one FDB network round-trip; large unpaginated
LISTs are the main remaining gap (down from 372 ms after pipelining the
per-record reads — see [docs/performance.md](docs/performance.md) for the
analysis and the denormalization plan to close the rest).

## CI

GitHub Actions (`.github/workflows/`):

- `ci.yml` — on every PR/push: `validate` (gofmt, vet, tidy), driver tests
  against a real FoundationDB, etcd-client smoke tests, and the kcp e2e as a
  matrix over the latest three kcp releases (currently v0.32.3, v0.31.6,
  v0.30.3).
- `conformance.yml` — on main, nightly, and on demand: upstream
  sig-api-machinery conformance as a matrix over the latest three Kubernetes
  minors via k3s (currently v1.36.3, v1.35.7, v1.34.10).

Both install FoundationDB from the official debs via the local composite
action `.github/actions/setup-fdb`. Bump the pinned matrix versions as new
releases appear.

## Status / limitations

- Single kine limitation applies: no general-purpose etcd API (only the subset
  API servers use); auth/member APIs are not served.
- FDB constraints: records ≤ ~2 MiB (chunked), transactions bounded by FDB's
  5 s / 10 MB limits (paginated internally).
- The Go bindings require cgo and a `libfdb_c` matching the cluster's
  major.minor version (7.3 here).
