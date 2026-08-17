# kube-foundationdb: etcd shim for Kubernetes/kcp backed by FoundationDB.
#
# FoundationDB client/server binaries are expected under .fdb/ (see README for
# how they were installed — no sudo required). All Go builds need cgo pointed
# at the bundled libfdb_c.

REPO_ROOT   := $(shell pwd)
FDB_DIR     := $(REPO_ROOT)/.fdb
FDB_CLUSTER := $(FDB_DIR)/fdb.cluster

export FDB_CLUSTER_FILE := $(FDB_CLUSTER)
export CGO_CFLAGS  := -I$(FDB_DIR)/include
export CGO_LDFLAGS := -L$(FDB_DIR)/lib -Wl,-rpath,$(FDB_DIR)/lib

KCP_BINARY ?= $(REPO_ROOT)/../../kcp-dev/kcp/bin/kcp

.PHONY: build
build:
	go build -o bin/kube-foundationdb ./cmd/kube-foundationdb

.PHONY: fdb-start
fdb-start: ## start a single-node FoundationDB server (idempotent)
	@if $(FDB_DIR)/bin/fdbcli -C $(FDB_CLUSTER) --exec "status minimal" --timeout 5 2>/dev/null | grep -q "available"; then \
		echo "FoundationDB already running"; \
	else \
		mkdir -p $(FDB_DIR)/data $(FDB_DIR)/logs; \
		nohup $(FDB_DIR)/bin/fdbserver -p 127.0.0.1:4500 -C $(FDB_CLUSTER) \
			--datadir $(FDB_DIR)/data --logdir $(FDB_DIR)/logs \
			> $(FDB_DIR)/logs/fdbserver.out 2>&1 & \
		sleep 2; \
		$(FDB_DIR)/bin/fdbcli -C $(FDB_CLUSTER) --exec "configure new single ssd" --timeout 20 || true; \
		$(FDB_DIR)/bin/fdbcli -C $(FDB_CLUSTER) --exec "status minimal" --timeout 20; \
	fi

.PHONY: fdb-stop
fdb-stop:
	pkill -f "fdbserver -p 127.0.0.1:4500" || true

.PHONY: run
run: build fdb-start ## run the shim on 127.0.0.1:2379
	./bin/kube-foundationdb --listen-address 127.0.0.1:2379

.PHONY: test-driver
test-driver: fdb-start ## driver unit/integration tests (real FDB)
	FDB_CONNECTION_STRING="kube:fdb@127.0.0.1:4500" go test ./pkg/drivers/fdb/ -count=1 -timeout 600s

.PHONY: test-smoke
test-smoke: ## etcd clientv3 smoke tests; needs `make run` in another terminal
	go test ./test/smoke/ -v -count=1

.PHONY: test-e2e
test-e2e: build fdb-start ## full e2e: kcp binary running on FoundationDB
	KCP_BINARY=$(KCP_BINARY) go test ./test/e2e/ -v -count=1 -timeout 900s

.PHONY: vet
vet:
	go vet ./...

.PHONY: test-robustness
test-robustness: fdb-start ## etcd robustness harness: kube traffic + linearizability validation
	cd test/robustness && FDB_CONNECTION_STRING="kube:fdb@127.0.0.1:4500" go test . -count=1 -timeout 1800s

.PHONY: docker
docker: ## build the linux container image
	docker build -t kube-foundationdb:dev .

.PHONY: bench
bench: ## kube-style workload benchmark against a running shim (see test/bench)
	go run ./test/bench --endpoint http://127.0.0.1:2379 --prefix /registry/bench-$$(date +%s)

# ---- Upstream Kubernetes conformance (sig-api-machinery) ----
# k3s runs in Docker with this shim as its datastore; the official conformance
# image then runs the sig-api-machinery suite against it. Mirrors f8n's harness.

K3S_VERSION ?= v1.34.1
CONFORMANCE_SKIP ?= StorageVersionAPI|DynamicResourceAllocation|MutatingAdmissionPolicy|CoordinatedLeaderElection|VolumeAttributesClass|OrderedNamespaceDeletion|Slow|Flaky|should\shonor\stimeout|verify\sResourceQuota\swith\sterminating\sscopes

.PHONY: conformance-shim
conformance-shim: build fdb-start ## shim for the k3s conformance cluster (0.0.0.0:2380)
	./bin/kube-foundationdb --listen-address 0.0.0.0:2380 \
		--metrics-bind-address 127.0.0.1:18081 --fdb-directory k3s-conformance

.PHONY: conformance-k3s
conformance-k3s: ## k3s in Docker using the shim as datastore (run conformance-shim first)
	docker rm -f k3s 2>/dev/null || true
	docker volume rm -f kubeconfig 2>/dev/null || true
	docker run -d --name k3s --privileged \
		-e K3S_DATASTORE_ENDPOINT="http://host.docker.internal:2380" \
		-e K3S_TOKEN=devtoken \
		-v kubeconfig:/etc/rancher/k3s \
		docker.io/rancher/k3s:$(K3S_VERSION)-k3s1 server \
		--kube-apiserver-arg=etcd-compaction-interval=10s \
		--disable=coredns,servicelb,traefik,local-storage,metrics-server \
		--kube-apiserver-arg=feature-gates=WatchList=true \
		--disable-network-policy
	@until docker exec k3s kubectl get --raw /readyz >/dev/null 2>&1; do sleep 2; done
	@echo "k3s ready on FoundationDB"

.PHONY: conformance
conformance: ## run upstream sig-api-machinery conformance against the k3s cluster
	docker rm -f kubeconformance 2>/dev/null || true
	docker run --rm --name kubeconformance \
		--network container:k3s \
		-e KUBECONFIG="/etc/rancher/k3s/k3s.yaml" \
		-e E2E_FOCUS="sig-api-machinery" \
		-e E2E_SKIP="$(CONFORMANCE_SKIP)" \
		-v kubeconfig:/etc/rancher/k3s:ro \
		--entrypoint /usr/local/bin/kubeconformance \
		registry.k8s.io/conformance:$(K3S_VERSION)
