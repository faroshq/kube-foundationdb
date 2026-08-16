# kube-foundationdb: etcd shim backed by FoundationDB.
# The FDB Go bindings need cgo + libfdb_c at build and run time; the client
# library major.minor must match the FDB cluster (7.3 here).

ARG FDB_VERSION=7.3.77

FROM golang:1.24-bookworm AS builder
ARG FDB_VERSION
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) FDB_ARCH=amd64 ;; \
      arm64) FDB_ARCH=aarch64 ;; \
      *) echo "unsupported arch $TARGETARCH" && exit 1 ;; \
    esac \
    && curl -fsSLo /tmp/fdb-clients.deb \
      "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${FDB_ARCH}.deb" \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/kube-foundationdb ./cmd/kube-foundationdb

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=builder /out/kube-foundationdb /usr/local/bin/kube-foundationdb

# Mount the FDB cluster file at /etc/foundationdb/fdb.cluster (the default the
# client looks for) or set FDB_CLUSTER_FILE.
EXPOSE 2379
ENTRYPOINT ["/usr/local/bin/kube-foundationdb"]
CMD ["--listen-address", "0.0.0.0:2379"]
