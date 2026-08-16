package fdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/stretchr/testify/require"
)

// Records written before the by-key-data subspace existed have no denormalized
// values; List must transparently fall back to the by-revision subspace.
func TestListFallsBackWithoutDenormalizedData(t *testing.T) {
	f := NewFDB(connectionString, tls.Config{}, "denorm-test", &sync.WaitGroup{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, f.Start(ctx))
	inner := ThisFDB

	// small, empty, and multi-chunk (> chunkSize) values
	values := map[string][]byte{
		"/denorm/small": []byte("hello"),
		"/denorm/empty": {},
		"/denorm/big":   make([]byte, 3*chunkSize+17),
	}
	rand.Read(values["/denorm/big"])
	for key, value := range values {
		_, err := f.Create(ctx, key, value, 0)
		require.NoError(t, err)
	}

	assertList := func(stage string) {
		rev, kvs, err := f.List(ctx, "/denorm/", "/denorm/", 0, 0, false)
		require.NoError(t, err, stage)
		require.NotZero(t, rev, stage)
		require.Len(t, kvs, len(values), stage)
		for _, kv := range kvs {
			expected, ok := values[kv.Key]
			require.True(t, ok, "%s: unexpected key %s", stage, kv.Key)
			require.True(t, bytes.Equal(expected, kv.Value),
				"%s: value mismatch for %s: len=%d want %d", stage, kv.Key, len(kv.Value), len(expected))
		}
	}

	assertList("denormalized")

	// simulate pre-denormalization data: wipe the current-value subspace
	_, err := transact("wipe", inner.db, 0, func(tr fdb.Transaction) (int, error) {
		tr.ClearRange(inner.byKeyCurrent.GetSubspace())
		return 0, nil
	})
	require.NoError(t, err)

	assertList("legacy fallback")

	// updates re-denormalize; a mixed batch must read both paths
	newValue := []byte(fmt.Sprintf("updated-%d", time.Now().UnixNano()))
	_, kv, err := f.Get(ctx, "/denorm/small", "", 0, 0, false)
	require.NoError(t, err)
	_, _, updated, err := f.Update(ctx, "/denorm/small", newValue, kv.ModRevision, 0)
	require.NoError(t, err)
	require.True(t, updated)
	values["/denorm/small"] = newValue

	assertList("mixed legacy and denormalized")
}
