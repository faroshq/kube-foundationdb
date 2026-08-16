package fdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/tls"
	"github.com/stretchr/testify/require"
)

// With a live poll loop (any active watcher), Count serves from the tracker
// and stays exact as objects are created and deleted.
func TestCountTracksLiveObjects(t *testing.T) {
	f := NewFDB(connectionString, tls.Config{}, "count-test", &sync.WaitGroup{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, f.Start(ctx))
	inner := ThisFDB

	// an active watcher keeps the poll loop (and therefore the tracker) live
	w := f.Watch(ctx, "/count/", 0)
	require.Zero(t, w.CompactRevision)

	for i := range 5 {
		_, err := f.Create(ctx, fmt.Sprintf("/count/obj-%d", i), []byte("v"), 0)
		require.NoError(t, err)
	}

	// first Count seeds the tracker via exact scan
	_, n, err := f.Count(ctx, "/count/", "/count/", 0)
	require.NoError(t, err)
	require.EqualValues(t, 5, n)

	// mutate: +2 creates, -1 delete, 1 update (no count change)
	for i := 5; i < 7; i++ {
		_, err := f.Create(ctx, fmt.Sprintf("/count/obj-%d", i), []byte("v"), 0)
		require.NoError(t, err)
	}
	_, kv, err := f.Get(ctx, "/count/obj-0", "", 0, 0, false)
	require.NoError(t, err)
	_, _, deleted, err := f.Delete(ctx, "/count/obj-0", kv.ModRevision)
	require.NoError(t, err)
	require.True(t, deleted)
	_, kv, err = f.Get(ctx, "/count/obj-1", "", 0, 0, false)
	require.NoError(t, err)
	_, _, updated, err := f.Update(ctx, "/count/obj-1", []byte("v2"), kv.ModRevision, 0)
	require.NoError(t, err)
	require.True(t, updated)

	require.Eventually(t, func() bool {
		_, n, err := f.Count(ctx, "/count/", "/count/", 0)
		return err == nil && n == 6
	}, 10*time.Second, 50*time.Millisecond, "tracked count should converge to 6")

	// the cached path must agree with a fresh exact scan
	if inner.pollActive.Load() {
		cached, _, ok := inner.counts.get("/count/")
		require.True(t, ok, "prefix should be tracked")
		require.EqualValues(t, 6, cached)
	}
}
