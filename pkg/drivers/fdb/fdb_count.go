package fdb

import (
	"sync"

	"github.com/k3s-io/kine/pkg/server"
)

// countTracker maintains live object counts per Counted prefix, updated from
// the watch poll loop, so the apiserver's periodic per-resource Count calls
// (storage_objects metrics, once a minute per resource) don't each pay a full
// prefix scan.
//
// A prefix is seeded by one exact scan at revision seedRev; the poll loop then
// applies create (+1) and delete (-1) events with revision > seedRev. Counts
// are served only while the poll loop is live and has caught up to seedRev,
// and the tracker is reset whenever the poll loop (re)starts, because a fresh
// poll cursor skips events from before it started. Anything else — historical
// counts, partial ranges, no live poll — falls back to the exact scan.
type countTracker struct {
	mu      sync.Mutex
	entries map[string]*countEntry
}

type countEntry struct {
	count   int64
	seedRev int64
}

func newCountTracker() *countTracker {
	return &countTracker{entries: map[string]*countEntry{}}
}

func (t *countTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = map[string]*countEntry{}
}

func (t *countTracker) get(prefix string) (int64, int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[prefix]
	if !ok {
		return 0, 0, false
	}
	return e.count, e.seedRev, true
}

func (t *countTracker) seed(prefix string, count, seedRev int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// keep an existing entry: it has been maintained since its own seed and
	// re-seeding with an older scan could lose events applied in between
	if _, ok := t.entries[prefix]; !ok {
		t.entries[prefix] = &countEntry{count: count, seedRev: seedRev}
	}
}

// apply folds a poll batch into every tracked prefix. Events arrive in
// revision order from a single goroutine (the poll loop).
func (t *countTracker) apply(events []*server.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.entries) == 0 {
		return
	}
	for _, event := range events {
		if !event.Create && !event.Delete {
			continue
		}
		for prefix, e := range t.entries {
			if event.KV.ModRevision <= e.seedRev || !doesEventHavePrefix(event.KV.Key, prefix) {
				continue
			}
			if event.Create {
				e.count++
			} else {
				e.count--
			}
		}
	}
}
