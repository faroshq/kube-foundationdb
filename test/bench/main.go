// Command bench measures etcd-API performance using the access patterns the
// Kubernetes apiserver actually uses: guarded-txn create/update/delete, point
// gets, prefix lists, and watch delivery latency. It works against any
// etcd-compatible endpoint (real etcd, kine, kube-foundationdb), which the
// general-purpose etcd benchmark tool does not — kine-based shims serve only
// the apiserver's subset of the API (no plain Put).
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	endpoint = flag.String("endpoint", "http://127.0.0.1:2379", "etcd endpoint")
	prefix   = flag.String("prefix", "/registry/bench", "key prefix (unique per run recommended)")
	workers  = flag.Int("workers", 20, "concurrent workers for write/read workloads")
	creates  = flag.Int("creates", 2000, "number of objects to create")
	updates  = flag.Int("updates", 2000, "number of guarded updates")
	gets     = flag.Int("gets", 5000, "number of point gets")
	lists    = flag.Int("lists", 50, "number of full prefix lists")
	watchN   = flag.Int("watch-events", 300, "number of watch events to measure")
	valSize  = flag.Int("value-size", 1536, "value size in bytes (~a small k8s object)")
	settle   = flag.Duration("compact-settle", 5*time.Second, "wait after pre-list compaction for storage-engine cleanup")
)

type stats struct {
	durations []time.Duration
	elapsed   time.Duration
}

func (s *stats) percentile(p float64) time.Duration {
	if len(s.durations) == 0 {
		return 0
	}
	idx := int(float64(len(s.durations)-1) * p)
	return s.durations[idx]
}

func (s *stats) report(name string, ops int) {
	slices.Sort(s.durations)
	fmt.Printf("%-22s %8.0f ops/s   p50=%-9s p95=%-9s p99=%-9s max=%s\n",
		name,
		float64(ops)/s.elapsed.Seconds(),
		s.percentile(0.50).Round(10*time.Microsecond),
		s.percentile(0.95).Round(10*time.Microsecond),
		s.percentile(0.99).Round(10*time.Microsecond),
		s.percentile(1.0).Round(10*time.Microsecond),
	)
}

// runParallel executes n ops across w workers; op receives the global op index.
func runParallel(n, w int, op func(i int) error) (*stats, error) {
	var (
		mu   sync.Mutex
		durs = make([]time.Duration, 0, n)
		wg   sync.WaitGroup
		errc = make(chan error, w)
		next = make(chan int, n)
	)
	for i := range n {
		next <- i
	}
	close(next)
	start := time.Now()
	for range w {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, n/w+1)
			for i := range next {
				t := time.Now()
				if err := op(i); err != nil {
					errc <- err
					return
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			durs = append(durs, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	select {
	case err := <-errc:
		return nil, err
	default:
	}
	return &stats{durations: durs, elapsed: elapsed}, nil
}

func key(i int) string { return fmt.Sprintf("%s/objs/obj-%06d", *prefix, i) }

func guardedCreate(ctx context.Context, c *clientv3.Client, k string, val []byte) error {
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, string(val))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("create %s: already exists", k)
	}
	return nil
}

func main() {
	flag.Parse()
	ctx := context.Background()

	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.Close()

	val := make([]byte, *valSize)
	rand.Read(val)

	fmt.Printf("endpoint=%s workers=%d value=%dB\n", *endpoint, *workers, *valSize)

	// 1. guarded creates (apiserver object creation)
	st, err := runParallel(*creates, *workers, func(i int) error {
		return guardedCreate(ctx, c, key(i), val)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	st.report("create (txn)", *creates)

	// 2. guarded updates: each worker owns a disjoint key set — no conflicts,
	// mirroring the apiserver's per-object optimistic concurrency.
	st, err = runParallel(*updates, *workers, func(i int) error {
		k := key(i % *creates)
		get, err := c.Get(ctx, k)
		if err != nil {
			return err
		}
		if len(get.Kvs) == 0 {
			return fmt.Errorf("missing %s", k)
		}
		resp, err := c.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(k), "=", get.Kvs[0].ModRevision)).
			Then(clientv3.OpPut(k, string(val))).
			Else(clientv3.OpGet(k)).
			Commit()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			return nil // conflict: counted, apiserver would retry
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "update:", err)
		os.Exit(1)
	}
	st.report("get+update (txn)", *updates)

	// Compact away superseded revisions before the read phase, as the
	// apiserver's periodic compaction would have in steady state, then let the
	// storage engine settle: freshly written range tombstones transiently slow
	// scans on both etcd (bbolt free pages) and FDB (deferred range clears).
	if head, err := c.Get(ctx, key(0)); err == nil && len(head.Kvs) > 0 {
		if _, err := c.Compact(ctx, head.Header.Revision); err != nil {
			fmt.Fprintln(os.Stderr, "compact (non-fatal):", err)
		}
		time.Sleep(*settle)
	}

	// 3. point gets
	st, err = runParallel(*gets, *workers, func(i int) error {
		_, err := c.Get(ctx, key(i%*creates))
		return err
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "get:", err)
		os.Exit(1)
	}
	st.report("get", *gets)

	// 4. full prefix list (apiserver LIST)
	listPrefix := *prefix + "/objs/"
	st, err = runParallel(*lists, 4, func(i int) error {
		resp, err := c.Get(ctx, listPrefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		if int(resp.Count) < *creates {
			return fmt.Errorf("list returned %d < %d", resp.Count, *creates)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "list:", err)
		os.Exit(1)
	}
	st.report(fmt.Sprintf("list %d objs", *creates), *lists)

	// 5. watch delivery latency: commit -> event receipt
	wprefix := *prefix + "/watch/"
	wch := c.Watch(ctx, wprefix, clientv3.WithPrefix())
	wst := &stats{durations: make([]time.Duration, 0, *watchN)}
	wstart := time.Now()
	for i := range *watchN {
		k := fmt.Sprintf("%sev-%06d", wprefix, i)
		if err := guardedCreate(ctx, c, k, val[:64]); err != nil {
			fmt.Fprintln(os.Stderr, "watch put:", err)
			os.Exit(1)
		}
		committed := time.Now()
		deadline := time.After(10 * time.Second)
	recv:
		for {
			select {
			case resp := <-wch:
				for _, ev := range resp.Events {
					if string(ev.Kv.Key) == k {
						wst.durations = append(wst.durations, time.Since(committed))
						break recv
					}
				}
			case <-deadline:
				fmt.Fprintln(os.Stderr, "watch: timed out waiting for event")
				os.Exit(1)
			}
		}
	}
	wst.elapsed = time.Since(wstart)
	wst.report("watch latency", *watchN)

	// cleanup
	if _, err := c.Delete(ctx, *prefix, clientv3.WithPrefix()); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup (non-fatal):", err)
	}
}
