package smoke

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Requires a running shim (see Makefile smoke target). Endpoint override via
// KUBE_FDB_ENDPOINT.
func client(t *testing.T) *clientv3.Client {
	endpoint := os.Getenv("KUBE_FDB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:2379"
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPutGetDelete(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := fmt.Sprintf("/registry/smoke/%d", time.Now().UnixNano())

	// kine only supports the transaction forms the apiserver uses; a create is
	// a txn guarded on mod_revision = 0.
	txn, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v1")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Succeeded {
		t.Fatal("create txn did not succeed")
	}

	get, err := c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(get.Kvs) != 1 || string(get.Kvs[0].Value) != "v1" {
		t.Fatalf("unexpected get result: %+v", get.Kvs)
	}
	rev := get.Kvs[0].ModRevision

	// guarded update
	txn, err = c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
		Then(clientv3.OpPut(key, "v2")).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Succeeded {
		t.Fatal("update txn did not succeed")
	}

	get, err = c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(get.Kvs[0].Value) != "v2" {
		t.Fatalf("expected v2, got %q", get.Kvs[0].Value)
	}

	// guarded delete
	txn, err = c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", get.Kvs[0].ModRevision)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Succeeded {
		t.Fatal("delete txn did not succeed")
	}

	get, err = c.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(get.Kvs) != 0 {
		t.Fatalf("key still present after delete: %+v", get.Kvs)
	}
}

func TestListAndWatch(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/registry/smokewatch/%d/", time.Now().UnixNano())

	// current revision before writes
	head, err := c.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	startRev := head.Header.Revision

	wch := c.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(startRev+1))

	for i := range 3 {
		key := fmt.Sprintf("%sitem-%d", prefix, i)
		txn, err := c.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, fmt.Sprintf("value-%d", i))).
			Commit()
		if err != nil {
			t.Fatal(err)
		}
		if !txn.Succeeded {
			t.Fatalf("create %s failed", key)
		}
	}

	list, err := c.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Kvs) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(list.Kvs))
	}

	seen := 0
	deadline := time.After(20 * time.Second)
	for seen < 3 {
		select {
		case resp, ok := <-wch:
			if !ok {
				t.Fatal("watch channel closed early")
			}
			if err := resp.Err(); err != nil {
				t.Fatal(err)
			}
			for _, ev := range resp.Events {
				if ev.Type == clientv3.EventTypePut {
					seen++
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for watch events, saw %d", seen)
		}
	}
}
