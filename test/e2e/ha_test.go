package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// HA e2e: kcp against TWO stateless shim replicas over one FoundationDB
// directory. The etcd client spreads RPCs across endpoints, so writes landing
// on one shim must surface in watches served by the other (bounded by the
// poll-interval floor). Then one shim is SIGKILLed and the control plane must
// keep working through the survivor.
func TestKCPOnTwoShims(t *testing.T) {
	root := repoRoot(t)
	shimBinary := envOr("KUBE_FDB_SHIM", filepath.Join(root, "bin", "kube-foundationdb"))
	kcpBinary := envOr("KCP_BINARY", filepath.Join(root, "..", "..", "kcp-dev", "kcp", "bin", "kcp"))
	clusterFile := envOr("KUBE_FDB_CLUSTER_FILE", filepath.Join(root, ".fdb", "fdb.cluster"))

	for name, path := range map[string]string{"shim": shimBinary, "kcp": kcpBinary, "fdb cluster file": clusterFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s not found at %s: %v", name, path, err)
		}
	}

	workDir := t.TempDir()
	fdbDir := fmt.Sprintf("ha-e2e-%d", time.Now().UnixNano())

	startShim := func(name string, port int, clean bool) *exec.Cmd {
		args := []string{
			"--listen-address", fmt.Sprintf("127.0.0.1:%d", port),
			"--metrics-bind-address", fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			"--fdb-directory", fdbDir,
		}
		if clean {
			args = append(args, "--fdb-clean-directory-on-start")
		}
		cmd := exec.Command(shimBinary, args...)
		cmd.Env = append(os.Environ(), "FDB_CLUSTER_FILE="+clusterFile)
		startProcess(t, filepath.Join(workDir, name+".log"), cmd)
		return cmd
	}

	shimPortA, shimPortB := freePort(t), freePort(t)
	shimA := startShim("shim-a", shimPortA, true)
	time.Sleep(2 * time.Second) // let A create the directory before B opens it
	startShim("shim-b", shimPortB, false)

	kcpPort := freePort(t)
	kcpRoot := filepath.Join(workDir, "kcp")
	kcpCmd := exec.Command(kcpBinary, "start",
		"--root-directory", kcpRoot,
		"--etcd-servers", fmt.Sprintf("http://127.0.0.1:%d,http://127.0.0.1:%d", shimPortA, shimPortB),
		"--secure-port", fmt.Sprintf("%d", kcpPort),
		"--bind-address", "127.0.0.1",
		"--external-hostname", "127.0.0.1",
	)
	startProcess(t, filepath.Join(workDir, "kcp.log"), kcpCmd)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	kubeconfigPath := filepath.Join(kcpRoot, "admin.kubeconfig")
	var cfg *rest.Config
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		c, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return false, nil
		}
		cfg = c
		return true, nil
	})
	if err != nil {
		t.Fatalf("admin kubeconfig never appeared: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		var status int
		client.RESTClient().Get().AbsPath("/readyz").Do(ctx).StatusCode(&status)
		return status == 200, nil
	})
	if err != nil {
		t.Fatalf("kcp never became ready: %v", err)
	}
	t.Log("kcp ready against two shims")

	ns := fmt.Sprintf("ha-e2e-%d", time.Now().Unix())
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	t.Run("cross-shim watch delivery", func(t *testing.T) {
		w, err := client.CoreV1().ConfigMaps(ns).Watch(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Stop()

		// RPCs round-robin across both shims, so some creates commit on the
		// shim not serving the watch; every event must still arrive promptly.
		const n = 10
		start := time.Now()
		for i := range n {
			if _, err := client.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("cm-%d", i)},
			}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		seen := 0
		deadline := time.After(30 * time.Second)
		for seen < n {
			select {
			case ev, ok := <-w.ResultChan():
				if !ok {
					t.Fatal("watch closed early")
				}
				if cm, ok := ev.Object.(*corev1.ConfigMap); ok && len(cm.Name) > 3 && cm.Name[:3] == "cm-" {
					seen++
				}
			case <-deadline:
				t.Fatalf("saw only %d/%d watch events", seen, n)
			}
		}
		t.Logf("all %d events delivered in %s", n, time.Since(start).Round(time.Millisecond))
	})

	t.Run("survives losing one shim", func(t *testing.T) {
		syscall.Kill(-shimA.Process.Pid, syscall.SIGKILL)
		t.Log("killed shim A")

		// writes must keep succeeding through shim B (client failover)
		err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := client.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("failover-%d", time.Now().UnixNano())},
			}, metav1.CreateOptions{})
			return err == nil, nil
		})
		if err != nil {
			t.Fatalf("writes did not recover after killing shim A: %v", err)
		}

		// reads and lists too
		list, err := client.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Items) < 10 {
			t.Fatalf("expected at least 10 configmaps after failover, got %d", len(list.Items))
		}
	})
}
