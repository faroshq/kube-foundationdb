package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// End-to-end test: kcp with its etcd storage served by kube-foundationdb on
// top of a real FoundationDB cluster.
//
// Prerequisites (see Makefile targets fdb-start and build):
//   - a FoundationDB cluster reachable via the cluster file at
//     KUBE_FDB_CLUSTER_FILE (default .fdb/fdb.cluster)
//   - bin/kube-foundationdb built
//   - a kcp binary (KCP_BINARY, default ../kcp-dev/kcp/bin/kcp relative to GOPATH layout)

func repoRoot(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(filepath.Dir(wd)) // test/e2e -> repo root
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// dumpLogOnFailure prints the tail of a process log when the test fails, so
// failures are diagnosable after the temp dir is cleaned up.
func dumpLogOnFailure(t *testing.T, logPath string) {
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Logf("could not read %s: %v", logPath, err)
			return
		}
		const tail = 8 * 1024
		if len(data) > tail {
			data = data[len(data)-tail:]
		}
		t.Logf("---- tail of %s ----\n%s", logPath, data)
	})
}

// startProcess starts cmd, streams output to a log file, and registers cleanup.
func startProcess(t *testing.T, logPath string, cmd *exec.Cmd) {
	dumpLogOnFailure(t, logPath)
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", cmd.Path, err)
	}
	t.Cleanup(func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		logFile.Close()
	})
}

func TestKCPOnFoundationDB(t *testing.T) {
	root := repoRoot(t)
	shimBinary := envOr("KUBE_FDB_SHIM", filepath.Join(root, "bin", "kube-foundationdb"))
	kcpBinary := envOr("KCP_BINARY", filepath.Join(root, "..", "..", "kcp-dev", "kcp", "bin", "kcp"))
	clusterFile := envOr("KUBE_FDB_CLUSTER_FILE", filepath.Join(root, ".fdb", "fdb.cluster"))

	for name, path := range map[string]string{"shim": shimBinary, "kcp": kcpBinary, "fdb cluster file": clusterFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s not found at %s (see Makefile targets fdb-start/build): %v", name, path, err)
		}
	}

	workDir := t.TempDir()
	shimPort := freePort(t)
	kcpPort := freePort(t)

	// 1. Start the etcd shim backed by FoundationDB. A per-run FDB directory
	// keeps runs isolated; --fdb-clean-directory-on-start wipes leftovers.
	fdbDir := fmt.Sprintf("kcp-e2e-%d", time.Now().UnixNano())
	shimCmd := exec.Command(shimBinary,
		"--listen-address", fmt.Sprintf("127.0.0.1:%d", shimPort),
		"--metrics-bind-address", fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"--fdb-directory", fdbDir,
		"--fdb-clean-directory-on-start",
	)
	shimCmd.Env = append(os.Environ(), "FDB_CLUSTER_FILE="+clusterFile)
	startProcess(t, filepath.Join(workDir, "shim.log"), shimCmd)
	t.Logf("shim listening on 127.0.0.1:%d (logs: %s)", shimPort, filepath.Join(workDir, "shim.log"))

	// 2. Start kcp against the shim.
	kcpRoot := filepath.Join(workDir, "kcp")
	kcpCmd := exec.Command(kcpBinary, "start",
		"--root-directory", kcpRoot,
		"--etcd-servers", fmt.Sprintf("http://127.0.0.1:%d", shimPort),
		"--secure-port", fmt.Sprintf("%d", kcpPort),
		"--bind-address", "127.0.0.1",
		"--external-hostname", "127.0.0.1",
	)
	startProcess(t, filepath.Join(workDir, "kcp.log"), kcpCmd)
	t.Logf("kcp starting on 127.0.0.1:%d (logs: %s)", kcpPort, filepath.Join(workDir, "kcp.log"))

	// 3. Wait for the admin kubeconfig, then for readiness.
	kubeconfigPath := filepath.Join(kcpRoot, "admin.kubeconfig")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var cfg *rest.Config
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if _, err := os.Stat(kubeconfigPath); err != nil {
			return false, nil
		}
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
		result := client.RESTClient().Get().AbsPath("/readyz").Do(ctx).StatusCode(&status)
		if result.Error() != nil {
			return false, nil
		}
		return status == 200, nil
	})
	if err != nil {
		t.Fatalf("kcp never became ready: %v", err)
	}
	t.Log("kcp is ready")

	// 4. CRUD + watch through the FDB-backed store, in the root workspace.
	ns := fmt.Sprintf("fdb-e2e-%d", time.Now().Unix())
	t.Run("namespace and configmap CRUD", func(t *testing.T) {
		_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "fdb-test"},
			Data:       map[string]string{"backend": "foundationdb"},
		}
		if _, err := client.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		got, err := client.CoreV1().ConfigMaps(ns).Get(ctx, "fdb-test", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Data["backend"] != "foundationdb" {
			t.Fatalf("unexpected configmap data: %v", got.Data)
		}

		got.Data["updated"] = "true"
		if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, got, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}

		list, err := client.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Items) == 0 {
			t.Fatal("expected at least one configmap")
		}
	})

	t.Run("watch delivers events", func(t *testing.T) {
		w, err := client.CoreV1().ConfigMaps(ns).Watch(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Stop()

		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "watched"}}
		if _, err := client.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		deadline := time.After(30 * time.Second)
		for {
			select {
			case ev, ok := <-w.ResultChan():
				if !ok {
					t.Fatal("watch channel closed")
				}
				if obj, ok := ev.Object.(*corev1.ConfigMap); ok && obj.Name == "watched" {
					return // saw the event
				}
			case <-deadline:
				t.Fatal("timed out waiting for watch event")
			}
		}
	})

	t.Run("delete", func(t *testing.T) {
		if err := client.CoreV1().ConfigMaps(ns).Delete(ctx, "fdb-test", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := client.CoreV1().ConfigMaps(ns).Get(ctx, "fdb-test", metav1.GetOptions{})
			return apierrors.IsNotFound(err), nil
		})
		if err != nil {
			t.Fatalf("configmap was not deleted: %v", err)
		}
	})

	// 5. kcp-specific: create a Workspace and wait for it to become ready.
	// Workspace scheduling exercises kcp's controllers, which lean heavily on
	// watches — a good end-to-end signal that etcd semantics hold.
	t.Run("kcp workspace becomes ready", func(t *testing.T) {
		dyn, err := dynamic.NewForConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		wsGVR := schema.GroupVersionResource{Group: "tenancy.kcp.io", Version: "v1alpha1", Resource: "workspaces"}
		ws := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "tenancy.kcp.io/v1alpha1",
			"kind":       "Workspace",
			"metadata":   map[string]interface{}{"name": "fdb-e2e"},
		}}
		if _, err := dyn.Resource(wsGVR).Create(ctx, ws, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			got, err := dyn.Resource(wsGVR).Get(ctx, "fdb-e2e", metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
			return phase == "Ready", nil
		})
		if err != nil {
			t.Fatalf("workspace never became ready: %v", err)
		}
	})
}
