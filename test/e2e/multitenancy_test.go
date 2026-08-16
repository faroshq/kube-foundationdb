package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// Multi-tenancy: ONE FoundationDB cluster backing TWO independent control
// planes, isolated by FDB directory. This is the deployment shape a fleet of
// kcp shards would use — one scalable storage cluster, one shim + directory
// per shard.
func TestTwoControlPlanesOneFDB(t *testing.T) {
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
	runID := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	type tenant struct {
		name   string
		client *kubernetes.Clientset
	}

	startTenant := func(name string) *tenant {
		shimPort := freePort(t)
		shimCmd := exec.Command(shimBinary,
			"--listen-address", fmt.Sprintf("127.0.0.1:%d", shimPort),
			"--metrics-bind-address", fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			"--fdb-directory", fmt.Sprintf("mt-%s-%d", name, runID),
			"--fdb-clean-directory-on-start",
		)
		shimCmd.Env = append(os.Environ(), "FDB_CLUSTER_FILE="+clusterFile)
		startProcess(t, filepath.Join(workDir, "shim-"+name+".log"), shimCmd)

		kcpRoot := filepath.Join(workDir, "kcp-"+name)
		kcpCmd := exec.Command(kcpBinary, "start",
			"--root-directory", kcpRoot,
			"--etcd-servers", fmt.Sprintf("http://127.0.0.1:%d", shimPort),
			"--secure-port", fmt.Sprintf("%d", freePort(t)),
			"--bind-address", "127.0.0.1",
			"--external-hostname", "127.0.0.1",
		)
		startProcess(t, filepath.Join(workDir, "kcp-"+name+".log"), kcpCmd)

		kubeconfigPath := filepath.Join(kcpRoot, "admin.kubeconfig")
		var client *kubernetes.Clientset
		err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 4*time.Minute, true, func(ctx context.Context) (bool, error) {
			cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
			if err != nil {
				return false, nil
			}
			c, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				return false, nil
			}
			var status int
			c.RESTClient().Get().AbsPath("/readyz").Do(ctx).StatusCode(&status)
			if status != 200 {
				return false, nil
			}
			client = c
			return true, nil
		})
		if err != nil {
			t.Fatalf("tenant %s never became ready: %v", name, err)
		}
		t.Logf("tenant %s ready", name)
		return &tenant{name: name, client: client}
	}

	a := startTenant("a")
	b := startTenant("b")

	// identical object names in both tenants, different payloads
	const ns = "mt-isolation"
	for _, tn := range []*tenant{a, b} {
		if _, err := tn.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := tn.client.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "who-am-i"},
			Data:       map[string]string{"tenant": tn.name},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	for _, tn := range []*tenant{a, b} {
		got, err := tn.client.CoreV1().ConfigMaps(ns).Get(ctx, "who-am-i", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Data["tenant"] != tn.name {
			t.Fatalf("tenant %s read %q — cross-tenant data leak", tn.name, got.Data["tenant"])
		}
	}

	// deleting in one tenant must not affect the other
	if err := a.client.CoreV1().ConfigMaps(ns).Delete(ctx, "who-am-i", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := a.client.CoreV1().ConfigMaps(ns).Get(ctx, "who-am-i", metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		t.Fatalf("tenant a configmap not deleted: %v", err)
	}
	got, err := b.client.CoreV1().ConfigMaps(ns).Get(ctx, "who-am-i", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("tenant b lost its configmap after tenant a's delete: %v", err)
	}
	if got.Data["tenant"] != "b" {
		t.Fatalf("tenant b data corrupted: %v", got.Data)
	}
	t.Log("tenants fully isolated on one FoundationDB cluster")
}
