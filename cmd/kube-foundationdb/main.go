package main

import (
	"context"
	"errors"
	"os"

	"github.com/k3s-io/kine/pkg/app"
	"github.com/sirupsen/logrus"

	"github.com/faroshq/kube-foundationdb/pkg/drivers/fdb"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	setFDBEndpointIfNotConfigured()
	a := app.New()
	a.Name = "kube-foundationdb"
	a.Usage = "etcd shim backed by FoundationDB, for Kubernetes/kcp API servers"
	a.Flags = append(a.Flags, fdb.ConfigFlags()...)
	if err := a.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}

// kine picks its driver from the --endpoint scheme; default to fdb:// so the
// binary is FoundationDB-backed out of the box.
func setFDBEndpointIfNotConfigured() {
	for _, arg := range os.Args {
		if arg == "--endpoint" {
			return
		}
	}
	os.Args = append(os.Args, "--endpoint", "fdb://")
}
