package fdb

import (
	"time"

	"github.com/urfave/cli/v2"
)

var (
	Directory          = "etcd"
	CleanDirOnStart    = false
	LogConflictingKeys = false

	// PollIntervalFloor bounds how stale the watch poll loop can be while
	// idle. Local writes wake the poller instantly (pollKick), but writes from
	// OTHER shim instances (HA) are only signalled by the FDB watch, which
	// fires 10-30ms after commit — enough informer-lister lag to lose upstream
	// controller conflict-retry races (see the CRD finalizer analysis). A few
	// ms of periodic polling caps remote-write delivery latency; one cheap
	// range read per tick per process.
	PollIntervalFloor = 5 * time.Millisecond

	// pollErrorBackoff is how long the watch poll loop waits after a
	// non-retryable read error before starting the next poll. Without it a
	// cluster that cannot serve reads (storage full, ratekeeper stalling
	// commits) turns the loop into a hot spin that logs the same error
	// millions of times.
	pollErrorBackoff = 100 * time.Millisecond

	// For testing only
	UseSequentialId = false
	APITest         = false
)

func ConfigFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "fdb-directory",
			Value:       "etcd",
			Usage:       "FoundationDB directory name where data is stored. Default is 'etcd'.",
			Destination: &Directory,
		},
		&cli.BoolFlag{
			Name:        "fdb-clean-directory-on-start",
			Usage:       "Clean the directory on start. Useful for testing.",
			Destination: &CleanDirOnStart,
		},
		&cli.DurationFlag{
			Name:        "fdb-poll-interval",
			Value:       5 * time.Millisecond,
			Usage:       "Upper bound on watch poll staleness for writes from other shim instances. 0 disables periodic polling (FDB watch only).",
			Destination: &PollIntervalFloor,
		},
		&cli.BoolFlag{
			Name:        "fdb-log-conflicting-keys",
			Usage:       "Log conflicting keys when a transaction conflict occurs. Useful for debugging.",
			Destination: &LogConflictingKeys,
		},
	}
}
