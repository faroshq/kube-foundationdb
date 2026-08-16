package fdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"strings"
)

type RevResult struct {
	currentRevision int64
	revRecords      []*RevRecord
}

// secondaryFetchPipeline is how many by-revision record reads are kept in
// flight before draining them, during List. Bounded so a single list batch
// holds at most this many outstanding range reads.
const secondaryFetchPipeline = 256

func (f *FDB) CurrentRevision(_ context.Context) (int64, error) {
	lastWatchRev := f.lastWatchRev.Load()
	if lastWatchRev != 0 {
		return lastWatchRev, nil
	} else {
		rev, err := transact("current_rev", f.db, 0, func(tr fdb.Transaction) (ret int64, e error) {
			if latestRev, err := f.rev.GetLatestRev(&tr); err != nil {
				return 0, err
			} else {
				return latestRev.Get()
			}
		})

		return rev, err
	}
}

func (f *FDB) List(_ context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	rev, kvs, err := f.listKeyValue("List", prefix, startKey, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}
	return rev, kvs, nil
}

func (f *FDB) Get(_ context.Context, key, rangeEnd string, _, revision int64, keysOnly bool) (revRet int64, kvRet *server.KeyValue, errRet error) {
	if rangeEnd != "" {
		return 0, nil, fmt.Errorf("invalid 'rangeEnd' for Get. Expected: '', got %s", rangeEnd)
	}
	rev, kvs, err := f.listKeyValue("Get", key, key, 1, revision, keysOnly)
	if err != nil {
		return 0, nil, err
	}
	if len(kvs) == 0 {
		return rev, nil, nil
	}
	return rev, kvs[0], nil
}

func (f *FDB) getLast(tr *fdb.Transaction, key string) (*ByKeyAndRevisionRecord, error) {
	keyRange := f.byKeyAndRevision.GetSubspace().Sub(key)
	it := tr.GetRange(keyRange, fdb.RangeOptions{Limit: 1, Mode: fdb.StreamingModeExact, Reverse: true}).Iterator()
	if !it.Advance() {
		return nil, nil
	}
	keyAndRevRecord, err := f.byKeyAndRevision.GetFromIterator(it)
	if err != nil {
		return nil, err
	} else {
		return keyAndRevRecord, nil
	}
}

func (f *FDB) Count(_ context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	collector := &countCollector{}
	rev, err := f.listWithCollector("Count", prefix, startKey, revision, collector)
	if err != nil {
		return 0, 0, err
	} else {
		return rev, collector.totalCount, nil
	}
}

type countCollector struct {
	totalCount int64
	batchCount int64
}

func (c *countCollector) startBatch() {
	c.batchCount = 0
}

func (c *countCollector) next(*fdb.Transaction, *ByKeyAndRevisionRecord) (fdb.Key, bool, error) {
	c.batchCount++
	return nil, true, nil
}

func (c *countCollector) endBatch(*fdb.Transaction, bool) error {
	return nil
}

func (c *countCollector) postBatch() {
	c.totalCount += c.batchCount
}

func (c *countCollector) String() string {
	return fmt.Sprintf("{count=%d}", c.totalCount)
}

func (f *FDB) listKeyValue(caller, prefix, startKey string, limit, maxRevision int64, keysOnly bool) (resRev int64, resEvents []*server.KeyValue, resErr error) {
	collector := newListCollector(f, limit, keysOnly)
	rev, err := f.listWithCollector(caller, prefix, startKey, maxRevision, collector)
	if err != nil {
		return 0, nil, err
	}
	kvs := make([]*server.KeyValue, 0, len(collector.records))
	for _, revRecord := range collector.records {
		event := revRecordToEvent(revRecord)
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

type listCollector struct {
	f            *FDB
	limit        int64
	keysOnly     bool
	records      []*RevRecord
	batchRecords []*RevRecord
}

func newListCollector(f *FDB, limit int64, keysOnly bool) *listCollector {
	capacity := limit
	if capacity == 0 {
		capacity = 100
	}
	return &listCollector{
		f:            f,
		limit:        limit,
		keysOnly:     keysOnly,
		records:      make([]*RevRecord, 0, capacity),
		batchRecords: make([]*RevRecord, 0, capacity),
	}
}

func (c *listCollector) startBatch() {
	c.batchRecords = c.batchRecords[len(c.batchRecords):]
}

func (c *listCollector) next(_ *fdb.Transaction, record *ByKeyAndRevisionRecord) (fdb.Key, bool, error) {
	c.batchRecords = append(c.batchRecords, &RevRecord{Rev: record.Key.Rev, Record: record.Value})
	return nil, c.needMore(), nil
}

func (c *listCollector) needMore() bool {
	return c.limit == 0 || int64(len(c.batchRecords)+len(c.records)) < c.limit
}

// endBatch fetches the values for every record collected in this batch with a
// single contiguous range read over the by-key-data subspace (chunks are laid
// out in the same key order as the index scan). Records without denormalized
// data — written before the subspace existed — fall back to pipelined point
// reads against the by-revision subspace.
func (c *listCollector) endBatch(tr *fdb.Transaction, _ bool) error {
	if c.keysOnly || len(c.batchRecords) == 0 {
		return nil
	}

	firstKey := c.batchRecords[0].Record.Key
	lastKey := c.batchRecords[len(c.batchRecords)-1].Record.Key
	selector, err := c.f.byKeyCurrent.RangeForKeys(firstKey, lastKey)
	if err != nil {
		return err
	}

	// merge-join: batch records and chunks are both ordered by key. The join
	// is only valid for records that are the newest revision of their key
	// (snapshot isolation then guarantees the current subspace holds exactly
	// that record's value); older pinned revisions go through fetchLegacy.
	bufs := make([][]byte, len(c.batchRecords))
	idx := 0
	it := tr.Snapshot().GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
	for it.Advance() {
		kv, err := it.Get()
		if err != nil {
			return err
		}
		chunk, err := c.f.byKeyCurrent.parseKV(kv)
		if err != nil {
			return err
		}
		for idx < len(c.batchRecords) && c.batchRecords[idx].Record.Key < chunk.key {
			idx++
		}
		if idx == len(c.batchRecords) {
			break
		}
		rec := c.batchRecords[idx]
		if rec.Record.Key == chunk.key && rec.Record.LatestForKey {
			if bufs[idx] == nil {
				bufs[idx] = make([]byte, 0, rec.Record.ValueSize)
			}
			bufs[idx] = append(bufs[idx], chunk.value...)
		}
	}

	var legacy []int
	for i, rec := range c.batchRecords {
		switch {
		case rec.Record.ValueSize == 0:
			rec.Record.Value = []byte{}
		case int64(len(bufs[i])) == rec.Record.ValueSize:
			rec.Record.Value = bufs[i]
		default:
			legacy = append(legacy, i)
		}
	}
	return c.fetchLegacy(tr, legacy)
}

// fetchLegacy resolves records that predate the by-key-data subspace via the
// by-revision subspace, keeping up to secondaryFetchPipeline range reads in
// flight so the fetches pipeline instead of paying a round-trip each.
func (c *listCollector) fetchLegacy(tr *fdb.Transaction, indices []int) error {
	for start := 0; start < len(indices); start += secondaryFetchPipeline {
		end := min(start+secondaryFetchPipeline, len(indices))
		iterators := make([]*fdb.RangeIterator, 0, end-start)
		for _, i := range indices[start:end] {
			it, err := c.f.byRevision.GetIterator(tr, c.batchRecords[i].Rev)
			if err != nil {
				return err
			}
			iterators = append(iterators, it)
		}
		for j, it := range iterators {
			rev, record, err := c.f.byRevision.GetFromIterator(it)
			if err != nil {
				return err
			}
			if rev == nil {
				return fmt.Errorf("no records in by rev iterator")
			}
			c.batchRecords[indices[start+j]] = &RevRecord{Rev: *rev, Record: record}
		}
	}
	return nil
}

func (c *listCollector) postBatch() {
	c.records = append(c.records, c.batchRecords...)
}

func (c *listCollector) String() string {
	return fmt.Sprintf("{records=%d,batchRecords=%d}", len(c.records), len(c.batchRecords))
}

type recordCollector struct {
	// Input
	f           *FDB
	maxRevision int64
	inner       Processor[*ByKeyAndRevisionRecord]

	// Output
	currentRecord      *ByKeyAndRevisionRecord
	batchCurrentRecord *ByKeyAndRevisionRecord
	firstRev           int64
	rev                int64
	batchRev           int64
}

func newRecordCollector(f *FDB, maxRevision int64, inner Processor[*ByKeyAndRevisionRecord]) *recordCollector {
	return &recordCollector{
		f:           f,
		maxRevision: maxRevision,
		inner:       inner,
	}
}

// streamingMode: an unbounded list consumes the whole range, where WantAll
// fetches it in the fewest round trips; limited lists keep the adaptive
// iterator mode so a small page doesn't over-fetch a huge prefix.
func (c *recordCollector) streamingMode() fdb.StreamingMode {
	if lc, ok := c.inner.(*listCollector); ok && lc.limit == 0 {
		return fdb.StreamingModeWantAll
	}
	return fdb.StreamingModeIterator
}

// snapshotReads: list and count scans are read-only, so conflict tracking is
// skipped; compaction (which writes) keeps regular reads.
func (c *recordCollector) snapshotReads() bool {
	switch c.inner.(type) {
	case *listCollector, *countCollector:
		return true
	}
	return false
}

func (c *recordCollector) startBatch() {
	c.inner.startBatch()
	c.batchCurrentRecord = c.currentRecord
	c.batchRev = 0
}

func (c *recordCollector) next(tr *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
	nextKeyAndRevRecord, err := c.f.byKeyAndRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if nextKeyAndRevRecord == nil {
		return nil, false, nil
	}

	needMore := true
	if c.batchCurrentRecord != nil && c.batchCurrentRecord.Key.Key != nextKeyAndRevRecord.Key.Key {
		if !c.batchCurrentRecord.Value.IsDelete {
			if _, innerNeedMore, err := c.inner.next(tr, c.batchCurrentRecord); err != nil {
				return nil, false, err
			} else {
				needMore = innerNeedMore
			}
		}
		c.batchCurrentRecord = nil
	}

	recordRev := VersionstampToInt64(nextKeyAndRevRecord.Key.Rev)
	if (c.maxRevision == 0 || recordRev <= c.maxRevision) && (c.firstRev == 0 || recordRev <= c.firstRev) {
		nextKeyAndRevRecord.Value.LatestForKey = true
		c.batchCurrentRecord = nextKeyAndRevRecord
	} else if c.batchCurrentRecord != nil && c.batchCurrentRecord.Key.Key == nextKeyAndRevRecord.Key.Key {
		// a newer revision exists beyond the requested window, so the
		// current-value subspace does not hold the candidate's value
		c.batchCurrentRecord.Value.LatestForKey = false
	}

	return c.f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{nextKeyAndRevRecord.Key.Key, nextKeyAndRevRecord.Key.Rev}), needMore, nil
}

func (c *recordCollector) endBatch(tr *fdb.Transaction, isLast bool) error {
	if isLast && c.batchCurrentRecord != nil && !c.batchCurrentRecord.Value.IsDelete {
		if _, _, err := c.inner.next(tr, c.batchCurrentRecord); err != nil {
			return err
		}
	}

	if err := c.inner.endBatch(tr, isLast); err != nil {
		return err
	}

	// Get the read revision for the first batch.
	// Do not read records that might've been concurrently added that are over this revision.
	if latestRevF, err := c.f.rev.GetLatestRev(tr); err != nil {
		return err
	} else if rev, err := latestRevF.Get(); err != nil {
		return err
	} else {
		c.batchRev = rev
	}

	// The requested revision has been compacted
	if c.maxRevision > 0 {
		if compactRev, err := c.f.compactRev.Get(tr); err != nil {
			return err
		} else if c.maxRevision < VersionstampToInt64(compactRev) {
			return server.ErrCompacted
		}
	}

	return nil
}

func (c *recordCollector) postBatch() {
	c.inner.postBatch()
	if c.firstRev == 0 {
		c.firstRev = c.batchRev
	}
	c.rev = c.batchRev
	c.currentRecord = c.batchCurrentRecord
}

func (c *recordCollector) String() string {
	return fmt.Sprintf("{firstRev=%d, rev=%d, currentRecord=%v, inner=%v}", c.firstRev, c.rev, c.currentRecord, c.inner)
}

func (f *FDB) listWithCollector(caller, prefix, startKey string, maxRevision int64, collector Processor[*ByKeyAndRevisionRecord]) (resRev int64, resErr error) {
	// Examples:
	// prefix=/bootstrap/, startKey=/bootstrap
	// prefix=/registry/secrets/, startKey=/registry/secrets/
	// prefix=/, startKey=/registry/health
	// prefix=/registry/health, startKey=/registry/health
	// prefix=/registry/ranges/serviceips, startKey=/registry/ranges/serviceips
	// prefix=/registry/podtemplates/chunking-6414/, startKey=/registry/podtemplates/chunking-6414/template-0016
	// prefix=/registry/masterleases/172.17.0.2, startKey=/registry/masterleases/172.17.0.2
	// prefix=/registry/clusterroles/system:aggregate-to-edit, startKey=/registry/clusterroles/system:aggregate-to-edit

	logrus.Tracef("listWithCollector start (%s): prefix=%s, startKey=%s, maxRevision=%d", caller, prefix, startKey, maxRevision)
	defer func() {
		logrus.Tracef("listWithCollector end (%s): prefix=%s, startKey=%s, maxRevision=%d => resRev=%d collector=%v resErr=%v", caller, prefix, startKey, maxRevision, resRev, collector, resErr)
	}()

	var begin, end fdb.Selectable
	if strings.HasSuffix(prefix, "/") {
		// searching for prefix
		packedStartKey := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{startKey})
		if prefix != startKey {
			// next key afterAll the packedStartKey
			packedStartKeyKey, err := fdb.Strinc(packedStartKey)
			if err != nil {
				return 0, fmt.Errorf("failed to create begin for listKeyValue: %w", err)
			}
			packedStartKey = packedStartKeyKey
		} else {
			// searching for equality
		}
		begin = fdb.FirstGreaterOrEqual(packedStartKey)

		packedPrefix := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{prefix})
		// Removing the last 0x00 from the string encoding in the tuple to have a prefixed search
		// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/2
		packedPrefixKey, err := fdb.Strinc(packedPrefix[:len(packedPrefix)-1])
		// err is always not nil, because tuple string is encoded as '0x02{string}0x00'
		if err != nil {
			return 0, fmt.Errorf("failed to create end for listKeyValue: %w", err)
		}
		// the last key is exclusive https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
		end = fdb.FirstGreaterOrEqual(fdb.Key(packedPrefixKey))
	} else if startKey != prefix {
		return 0, fmt.Errorf("prefix is not equal to startKey. Prefix: %s, startKey: %s", prefix, startKey)
	} else {
		// searching for equality
		k := f.byKeyAndRevision.GetSubspace().Sub(prefix)
		begin, end = k.FDBRangeKeySelectors()
	}

	rc := newRecordCollector(f, maxRevision, collector)
	err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, rc)
	if err != nil {
		return 0, err
	}

	if maxRevision > rc.firstRev {
		return rc.firstRev, server.ErrFutureRev
	}

	if rc.rev != rc.firstRev {
		logrus.Warnf("listWithCollector serializable read (%s): rev=%s, startKey=%s, maxRevision=%d => firstRev=%v rev=%v", caller, prefix, startKey, maxRevision, rc.firstRev, rc.rev)
	}
	return rc.firstRev, nil
}
