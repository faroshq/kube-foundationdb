package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ByKeyCurrentSubspace stores the CURRENT value of each live key, chunked
// under (key, offset) — no revision in the key. It is written atomically with
// the index in the same transaction, so within any read transaction it is
// exactly consistent with the newest index entry per key (snapshot isolation).
//
// This makes an unpaginated LIST scan exactly the live bytes of the prefix in
// one contiguous range read — no per-record point reads, no transfer of
// superseded revisions — and the subspace never needs compaction: updates
// replace chunks in place and deletes clear them.
//
// Readers may only join this data against a record that is the newest revision
// of its key within the scan (recordCollector tracks that); revision-pinned
// reads of older versions, and records that predate this subspace, fall back
// to by-revision point reads.
type ByKeyCurrentSubspace struct {
	subspace subspace.Subspace
}

func CreateByKeyCurrentSubspace(directory directory.DirectorySubspace) *ByKeyCurrentSubspace {
	return &ByKeyCurrentSubspace{subspace: directory.Sub("byKeyCurrent")}
}

func (s *ByKeyCurrentSubspace) GetSubspace() subspace.Subspace {
	return s.subspace
}

// Replace atomically swaps the stored value for key. prevSize is the size of
// the previously stored value (0 if none, negative if unknown). Chunk offsets
// are deterministic, so new chunks overwrite old ones in place; a clear is
// only needed for stale tail chunks when the value shrank — range clears
// leave tombstones that slow subsequent scans until the storage engine
// compacts them, so avoiding gratuitous clears keeps LISTs fast.
func (s *ByKeyCurrentSubspace) Replace(tr *fdb.Transaction, key string, value []byte, prevSize int64) error {
	if prevSize < 0 {
		if err := s.Clear(tr, key); err != nil {
			return err
		}
	} else if numChunks(prevSize) > numChunks(int64(len(value))) {
		begin := s.subspace.Pack(tuple.Tuple{key, int(numChunks(int64(len(value)))) * chunkSize})
		endPrefix, err := fdb.Strinc(s.subspace.Pack(tuple.Tuple{key}))
		if err != nil {
			return err
		}
		tr.ClearRange(fdb.KeyRange{Begin: begin, End: fdb.Key(endPrefix)})
	}
	for offset := 0; offset < len(value); offset += chunkSize {
		end := min(offset+chunkSize, len(value))
		tr.Set(s.subspace.Pack(tuple.Tuple{key, offset}), value[offset:end])
	}
	return nil
}

func numChunks(size int64) int64 {
	return (size + chunkSize - 1) / chunkSize
}

func (s *ByKeyCurrentSubspace) Clear(tr *fdb.Transaction, key string) error {
	selector, err := fdb.PrefixRange(s.subspace.Pack(tuple.Tuple{key}))
	if err != nil {
		return err
	}
	tr.ClearRange(selector)
	return nil
}

type dataChunk struct {
	key   string
	value []byte
}

func (s *ByKeyCurrentSubspace) parseKV(kv fdb.KeyValue) (*dataChunk, error) {
	t, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return nil, err
	}
	return &dataChunk{
		key:   t[0].(string),
		value: kv.Value,
	}, nil
}

// RangeForKeys returns a selector range covering every chunk of the keys in
// [firstKey, lastKey].
func (s *ByKeyCurrentSubspace) RangeForKeys(firstKey, lastKey string) (fdb.SelectorRange, error) {
	begin := s.subspace.Pack(tuple.Tuple{firstKey})
	endPrefix, err := fdb.Strinc(s.subspace.Pack(tuple.Tuple{lastKey}))
	if err != nil {
		return fdb.SelectorRange{}, err
	}
	return fdb.SelectorRange{
		Begin: fdb.FirstGreaterOrEqual(begin),
		End:   fdb.FirstGreaterOrEqual(fdb.Key(endPrefix)),
	}, nil
}
