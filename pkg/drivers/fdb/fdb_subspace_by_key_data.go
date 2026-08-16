package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ByKeyDataSubspace stores record values chunked under (key, rev, offset),
// ordered the same way as the by-key-and-revision index. A LIST batch can
// therefore fetch all its values with one contiguous range read instead of a
// point read per record; see listCollector.endBatch.
//
// Tombstones (deletes) are not written here — LIST skips them, and watch/delete
// events read their value from the by-revision subspace. Records written
// before this subspace existed are absent here; readers fall back to the
// by-revision point-read path (lazy migration).
type ByKeyDataSubspace struct {
	subspace subspace.Subspace
}

func CreateByKeyDataSubspace(directory directory.DirectorySubspace) *ByKeyDataSubspace {
	return &ByKeyDataSubspace{subspace: directory.Sub("byKeyData")}
}

func (s *ByKeyDataSubspace) GetSubspace() subspace.Subspace {
	return s.subspace
}

func (s *ByKeyDataSubspace) WriteBlob(tr *fdb.Transaction, key string, rev tuple.Versionstamp, value []byte) error {
	for offset := 0; offset < len(value); offset += chunkSize {
		end := min(offset+chunkSize, len(value))
		packKey, setValue := GetWriteOps(tr, s.subspace)
		chunkKey, err := packKey(tuple.Tuple{key, rev, offset})
		if err != nil {
			return err
		}
		setValue(chunkKey, value[offset:end])
	}
	return nil
}

func (s *ByKeyDataSubspace) Clear(tr *fdb.Transaction, key string, rev tuple.Versionstamp) error {
	selector, err := fdb.PrefixRange(s.subspace.Pack(tuple.Tuple{key, rev}))
	if err != nil {
		return err
	}
	tr.ClearRange(selector)
	return nil
}

type dataChunk struct {
	key   string
	rev   tuple.Versionstamp
	value []byte
}

func (s *ByKeyDataSubspace) parseKV(kv fdb.KeyValue) (*dataChunk, error) {
	t, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return nil, err
	}
	return &dataChunk{
		key:   t[0].(string),
		rev:   t[1].(tuple.Versionstamp),
		value: kv.Value,
	}, nil
}

// RangeForKeys returns a selector range covering every chunk of every revision
// of the keys in [firstKey, lastKey].
func (s *ByKeyDataSubspace) RangeForKeys(firstKey, lastKey string) (fdb.SelectorRange, error) {
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
