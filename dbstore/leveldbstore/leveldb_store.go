package leveldbstore

import (
	"github.com/DSiSc/blockstore/dbstore"
	"github.com/DSiSc/craft/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDBStore struct {
	db *leveldb.DB
}

// used to compute the size of bloom filter bits array .
// too small will lead to high false positive rate.
const BITSPERKEY = 10

func NewLevelDBStore(file string) (*LevelDBStore, error) {
	// default opt
	o := opt.Options{
		NoSync: false,
		Filter: filter.NewBloomFilter(BITSPERKEY),
	}
	db, err := leveldb.OpenFile(file, &o)

	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		log.Error("Recover db file.")
		db, err = leveldb.RecoverFile(file, nil)
	}

	if err != nil {
		log.Error("Open db file failed..")
		return nil, err
	}

	return &LevelDBStore{
		db: db,
	}, nil
}

// Put a key-value pair to leveldb
func (self *LevelDBStore) Put(key []byte, value []byte) error {
	return self.db.Put(key, value, nil)
}

// Get the value of a key from leveldb
func (self *LevelDBStore) Get(key []byte) ([]byte, error) {
	value, err := self.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, err
		}
		return nil, err
	}
	return value, nil
}

// Has return whether the key is exist in leveldb
func (self *LevelDBStore) Has(key []byte) (bool, error) {
	return self.db.Has(key, nil)
}

// Delete the the in leveldb
func (self *LevelDBStore) Delete(key []byte) error {
	return self.db.Delete(key, nil)
}

//NewBatch create db batch
func (self *LevelDBStore) NewBatch() dbstore.Batch {
	return &ldbBatch{db: self.db, b: new(leveldb.Batch)}
}

// Close leveldb
func (self *LevelDBStore) Close() error {
	err := self.db.Close()
	return err
}

type ldbBatch struct {
	db   *leveldb.DB
	b    *leveldb.Batch
	size int
}

func (b *ldbBatch) Put(key, value []byte) error {
	b.b.Put(key, value)
	b.size += 1
	return nil
}

func (b *ldbBatch) Delete(key []byte) error {
	b.b.Delete(key)
	b.size -= 1
	return nil
}

func (b *ldbBatch) Write() error {
	return b.db.Write(b.b, nil)
}

func (b *ldbBatch) ValueSize() int {
	return b.size
}

func (b *ldbBatch) Reset() {
	b.b.Reset()
	b.size = 0
}
