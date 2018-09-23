package memorystore

import (
	"errors"
	"github.com/DSiSc/blockstore/dbstore"
	"sync"
)

// MemDBStore is a test memory database.
type MemDBStore struct {
	db   map[string][]byte
	lock sync.RWMutex
}

// NewMemDBStore create a memory database instance.
func NewMemDBStore() *MemDBStore {
	return &MemDBStore{
		db: make(map[string][]byte),
	}
}

// Put save content to database
func (db *MemDBStore) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.db[string(key)] = copyBytes(value)
	return nil
}

// Get get content from database.
func (db *MemDBStore) Get(key []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if entry, ok := db.db[string(key)]; ok {
		return copyBytes(entry), nil
	}
	return nil, errors.New("not found")
}

func (db *MemDBStore) Delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	delete(db.db, string(key))
	return nil
}

//NewBatch create db batch
func (self *MemDBStore) NewBatch() dbstore.Batch {
	return &memBatch{db: self, batchCache: make(map[string][]byte)}
}

// copy byte from sources byte array.
func copyBytes(b []byte) (copiedBytes []byte) {
	if b == nil {
		return nil
	}
	copiedBytes = make([]byte, len(b))
	copy(copiedBytes, b)
	return copiedBytes
}

type memBatch struct {
	db         *MemDBStore
	batchCache map[string][]byte
}

func (b *memBatch) Put(key, value []byte) error {
	b.batchCache[string(key)] = value
	return nil
}

func (b *memBatch) Delete(key []byte) error {
	delete(b.batchCache, string(key))
	return nil
}

func (b *memBatch) Write() error {
	b.db.lock.Lock()
	defer b.db.lock.Unlock()
	for key, value := range b.batchCache {
		b.db.db[key] = value
	}
	b.batchCache = make(map[string][]byte)
	return nil
}

func (b *memBatch) ValueSize() int {
	return len(b.batchCache)
}

func (b *memBatch) Reset() {
	b.batchCache = make(map[string][]byte)
}
