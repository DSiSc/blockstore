package memorystore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	key   = []byte("hello")
	value = []byte("world")
)

// test new MemDBStore
func TestNewMemDBStore(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
}

// test put content to database.
func TestMemDBStore_Put(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	err := memDB.Put(key, value)
	assert.Nil(err)
}

// test get content from database.
func TestMemDBStore_Get(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	err := memDB.Put(key, value)
	assert.Nil(err)

	dbContent, err := memDB.Get(key)
	assert.Nil(err)
	assert.Equal(value, dbContent)
}

// test delete from database.
func TestMemDBStore_Delete(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	err := memDB.Put(key, value)
	assert.Nil(err)

	err = memDB.Delete(key)
	assert.Nil(err)
}

func TestMemDBStore_NewBatch(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	batch := memDB.NewBatch()
	assert.NotNil(batch)
}

func TestMemBatch_Put(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	batch := memDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
}

func TestMemBatch_Delete(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	batch := memDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
	batch.Delete([]byte("key"))
	assert.Equal(0, batch.ValueSize())
}

func TestMemBatch_Reset(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	batch := memDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	batch.Put([]byte("key1"), []byte("value1"))
	assert.Equal(2, batch.ValueSize())
	batch.Reset()
	assert.Equal(0, batch.ValueSize())
}

func TestMemBatch_Write(t *testing.T) {
	assert := assert.New(t)
	memDB := NewMemDBStore()
	assert.NotNil(memDB)
	batch := memDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
	batch.Write()
	savedValue, err := memDB.Get([]byte("key"))
	assert.Nil(err)
	assert.Equal([]byte("value"), savedValue)
}
