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
