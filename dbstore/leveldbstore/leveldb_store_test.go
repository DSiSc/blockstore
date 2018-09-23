package leveldbstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var testLevelDB *LevelDBStore

func TestMain(m *testing.M) {
	dbFile := "./testdata"
	var err error
	testLevelDB, err = NewLevelDBStore(dbFile)
	if err != nil {
		fmt.Printf("NewLevelDBStore error:%s\n", err)
		return
	}
	m.Run()
	testLevelDB.Close()
	os.RemoveAll(dbFile)
	os.RemoveAll("ActorLog")
}

func TestLevelDB(t *testing.T) {
	key := "foo"
	value := "bar"
	err := testLevelDB.Put([]byte(key), []byte(value))
	if err != nil {
		t.Errorf("Put error:%s", err)
		return
	}
	v, err := testLevelDB.Get([]byte(key))
	if err != nil {
		t.Errorf("Get error:%s", err)
		return
	}
	if string(v) != value {
		t.Errorf("Get error %s != %s", v, value)
		return
	}
	err = testLevelDB.Delete([]byte(key))
	if err != nil {
		t.Errorf("Delete error:%s", err)
		return
	}
	ok, err := testLevelDB.Has([]byte(key))
	if err != nil {
		t.Errorf("Has error:%s", err)
		return
	}
	if ok {
		t.Errorf("Key:%s shoule delete", key)
		return
	}
}

func TestLevelDBStore_NewBatch(t *testing.T) {
	assert := assert.New(t)
	batch := testLevelDB.NewBatch()
	assert.NotNil(batch)
}

func TestLdbBatch_Put(t *testing.T) {
	assert := assert.New(t)
	batch := testLevelDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
}

func TestLdbBatch_Delete(t *testing.T) {
	assert := assert.New(t)
	batch := testLevelDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
	batch.Delete([]byte("key"))
	assert.Equal(0, batch.ValueSize())
}

func TestLdbBatch_Reset(t *testing.T) {
	assert := assert.New(t)
	batch := testLevelDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	batch.Put([]byte("key1"), []byte("value1"))
	assert.Equal(2, batch.ValueSize())
	batch.Reset()
	assert.Equal(0, batch.ValueSize())
}

func TestLdbBatch_Write(t *testing.T) {
	assert := assert.New(t)
	batch := testLevelDB.NewBatch()
	assert.NotNil(batch)
	batch.Put([]byte("key"), []byte("value"))
	assert.Equal(1, batch.ValueSize())
	batch.Write()
	savedValue, err := testLevelDB.Get([]byte("key"))
	assert.Nil(err)
	assert.Equal([]byte("value"), savedValue)
}
