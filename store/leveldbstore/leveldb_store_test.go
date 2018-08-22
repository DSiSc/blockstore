package leveldbstore

import (
	"fmt"
	"os"
	"testing"
)

var testLevelDB *LevelDBStore

func TestMain(m *testing.M) {
	dbFile := "./test"
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
