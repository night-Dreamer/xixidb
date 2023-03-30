package xixidb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}
func BenchmarkOpen(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Open("/tmp/minidb")
	}
}

func TestMiniDB_Put(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, val)
	}

	if err != nil {
		t.Log(err)
	}
}
func Benchmark_Put(b *testing.B) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		db.Put([]byte("test_key_0"), []byte("test_val_4804556042014581903"))
	}
}

func TestMiniDB_Get(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))
	//getVal([]byte("test_key_5"))
}
func Benchmark_Get(b *testing.B) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		b.Error(err)
	}
	db.Put([]byte("test_key_0"), []byte("test_val_4804556042014581903"))
	getVal := func(key []byte) {
		_, err := db.Get(key)
		if err != nil {
			b.Error("read val err: ", err)
		} else {
			//b.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getVal([]byte("test_key_0"))
	}
}

func TestMiniDB_Del(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_78")
	err = db.Del(key)

	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestMiniDB_Merge(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}
