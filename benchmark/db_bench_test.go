package benchmark

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/herott-ai/godb"
	"github.com/stretchr/testify/assert"
)

var roseDB *godb.GoDb

func init() {
	path := filepath.Join("/tmp", "godb_bench")
	opts := godb.DefaultOptions(path)
	var err error
	roseDB, err = godb.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("open godb err: %v", err))
	}
	initDataForGet()
}

func initDataForGet() {
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := roseDB.Set(getKey(i), getValue128B())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGoDb_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Set(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkGoDb_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := roseDB.Get(getKey(i))
		assert.Nil(b, err)
	}
}

func BenchmarkGoDb_LPush(b *testing.B) {
	keys := [][]byte{
		[]byte("my_list-1"),
		[]byte("my_list-2"),
		[]byte("my_list-3"),
		[]byte("my_list-4"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := roseDB.LPush(keys[k], getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkGoDb_ZAdd(b *testing.B) {
	keys := [][]byte{
		[]byte("my_zset-1"),
		[]byte("my_zset-2"),
		[]byte("my_zset-3"),
		[]byte("my_zset-4"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := roseDB.ZAdd(keys[k], float64(i+100), getValue128B())
		assert.Nil(b, err)
	}
}
