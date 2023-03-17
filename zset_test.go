package godb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestGoDb_ZAdd(t *testing.T) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	opts.IndexMode = KeyOnlyMemMode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key    []byte
		score  float64
		member []byte
	}
	tests := []struct {
		name    string
		db      *GoDb
		args    args
		wantErr bool
	}{
		{
			"normal-1", db, args{key: GetKey(1), score: 100, member: GetValue16B()}, false,
		},
		{
			"normal-2", db, args{key: GetKey(1), score: 100, member: GetValue16B()}, false,
		},
		{
			"normal-3", db, args{key: GetKey(1), score: 200, member: GetValue16B()}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.ZAdd(tt.args.key, tt.args.score, tt.args.member); (err != nil) != tt.wantErr {
				t.Errorf("ZAdd() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGoDb_ZScore(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testGoDbZScore(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testGoDbZScore(t, MMap, KeyValueMemMode)
	})
}

func testGoDbZScore(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	ok1, score1 := db.ZScore(zsetKey, GetKey(0))
	assert.Equal(t, false, ok1)
	assert.Equal(t, float64(0), score1)

	err = db.ZAdd(zsetKey, 123.33, GetKey(0))
	assert.Nil(t, err)

	ok2, score2 := db.ZScore(zsetKey, GetKey(0))
	assert.Equal(t, true, ok2)
	assert.Equal(t, 123.33, score2)

	err = db.ZAdd(zsetKey, 223.33, GetKey(0))
	assert.Nil(t, err)

	ok3, score3 := db.ZScore(zsetKey, GetKey(0))
	assert.Equal(t, true, ok3)
	assert.Equal(t, 223.33, score3)

	// reopen and get
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	ok4, score4 := db2.ZScore(zsetKey, GetKey(0))
	assert.Equal(t, true, ok4)
	assert.Equal(t, 223.33, score4)
}

func TestGoDb_ZRem(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testGoDbZRem(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testGoDbZRem(t, MMap, KeyValueMemMode)
	})
}

func testGoDbZRem(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	err = db.ZAdd(zsetKey, 11.33, GetKey(0))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 21.33, GetKey(1))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 31.33, GetKey(2))
	assert.Nil(t, err)

	c1 := db.ZCard(zsetKey)
	assert.Equal(t, 3, c1)

	err = db.ZRem(zsetKey, GetKey(1))
	assert.Nil(t, err)

	c2 := db.ZCard(zsetKey)
	assert.Equal(t, 2, c2)
	ok, _ := db.ZScore(zsetKey, GetKey(1))
	assert.Equal(t, false, ok)
}

func TestGoDb_ZCard(t *testing.T) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	c1 := db.ZCard(zsetKey)
	assert.Equal(t, 0, c1)

	err = db.ZAdd(zsetKey, 11.33, GetKey(0))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 21.33, GetKey(1))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 31.33, GetKey(2))
	assert.Nil(t, err)

	c2 := db.ZCard(zsetKey)
	assert.Equal(t, 3, c2)
}

func TestGoDb_ZRange(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testGoDbZRange(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testGoDbZRange(t, MMap, KeyOnlyMemMode)
	})
}

func testGoDbZRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	err = db.ZAdd(zsetKey, 32.55, GetKey(0))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 99.34, GetKey(1))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 31.33, GetKey(2))
	assert.Nil(t, err)
	err = db.ZAdd(zsetKey, 54.10, GetKey(3))
	assert.Nil(t, err)

	values, err := db.ZRange(zsetKey, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(values))
}

func TestGoDb_ZRevRange(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testGoDbZRevRange(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testGoDbZRevRange(t, MMap, KeyOnlyMemMode)
	})
}

func testGoDbZRevRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	for i := 0; i < 100; i++ {
		err := db.ZAdd(zsetKey, float64(i+100), GetKey(i))
		assert.Nil(t, err)
	}

	ok, score := db.ZScore(zsetKey, GetKey(3))
	assert.True(t, ok)
	assert.Equal(t, float64(103), score)

	values, err := db.ZRevRange(zsetKey, 1, 10)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(values))
}

func TestGoDb_ZRank(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testGoDbZRank(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testGoDbZRank(t, MMap, KeyOnlyMemMode)
	})
}

func testGoDbZRank(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	for i := 0; i < 100; i++ {
		err := db.ZAdd(zsetKey, float64(i+100), GetKey(i))
		assert.Nil(t, err)
	}

	ok, r1 := db.ZRank(zsetKey, GetKey(-1))
	assert.False(t, ok)
	assert.Equal(t, 0, r1)

	ok, r2 := db.ZRank(zsetKey, GetKey(3))
	assert.True(t, ok)
	assert.Equal(t, 3, r2)
	ok, r3 := db.ZRevRank(zsetKey, GetKey(1))
	assert.True(t, ok)
	assert.Equal(t, 98, r3)
}

func TestGoDb_ZSetGC(t *testing.T) {
	path := filepath.Join("/tmp", "godb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	zsetKey := []byte("my_zset")
	writeCount := 500000
	for i := 0; i < writeCount; i++ {
		err := db.ZAdd(zsetKey, float64(i+100), GetKey(i))
		assert.Nil(t, err)
	}

	for i := 0; i < writeCount/2; i++ {
		err := db.ZRem(zsetKey, GetKey(i))
		assert.Nil(t, err)
	}

	err = db.RunLogFileGC(ZSet, 0, 0.1)
	assert.Nil(t, err)

	card := db.ZCard(zsetKey)
	assert.Equal(t, writeCount/2, card)
}
