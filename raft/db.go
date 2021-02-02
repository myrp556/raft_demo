package raft

import (
    "errors"
    "github.com/tecbot/gorocksdb"
    "log"
    //"strconv"
    "encoding/binary"
)

const (
    DB_PATH = "/gorocksdb"
)

func intToBytes(i int) []byte {
    var buf = make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(i))
    return buf
}

func bytesToInt(buf []byte) int {
    return int(binary.BigEndian.Uint64(buf))
}

type RocksDb struct {
    Name string
    path string
    db *gorocksdb.DB
    readOptions *gorocksdb.ReadOptions
    writeOptions *gorocksdb.WriteOptions
}

func (rocks *RocksDb) Init() bool {
    if db, err:=rocks.openDB(); err!=nil {
        return false
    } else {
        rocks.db = db
    }

    rocks.readOptions = gorocksdb.NewDefaultReadOptions()
    //rocks.readOptions.SetFillCache(true)

    rocks.writeOptions = gorocksdb.NewDefaultWriteOptions()
    //rocks.writeOptions.SetSync(true)
    return true
}

func (rocks *RocksDb) openDB() (*gorocksdb.DB, error) {
    /*
    options := gorocksdb.NewDefaultOptions()
    options.SetCreateIfMissing(true)

    bloomFilter := gorocksdb.NewBloomFilter(10)

    readOptions := gorocksdb.NewDefaultReadOptions()
    readOptions.SetFillCache(false)

    rateLimiter := gorocksdb.NewRateLimiter(10000000, 10000, 10)
    options.SetRateLimiter(rateLimiter)
    options.SetCreateIfMissing(true)
    options.EnableStatistics()
    options.SetWriteBufferSize(8 * 1024)
    options.SetMaxWriteBufferNumber(3)
    options.SetMaxBackgroundCompactions(10)
    // options.SetCompression(gorocksdb.SnappyCompression)
    // options.SetCompactionStyle(gorocksdb.UniversalCompactionStyle)

    options.SetHashSkipListRep(2000000, 4, 4)

    blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
    blockBasedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024))
    blockBasedTableOptions.SetFilterPolicy(bloomFilter)
    blockBasedTableOptions.SetBlockSizeDeviation(5)
    blockBasedTableOptions.SetBlockRestartInterval(10)
    blockBasedTableOptions.SetBlockCacheCompressed(gorocksdb.NewLRUCache(64 * 1024))
    blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
    blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)

    options.SetBlockBasedTableFactory(blockBasedTableOptions)
    //log.Println(bloomFilter, readOptions)
    options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))

    options.SetAllowConcurrentMemtableWrites(false)
    */
    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
    options := gorocksdb.NewDefaultOptions()
    options.SetBlockBasedTableFactory(bbto)
    options.SetCreateIfMissing(true)

    rocks.path = "/rocks/" + rocks.Name
    db, err := gorocksdb.OpenDb(options, rocks.path)

    if err != nil {
        log.Fatalln("OPEN DB error", db, err)
        db.Close()
        return nil, errors.New("fail to open db")
    } else {
        log.Println("OPEN DB success", db)
    }
    return db, nil
}

func (rocks *RocksDb) Put(key []byte, value []byte) {
    rocks.db.Put(rocks.writeOptions, key, value)
}

func (rocks *RocksDb) PutInt(key string, value int) {
    rocks.Put([]byte(key), intToBytes(value))
}

func (rocks *RocksDb) Get(key []byte) []byte {
    slice, err := rocks.db.Get(rocks.readOptions, key)
    if err != nil {
        return nil
    }
    return slice.Data()
}

func (rocks *RocksDb) GetInt(key string) int {
    data := rocks.Get([]byte(key))
    if data != nil {
        return bytesToInt(data)
    } else {
        return -1
    }
}

func (rocks *RocksDb) GetStr(key string) string {
    data := rocks.Get([]byte(key))
    if data != nil {
        return string(data)
    } else {
        return ""
    }
}

func (rocks *RocksDb) Delete(key string) {
    rocks.db.Delete(rocks.writeOptions, []byte(key))
}
