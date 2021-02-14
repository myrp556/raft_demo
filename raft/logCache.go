package raft

import (
    "github.com/myrp556/raft_demo/raft/pb"
)

type LogCache struct {
    entries []pb.Entry
    snapshot *pb.Snapshot
    offset uint64
}

func (cache *LogCache) hasSnapshot() bool {
    return cache.snapshot!=nil && cache.snapshot.Index!=0
}

// only if cache has a snapshot
func (cache *LogCache) firstIndex() (uint64, bool) {
    if cache.snapshot != nil {
        return cache.snapshot.Index + 1, true
    }
    return 0, false
}

func (cache *LogCache) lastIndex() (uint64, bool) {
    if l:=len(cache.entries); l!=0 {
        return cache.offset + uint64(l) -1, true
    }
    if cache.snapshot != nil {
        return cache.snapshot.Index, true
    }
    return 0, false
}

func (cache *LogCache) getTerm(index uint64) (uint64, bool) {
    if index < cache.offset {
        if cache.snapshot!=nil && cache.snapshot.Index==index {
            return cache.snapshot.Term, true
        }
        return 0, false
    }
    lastIndex, ok := cache.lastIndex()
    if ok && index<=lastIndex {
        return cache.entries[index-cache.offset].Term, true
    }
    return 0, false
}

func (cache *LogCache) stableTo(index uint64, term uint64) bool {
    t, ok := cache.getTerm(index)
    if ok && (t==term && index>=cache.offset) {
        // check
        cache.entries = cache.entries[index-cache.offset+1:]
        cache.offset = index+1
        // TODO: shrinkEntries?
        return true
    }
    return false
}

func (cache *LogCache) stableSnapshotTo(index uint64) bool {
    if cache.snapshot!=nil && cache.snapshot.Index == index {
        cache.snapshot = nil
        return true
    }
    return false
}

func (cache *LogCache) restore(snapshot pb.Snapshot) {
    cache.offset = snapshot.Index + 1
    cache.entries = nil
    cache.snapshot = &snapshot
}

func (cache *LogCache) appendEntriesToCache(entries []pb.Entry) (uint64, bool) {
    if len(entries) == 0 {
        return cache.lastIndex()
    }

    index := entries[0].Index
    switch {
    case index == cache.offset+uint64(len(cache.entries)):
        // new entries just next to existing cache entries
        cache.entries = append(cache.entries, entries...)

    case index <= cache.offset:
        // append entries behind offset, then reset offset
        cache.offset = index
        cache.entries = entries

    default:
        // cacheEntry[0:index-offset) is useful, reset of them dispatch
        // then append new entries
        cache.entries = append(cache.entries[:index-cache.offset], entries...)
    }
    return cache.lastIndex()
}

func (cache *LogCache) getEntries(leftIdx uint64, rightIdx uint64) []pb.Entry {
    var entries []pb.Entry
    if leftIdx >= rightIdx {
        return entries
    }
    if leftIdx<cache.offset || rightIdx>cache.offset+uint64(len(cache.entries)) {
        return entries
    }
    return cache.entries[leftIdx-cache.offset : rightIdx-cache.offset]
}
