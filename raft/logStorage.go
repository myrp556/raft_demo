package raft

import (
    "sync"
    "errors"
    "github.com/myrp556/raft_demo/raft/pb"
)

type LogStorage interface {
    GetState() (pb.Milestone, error)
    GetEntries(left uint64, right uint64) ([]pb.Entry, error)
    GetTerm(index uint64) (uint64, error)
    FirstIndex() (uint64, error)
    LastIndex() (uint64, error)
    GetSnapshot() (pb.Snapshot, error)
}

type MemoryStorage struct {
    sync.Mutex

    milestone pb.Milestone
    snapshot pb.Snapshot
    entries []pb.Entry
}

func NewMemoryStorage() *MemoryStorage {
    return &MemoryStorage{entries: make([]pb.Entry, 1)}
}

func (storage *MemoryStorage) GetState() (pb.Milestone, error) {
    return storage.milestone, nil
}

func (storage *MemoryStorage) SetMilestone(stone pb.Milestone) error {
    storage.Lock()
    defer storage.Unlock()
    storage.milestone = stone
    return nil
}

func (storage *MemoryStorage) GetEntries(left uint64, right uint64) ([]pb.Entry, error) {
    storage.Lock()
    defer storage.Unlock()
    offset := storage.entries[0].Index
    if left <= offset {
        return nil, ErrCompacted
    }
    if right > storage.lastIndex()+1 {
        return nil, ErrCompacted
    }
    if len(storage.entries) == 1 {
        return nil, ErrUnavailable
    }

    entries := storage.entries[left-offset : right-offset]
    return entries, nil
}

func (storage *MemoryStorage) GetTerm(index uint64) (uint64, error) {
    storage.Lock()
    defer storage.Unlock()
    offset := storage.entries[0].Index
    if index < offset {
        return 0, ErrCompacted
    }
    if index >= offset+uint64(len(storage.entries)) {
        return 0, ErrCompacted
    }
    return storage.entries[index-offset].Term, nil
}

func (storage *MemoryStorage) FirstIndex() (uint64, error) {
    storage.Lock()
    defer storage.Unlock()
    return storage.firstIndex(), nil
}

func (storage *MemoryStorage) firstIndex() uint64 {
    return storage.entries[0].Index + 1
}

func (storage *MemoryStorage) LastIndex() (uint64, error) {
    storage.Lock()
    defer storage.Unlock()
    return storage.lastIndex(), nil
}

func (storage *MemoryStorage) lastIndex() uint64 {
    return storage.entries[0].Index + uint64(len(storage.entries)) - 1
}

func (storage *MemoryStorage) GetSnapshot() (pb.Snapshot, error) {
    storage.Lock()
    defer storage.Unlock()
    return storage.snapshot, nil
}

func (storage *MemoryStorage) ApplySnapshot(snapshot pb.Snapshot) error {
    storage.Lock()
    defer storage.Unlock()

    newIndex := snapshot.Index
    index := storage.snapshot.Index
    if index >= newIndex {
        return ErrSnapOutOfDate
    }
    
    storage.snapshot = snapshot
    storage.entries = []pb.Entry{{Term: snapshot.Term, Index: snapshot.Index}}
    return nil
}

// compact [:compactIndex], remain [compatIndex+1:]
func (storage *MemoryStorage) Compact(compactIndex uint64) error {
    storage.Lock()
    defer storage.Unlock()

    offset := storage.entries[0].Index
    if compactIndex <= offset {
        return ErrCompacted
    }
    if compactIndex > storage.lastIndex() {
        return ErrCompacted
    }

    i := compactIndex - offset
    entries := make([]pb.Entry, 1)
    entries[0].Term = storage.entries[i].Term
    entries[0].Index = storage.entries[i].Index
    entries = append(entries, storage.entries[i+1:]...)
    storage.entries = entries
    return nil
}

func (storage *MemoryStorage) Append(entries []pb.Entry) error {
    if len(entries) == 0 {
        return nil
    }

    storage.Lock()
    defer storage.Unlock()

    firstIndex := storage.firstIndex()
    last := entries[0].Index + uint64(len(entries)) - 1

    if last < firstIndex {
        return nil
    }

    if firstIndex > entries[0].Index {
        entries = entries[firstIndex-entries[0].Index:]
    }

    offset := entries[0].Index - storage.entries[0].Index
    switch {
    case uint64(len(storage.entries)) > offset:
        storage.entries = append([]pb.Entry{}, storage.entries[:offset]...)
        storage.entries = append(storage.entries, entries...)
    case uint64(len(storage.entries)) == offset:
        storage.entries = append(storage.entries, entries...)
    default:
        // miss log
    }
    return nil
}

var ErrCompacted = errors.New("invailed index for compacted log")
var ErrUnavailable = errors.New("invailed index for unavailable log")
var ErrSnapOutOfDate = errors.New("snapshot out of date")
