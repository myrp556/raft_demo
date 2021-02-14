package raft

import (
    //logger "log"
    "fmt"
    "github.com/myrp556/raft_demo/raft/pb"
)

// note: index form 1...
type LogManager struct {
    storage LogStorage
    // cache LogCache
    cache LogCache
    // index of log which has been committed
    committed uint64
    // index of log which has been applied
    applied uint64
}

func NewLogManager(storage LogStorage) *LogManager {
    if storage == nil {
        ERROR("no storage specificed!")
        return nil
    }
    log := &LogManager {
        storage: storage,
    }
    firstIndex, err := storage.FirstIndex()
    if err != nil {
        ERROR("%v", err)
        return nil
    }
    lastIndex, err := storage.LastIndex()
    if err != nil {
        ERROR("%v", err)
        return nil
    }
    log.cache.offset = lastIndex+1
    log.committed = firstIndex-1
    log.applied = firstIndex-1

    return log
}

func (log *LogManager) getTerm(index uint64) (uint64, bool) {
    if index < log.firstIndex()-1 {
        return 0, true
    }
    if index > log.lastIndex() {
        return 0, true
    }

    if term, ok:=log.cache.getTerm(index); ok {
        return term, true
    }

    term, err := log.storage.GetTerm(index)
    if err != nil {
        ERROR("%v", err)
        return 0, false
    }
    return term, true
}

func (log *LogManager) matchLog(index uint64, term uint64) bool {
    t, ok := log.getTerm(index)
    if !ok {
        return false
    }
    return t == term
}

// return the index of input entries which is the first has conflic with existing log
func (log *LogManager) findConflict(entries []pb.Entry) uint64 {
    for _, entry := range entries {
        if !log.matchLog(entry.Index, entry.Term) {
            return entry.Index
        }
    }
    return 0
}

// append entries to cache
func (log *LogManager) appendEntriesToCache(entries []pb.Entry) (uint64, bool) {
    if len(entries) == 0 {
        return log.lastIndex(), true
    }
    if entries[0].Index <= log.committed {
        ERROR("append enties out of range")
        return 0, false
    }

    return log.cache.appendEntriesToCache(entries)
}

// append entires to log, return the last index after append and if success
// add entries from (index, term)
func (log *LogManager) appendEntries(index uint64, term uint64, committed uint64, entries ...pb.Entry) (uint64, bool) {
    if log.matchLog(index, term) {
        // index which log does not have, entries from startIndex are new
        startIndex := log.findConflict(entries)
        switch  {
        case startIndex == 0:
        case startIndex <= log.committed:
            // TODO: conflic with commited entry
        default:
            // append entries which are log not have/conflict from
            // NOTE: index is progress.NextIndex-1
            log.appendEntriesToCache(entries[startIndex-(index+1):])
        }
        lastIdx := index + uint64(len(entries))
        // compare committed index
        log.commitTo(min(committed, lastIdx))
        return lastIdx, true
    }
    return 0, false
}

func (log *LogManager) commitTo(commit uint64) bool {
    if commit>log.committed {
        log.committed = commit
        return true
    }
    return false
}

func (log *LogManager) appliedTo(apply uint64) bool {
    if apply>0 && apply>=log.committed && apply>log.applied {
        log.applied = apply
        return true
    }
    return false
}

// logs' index stop at idx are storaged,
// so move cache and offset make start from idx+1
func (log *LogManager) stableTo(index uint64, term uint64) bool {
    return log.cache.stableTo(index, term)
}

func (log *LogManager) stableSnapshotTo(index uint64) bool {
    return log.cache.stableSnapshotTo(index)
}

func (log *LogManager) firstIndex() uint64 {
    if index, ok:=log.cache.firstIndex(); ok {
        return index
    }
    index, err := log.storage.FirstIndex()
    if err != nil {
        ERROR("%v", err)
        return 0
    }
    return index
}

func (log *LogManager) lastIndex() uint64 {
    if index, ok:=log.cache.lastIndex(); ok {
        return index
    }
    index, err := log.storage.LastIndex()
    if err != nil {
        ERROR("%v", err)
        return 0
    }
    return index
}

func (log *LogManager) getSnapshot() (pb.Snapshot, error) {
    if log.cache.snapshot != nil {
        return *log.cache.snapshot, nil
    }
    return log.storage.GetSnapshot()
}

func (log *LogManager) hasPendingSnapshot() bool {
    return log.cache.hasSnapshot()
}

func (log *LogManager) getEntriesFrom(leftIdx uint64) []pb.Entry {
    return log.getEntries(leftIdx, log.lastIndex()+1)
}

// return entries which index in [left, right)
func (log *LogManager) getEntries(leftIdx uint64, rightIdx uint64) []pb.Entry {
    //logger.Printf(fmt.Sprintf("getEntries: [%d %d), lastIdx=%d, offset=%d", leftIdx, rightIdx, log.lastIndex(), log.offset))
    var entries []pb.Entry
    if leftIdx >= rightIdx {
        return entries
    }

    if leftIdx < log.cache.offset {
        stableEntries, err := log.storage.GetEntries(leftIdx, min(rightIdx, log.cache.offset))
        if err != nil {
            ERROR("%v", err)
            return entries
        }
        entries = stableEntries
    }

    if rightIdx > log.cache.offset {
        cacheEntries := log.cache.getEntries(max(leftIdx, log.cache.offset), rightIdx)
        if len(entries)>0 {
            combine := make([]pb.Entry, len(entries)+len(cacheEntries))
            n := copy(combine, entries)
            copy(combine[n:], cacheEntries)
            entries = combine
        } else {
            entries = cacheEntries
        }
    }

    return entries
}

// return entries which have been committed but not applied
// entries[applied+1, committed+1)
func (log *LogManager) entriesForApply() []pb.Entry {
    start := max(log.applied+1, log.firstIndex())
    return log.getEntries(start, log.committed+1)
}

func (log *LogManager) hasApply() bool {
    return max(log.applied+1, log.firstIndex()) <= log.committed
}

func (log *LogManager) entriesInCache() []pb.Entry {
    return log.cache.entries
}

func (log *LogManager) hasCache() bool {
    return len(log.cache.entries) > 0
}

func (log *LogManager) isUpToDate(index uint64, term uint64) (bool, int) {
    lastTerm := log.lastTerm()
    lastIndex := log.lastIndex()
    if term>lastTerm || (lastTerm==term && index>=lastIndex) {
        return true, 0
    } else {
        if term < lastTerm {
            return false, 1
        } else {
            return false, 2
        }
    }
}

func (log *LogManager) lastTerm() uint64 {
    term, ok := log.getTerm(log.lastIndex())
    if ok {
        return term
    }
    return 0
}

func (log *LogManager) restore(snapshot pb.Snapshot) {
    log.committed = snapshot.Index
    log.cache.restore(snapshot)
}

func IsEmptySnapshot(snapshot pb.Snapshot) bool {
    return snapshot.Index == 0
}

func (node *Node) ReportLog() string {
    str := getEntriesIndexStr(node.logManager.getEntries(node.logManager.firstIndex(), node.logManager.lastIndex()+1))
    str = str + fmt.Sprintf(" com=%d off=%d", node.logManager.committed, node.logManager.cache.offset)
    return str
}

func max(a uint64, b uint64) uint64 {
    if a > b {
        return a
    }
    return b
}

func min(a uint64, b uint64) uint64 {
    if a <  b {
        return a
    }
    return b
}
