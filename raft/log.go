package raft

import (
    //logger "log"
    //"fmt"
    "github.com/myrp556/raft_demo/raft/pb"
)

// note: index form 1...
type LogManager struct {
    // cache LogCache
    cacheEntries []pb.Entry
    // index of log which has been committed
    committed uint64
    // index of log which has been applied
    applied uint64
    // cacheEntry[i] has index=offset+i
    offset uint64
}

func CreateLog() *LogManager {
    log := &LogManager {}
    log.reset()

    return log
}

func (log *LogManager) reset() {
    log.cacheEntries = []pb.Entry {}
    log.committed = 0
    log.applied = 0
    // offset+i == index (1, 2, 3...)
    log.offset = 1
}

func (log *LogManager) indexToPos(index uint64) uint32 {
    return uint32(index)-uint32(log.offset)
}

func (log *LogManager) posToIndex(pos uint32) uint64 {
    return uint64(pos) + log.offset
}

func (log *LogManager) getTerm(index uint64) (uint64, bool) {
    // special for 0 index, which could be use for none log exist
    if index == 0 {
        return 0, true
    }
    lastIdx := log.lastIndex()
    if index > lastIdx {
        return 0, false
    }
    return log.cacheEntries[log.indexToPos(index)].Term, true
}

func (log *LogManager) empty() bool {
    return len(log.cacheEntries) == 0
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
        return log.lastIndex(), false
    }
    if entries[0].Index <= log.committed {
        return log.lastIndex(), false
    }

    index := entries[0].Index
    switch {
    case index == log.offset+uint64(len(log.cacheEntries)):
        // new entries just next to existing cache entries
        log.cacheEntries = append(log.cacheEntries, entries...)

    case index <= log.offset:
        // append entries behind offset, then reset offset
        log.offset = index
        log.cacheEntries = entries

    default:
        // cacheEntry[0:index-offset) is useful, reset of them dispatch
        // then append new entries
        log.cacheEntries = append(log.cacheEntries[:log.indexToPos(index)], entries...)
    }
    return log.lastIndex(), true
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
    if (commit > log.committed) {
        log.committed = commit
        return true
    }
    return false
}

func (log *LogManager) appliedTo(apply uint64) bool {
    if (apply>0 && apply>=log.committed && apply>log.applied) {
        log.applied = apply
        return true
    }
    return false
}

// logs' index stop at idx are storaged,
// so move cache and offset make start from idx+1
func (log *LogManager) stableTo(index uint64, term uint64) bool {
    t, ok := log.getTerm(index)
    if ok && (t==term && index>=log.offset) {
        // check
        log.cacheEntries = log.cacheEntries[log.indexToPos(index)+1:]
        log.offset = index+1
        // TODO: shrinkEntries?
        return true
    }
    return false
}

func (log *LogManager) firstIndex() uint64 {
    // TODO: add storage
    return uint64(0)
}

func (log *LogManager) lastIndex() uint64 {
    return log.offset+uint64(len(log.cacheEntries))-1
}

func (log *LogManager) getEntriesFrom(leftIdx uint64) []pb.Entry {
    return log.getEntries(leftIdx, log.lastIndex()+1)
}

// return entries which index in [left, right)
func (log *LogManager) getEntries(leftIdx uint64, rightIdx uint64) []pb.Entry {
    //logger.Printf(fmt.Sprintf("getEntries: [%d %d), lastIdx=%d, offset=%d", leftIdx, rightIdx, log.lastIndex(), log.offset))
    var entries []pb.Entry
    if leftIdx>0 && rightIdx<=log.lastIndex()+1 && leftIdx < rightIdx {
        entries = log.cacheEntries[log.indexToPos(leftIdx) : log.indexToPos(rightIdx)]
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
    return log.cacheEntries
}

func (log *LogManager) hasCache() bool {
    return len(log.cacheEntries) > 0
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
