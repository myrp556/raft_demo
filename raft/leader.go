package raft

import (
    "fmt"
    "github.com/myrp556/raft_demo/raft/pb"
)

func (node *Node) becomeLeader() {
    if node.Type == Follower {
        node.ERROR("invalid change from follower to leader")
        return
    }
    node.resetNode(node.Term)
    node.Leader = node.ID
    node.Type = Leader
    node.typeTick = node.tickLeader
    node.typeProcessMessage = node.processLeaderMessage
    node.randomElectionTimeout()

    node.INFO("become leader")
}

func (node *Node) tickLeader() {
    node.heartbeatElapse ++
    node.electionElapse ++

    if node.electionElapse >= node.electionRandomTimeout {
        node.electionElapse = 0
    }

    if node.heartbeatElapse >= node.heartbeatTimeout {
        node.heartbeatTimeout = 0
        node.DEBUG("Heartbeat timeout")
        message := pb.Message{Type: pb.SendHeartbeat, Src: node.ID}
        node.processMessage(message)
    }
}

func (node *Node) boardcastHeartbeat() {
    if node.Type!=Leader || node.Leader!=node.ID {
        node.ERROR("can not send heartbeat, you are %s", NodeTypeName[node.Type])
        return
    }
    for ID := range node.nodeProgress {
        if node.nodeProgress[ID].Live && ID!=node.ID {
            node.sendHeartbeat(ID)
        }
    }
}

func (node *Node) sendHeartbeat(ID uint64) {
    if node.nodeProgress[ID].Live {
        node.DEBUG("send Hearbeat to node %d", ID)
        message := pb.Message{
            Src: node.ID, 
            Dst: ID, 
            Type: pb.HeartbeatRequest, 
            Term: node.Term, 
            Commit: min(node.logManager.committed, node.nodeProgress[ID].MarchIndex)}
        node.sendMessage(message)
    }
}

func (node *Node) boardcastAppendEntries () {
    for ID := range node.nodeProgress {
        if node.nodeProgress[ID].Live && ID != node.ID {
            node.sendAppendEntries(ID, true)
        }
    }
}

func getEntriesIndexStr(entries []pb.Entry) string {
    ret := ""
    for _, entry:=range entries {
        ret += fmt.Sprintf("%d ", entry.Index)
    }
    return ret
}

func (node *Node) sendAppendEntries (ID uint64, sendIfEmpty bool) bool {
    if node.Leader!=node.ID || node.Type!=Leader {
        node.ERROR("can not sendAppendEntries to node %d cause to type %s", ID, NodeTypeName[node.Type])
        return false
    }
    
    progress := node.nodeProgress[ID]
    term, ok := node.logManager.getTerm(progress.NextIndex-1)
    entries := node.logManager.getEntriesFrom(progress.NextIndex)

    if !ok {
        node.ERROR("get term for next index %d failed", progress.NextIndex-1)
        return false
    }
    // if node's entry has up to date, not send
    if len(entries)==0 && !sendIfEmpty {
        return false
    }
    
    message := pb.Message{}
    message.Src = node.ID
    message.Dst = ID
    message.Type = pb.AppendEntriesRequest
    message.Term = node.Term
    // NOTE: message.Index indicates Index which should insert logs from, like (..., Index, AppendEntries...)
    message.Index = progress.NextIndex - 1
    message.LogTerm = term
    message.Commit = node.logManager.committed
    message.Entries = entries
    //message.Entries = node.logManager.getEntriesFrom(progress.NextIndex)

    node.DEBUG("send entries [%s](%d) index %d, term %d, logTerm %d, commit %d, to node %d", getEntriesIndexStr(message.Entries), (len(entries)), message.Index, message.Term, message.LogTerm, message.Commit, ID)
    node.sendMessage(message)
    return true
}

func (node *Node) appendEntry(entries ...pb.Entry) bool {
    // to do add entries to log
    lastIndex := node.logManager.lastIndex()
    for i, _ := range entries {
        entries[i].Term = node.Term
        entries[i].Index = lastIndex + uint64(i+1)
    }

    // the lastest index
    //lastestIndex, ok := node.logManager.appendEntries(entries...)
    lastestIndex, ok := node.logManager.appendEntriesToCache(entries)
    if ok {
        node.DEBUG("append entries to cache success, index=%d, cache=[%s]", lastestIndex, getEntriesIndexStr(node.logManager.cacheEntries))
        // update progress
        node.updateProgress(node.ID, lastestIndex)
        node.commitEntries()
        return true
    } else {
        node.DEBUG("append entries to cache failed")
    }
    return false
}

func (node *Node) commitEntries() bool {
    index := node.getLowMarch()
    if node.logManager.commitTo(index) {
        node.INFO("commit to %d", index)
        return true
    }
    return false
}

func (node *Node) processLeaderMessage(message pb.Message) error {
    srcID := message.Src

    switch message.Type {
    case pb.SendHeartbeat:
        node.boardcastHeartbeat()

    case pb.HeartbeatResponse:
        node.DEBUG("receive HeartbeatResponse from node %d", srcID)

        progress := node.nodeProgress[srcID]
        progress.Live = true
        if progress.MarchIndex < node.logManager.lastIndex() {
            node.sendAppendEntries(srcID, true)
        }

    case pb.AppendEntriesResponse:
        if message.Reject {
            node.INFO("AppendEntries REJECTED by node %d, lastest index %d, send index %d", srcID, message.RejectHint, message.Index)
            if node.decreseProgress(srcID, message.Index, message.RejectHint) {
                node.INFO("decrese node %d next to %d, retry appendEntries", srcID, node.nodeProgress[srcID].NextIndex)
                node.sendAppendEntries(srcID, false)
            }
        } else {
            // update progress, and commit
            node.updateProgress(srcID, message.Index)
            node.INFO("update node %d progress to %d", srcID, message.Index)
            // if commit success, boardcast append
            if node.commitEntries() {
                node.boardcastAppendEntries()
            }
        }

    case pb.ProposeMessage:
        node.DEBUG("get propose message")

        ok:=node.appendEntry(message.Entries...)
        if !ok {
            node.ERROR("append entries failed")
            return ErrDropProposeMessage
        }
        node.boardcastAppendEntries()

    }
    return nil
}
