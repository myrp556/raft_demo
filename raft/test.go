package raft

import (
    "time"
    "log"
    "fmt"
    "github.com/myrp556/raft_demo/raft/pb"
)

func ERROR(pattern string, args ...interface{}) {
    log.Println("ERROR: " + fmt.Sprintf(pattern, args...))
}

func SUCCESS(pattern string, args ...interface{}) {
    log.Println("SUCCESS: " + fmt.Sprintf(pattern, args...))
}

func newTestNode(numNode int) *Node {
    peers := []Peer{}
    for i:=0; i<numNode; i++ {
        peers = append(peers, Peer{ID: uint64(i+1)})
    }
    node := CreateNode(1, NewMemoryStorage(), peers)
    //node.OpenLog = false
    //node.OpenDebug = false
    return node
}

func TestNodeTick() bool {
    node := newTestNode(1)
    node.StartNode()
    elapsed := node.electionElapse
    node.Tick()
    for len(node.tickChannel) != 0 {
        time.Sleep(100 * time.Millisecond)
    }
    node.StopNode()
    if node.electionElapse != elapsed+1 {
        ERROR("elapsed != %d", elapsed+1)
        return false
    } else {
        SUCCESS("passed node tick test")
        return true
    }
}

func TestNodeCampaign() bool {
    node := newTestNode(3)
    node.campaign(VoteCampaign)

    if node.voteTo != node.ID {
        ERROR("not vote to self")
        return false
    }
    if len(node.messagesToSend) != 2 {
        ERROR("not 2 messages to send for requesting vote, there are %d", len(node.messagesToSend))
        return false
    }
    for _, message:= range node.messagesToSend {
        if message.Type != pb.RequestVoteRequest {
            ERROR("request vote message error: $v", message.Type)
            return false
        }
    }

    message1 := pb.Message{Src: 2, Dst: 1, Type: pb.RequestVoteResponse, Term: 1, Reject: false}
    //message2 := pb.Message{Src: 3, Dst: 1, Type: pb.RequestVoteResponse, Term: 0, Reject: false}
    node.processVoteResponse(message1)
    //node.processVoteResponse(message2)

    if node.Leader != node.ID {
        ERROR("did not win election")
        return false
    }
    return true
}

func TestNodeVote() bool {
    node := newTestNode(3)
    node.becomeFollower(0, 0)
    message := pb.Message{Src: 2, Dst: 1, Type: pb.RequestVoteRequest, Term: 1, Index:0, Commit: 0}

    node.processVoteRequest(message)
    if len(node.messagesToSend) == 1 {
        response := node.messagesToSend[0]
        if response.Type==pb.RequestVoteResponse && !response.Reject && node.voteTo==2 {
            return true
        } else {
            ERROR("response type=%v, reject=%v, vote to=%d", response.Type, response.Reject, node.voteTo)
            return false
        }
    } else {
        ERROR("no vote response found")
        return false
    }
}

func TestNodeVoted() bool {
    node := newTestNode(3)
    node.becomeFollower(1, 0)
    node.voteTo = 2
    message := pb.Message{Src: 3, Dst: 1, Type: pb.RequestVoteRequest, Term: 2, Index:0, Commit: 0}

    node.processVoteRequest(message)
    if len(node.messagesToSend) == 1 {
        response := node.messagesToSend[0]
        if response.Reject==true && response.RejectType==pb.RejectVoted {
            return true
        } else {
            ERROR("response reject=%v type=%v", response.Reject, response.RejectType)
            return false
        }
    } else {
        ERROR("no vote response found")
        return false
    }
}

func TestNodePropose() bool {
    node := newTestNode(3)
    node.becomeLeader()
    node.Term = 1

    message := pb.Message{Src:1, Dst:1, Type: pb.ProposeMessage, Term: 1, Entries: []pb.Entry{{Data: []byte("data")}}}
    err := node.processMessage(message)
    if err != nil {
        ERROR("process Propose message err: $v", err)
        return false
    }

    if node.logManager.lastIndex() != 1 {
        ERROR("append entry error last index: %d", node.logManager.lastIndex())
        return false
    }
    if len(node.messagesToSend) != 2 {
        ERROR("no message to boardcast len=%d", len(node.messagesToSend))
        return false
    }

    for _, appendMessage := range node.messagesToSend {
        if appendMessage.Type != pb.AppendEntriesRequest {
            ERROR("append entry request type error: %v", appendMessage.Type)
            return false
        }
        if len(appendMessage.Entries) != 1 {
            ERROR("append entry request entry len error: %d", len(appendMessage.Entries))
            return false
        }
    }
    return true
}

func TestAppendEntry() bool {
    node := newTestNode(3)
    node.becomeLeader()
    node.Term = 3
    node.logManager.cache.entries = []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
    node.nodeProgress[1].NextIndex = 3
    node.nodeProgress[1].MarchIndex = 2

    entries := []pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}
    node.logManager.appendEntriesToCache(entries)
    if len(node.logManager.cache.entries)!=3 {
        ERROR("append entries to cache error, len=%d", len(node.logManager.cache.entries))
        return false
    }
    if node.logManager.cache.entries[0].Index!=1 || node.logManager.cache.entries[1].Index!=2 || node.logManager.cache.entries[2].Index!=3 {
        ERROR("append entries to cache error, index=%s", getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }

    entries1 := []pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}}
    node.logManager.appendEntries(2, 2, 0, entries1...)
    if len(node.logManager.cache.entries)!=4 {
        ERROR("append entries error, len=%d, %s", len(node.logManager.cache.entries), getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }

    entries2 := []pb.Entry{{Index: 6, Term: 6}}
    node.logManager.appendEntries(5, 5, 0, entries2...)
    if len(node.logManager.cache.entries)!=4 {
        ERROR("append no-exist entries error, len=%d, %s", len(node.logManager.cache.entries), getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }

    node.logManager.cache.offset = 2
    node.logManager.cache.entries = node.logManager.cache.entries[1:]
    entries3 := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
    node.logManager.appendEntriesToCache(entries3)
    if len(node.logManager.cache.entries)!=2 {
        ERROR("append offset cache entries error, len=%d, %s", len(node.logManager.cache.entries), getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }

    entries4 := []pb.Entry{{Index: 2, Term: 3}}
    node.logManager.appendEntriesToCache(entries4)
    if node.logManager.cache.entries[1].Term!=3 {
        ERROR("append truncate cache entries error, len=%d, %s", len(node.logManager.cache.entries), getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }

    return true
}

func TestNodeProgress() bool {
    node := newTestNode(3)
    node.becomeLeader()
    node.Term = 2
    node.nodeProgress[1] = &Progress {NextIndex: 4, MarchIndex: 3, Live: true}
    node.nodeProgress[2] = &Progress {NextIndex: 2, MarchIndex: 1, Live: true}
    node.nodeProgress[3] = &Progress {NextIndex: 3, MarchIndex: 2, Live: true}
    node.logManager.cache.entries = []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
    node.logManager.committed = 1

    message1 := pb.Message{Src: 2, Dst: 1, Type: pb.AppendEntriesResponse, Term: 2, Reject: false, Index: 2}
    message2 := pb.Message{Src: 3, Dst: 1, Type: pb.AppendEntriesResponse, Term: 2, Reject: false, Index: 3}

    node.processMessage(message1)
    node.processMessage(message2)
    if node.nodeProgress[2].NextIndex!=3 || node.nodeProgress[3].NextIndex!=4 {
        ERROR("update node progress error: node 2 next=%d, node 3 next=%d",
            node.nodeProgress[2].NextIndex, node.nodeProgress[3].NextIndex)
        return false
    }
    if node.logManager.committed != 2 {
        ERROR("commit error: committed=%d", node.logManager.committed)
        return false
    }
    if len(node.messagesToSend) != 2 {
        ERROR("no boardcast append: %d", len(node.messagesToSend))
        return false
    }
    for _, message := range node.messagesToSend {
        if message.Commit != 2 {
            ERROR("commit in boardcast error: commit=%d", message.Commit)
            return false
        }
    }

    return true
}

func TestNodeAppendReceive() bool {
    node := newTestNode(3)
    node.becomeFollower(1, 2)
    node.nodeProgress[1] = &Progress {NextIndex: 2, MarchIndex: 1, Live: true}
    node.nodeProgress[2] = &Progress {NextIndex: 1, MarchIndex: 0, Live: true}
    node.nodeProgress[3] = &Progress {NextIndex: 1, MarchIndex: 0, Live: true}
    node.logManager.cache.entries = []pb.Entry {{Index: 1, Term: 1}}

    message := pb.Message{Src: 2, Dst: 1, Type: pb.AppendEntriesRequest, Index: 1, LogTerm: 1, Term: 1, Entries: []pb.Entry{{Index: 2, Term: 1}}, Commit: 1}
    node.processMessage(message)
    if len(node.logManager.cache.entries) != 2 {
        ERROR("receive append entry error: %s", getEntriesIndexStr(node.logManager.cache.entries))
        return false
    }
    if node.logManager.committed != 1 {
        ERROR("node receive and commit error: %d", node.logManager.committed)
        return false
    }

    return true
}

func TestAppendReceiveReject() bool {
    node := newTestNode(3)
    node.becomeFollower(1, 2)
    node.nodeProgress[1] = &Progress {NextIndex: 2, MarchIndex: 1, Live: true}
    node.nodeProgress[2] = &Progress {NextIndex: 1, MarchIndex: 0, Live: true}
    node.nodeProgress[3] = &Progress {NextIndex: 1, MarchIndex: 0, Live: true}
    node.logManager.cache.entries = []pb.Entry {{Index: 1, Term: 1}}
    node.Term = 2
    node.logManager.committed = 1

    message := pb.Message{Src: 2, Dst: 1, Type: pb.AppendEntriesRequest, Index: 2, Term: 2, Commit: 1, Entries: []pb.Entry{{Index: 3, Term: 3}}}
    node.processMessage(message)
    
    if len(node.messagesToSend) != 1 {
        ERROR("no response to send: %d", len(node.messagesToSend))
        return false
    }
    response := node.messagesToSend[0]
    if response.Type != pb.AppendEntriesResponse {
        ERROR("response type error: %v", response.Type)
        return false
    }
    if !response.Reject || response.RejectType!=pb.RejectAppend || response.RejectHint!=1 {
        ERROR("response reject error: reject=%v type=%v hint=%d", response.Reject, response.RejectType, response.RejectHint)
        return false
    }
    return true
}

func TestNodeAppendReject() bool {
    node := newTestNode(3)
    node.becomeLeader()
    node.Term = 1
    node.nodeProgress[1] = &Progress {NextIndex: 3, MarchIndex: 2, Live: true}
    node.nodeProgress[2] = &Progress {NextIndex: 2, MarchIndex: 1, Live: true}
    node.nodeProgress[3] = &Progress {NextIndex: 1, MarchIndex: 0, Live: true}
    node.logManager.cache.entries = []pb.Entry {{Index: 1, Term: 1}, {Index: 2, Term: 1}}

    message := pb.Message{Src: 2, Dst: 1, Type: pb.AppendEntriesResponse, Term: 1, Index: 1, Reject: true, RejectHint: 2}
    node.processMessage(message)

    if len(node.messagesToSend) != 1 {
        ERROR("no retry message: %d", len(node.messagesToSend))
        return false
    }
    response := node.messagesToSend[0]
    if response.Type != pb.AppendEntriesRequest {
        ERROR("no append entry request send: %v", response.Type)
        return false
    }
    if response.Index!=0 || response.LogTerm!=0 {
        ERROR("retry message error: index=%d logTerm=%d", response.Index, response.LogTerm)
        return false
    }

    return true
}
