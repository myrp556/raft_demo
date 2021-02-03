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
    node, _ := CreateNode(1, peers)
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
    node.campaign()

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
