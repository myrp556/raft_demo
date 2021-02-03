package raft

import (
    "fmt"
    "github.com/myrp556/raft_demo/raft/pb"
)

type CampignBoard struct {
}

func (node *Node) campaign() {
    if node.Type == Leader {
        node.INFO("already been a leader, no campaign start")
        return
    }
    // be condidate
    node.becomeCandidate()
    // vote self
    node.voteTo = node.ID
    if node.receiveVote(node.ID, true) {
        node.winElection()
        return
    }

    // request vote for others
    for ID := range node.nodeProgress {
        if ID == node.ID {
            continue
        }
        message := pb.Message {
            Src: node.ID,
            Dst: ID,
            Type: pb.RequestVoteRequest,
            Term: node.Term,
            Index: node.logManager.lastIndex(),
            LogTerm: node.logManager.lastTerm(),
        }
        node.sendMessage(message)
    }
}

func (node *Node) processVoteRequest(message pb.Message) {
    srcID := message.Src

    if message.Term < node.Term {
        response := pb.Message{Src: node.ID, Dst: srcID, Type: pb.RequestVoteResponse, Term: message.Term, Reject: true, RejectType: pb.RejectPastTerm}
        node.sendMessage(response)
        return
    }

    if message.Term == node.Term && node.Leader!=0 && node.Leader!=srcID {
        response := pb.Message{Src: node.ID, Dst: srcID, Type: pb.RequestVoteResponse, Term: message.Term, Reject: true, RejectType: pb.RejectHasLeader}
        node.sendMessage(response)
        return
    }

    response := pb.Message{Src: node.ID, Dst: srcID, Type: pb.RequestVoteResponse, Term: message.Term, Reject: false}
    if (node.voteTo==0 && node.Leader==0) || (node.voteTo==message.Src) {
        if ok, t:=node.logManager.isUpToDate(message.Index, message.LogTerm); ok {
            node.INFO("vote to node %d", srcID)
            node.electionElapse = 0
            node.voteTo = srcID
        } else {
            response.Reject = true
            if t == 1 {
                response.RejectType = pb.RejectPastLogTerm
            } else {
                response.RejectType = pb.RejectPastIndex
            }
            node.INFO("can not vote to node %d for OUT OF DATE(%v) index=%d term=%d",  srcID, message.RejectType, message.Index, message.Term)
        }
    } else {
        response.Reject = true
        response.RejectType = pb.RejectHasLeader
        if (node.Leader!=0) {
            node.INFO("can not vote to node %d for has leader node %d", srcID, node.Leader)
        } else {
            response.RejectType = pb.RejectVoted
            node.INFO("can not vote to node %d for voted to node %d", srcID, node.voteTo)
        }
    }
    node.sendMessage(response)
}

func (node *Node) processVoteResponse(message pb.Message) {
    srcID := message.Src
    term := message.Term

    if term != node.Term {
        node.DEBUG("receive vote from node %d in different term %d, ignore", srcID, term)
        return
    }

    if node.receiveVote(srcID, !message.Reject) {
        node.winElection()
        return
    }
}

func (node *Node) receiveVote(voterID uint64, voteChecked bool) bool {
    node.voteMap[voterID] = voteChecked

    var voteCount uint64
    voteCount = 0
    for ID := range node.voteMap {
        if node.voteMap[ID] {
            voteCount ++
        }
    }
    node.INFO(fmt.Sprintf("receive vote form %d, voteChecked=%t, votes=%d/%d", voterID, voteChecked, voteCount, node.nodeLive))

    // receive votes form most of the nodes, elect success
    if voteCount >= node.nodeLive/2+1 {
        return true
    }
    return false
}

func (node *Node) winElection() {
    node.INFO("win election!")
    node.becomeLeader()
    // boardcast nodes 
    node.boardcastAppendEntries()
}
