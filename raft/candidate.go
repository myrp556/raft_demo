package raft

import (
    "github.com/myrp556/raft_demo/raft/pb"
)

func (node *Node) becomeCandidate() {
    if node.Type == Leader {
        node.ERROR("invalied change from leader to candidate")
        return
    }
    node.resetNode(node.Term+1)
    node.Leader = 0
    node.Type = Candidate
    node.typeTick = node.tickCandidate
    node.typeProcessMessage = node.processCandidateMessage
    node.randomElectionTimeout()
    node.voteMap = make(map[uint64] bool)
    node.INFO("become candidate")
}

func (node *Node) tickCandidate() {
    node.tickElection()
}

func (node *Node) processCandidateMessage(message pb.Message) error {
    switch message.Type {
    case pb.RequestVoteResponse:
        if node.receiveVote(message.Src, !message.Reject) {
            node.winElection()
        }

    case pb.HeartbeatRequest:
        node.becomeFollower(message.Term, message.Src)
        node.processHeartbeat(message)

    case pb.AppendEntriesRequest:
        node.becomeFollower(message.Term, message.Src)
        node.processAppendEntries(message)

    case pb.ProposeMessage:
        node.INFO("candidate node has no leader for propose message, DROP PROPOSE MESSAGE")
        return ErrDropProposeMessage

    }

    return nil
}
