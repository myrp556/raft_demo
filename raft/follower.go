package raft

import (
    "github.com/myrp556/raft_demo/raft/pb"
)

func (node *Node) becomeFollower(term uint64, leader uint64) {
    node.resetNode(term)
    node.Leader = 0
    node.Type = Follower
    node.typeTick = node.tickFollower
    node.typeProcessMessage = node.processFollowerMessage
    node.randomElectionTimeout()
    //node.typeTick()
    node.INFO("become follower")
}

func (node *Node) tickFollower() {
    node.tickElection()
}

func (node *Node) processFollowerMessage(message pb.Message) error {
    srcID := message.Src

    switch message.Type {
    case pb.AppendEntriesRequest:
        node.electionElapse = 0
        node.Leader = srcID
        node.processAppendEntries(message)

    case pb.HeartbeatRequest:
        node.heartbeatElapse = 0
        node.Leader = srcID
        node.processHeartbeat(message)

    case pb.ProposeMessage:
        if node.Leader == 0 {
            node.INFO("no leader found for propose message, DROP MESSAGE")
            return ErrDropProposeMessage
        }

        node.DEBUG("forward propose message to leader")
        message.Dst = node.Leader
        node.sendMessage(message)
    }

    return nil
}
