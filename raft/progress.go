package raft

import (
    "sort"
)

type Progress struct {
    // mostly have Next = March+1
    // next for next entry's index begin with
    NextIndex uint64
    // march for leader has known max index this node has
    MarchIndex uint64
    Live bool
}

func (node *Node) initProgress(peers []Peer) {
    for _, peer := range peers {
        progress := Progress {NextIndex: 1, MarchIndex: 0, Live: true}
        node.nodeLive ++
        node.nodeProgress[peer.ID] = &progress
    }
}

func (node *Node) updateProgress(ID uint64, index uint64) bool {
    progress := node.nodeProgress[ID]
    progress.Live = true
    progress.MarchIndex = max(progress.MarchIndex, index)
    progress.NextIndex = max(progress.NextIndex, index+1)
    return true
}

func (node *Node) decreseProgress(ID uint64, rejectIndex uint64, hintIndex uint64) bool {
    progress := node.nodeProgress[ID]

    if !progress.Live {
        return false
    }

    if progress.NextIndex-1 > 0 {
        progress.NextIndex --
        if progress.NextIndex <= progress.MarchIndex {
            progress.MarchIndex = progress.NextIndex-1
        }
        return true
    }

    return false
}

// return the most lowe march index which
// all nodes have
func (node *Node) getLowMarch() uint64 {
    lis := []int {}
    for _, progress := range node.nodeProgress {
        if progress.Live {
            lis = append(lis, int(progress.MarchIndex))
        }
    }
    sort.Ints(lis)

    return uint64(lis[int(len(lis)/2)])
}

