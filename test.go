package main

import (
    "github.com/myrp556/raft_demo/raft"
    "log"
    "fmt"
)

func test(title string, testFunc func() bool) {
    if testFunc != nil {
        if testFunc() {
            log.Println(fmt.Sprintf("%s PASSED!", title))
        } else {
            log.Println(fmt.Sprintf("%s FAILED!", title))
        }
    }
}

func main() {
    raft.OpenLog = false
    raft.OpenDebug = false

    //test("node tick", raft.TestNodeTick)
    //test("node campaign", raft.TestNodeCampaign)
    //test("node vote", raft.TestNodeVote)
    //test("node voted", raft.TestNodeVoted)
    //test("node propose", raft.TestNodePropose)
    //test("node append entry", raft.TestAppendEntry)
    //test("node append entry reject", raft.TestAppendReceiveReject)
    //test("node progress", raft.TestNodeProgress)
    //test("node append receive", raft.TestNodeAppendReceive)
    test("node append reject", raft.TestNodeAppendReject)
}
