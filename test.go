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
    //test("node tick", raft.TestNodeTick)
    //test("node campaign", raft.TestNodeCampaign)
    //test("node vote", raft.TestNodeVote)
    //test("node voted", raft.TestNodeVoted)
    //test("node propose", raft.TestNodePropose)
    //test("node append entry", raft.TestAppendEntry)
    test("node progress", raft.TestNodeProgress)
}
