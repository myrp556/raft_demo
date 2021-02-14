package main

import (
    "log"
    "github.com/myrp556/raft_demo/raft"
    "github.com/myrp556/raft_demo/raft/pb"
    //"github.com/influxdata/telegraf/agent"

    "time"
    "fmt"
    "strings"
    //"strconv"
)

var (
    chans []chan pb.Message = []chan pb.Message {
        make(chan pb.Message),
        make(chan pb.Message),
        make(chan pb.Message),
    }
    done chan bool = make (chan bool)
)

const (
    tickInterval      = 100 * time.Millisecond
    jitterMillisecond = 15 * time.Millisecond
)

type Data struct {
    src uint64
    content string
}

type Node struct {
    ID uint64
    node *raft.Node
    storage *raft.MemoryStorage
    db *raft.RocksDb
    alive bool
}

var dataMap map[int]Data = map[int]Data {
    1: {src: 2, content: "c"},
    22: {src: 1, content: "a=4",},
    25: {src: 2, content: "b=5",},
    27: {src: 3, content: "a=1",},
    29: {src: 3, content: "c=3",},
    30: {src: 2, content: "b=2",},
    32: {src: 2, content: "c=9",},
    35: {src: 2, content: "x",},
    37: {src: 1, content: "a=2"},
    40: {src: 3, content: "c=1"},
    42: {src: 3, content: "b=3"},
    100: {src: 2, content: "v"},
    101: {src: 2, content: "t"},
    110: {src: 3, content: "a=10"},
    120: {src: 1, content: "c=22"},
}

func callDb(ID uint64, db *raft.RocksDb, cmd string) {
    s := strings.Split(cmd, "=")
    if len(s)==2 {
        key := s[0]
        valueStr := s[1]
        db.Put([]byte(key), []byte(valueStr))
        log.Println(fmt.Sprintf("node %d call db: set %s = %v", ID, key, valueStr))
    }
}

func checkDb(ID uint64, db *raft.RocksDb) {
    log.Println(fmt.Sprintf("node %d check, a=%v b=%v c=%v", ID, db.GetStr("a"), db.GetStr("b"), db.GetStr("c")))
}

func reportLog(ID uint64, node *raft.Node) {
    log.Println(fmt.Sprintf("node %d log, %s", ID, node.ReportLog()))
}

func touchData(node *Node, count int) {
    if data, ok:=dataMap[count]; ok {
        if data.src == node.ID {
            switch data.content {
            case "x":
                node.alive = false
                log.Println(fmt.Sprintf("node %d down", node.ID))
            case "v":
                node.alive = true
                log.Println(fmt.Sprintf("node %d up", node.ID))
            case "c":
                node.node.OpenElection = false
                log.Println(fmt.Sprintf("node %d close election", node.ID))
            case "t":
                node.node.OpenElection = true
                log.Println(fmt.Sprintf("node %d open election", node.ID))
            default:
                node.node.ProposeMessage([]byte(data.content))
            }
        }
    }
}

//func runNode(ID uint64, node *raft.Node, db *raft.RocksDb) {
func runNode(node *Node) {
    node.node.StartNode()

    ticker := time.NewTicker(tickInterval)
    tickCount := 0
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            node.node.Tick()
            tickCount++
            //touchData(node.ID, tickCount, node.node)
            touchData(node, tickCount)

        case box := <-node.node.GetPullChannel():
            //log.Println(fmt.Sprintf("node %d get pull data", ID))
            // storange
            node.storage.Append(box.CacheEntries)
            // execute
            for _, entry := range box.CommittedEntries {
                execCmd := string(entry.Data)
                log.Println(fmt.Sprintf("node %d exec index=%d [%s]", node.ID, entry.Index, execCmd))
                if node.db != nil {
                    callDb(node.ID, node.db, execCmd)
                }
            }
            if node.alive {
                go sendMessages(node.ID, box.Messages)
            }

            node.node.Forward()

        case message := <-chans[node.ID-1]:
            if message.Dst != node.ID {
                log.Println(fmt.Sprintf("node %d can not receive message to node %d", node.ID, message.Dst))
            } else {
                //log.Println(fmt.Sprintf("node %d receive message from node %d, type %v", ID, message.Src, message.Type))
                if node.alive {
                    node.node.ReceiveMessage(message)
                }
            }

        case <-done:
            return
        }
    }
}

func sendMessages(ID uint64, messages []pb.Message) {
    for _, message := range messages {
        src := message.Src
        if src != ID {
            log.Println(fmt.Sprintf("node %s can not send message for node %d", ID, src))
        } else {
            dst := message.Dst
            chans[dst-1] <- message
            //log.Println(fmt.Sprintf("node %d send message to node %d, type %v", src, dst, message.Type))
        }
    }
}

func start(ID uint64, peers []raft.Peer) (*raft.Node, *raft.RocksDb) {
    storage := raft.NewMemoryStorage()
    node := &Node {
        ID: ID,
        storage: storage,
        node: raft.CreateNode(ID, storage, peers),
        db: &raft.RocksDb{Name: fmt.Sprintf("node%d", ID)},
        alive: true,
    }

    if node.db.Init() {
        fmt.Println("db set up")
    } else {
        fmt.Println("db failed")
        node.db = nil
    }
    node.db.Delete("a")
    node.db.Delete("b")
    node.db.Delete("c")

    go runNode(node)
    return node.node, node.db
}

func main() {
    log.Println("yes")
    //config, _ := raft.GetDefaultConfig()
    peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

    node1, db1 := start(1, peers)
    node2, db2 := start(2, peers)
    node3, db3 := start(3, peers)

    time.Sleep(20 * time.Second)
    done <- true
    reportLog(1, node1)
    checkDb(1, db1)
    reportLog(2, node2)
    checkDb(2, db2)
    reportLog(3, node3)
    checkDb(3, db3)
}
