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

var dataMap map[int]Data = map[int]Data {
    22: {src: 1, content: "a=4",},
    25: {src: 2, content: "b=5",},
    27: {src: 3, content: "a=1",},
    29: {src: 3, content: "c=3",},
    30: {src: 2, content: "b=2",},
    32: {src: 2, content: "c=9",},
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

func touchData(ID uint64, count int, node *raft.Node) {
    if data, ok:=dataMap[count]; ok {
        if data.src == ID {
            node.ProposeMessage([]byte(data.content))
        }
    }
}

func runNode(ID uint64, node *raft.Node, db *raft.RocksDb) {
    node.StartNode()

    ticker := time.NewTicker(tickInterval)
    tickCount := 0
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            node.Tick()
            tickCount++
            touchData(ID, tickCount, node)

        case box := <-node.GetPullChannel():
            //log.Println(fmt.Sprintf("node %d get pull data", ID))
            go sendMessages(ID, box.Messages)
            // storange
            for _, entry := range box.CommittedEntries {
                execCmd := string(entry.Data)
                log.Println(fmt.Sprintf("node %d exec index=%d [%s]", ID, entry.Index, execCmd))
                if db != nil {
                    callDb(ID, db, execCmd)
                }
            }

            node.Forward()

        case message := <-chans[ID-1]:
            if message.Dst != ID {
                log.Println(fmt.Sprintf("node %d can not receive message to node %d", ID, message.Dst))
            } else {
                //log.Println(fmt.Sprintf("node %d receive message from node %d, type %v", ID, message.Src, message.Type))
                node.ReceiveMessage(message)
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
    node, _ := raft.CreateNode(ID, peers)
    db := &raft.RocksDb{Name: fmt.Sprintf("node%d", ID)}
    if db.Init() {
        fmt.Println("db set up")
    } else {
        fmt.Println("db failed")
        db = nil
    }
    db.Delete("a")
    db.Delete("b")
    db.Delete("c")

    go runNode(ID, node, db)
    return node, db
}

func main() {
    log.Println("yes")
    //config, _ := raft.GetDefaultConfig()
    peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

    _, db1 := start(1, peers)
    _, db2 := start(2, peers)
    _, db3 := start(3, peers)

    time.Sleep(8 * time.Second)
    done <- true
    checkDb(1, db1)
    checkDb(2, db2)
    checkDb(3, db3)
}
