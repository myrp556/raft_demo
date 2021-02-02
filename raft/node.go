package raft

import (
    "fmt"
    "math/rand"
    "time"
    "errors"
    "github.com/myrp556/raft_demo/raft/pb"
)

// contents that server need receive and do storage
type Box struct {
    Messages []pb.Message
    // for storage
    CacheEntries []pb.Entry
    // for execution
    CommittedEntries []pb.Entry
}

var OpenLog bool = true
var OpenDebug bool = false

func (box *Box) getAppliedEndIndexTerm() (uint64, uint64) {
    if n:=len(box.CommittedEntries); n>0 {
        entry := box.CommittedEntries[n-1]
        return entry.Index, entry.Term
    }
    return 0, 0
}

func (box *Box) getStoragedEndIndexTerm() (uint64, uint64) {
    if n:=len(box.CacheEntries); n>0 {
        entry := box.CacheEntries[n-1]
        return entry.Index, entry.Term
    }
    return 0, 0
}

var ErrDropProposeMessage = errors.New("Drop Propose Message")

type MessageWithResult struct {
    message pb.Message
    result chan error
}

type Node struct {
    ID uint64
    Type NodeType
    Config RaftConfig

    Term uint64
    Leader uint64
    nodeLive uint64
    nodeProgress map[uint64] *Progress
    logManager *LogManager

    // tick
    electionTimeout uint32
    heartbeatTimeout uint32
    electionElapse uint32
    heartbeatElapse uint32
    electionRandomTimeout uint32
    electionRand *rand.Rand

    // campaign
    voteMap map[uint64] bool    // map nodes who votes to me in this term
    voteTo uint64               // indicate who me vote to in this term

    typeTick func()
    typeProcessMessage func(pb.Message) error

    // channels
    tickChannel chan struct{}
    pullChannel chan Box
    forwardChannel chan struct{}
    messageProcessChannel chan pb.Message
    proposeMessageChannel chan MessageWithResult
    stopChannel chan struct{}
    doneChannel chan struct{}

    messagesToSend []pb.Message
}

type Peer struct {
    ID uint64
    Name string
}

type NodeType uint32

const (
    Follower    NodeType = 0
    Candidate   NodeType = 1
    Leader      NodeType = 2
    Unknown     NodeType = 3
)

var NodeTypeName []string = []string {
    "Follower",
    "Candidate",
    "Leader",
    "Unknown",
}

func CreateNode(ID uint64, peers []Peer) (*Node, error) {
    Info.Println(fmt.Sprintf("Create node for ID: %d", ID))
    node := &Node {
        ID: ID,
        Type: Unknown,
        Term: 0,
        Leader: 0,
        nodeLive: 0,
        nodeProgress: make(map[uint64] *Progress),
        logManager: CreateLog(),

        electionTimeout: 10,
        heartbeatTimeout: 2,
        electionElapse: 0,
        heartbeatElapse: 0,
        electionRandomTimeout: 0,
        electionRand: rand.New(rand.NewSource(time.Now().UnixNano())),

        tickChannel: make(chan struct{}, 128),
        pullChannel: make(chan Box),
        forwardChannel: make(chan struct{}),
        messageProcessChannel: make(chan pb.Message),
        proposeMessageChannel: make(chan MessageWithResult),
        stopChannel: make(chan struct{}),
        doneChannel: make(chan struct{}),
    }
    node.initProgress(peers)

    return node, nil
}

func (node *Node) resetNode(term uint64) {
    // reset term
    if node.Term != term {
        node.Term = term
        node.voteMap = make(map[uint64] bool)
        node.voteTo = 0
    }
    node.Leader = 0
    node.electionElapse = 0
    node.heartbeatElapse = 0
}

func (node *Node) randomElectionTimeout() {
    node.electionRandomTimeout = node.electionTimeout + uint32(node.electionRand.Intn(int(node.electionTimeout)))
    //node.DEBUG("random election timeout=%d", node.electionRandomTimeout)
}

func (node *Node) StartNode() {
    node.becomeFollower(0, 0)

    node.INFO("start node")
    go node.run()
}

func (node *Node) Tick() {
    select {
    case node.tickChannel <- struct{} {}:
    case <-node.doneChannel:
    }
}

func (node *Node) tickElection() {
    node.electionElapse ++;
    node.DEBUG("node tick election %d/%d, hasBox %v, message %d", node.electionElapse, node.electionRandomTimeout, node.hasBox(), node.pendingMessageNum())

    if node.electionElapse >= node.electionRandomTimeout {
        node.electionElapse = 0
        node.DEBUG("Election timeout!")
        message := pb.Message{Type: pb.StartCampaign, Term: node.Term}
        //node.ReceiveMessage(message)
        node.processMessage(message)
    }
}

func (node *Node) ReceiveMessage(message pb.Message) {
    node.messageProcessChannel <- message
}

func (node *Node) ProposeMessage(data []byte) error {
    node.INFO("proposeMessage: %v", data)
    message := pb.Message{Type: pb.ProposeMessage, Term: node.Term, Entries: []pb.Entry{{Data: data}}}
    proposeMessage := MessageWithResult{message: message, result: make(chan error)}

    node.proposeMessageChannel <- proposeMessage
    return <-proposeMessage.result
}

func (node *Node) forwardProcess(box Box) {
    // push forward committed and storaged enties' index
    node.DEBUG("forward process")
    //appliedIndex, appliedTerm := box.getAppliedEndIndexTerm()
    appliedIndex, _ := box.getAppliedEndIndexTerm()
    storageIndex, storageTerm := box.getStoragedEndIndexTerm()
    if appliedIndex>0 {
        node.INFO("node applied to %d", appliedIndex)
        node.logManager.appliedTo(appliedIndex)
    }
    if storageIndex>0 && storageTerm>0{
        //node.logManager.stableTo(sotrageIndex, storageTerm)
    }
}

func (node *Node) processMessage(message pb.Message) error {
    srcID := message.Src
    srcTerm := message.Term
    messageType := message.Type

    // message form past term, reject
    if srcID!=node.ID && srcTerm<node.Term {
        node.INFO("receive message form node %d in LOW term %d(<%d), REJECT", srcID, srcTerm, node.Term)
        response := pb.Message{Src: node.ID, Dst: srcID, Type: pb.RequestVoteResponse, Reject: true, RejectType: pb.RejectPastTerm}

        node.sendMessage(response)
        return nil
    }

    if srcTerm > node.Term {
        node.INFO("receive message from node %d in HIGH term %d(>%d), become follower", srcID, srcTerm, node.Term)
        if message.Type==pb.HeartbeatRequest || message.Type==pb.AppendEntriesRequest {
            node.becomeFollower(message.Term, message.Src)
        } else {
            node.becomeFollower(message.Term, 0)
        }
    }
    node.DEBUG("process message from node %d, type %v", message.Src, messageType)

    switch messageType {
        case pb.StartCampaign:
            node.campaign()

        case pb.RequestVoteRequest:
            node.processVoteRequest(message)

        default:
    }

    return node.typeProcessMessage(message)
}

func (node *Node) run() {
    var box Box
    var proposeInterface chan MessageWithResult
    var pullInterface chan Box
    var forwardInterface chan struct{}
    var leader uint64

    leader = 0

    node.DEBUG("run")
    for {
        //node.Status()
        // if next require forward opreation, cancel pull listening
        if forwardInterface != nil {
            pullInterface = nil
            //node.DEBUG("open forwar channel!")
        // else check if need pull
        } else if node.hasBox() {
            box = node.makeupBox()
            pullInterface = node.pullChannel
            //node.DEBUG("setup box for pull!")
        }

        if leader != node.Leader {
            if node.Leader > 0 {
                node.INFO("change leader from node %d to node %d at term %d, open propose channel", leader, node.Leader, node.Term)
                proposeInterface = node.proposeMessageChannel
            } else {
                node.INFO("node %d lost leader term %d, shut propose channel", leader, node.Term)
                proposeInterface = nil
            }
            leader = node.Leader
        }

        select {
        case proposedMessage := <-proposeInterface:
            message := proposedMessage.message
            message.Src = node.ID
            message.Dst = node.ID
            err := node.processMessage(message)
            if proposedMessage.result != nil {
                proposedMessage.result <- err
                close(proposedMessage.result)
            }

        case message := <-node.messageProcessChannel:
            //node.DEBUG("run: receive message from channel, node %d, type %v", message.Src, message.Type)
            node.processMessage(message)

        case pullInterface <- box:
            //node.DEBUG("run: pull box out")
            // listening for forward
            node.messagesToSend = nil
            forwardInterface = node.forwardChannel

        case <-node.tickChannel:
            //node.DEBUG("run: tick")
            node.typeTick()

        case <-forwardInterface:
            //node.DEBUG("run: forward next")
            node.forwardProcess(box)
            box = Box{}
            forwardInterface = nil

        case <- node.stopChannel:
            break
        }
    }
    node.INFO("exit run")
}

func (node *Node) processAppendEntries(message pb.Message) {
    srcID := message.Src
    messageType := message.Type
    if messageType != pb.AppendEntriesRequest {
        node.DEBUG("not a AppendEntries type message")
        return
    }

    node.DEBUG("receive entries [%s] index %d, term %d, logTerm %d, commit %d, from node %d", getEntriesIndexStr(message.Entries), message.Index, message.Term, message.LogTerm, message.Commit, srcID)

    response := pb.Message{Src: node.ID, Dst: srcID, Type: pb.AppendEntriesResponse, Index: 0}
    if message.Index < node.logManager.committed {
        response.Index = node.logManager.committed
    } else {
        lastestIndex, ok := node.logManager.appendEntries(message.Index, message.LogTerm, message.Commit, message.Entries...)
        if ok {
            node.DEBUG("append success entries to %d", lastestIndex)
            response.Index = lastestIndex
        } else {
            node.ERROR("process appendEntries from node %d failed, REJECT", message.Src)
            response.Reject = true
            response.RejectType = pb.RejectAppend
            response.RejectHint = node.logManager.lastIndex()
            response.Index = message.Index
        }
    }

    node.sendMessage(response)
}

func (node *Node) processHeartbeat(message pb.Message) {
    srcID := message.Src
    node.logManager.commitTo(message.Commit)

    node.DEBUG("receive HeartbeatRequest form node %d, committed=%d", srcID, message.Commit)
    response := pb.Message{Src: node.ID, Dst: srcID, Term: node.Term, Type: pb.HeartbeatResponse}
    node.sendMessage(response)
}

func (node *Node) hasBox() bool {
    // has messages to be send
    if node.messagesToSend != nil && len(node.messagesToSend) > 0 {
        node.DEBUG("has %d messages to send", len(node.messagesToSend))
        return true
    }

    // TODO: change if here
    //if node.logManager.hasApply() || node.logManager.hasCache() {
    if node.logManager.hasApply() {
        node.DEBUG("has entries to apply, applied=%d commit=%d", node.logManager.applied, node.logManager.committed)
        return true
    }

    return false
}

func (node *Node) makeupBox() Box {
    box := Box {
        Messages: node.messagesToSend,
        CacheEntries: node.logManager.entriesInCache(),
        CommittedEntries: node.logManager.entriesForApply(),
    }
    return box
}

func (node *Node) GetPullChannel() <-chan Box {
    return node.pullChannel
}

func (node *Node) Forward() {
    select {
    case node.forwardChannel <- struct{} {}:
        //node.DEBUG("push Forward here!")
    case <-node.doneChannel:
    }
}

func (node *Node) pendingMessageNum() int {
    return len(node.messagesToSend)
}

func (node *Node) sendMessage(message pb.Message) {
    if node.messagesToSend == nil {
        node.messagesToSend = []pb.Message {}
    }

    if message.Src == 0 {
        message.Src = node.ID
    }
    if message.Term == 0 {
        message.Term = node.Term
    }

    node.messagesToSend = append(node.messagesToSend, message)
    //node.DEBUG("has %d messages to send", len(node.messagesToSend))
}

func (node *Node) Status() {
    node.INFO("ID=%v term=%v type=%v leader=%v randomElecTimout=%v", node.ID, node.Term, NodeTypeName[node.Type], node.Leader, node.electionRandomTimeout)
}

func (node *Node) INFO(pattern string, args ...interface{}) {
    if !OpenLog {
        return
    }
    Info.Println(fmt.Sprintf("[node %d, term %d] ", node.ID, node.Term) + fmt.Sprintf(pattern, args...))
}

func (node *Node) WARN(pattern string, args ...interface{}) {
    Warn.Println(fmt.Sprintf("[node %d, term %d] ", node.ID, node.Term) + fmt.Sprintf(pattern, args...))
}

func (node *Node) ERROR(pattern string, args ...interface{}) {
    Error.Println(fmt.Sprintf("[node %d, term %d] ", node.ID, node.Term) + fmt.Sprintf(pattern, args...))
}

func (node *Node) DEBUG(pattern string, args ...interface{}) {
    if !OpenLog || !OpenDebug {
        return
    }
    Debug.Println(fmt.Sprintf("[node %d, term %d] ", node.ID, node.Term) + fmt.Sprintf(pattern, args...))
}
