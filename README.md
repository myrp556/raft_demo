## a simple raft demo

this is a demo for RAFT, which uses golang to implement a simple RAFT protocal.

**this implementation is refering to [ETCD/raft](https://github.com/etcd-io/etcd/tree/master/raft)**

run main.go for test

and we add RocksDB for supporting KV operations, please run and check codes in `main.go`
```
go run main.go
```

### some explain to code

`./main.go` main function to execute our code and run for dmeo

`./raft/node.go` mainly implenments RAFT protocal, all functional calls are started from here

`./raft/leader.go, candidate.go, follower.go` these files are code for leader, candidate, follower nodes, which implement differnt operations for nodes

`./raft/campaign.go` this file implements all the operation in campaign stage

`./raft/log.go` this file implements log manage in RAFT, like append entry to log

`./raft/progress.go` this file implements node progress operations, node should trace and update their progress in replications

`./raft/logger.go` a simple log output

`./raft/db.go` simple **RocksDB** KV operation support

`./test` this is a test program, you can run this file for testing

`./raft/test.go` including some unit test for our implementation


to make proto use `protoc -I=${GOPATH}/src -I=$GOPATH/src/github.com/gogo/protobuf/gogoproto -I=raft/pb/. --gogo_out=raft/pb/. raft/pb/*.proto`
