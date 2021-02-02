package raft

import (
    //"errors"
)

type RaftConfig struct {
    HeartbeatTimeout int
    ElectionTimeout int
    Nodes []Node
}

func GetDefaultConfig() (RaftConfig, error) {
    return RaftConfig {
        HeartbeatTimeout:   5,
        ElectionTimeout:    10,
    }, nil
}
