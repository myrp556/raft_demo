package raft

import (
    "log"
    "os"
)

var (
    Debug *log.Logger
    Info  *log.Logger
    Error *log.Logger
    Warn  *log.Logger
)

func init() {
    log.Println("init ...")
    Debug = log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime)
    Info = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime)
    Error = log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime)
    Warn = log.New(os.Stderr, "[Warn] ", log.Ldate|log.Ltime)
}


