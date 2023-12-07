package raft

import (
	"net/http"
	_ "net/http/pprof"
)

const pprofAddr string = ":7890"

func StartHttpDebugger() {
	go http.ListenAndServe(pprofAddr, nil)
}
