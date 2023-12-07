package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import "6.824/src/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	// 创建Master，reduce数量设为10
	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false { // 完成后Done()返回true
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
