package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/src/labgob"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/src/labrpc"

// raft server的当前状态
type ServerState int

// 枚举server状态类型
const (
	Follower  ServerState = iota // 跟随者
	Candidate                    // 候选者
	Leader                       // 领导者
)

// 日志条目结构体
type LogEntry struct {
	Command interface{} // 客户端要求的指令
	Term    int         // 此日志条目的term
	Index   int         // 此日志条目的index
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool // 当ApplyMsg用于apply指令时为true，其余时候为false
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 指令执行时的term，便于kvserver的handler比较

	SnapshotValid     bool   // 当ApplyMsg用于传快照时为true，其余时候为false
	SnapshotIndex     int    // 本快照包含的最后一个日志的index
	StateMachineState []byte // 状态机状态，就是快照数据
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // Raft server的当前任期
	votedFor    int        // 此server当前任期投票给的节点的ID。如果还没有投票，就为-1
	leaderId    int        // 该raft server知道的最新的leader id，初始为-1
	log         []LogEntry // 此Server的日志，包含了若干日志条目，类型是日志条目的切片，第一个日志索引是1

	// 所有servers上易变的状态
	commitIndex int // 已知的已提交的日志的最大index
	lastApplied int // 应用到状态机的日志的最大index

	// leader上易变的状态（在选举后被重新初始化）
	nextIndex  []int // 对于每一个server来说，下一次要发给对应server的日志项的起始index（初始化为leader的最后一个日志条目index+1）
	matchIndex []int // 对于每一个server来说，已知成功复制到该server的最高日志项的index（初始化为0,且单调递增）

	state  ServerState   // 这个raft server当前所处的角色/状态
	timer  *time.Timer   // 计时器指针
	ready  bool          // 标志candidate是否准备好再次参加选举，当candidate竞选失败并等待完竞选等待超时时间后变为true
	hbTime time.Duration // 心跳间隔（要求每秒心跳不超过十次）

	applyCh chan ApplyMsg //  根据Make()及其他部分的注释，raft server需要维护一个发送ApplyMsg的管道

	lastIncludedIndex int // 上次快照替换的最后一个条目的index
	lastIncludedTerm  int // 上次快照替换的最后一个条目的term

	passiveSnapshotting bool // 该raft server正在进行被动快照的标志（若为true则这期间不进行主动快照）
	activeSnapshotting  bool // 该raft server正在进行主动快照的标志（若为true则这期间不进行被动快照）
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// 由kvserver调用，获取rf.passiveSnapshotting标志，若其为false，则设activeSnapshotting为true
func (rf *Raft) GetPassiveFlagAndSetActiveFlag() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.passiveSnapshotting {
		rf.activeSnapshotting = true // 若没有进行被动快照则将主动快照进行标志设为true，以便后续的主动快照检查
	}
	return rf.passiveSnapshotting
}

// 由kvserver调用修改rf.passiveSnapshotting
func (rf *Raft) SetPassiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.passiveSnapshotting = flag
}

// 由kvserver调用修改rf.activeSnapshotting
func (rf *Raft) SetActiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.activeSnapshotting = flag
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 保存Raft的持久化状态（编码）
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据Figure2来确定应该持久化的变量，对它们编码
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	// raft快照中恢复时需要用到这两个，因此也持久化了
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	// 调用SaveRaftState()将编码后的字节数组传递给Persister
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
// 恢复之前的持久化状态（解码）
// 传入的是一个byte数组，而ReadRaftState()返回的就是[]byte
// 因此恢复持久化状态应该：
// 1. data := rf.persister.ReadRaftState()
// 2. rf.readPersist(data)
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

	}
}

func (rf *Raft) recoverFromSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // 如果没有快照则直接返回
		return
	}

	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// raft恢复后向kvserver发送快照
	snapshotMsg := ApplyMsg{
		SnapshotValid:     true,
		CommandValid:      false,
		SnapshotIndex:     rf.lastIncludedIndex,
		StateMachineState: snapshot, // sm_state
	}

	go func(msg ApplyMsg) {
		rf.applyCh <- msg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	}(snapshotMsg)

	DPrintf("Server %d recover from crash and send SnapshotMsg to ApplyCh.\n", rf.me)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的当前任期
	CandidatedId int // 请求投票的candidate的ID
	LastLogIndex int // candidate最后一个日志条目的index（确保安全性的选举限制用）
	LastLogTerm  int // candidate最后一个日志条目的term（确保安全性的选举限制用）
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm，用来更新candidate的term（如果需要的话）
	VoteGranted bool // 当candidate收到这张选票时为true
}

// 追加日志条目的RPC请求结构
// 心跳包是没有log entries的AppendEntries RPCs
type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int
	PreLogIndex  int        // 新条目之前的紧接着的日志的索引
	PreLogTerm   int        // 新条目之前的紧接着的日志的任期
	Entries      []LogEntry // 要存储/追加到server的日志条目，为了效率可一次追加多条（若为心跳包则此字段为空）
	LeaderCommit int        // leader提交到的日志索引位置
}

// 追加日志条目的RPC回复结构
type AppendEntriesReply struct {
	Term          int  // RPC接收server的current term，leader更新自己用（如果需要的话）
	Success       bool // 如果follower包含有匹配leader的preLogIndex以及preLogTerm的日志条目则返回true
	ConflictIndex int
	ConflictTerm  int
}

// leader向follower发送快照的RPC请求结构
type InstallSnapshotArgs struct {
	Term              int    // leader的任期
	LeaderId          int    // 便于follower将client重定向到leader
	LastIncludedIndex int    // 快照替换的最后一个条目的index
	LastIncludedTerm  int    // 快照替换的最后一个条目的term
	SnapshotData      []byte // 快照的数据
}

// leader向follower发送快照的RPC回复结构
type InstallSnapshotReply struct {
	Term   int  // RPC接收server的current term，leader更新自己用（如果需要的话）
	Accept bool // follower是否接受这个快照
}

// server转换状态成candidate
// 两种情况下会用到：
// 1. Follower未接到Leader的心跳或Candidate的投票请求直至超时，会转为candidate参加竞选，follower -> candidate
// 2. Candidate由于分票等原因本轮未选出leader，经过一个超时时间后参加下一轮竞选，candidate -> candidate
func (rf *Raft) Convert2Candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate // 宣布自己成为candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderId = -1 // 参加竞选则leaderId更新为C待定（可之后更新为自己或其他server id）
	rf.persist()
	DPrintf("Candidate %d run for election! Its current term is %d\n", rf.me, rf.currentTerm)
}

// server转换状态成leader
// candidate收到大多数servers的投票后成功当选转为leader，candidate -> leader
func (rf *Raft) Convert2Leader() {
	rf.mu.Lock()
	DPrintf("Candidate %d was successfully elected as the leader! Its current term is %d\n", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.leaderId = rf.me // 成功当选，则leaderId是自己

	// leader上任时初始化nextIndex以及matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // 初始化为leader的最后一个日志条目index+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0 // 初始化为0
	}
	rf.mu.Unlock()

	// 为了leader上任时获知之前commit到了哪里，让leader在其任期开始时先在日志中提交一个空白的无操作条目
	// 解决当前term无提交日志时不能服务读请求的问题
	// 但是这样的话会有部分lab 2的测试通过不了，原因是lab2的部分测试不会考虑空白日志占据一个日志位
	// 实际应用时可能需要应用层添加一个空白指令，此处为通过实验测试就不加了
	// rf.Start(0) // 代表空白指令

	// 起一个协程循环发送心跳包，心跳间隔为100ms
	go func() {
		for !rf.killed() { // leader没有被kill就一直发送
			rf.mu.Lock()
			stillLeader := (rf.state == Leader)
			rf.mu.Unlock()

			if stillLeader { // 如果leader仍然是leader
				go rf.LeaderAppendEntries()
				time.Sleep(rf.hbTime) // 心跳间隔
			} else { // 如果当前server不再是leader（变为了follower）则重启计时器并停止发送心跳包
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
				go rf.HandleTimeout()
				return
			}
		}
	}()
}

// 处理超时的协程，会一直循环检测超时
func (rf *Raft) HandleTimeout() {
	for {
		select {
		case <-rf.timer.C: // 当timer超时后会向C中发送当前时间，此时case的逻辑就会执行，从而实现超时处理
			if rf.killed() { // 如果rf被kill了就不继续检测了
				return
			}
			rf.mu.Lock()
			nowState := rf.state // 记录下状态，以免switch访问rf.state时发送DATA RACE
			rf.mu.Unlock()

			switch nowState { // 根据当前的角色来判断属于哪种超时情况，执行对应的逻辑
			case Follower: // 如果是follower，则超时是因为一段时间没接收到leader的心跳或candidate的投票请求
				// 竞选前重置计时器（选举超时时间）
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

				go rf.RunForElection() // follower宣布参加竞选
			case Candidate: // 如果是candidate，则超时是因为出现平票等造成上一任期竞选失败
				// 重置计时器
				// 对于刚竞选失败的candidate，这个计时是竞选失败等待超时设定
				// 对于已经等待完竞选失败等待超时设定的candidate，这个计时是选举超时设定
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

				rf.mu.Lock()
				if rf.ready { // candidate等待完竞选等待超时时间准备好再次参加竞选
					rf.mu.Unlock()
					go rf.RunForElection()
				} else {
					rf.ready = true // candidate等这次超时后就可以再次参选
					rf.mu.Unlock()
				}
			case Leader: // 成为leader就不需要超时计时了，直至故障或发现自己的term过时
				return
			}

		}
	}
}

// 当follower一段时间没联系到leader时宣布自己成为candidate参加竞选，流程如下：
// 1. 自增current term
// 2. 给自己投一票
// 3. 重置选举计时器
// 4. 向所有其他servers发送请求投票RPC
func (rf *Raft) RunForElection() {
	rf.Convert2Candidate() // 转换成candidate宣布开始竞选

	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term，防止过程中rf.currentTerm改变导致args.Term不一致
	rf.ready = false           // 不管是follower第一次参选还是candidate再次参选，只要参加竞选就将ready设为false以便超时后的等待判定
	rf.mu.Unlock()

	// 使用条件变量来检查得到大多数票的条件
	votes := 1                    // 得票数（自己一开始给自己投的票要先算上，否则最后少了一张赞成票）
	finished := 1                 // 收到的请求投票回复数（自己的票也算）
	var voteMu sync.Mutex         // 用于保护votes和finished的锁
	cond := sync.NewCond(&voteMu) // 将条件变量与锁关联

	// candidate向除自己以外的其他server发送请求投票RPC
	for i, _ := range rf.peers { // i是目的server在rf.peers[]中的索引（id）

		if rf.killed() { // 如果在竞选过程中Candidate被kill了就直接结束
			return
		}

		rf.mu.Lock()
		if rf.state != Candidate { // 如果自己不再是Candidate则不继续请求投票
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if i == rf.me { // 读到自己则跳过
			continue
		}

		// 利用协程并行地发送请求投票RPC，参考lec5的code示例vote-count-4.go
		go func(idx int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         sameTerm, // 此处用之前记录的currentTerm副本
				CandidatedId: rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			// 注意传的是args和reply的地址而不是结构体本身！
			ok := rf.sendRequestVote(idx, &args, &reply) // candidate向 server i 发送请求投票RPC
			if !ok {
				DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, idx)
			}

			// 如果candidate任期比其他server的小，则candidate更新自己的任期并转为follower，并跟随此server
			rf.mu.Lock()

			// 处理RPC回复之前先判断，如果自己不再是Candidate了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Candidate || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}

			if rf.currentTerm < reply.Term {
				rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
				rf.state = Follower         // 变回Follower
				rf.currentTerm = reply.Term // 更新自己的term为较新的值
				rf.persist()
				rf.mu.Unlock()

				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
				return // 这里只是退出了协程.
			}
			rf.mu.Unlock()

			vote := reply.VoteGranted // 查看是否收到选票。如果RPC发送失败，reply中的投票仍是默认值，相当于没收到投票
			voteMu.Lock()             // 访问votes和finished前加锁
			if vote {
				DPrintf("Candidate %d got a vote from server %d!\n", rf.me, idx)
				votes++
			}
			finished++
			voteMu.Unlock()
			cond.Broadcast() // // Broadcast 会清空队列，唤醒全部的等待中的 goroutine
		}(i) // 因为i随着for循环在变，因此将它作为参数传进去
	}

	sumNum := len(rf.peers)     // 集群中总共的server数
	majorityNum := sumNum/2 + 1 // 满足大多数至少需要的server数量

	voteMu.Lock() // 调用 Wait 方法的时候一定要持有锁
	// 检查是否满足“获得大多数选票”的条件
	for votes < majorityNum && finished != sumNum { // 投票数尚不够，继续等待剩余server的投票
		cond.Wait() // 调用该方法的 goroutine 会被放到 Cond 的等待队列中并阻塞，直到被 Signal 或者 Broadcast 方法唤醒

		// 当candidate收到比自己term大的rpc回复时它就回到follower，此时直接结束自己的竞选
		// 或者该candidate得不到多数票但又由于有server崩溃而得不到sumNum张选票而一直等待，此时只有当有leader出现并发送心跳让该candidate变回follower跳出循环
		// 所以每次都要检测该candidate是否仍然在竞选，如果它已经退选，就不用一直等待选票了
		rf.mu.Lock()
		stillCandidate := (rf.state == Candidate)
		rf.mu.Unlock()
		if !stillCandidate {
			voteMu.Unlock() // 如果提前退出，不要忘了把这个锁释放掉
			return
		}
	}

	if votes >= majorityNum { // 满足条件，直接当选leader
		rf.Convert2Leader() // 成为leader就不需要超时计时了，直至故障或发现自己的term过时
	} else { // 收到所有回复但选票仍不够的情况，即竞选失败
		DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
	}
	voteMu.Unlock()
}

// 返回两int中的较大值（由于Go只有求两个float64的较大值的函数，且Go不支持三元运算符）
func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// 返回两int中的较小值（由于Go只有求两个float64的较大值的函数，且Go不支持三元运算符）
func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

// leader发送日志追加RPC或心跳包的相关逻辑
// leader调用LeaderAppendEntries()的两种情况：
// 1. 周期性发送心跳包
// 2. 客户端发来新的command，复制日志到各server
func (rf *Raft) LeaderAppendEntries() {

	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	// 由于leader永远不会覆盖或删除自己日志中的条目，因此matchIndex当然单调递增
	// 这里更新leader自己的matchIndex和nextIndex其实只是为了在判断“大多数复制成功”更新commitIndex时正确达成共识（毕竟leader不用往自己发送RPC复制日志）
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index // 更新leader自己的matchIndex，就等于其log最后一个日志条目的index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1     // 更新leader自己的nextIndex
	rf.mu.Unlock()

	// leader向除自己以外的其他server发送AppendEntries RPC
	for i, _ := range rf.peers { // i是目的server在rf.peers[]中的索引（id）

		if i == rf.me { // 读到自己则跳过
			continue
		}

		// 利用协程并行地发送AppendEntries RPC（包括心跳包）
		go func(idx int) {

			if rf.killed() { // 如果在发送AppendEntries RPC过程中leader被kill了就直接结束
				return
			}

			rf.mu.Lock()

			// 发送RPC之前先判断，如果自己不再是leader了则直接返回
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			appendLogs := []LogEntry{} // 若为心跳包则要追加的日志条目为空切片
			nextIdx := rf.nextIndex[idx]

			if nextIdx <= rf.lastIncludedIndex { // 如果要追加的日志已经被截断了则向该follower发送快照
				go rf.LeaderSendSnapshot(idx, rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				return
			}

			// 根据Figure2的Leader Rule 3
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			// 如果leader的日志从nextIdx开始有要发送的日志，则此AppendEntries RPC需要携带从nextIdx开始的日志条目
			if rf.log[len(rf.log)-1].Index >= nextIdx {
				// copy:目标切片必须分配过空间且足够承载复制的元素个数，并且来源和目标的类型必须一致
				appendLogs = make([]LogEntry, len(rf.log)-nextIdx+rf.lastIncludedIndex)
				copy(appendLogs, rf.log[nextIdx-rf.lastIncludedIndex:]) // 将leader日志nextIdx及之后的条目复制到appendLogs
			}
			preLog := rf.log[nextIdx-rf.lastIncludedIndex-1] // preLog是leader要发给server idx的日志条目的前一个日志条目
			args := AppendEntriesArgs{
				Term:         sameTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLog.Index,
				PreLogTerm:   preLog.Term,
				Entries:      appendLogs, // 若为心跳包则要追加的日志条目为空切片，否则为携带日志的切片
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d, PreLogIndex:%v,leaderCommit:%v) to server %d...\n",
				rf.me, sameTerm, len(args.Entries), args.PreLogIndex, args.LeaderCommit, idx)
			// 注意传的是args和reply的地址而不是结构体本身！
			ok := rf.sendAppendEntries(idx, &args, &reply) // leader向 server i 发送AppendEntries RPC

			if !ok {
				// 如果由于网络原因或者follower故障等收不到RPC回复（不是follower将回复设为false）
				// 则leader无限期重复发送同样的RPC（nextIndex不前移），等到下次心跳时间到了后再发送
				DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, idx)
				return
			}

			// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
			rf.mu.Lock() //要整体加锁，不能只给if加锁然后解锁
			defer rf.mu.Unlock()

			// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}

			if rf.currentTerm < reply.Term {
				rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
				rf.state = Follower         // 变回Follower
				rf.currentTerm = reply.Term // 更新自己的term为较新的值
				rf.persist()
				return // 这里只是退出了协程
			}

			// 如果出现follower的日志与leader的不一致，即append失败
			if reply.Success == false { // follower拒绝接受日志的情况（不一致）
				possibleNextIdx := 0 // 可能的nextIndex[idx]

				if reply.ConflictTerm == -1 { // 如果follower的日志中没有prevLogIndex
					possibleNextIdx = reply.ConflictIndex // 这里需要提前判断节省时间，否则后面2C部分测试会FAIL
				} else {
					foundConflictTerm := false

					// 从后往前找
					k := len(rf.log) - 1
					for ; k > 0; k-- {
						if rf.log[k].Term == reply.ConflictTerm {
							foundConflictTerm = true
							break
						}
					}

					if foundConflictTerm {
						possibleNextIdx = rf.log[k+1].Index // 若找到了对应的term，则找到对应term出现的最后一个日志条目的下一个日志条目
					} else {
						possibleNextIdx = reply.ConflictIndex
					}

				}
				if possibleNextIdx < rf.nextIndex[idx] && possibleNextIdx > rf.matchIndex[idx] {
					rf.nextIndex[idx] = possibleNextIdx
				} else { // 若不满足则视为过时，舍弃掉这次RPC回复
					return
				}

			} else { // 若追加成功
				// 更新对应follower的nextIndex和matchIndex

				// 根据guide，你不能假设server的状态在它发送RPC和收到回复之间没有变化。
				// 因为可能在这期间收到新的指令而改变了log和nextIndex
				// 通常 nextIndex = matchIndex + 1
				possibleMatchIdx := args.PreLogIndex + len(args.Entries)
				rf.matchIndex[idx] = max(possibleMatchIdx, rf.matchIndex[idx]) // 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1                     // matchIndex安全则nextIndex这样也安全

				// 根据Figure2 Leader Rule 4，确定满足commitIndex < N <= 大多数matchIndex[i]且在当前任期的N
				// 先对matchIndex升序排序，为了不影响到matchIndex原来的值，此处对副本排序
				sortMatchIndex := make([]int, len(rf.peers))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex)                         // 升序排序
				maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2] // 满足N <= 大多数matchIndex[i] 的最大的可能的N
				for N := maxN; N > rf.commitIndex; N-- {
					if rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
						rf.commitIndex = N // 如果log[N]的任期等于当前任期则更新commitIndex
						DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
						break
					}
				}
			}

		}(i) // 因为i随着for循环在变，因此将它作为参数传进去
	}

}

// leader发送SnapShot信息给落后的Follower
// idx是要发送给的follower的序号
func (rf *Raft) LeaderSendSnapshot(idx int, snapshotData []byte) {
	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	rf.mu.Unlock()

	// leader向follower[idx]发送InstallSnapshot RPC

	if rf.killed() { // 如果在发送InstallSnapshot RPC过程中leader被kill了就直接结束
		return
	}

	rf.mu.Lock()

	// 发送RPC之前先判断，如果自己不再是leader了则直接返回
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              sameTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		SnapshotData:      snapshotData,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}

	DPrintf("Leader %d sends InstallSnapshot RPC(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) to server %d...\n",
		rf.me, sameTerm, args.LastIncludedIndex, args.LastIncludedTerm, idx)
	// 注意传的是args和reply的地址而不是结构体本身！
	ok := rf.sendSnapshot(idx, &args, &reply) // leader向 server idx 发送InstallSnapshot RPC

	if !ok {
		DPrintf("Leader %d calls server %d for InstallSnapshot(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) failed!\n",
			rf.me, idx, sameTerm, args.LastIncludedIndex, args.LastIncludedTerm)
		// 如果由于网络原因或者follower故障等收不到RPC回复
		return
	}

	// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
	rf.mu.Lock() //要整体加锁，不能只给if加锁然后解锁
	defer rf.mu.Unlock()

	// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
	// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
		rf.state = Follower         // 变回Follower
		rf.currentTerm = reply.Term // 更新自己的term为较新的值
		rf.persist()
		return // 这里只是退出了协程
	}

	// 若follower接受了leader的快照则leader需要更新对应的matchIndex和nextIndex等（也得保证递增性，不能回退）
	if reply.Accept {
		possibleMatchIdx := args.LastIncludedIndex
		rf.matchIndex[idx] = max(possibleMatchIdx, rf.matchIndex[idx]) // 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1                     // matchIndex安全则nextIndex这样也安全
	}

}

// example RequestVote RPC handler.
// 另一个raft server收到candidate请求投票的rpc后进行逻辑处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 见Figure2 "RequestVote RPC"
	// 根据Figure2的说明，当候选者term < 投票者的current term时直接返回false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm // 将自己的current term附在回复中
		return
	}

	// 候选者term >= 投票者的current term的情况，需要进一步判断
	// Raft通过比较最后一个日志条目的index和term来决定两个日志哪个更新

	// 见Figure2 "Rules for Servers“
	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	if rf.currentTerm < args.Term {
		// 下台并采用更高的任期，这会重新设置votedFor，就拥有新任期的投票权
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.leaderId = -1           // term改变，leaderId也要重置
		rf.state = Follower        // 变回Follower
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	// 判断candidate的log是否至少与这个待投票的server的log一样新，见论文5.4节，两个规则
	// 如果日志的最后条目具有不同的term，那么具有较后term的日志是更新
	// 如果日志的最后条目具有相同的term，那么哪个日志更长，哪个日志就更新
	uptodate := false
	voterLastLog := rf.log[len(rf.log)-1] // 获取投票者最后一个日志条目（如果是空日志，则获取到的是初始化时加在下标0的“占位”元素）

	// args.LastLogIndex >= voterLastLog.index需要加等号是因为第一届leader选举过程中，两个server都还没有日志
	// 它们比较的最后一个日志的term和index事实上都是初始化加入的占位元素的term和index，即term -1 == -1， index 0 == 0
	// 显然这种情况投票者是会投票给这个candidate的
	if (args.LastLogTerm > voterLastLog.Term) || (args.LastLogTerm == voterLastLog.Term && args.LastLogIndex >= voterLastLog.Index) {
		uptodate = true
	}

	// 见Figure2 "RequestVote RPC"
	if (rf.votedFor == -1 || rf.votedFor == args.CandidatedId) && uptodate { // 投票给这个candidate
		rf.votedFor = args.CandidatedId
		rf.leaderId = -1 // 你投票了，说明你不信之前的leader了
		reply.VoteGranted = true
		rf.persist()
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
	} else { // 不符合的不予投票
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm // 其实这里reply.Term就等于请求投票的candidate的term

}

// AppendEntries RPC handler
// 其他servers收到leader的追加日志rpc或心跳包后进行逻辑处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 检查日志是匹配的才接收
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 根据Figure2 AppendEntries RPC的receiver实现规则
	// leader自己的term比RPC接收server的term还要小，则追加日志失败
	// 如果AppendEntries RPC中的任期过时，则不应该重启计时器！
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm // 将自己的current term附在回复中
		return
	}

	// 见Figure2 "Rules for Servers“及5.3节
	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	// 另外，当candidate收到来自另一个声称是leader的server的RPC时，
	// 如果这个leader的term >= 这个candidate的current term，则candidate将承认这个leader是合法的，并返回到follower状态
	wasLeader := (rf.state == Leader) // 标志rf曾经是leader

	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	// 这里实现了candidate或follower在收到leader的心跳包或日志追加RPC后重置计时器并维持follower状态
	rf.state = Follower         // 变回或维持Follower
	rf.leaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）
	DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
		rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	// 如果follower的term与leader的term相等（大多数情况），那么follower收到AppendEntries RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

	if wasLeader { // 如果是leader收到AppendEntries RPC（虽然概率很小）
		go rf.HandleTimeout() // 如果是leader重回follower则要重新循环进行超时检测
	}

	// 如果args.PrevLogIndex < rf.lastIncludedIndex，对于参数中index < rf.lastIncludedIndex部分的log，按照index转换规则会导致index<0，不要处理这部分log
	if args.PreLogIndex < rf.lastIncludedIndex {
		// 如果要追加的日志段过于陈旧（该follower早就应用了更新的快照），则不进行追加
		if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.lastIncludedIndex {
			// 这种情况下要是reply false则会导致leader继续回溯发送更长的日志，但实际应该发送更后面的日志段，因此reply true但不实际修改自己的log
			reply.Success = true
			reply.Term = rf.currentTerm
			return
		} else {
			args.Entries = args.Entries[rf.lastIncludedIndex-args.PreLogIndex:]
			args.PreLogIndex = rf.lastIncludedIndex
			args.PreLogTerm = rf.lastIncludedTerm
			// 之后执行下面匹配上了的else分支
		}
	}

	// 如果follower中没有leader在preLogIndex处相匹配的日志
	if rf.log[len(rf.log)-1].Index < args.PreLogIndex || rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term != args.PreLogTerm {
		// 日志回溯加速优化修改
		if rf.log[len(rf.log)-1].Index < args.PreLogIndex { // 如果follower的日志中没有prevLogIndex
			reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
			reply.ConflictTerm = -1
		} else { // 如果follower在其日志中确实有prevLogIndex，但是任期不匹配
			reply.ConflictTerm = rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term
			i := args.PreLogIndex - 1 - rf.lastIncludedIndex
			for i >= 0 && rf.log[i].Term == reply.ConflictTerm { // 在其日志中搜索其条目中任期等于conflictTerm的第一个索引
				i--
			}
			reply.ConflictIndex = i + 1 + rf.lastIncludedIndex
		}

		reply.Success = false // 返回false
		reply.Term = rf.currentTerm
		return
	} else { // 匹配到了两个日志一致的最新日志条目
		// 日志一致性检查到leader让follower追加日志操作中，都用AppendEntries RPC，这样leader不用专门去恢复日志一致性
		// 即使是心跳包也无需特别处理，因为追加的日志为空，但注意心跳包也要通过一致性检查才会返回true

		// PreLogIndex与PrevLogTerm匹配到的情况，还要额外检查新同步过来的日志和已存在的日志是否存在冲突:
		// 如果一个已经存在的日志项和新的日志项冲突（相同index但是不同term），那么要删除这个冲突的日志项及其往后的日志，并将新的日志项追加到日志中。
		misMatchIndex := -1
		for i, entry := range args.Entries {
			if args.PreLogIndex+1+i > rf.log[len(rf.log)-1].Index || rf.log[args.PreLogIndex-rf.lastIncludedIndex+1+i].Term != entry.Term { // 找到第一个冲突项
				misMatchIndex = args.PreLogIndex + 1 + i
				break
			}
		}

		if misMatchIndex != -1 { // 已存在的日志与RPC中的Entries有冲突的情况
			newLog := rf.log[:misMatchIndex-rf.lastIncludedIndex]                       // 从头截取到misMatchIndex（但不包括）的是一致的日志
			newLog = append(newLog, args.Entries[misMatchIndex-args.PreLogIndex-1:]...) // 追加日志中没有的任何新条目（也即leader在preLogIndex之后的日志）
			rf.log = newLog
		}

		rf.persist()

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			// leader都还没有将所有日志提交，则follower最多提交到leader提交的位置
			// leader已经至少提交到了发给follower的最后一个日志，则follower就把自己现有的日志提交
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

// 被动快照
// follower接收leader发来的InstallSnapshot RPC的handler
func (rf *Raft) CondInstallSnap(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Figure 13 rules[1]
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	// 接受leader的被动快照前先检查给server是否正在进行主动快照，若是则本次被动快照取消
	// 避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
	if rf.activeSnapshotting {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	wasLeader := (rf.state == Leader) // 标志rf曾经是leader

	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	rf.state = Follower         // 变回或维持Follower
	rf.leaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）

	// 如果follower的term与leader的term相等（大多数情况），那么follower收到AppendEntries RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

	if wasLeader { // 如果是leader收到AppendEntries RPC（虽然概率很小）
		go rf.HandleTimeout() // 如果是leader重回follower则要重新循环进行超时检测
	}

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm
	reply.Term = rf.currentTerm

	if snapshotIndex <= rf.lastIncludedIndex { // 说明snapshotIndex之前的log已经做成snapshot并删除了
		DPrintf("Server %d refuse the snapshot from leader.\n", rf.me)
		reply.Accept = false
		return
	}

	var newLog []LogEntry

	// 如果leader传来的快照比本地的快照更新

	rf.lastApplied = args.LastIncludedIndex // 下一条指令直接从快照后开始（重新）apply

	if snapshotIndex < rf.log[len(rf.log)-1].Index { // 若应用此次快照，本地还有日志要接上
		// 若快照和本地日志在snapshotIndex索引处的日志term不一致，则扔掉本地快照后的所有日志
		if rf.log[snapshotIndex-rf.lastIncludedIndex].Term != snapshotTerm {
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			rf.commitIndex = args.LastIncludedIndex // 由于后面被截断的日志无效，故等重新计算commitIndex
		} else { // 若term没冲突，则在snapshotIndex处截断并保留后续日志
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			newLog = append(newLog, rf.log[snapshotIndex-rf.lastIncludedIndex+1:]...)
			rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex) // 后面日志有效则保证commit不回退
		}
	} else { // 若此快照比本地日志还要长，则应用后日志就清空了
		newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
		rf.commitIndex = args.LastIncludedIndex
	}

	// 更新相关变量
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = newLog
	rf.passiveSnapshotting = true

	// 通过persister进行持久化存储
	rf.persist()                                                                       // 先持久化raft state（因为rf.log，rf.lastIncludedIndex，rf.lastIncludedTerm改变了）
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.SnapshotData) // 持久化raft state及快照

	// 向kvserver发送带快照的ApplyMsg通知它安装
	rf.InstallSnapFromLeader(snapshotIndex, args.SnapshotData)

	DPrintf("Server %d accept the snapshot from leader(lastIncludedIndex=%v, lastIncludedTerm=%v).\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	reply.Accept = true
	return
}

// 返回raft state size的字节数
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// raft层接收对应kvserver传来的快照将包含的最后一个index以及快照数据并进行快照
// leader or follower都可以主动快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()

	// 如果主动快照的index不大于rf之前的lastIncludedIndex（这次快照其实是重复或更旧的），则不应用该快照
	if index <= rf.lastIncludedIndex {
		DPrintf("Server %d refuse this positive snapshot(index=%v, rf.lastIncludedIndex=%v).\n", rf.me, index, rf.lastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	DPrintf("Server %d start to positively snapshot(rf.lastIncluded=%v, snapshotIndex=%v).\n", rf.me, rf.lastIncludedIndex, index)

	// 修剪log[]，将index及以前的日志条目剪掉
	var newLog = []LogEntry{{Term: rf.log[index-rf.lastIncludedIndex].Term, Index: index}} // 裁剪后依然log索引0处用一个占位entry，不实际使用
	newLog = append(newLog, rf.log[index-rf.lastIncludedIndex+1:]...)                      // 这样可以避免原log底层数组由于有部分在被引用而无法将剪掉的部分GC（真正释放）
	rf.log = newLog
	rf.lastIncludedIndex = newLog[0].Index
	rf.lastIncludedTerm = newLog[0].Term
	// 主动快照时lastApplied、commitIndex一定在snapshotIndex之后，因此不用更新

	// 通过persister进行持久化存储
	rf.persist() // 先持久化raft state（因为rf.log，rf.lastIncludedIndex，rf.lastIncludedTerm改变了）
	state := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	isLeader := (rf.state == Leader)
	rf.mu.Unlock()

	// leader通过InstallSnapshot RPC将本次的SnapShot信息发送给其他Follower
	if isLeader {
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.LeaderSendSnapshot(i, snapshot)
		}
	}
	return

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 由candidate调用此方法请求其他server投票
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // 调用对应server的Raft.RequestVote方法进行请求投票逻辑处理
	return ok
}

// 由leader调用，向其他servers发送日志条目追加请求或心跳包（没有携带日志条目的AppendEntries RPCs）
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) // 调用对应server的Raft.AppendEntries方法进行请求日志追加处理
	return ok
}

// 由leader调用，向其他servers发送快照
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.CondInstallSnap", args, reply) // 调用对应server的Raft.GetSnapshot方法安装日志
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 接受客户端的command，并且应用raft算法
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState() // 直接调用GetState()来获取该server的当前任期以及是否为leader

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.log[len(rf.log)-1].Index + 1 // index：如果这条日志最后被提交，那么它将在日志中的索引（注意索引0处的占位元素也算在切片len里面）

	if isLeader == false { // 如果这个Server不是leader则直接返回false，不继续执行后面的追加日志
		return index, term, false
	}

	// 如果是leader则准备开始向其他server复制日志
	newLog := LogEntry{ // 将指令形成一个新的日志条目
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newLog) // leader首先将新日志条目追加到自己的日志中
	rf.persist()
	DPrintf("[Start]Client sends a new commad(%v) to Leader %d!\n", command, rf.me)
	// 客户端发来新的command，复制日志到各server，调用LeaderAppendEntries()
	// 如果追加失败（网络问题或日志不一致被拒绝），则重复发送由携带日志条目的周期性的心跳包来完成
	go rf.LeaderAppendEntries() // 由新日志触发AppendEntries RPC的发送
	return index, term, true
}

// 循环检查是否有需要apply的日志
func (rf *Raft) applier() {
	for !rf.killed() { // 如果server没有被kill就一直检测
		rf.mu.Lock()

		var applyMsg ApplyMsg
		needApply := false

		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++

			if rf.lastApplied <= rf.lastIncludedIndex { // 说明这条命令已经被做成snapshot了，不需要提交
				rf.lastApplied = rf.lastIncludedIndex // 直接将lastApplied提前到lastIncludedIndex
				rf.mu.Unlock()                        // continue前不要忘了先解锁！！！
				continue
			}

			applyMsg = ApplyMsg{
				CommandValid:  true, // true代表此applyMsg包含一个新的已提交的日志条目
				SnapshotValid: false,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastApplied,
				CommandTerm:   rf.log[rf.lastApplied-rf.lastIncludedIndex].Term,
			}
			needApply = true

		}
		rf.mu.Unlock()

		// 本日志需要apply
		if needApply {
			rf.applyCh <- applyMsg // 将ApplyMsg发送到管道
		} else {
			time.Sleep(10 * time.Millisecond) // 不要让循环一直连续执行，可能占用很多时间而测试失败
		}
	}
}

// 如果接受，则follower将leader发来的快照发到applyCh便于状态机安装
func (rf *Raft) InstallSnapFromLeader(snapshotIndex int, snapshotData []byte) {
	// follower接收到leader发来的InstallSnapshot RPC后先不要安装快照，而是发给状态机，判断为较新的快照时raft层才进行快照
	snapshotMsg := ApplyMsg{
		SnapshotValid:     true,
		CommandValid:      false,
		SnapshotIndex:     snapshotIndex,
		StateMachineState: snapshotData, // sm_state
	}

	rf.applyCh <- snapshotMsg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	DPrintf("Server %d send SnapshotMsg(snapIndex=%v) to ApplyCh.\n", rf.me, snapshotIndex)

}

// 检查raft是否有当前term的日志
func (rf *Raft) CheckCurrentTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	latestLog := rf.log[len(rf.log)-1]
	if rf.currentTerm == latestLog.Term {
		return true
	}

	return false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// Kill()将rf.dead原子地设为1（不需要加锁）
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1) // StoreInt32()函数用于将val原子地存储到*addr中
	// Your code here, if desired.

}

// killed()检查Kill()是否被调用了，也即raft是否被kill
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // LoadInt32()函数用于原子地加载*addr并返回其中存的int值
	return z == 1
}

// 获取l~r毫秒范围内一个随机毫秒数
func getRandMS(l int, r int) int {
	// 如果每次调rand.Intn()前都调了rand.Seed(x)，每次的x相同的话，每次的rand.Intn()也是一样的（伪随机）
	// 推荐做法：只调一次rand.Seed()：在全局初始化调用一次seed，每次调rand.Intn()前都不再调rand.Seed()。
	// 此处采用使Seed中的x每次都不同来生成不同的随机数，x采用当前的时间戳
	rand.Seed(time.Now().UnixNano())
	ms := l + (rand.Intn(r - l)) // 生成l~r之间的随机数（毫秒）
	return ms
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 在config.go的start1(i int)中被用到起一个Raft
// 创建一个raft peer
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers         // peers参数是Raft peers（包括这一个）的网络标识符的一个数组
	rf.persister = persister //调用Raft.Make()的会提供一个初始保存Raft最近持久化状态的Persister
	rf.me = me               // me参数是这个peer在peer数组里的索引
	rf.dead = 0              // 0代表该server还存活，1代表它被kill了

	// Your initialization code here (2A, 2B, 2C).
	var mutex sync.Mutex
	rf.mu = mutex
	rf.currentTerm = 0
	rf.state = Follower // server刚开始为follower，且current term为0
	rf.votedFor = -1    // 初始时还没投票，就为-1
	rf.leaderId = -1
	// 一开始没有日志条目
	// 由于合法日志索引从1开始，为了让日志index与切片下标对应，故先填充一个元素
	// 这个元素不是日志，term值和index值非法，在leader选举过程中也免去了单独讨论空日志的情况，不用怕空指针报错
	// 注意，这里占位日志的index应设为0而非其他值！！！
	rf.log = []LogEntry{{Term: -1, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.ready = false
	rf.hbTime = 100 * time.Millisecond // 心跳间隔设为100ms一次
	rf.applyCh = applyCh

	// 一开始每个follower都会开始计时一个随机的选举超时时间，到点后如果没有收到leader或candidate的消息，则宣布竞选
	// 根据lab提示，选举超时设定应该比150ms~300ms更大但又不至于太大，这里选择250ms~400ms内的一个随机值
	// 因为限制心跳不超过每秒10次，且还要保证5s内选出leader
	// time.Duration(x) 是进行的类型转换，把整型x转换成了time.Duration类型
	rf.timer = time.NewTimer(time.Duration(getRandMS(300, 500)) * time.Millisecond)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	rf.passiveSnapshotting = false
	rf.activeSnapshotting = false

	// initialize from state persisted before a crash
	// 从crash中恢复之前的状态
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnap(persister.ReadSnapshot()) // 从快照中恢复
	rf.persist()

	DPrintf("Server %v (Re)Start and lastIncludedIndex=%v, rf.lastIncludedTerm=%v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 起一个goroutine循环处理超时
	go rf.HandleTimeout()

	// 起一个goroutine循环检查是否有需要应用到状态机日志
	go rf.applier()

	return rf
}
