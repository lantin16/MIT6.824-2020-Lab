package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0 // 设为1则将打印信息输出到终端

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const RespondTimeout = 500 // kvserver回复client的超时时间，单位：毫秒

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64 // 标识客户端
	CmdNum   int
	OpType   string // 操作类型，put/append/get
	Key      string
	Value    string // 若为Get命令则value可不设
}

// session跟踪为client处理的最新序列号，以及相关的响应。如server接收到序列号已经执行过的命令，它会立即响应，而不需要重新执行请求
type Session struct {
	LastCmdNum int    // 该server为该client处理的上一条指令的序列号
	OpType     string // 最新处理的指令的类型
	Response   Reply  // 对应的响应
}

// GetReply和PutAppendReply的统一结构，用于kvserver保存请求的回复用
type Reply struct {
	Err   Err
	Value string // Get命令时有效
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft // 每个kvserver都有一个关联的raft peer
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your definitions here.
	kvDB                  map[string]string  // 服务器保存的键值对（具体的业务逻辑）
	sessions              map[int64]Session  // 此server的状态机为各个client维护的会话（session）
	maxraftstate          int                // snapshot if log grows this big
	notifyMapCh           map[int]chan Reply // kvserver apply到了等待回复的日志则通过chan通知对应的handler方法回复client，key为日志的index
	logLastApplied        int                // 此kvserver apply的上一个日志的index
	passiveSnapshotBefore bool               // 标志着applyMessage上一个从channel中取出的是被动快照并已安装完
}

// 为leader在kv.notifyMapCh中初始化一个缓冲为1的channel完成本次与applyMessage()的通信
func (kv *KVServer) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	notifyCh := make(chan Reply, 1)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

// KVServer回复client后关闭对应index的notifyCh
func (kv *KVServer) CloseNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyMapCh[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

// Get handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	getOp := Op{
		ClientId: args.ClientId,
		CmdNum:   args.CmdNum,
		OpType:   "Get",
		Key:      args.Key,
	}

	index, _, isLeader := kv.rf.Start(getOp) // 调用rf.Start()，将client的cmd传给raft server

	if !isLeader { // 如果client联系的这个server不是leader
		reply.Err = ErrWrongLeader // 回复的报错信息
		return
	}

	notifyCh := kv.createNotifyCh(index)

	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Value = res.Value // Get请求要返回value
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.CloseNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel

}

// PutAppend handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	// KVServer在处理每个Client的新的请求的时候，首先判断该请求的CmdNum和该Client上一个处理完成的请求的CmdNum（存在session里）
	if args.CmdNum < kv.sessions[args.ClientId].LastCmdNum { // 如果这个请求的序号小于该client上一个已经完成的请求的序号
		// 标志这个请求Client已经收到正确的回复了，只是这个重发请求到的太慢了
		kv.mu.Unlock()
		return // 直接返回。因为Client在一个命令没有成功之前会无限重试。所以如果收到了一个更小CmdNum的请求，可以断定这个请求Client已经收到正确的回复了
	} else if args.CmdNum == kv.sessions[args.ClientId].LastCmdNum { // 回复丢失或延迟导致client没收到而重发了这个请求
		// 将session中记录的之前apply该命令的结果直接返回，使得不会apply一个命令多次
		reply.Err = kv.sessions[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	} else { // 如果是未处理的新请求
		kv.mu.Unlock() // 一定要先解锁！！否则Start()会发生四向锁
		paOp := Op{
			ClientId: args.ClientId,
			CmdNum:   args.CmdNum,
			OpType:   args.Op,
			Key:      args.Key,
			Value:    args.Value,
		}

		index, _, isLeader := kv.rf.Start(paOp) // 调用rf.Start()，将client的cmd传给raft server

		if !isLeader { // 如果client联系的这个server不是leader
			reply.Err = ErrWrongLeader // 回复的报错信息
			return
		}

		notifyCh := kv.createNotifyCh(index)

		select {
		case res := <-notifyCh:
			reply.Err = res.Err
		case <-time.After(RespondTimeout * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go kv.CloseNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// kvserver执行apply的指令为了与leader server的kvDB保持一致
// 并发请求的一致顺序靠raft达成共识的一致日志来保证
// kvserver执行从applyCh取出来的命令或安装快照
func (kv *KVServer) applyMessage() {
	DPrintf("KVserver[%d] (re)start applymessage.\n", kv.me)
	for !kv.killed() {
		applyMsg := <-kv.applyCh // 不断从applyCh中取applyMsg

		if applyMsg.CommandValid { // 如果取出的是command则执行
			DPrintf("KVServer[%d] get a Command applyMsg(= %v) from applyCh.\n", kv.me, applyMsg)

			kv.mu.Lock()

			if applyMsg.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}

			// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
			// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
			if kv.passiveSnapshotBefore {
				if applyMsg.CommandIndex-kv.logLastApplied != 1 {
					kv.mu.Unlock()
					continue
				}
				kv.passiveSnapshotBefore = false
			}

			// 否则就将logLastApplied更新为较大值applyMsg.CommandIndex
			kv.logLastApplied = applyMsg.CommandIndex
			DPrintf("KVServer[%d] update logLastApplied to %d.\n", kv.me, kv.logLastApplied)

			op, ok := applyMsg.Command.(Op) // 类型断言
			if !ok {                        // 断言失败也不报panic，继续执行后面的
				DPrintf("convert fail!\n") // 可能是掺杂的leader上任后添加的空白指令
			} else { // 断言成功
				reply := Reply{}
				sessionRec, exist := kv.sessions[op.ClientId]

				// 如果apply的指令之前已经apply过且不是Get指令则不重复执行，直接返回session中保存的结果
				if exist && op.OpType != "Get" && op.CmdNum <= sessionRec.LastCmdNum { // 若为重复的Get请求可以重复执行
					reply = kv.sessions[op.ClientId].Response // 返回session中记录的回复
					DPrintf("KVServer[%d] use the reply(=%v) in sessions for %v.\n", kv.me, reply, op.OpType)
				} else { // 没有apply过的指令就在状态机上执行，重复的Get指令可以重新执行
					switch op.OpType {
					case "Get":
						v, existKey := kv.kvDB[op.Key]
						if !existKey { // 键不存在
							reply.Err = ErrNoKey
							reply.Value = "" // key不存在则Get返回空字符串
						} else {
							reply.Err = OK
							reply.Value = v
						}
						DPrintf("KVServer[%d] apply Get(key:%v, value:%v) Op!\n", kv.me, op.Key, reply.Value)
					case "Put":
						kv.kvDB[op.Key] = op.Value
						reply.Err = OK
						DPrintf("KVServer[%d] apply Put(key:%v, value:%v) Op!\n", kv.me, op.Key, op.Value)
					case "Append":
						oldValue, existKey := kv.kvDB[op.Key] // 若key不存在则取出的为该类型的零值
						if !existKey {                        // key不存在则append类似于put
							reply.Err = ErrNoKey
							kv.kvDB[op.Key] = op.Value
						} else {
							reply.Err = OK
							kv.kvDB[op.Key] = oldValue + op.Value // 追加到原value的后面
						}
						DPrintf("KVServer[%d] apply Append(key:%v, value:%v) Op!\n", kv.me, op.Key, op.Value)
					default:
						DPrintf("Unexpected OpType!\n")
					}

					// 将最近执行的除Get指令外的指令执行结果存放到session中以便后续重复请求直接返回而不重复执行
					if op.OpType != "Get" { // Get请求的回复就不用放到session了
						session := Session{
							LastCmdNum: op.CmdNum,
							OpType:     op.OpType,
							Response:   reply,
						}
						kv.sessions[op.ClientId] = session
						DPrintf("KVServer[%d].sessions[%d] = %v\n", kv.me, op.ClientId, session)
					}

				}

				if _, existCh := kv.notifyMapCh[applyMsg.CommandIndex]; existCh {
					if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
						kv.notifyMapCh[applyMsg.CommandIndex] <- reply // 向对应client的notifyMapCh发送reply通知对应的handle回复client
					}
				}

			}
			kv.mu.Unlock()

		} else if applyMsg.SnapshotValid { // 如果取出的是snapshot
			DPrintf("KVServer[%d] get a Snapshot applyMsg from applyCh.\n", kv.me)

			// 在raft层已经实现了follower是否安装快照的判断
			// 只有followr接受了快照才会通过applyCh通知状态机，因此这里状态机只需要安装快照即可
			kv.mu.Lock()
			kv.applySnapshotToSM(applyMsg.StateMachineState) // 将快照应用到状态机
			kv.logLastApplied = applyMsg.SnapshotIndex       // 更新logLastApplied避免回滚
			kv.passiveSnapshotBefore = true                  // 刚安装完被动快照，提醒下一个从channel中取出的若是指令则注意是否为“跨快照指令”
			kv.mu.Unlock()
			DPrintf("KVServer[%d] finish a negative Snapshot, kv.logLastApplied become %v.\n", kv.me, kv.logLastApplied)

			kv.rf.SetPassiveSnapshottingFlag(false) // kvserver已将被动快照安装完成，修改对应raft的passiveSnapshotting标志
		} else { // 错误的ApplyMsg类型
			DPrintf("KVServer[%d] get an unexpected ApplyMsg!\n", kv.me)
		}
	}
}

// 将快照应用到state machine
func (kv *KVServer) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 { // 如果传进来的快照为空或无效则不应用
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[string]string
	var sessions map[int64]Session

	if d.Decode(&kvDB) != nil || d.Decode(&sessions) != nil {
		DPrintf("KVServer %d applySnapshotToSM ERROR!\n", kv.me)
	} else {
		kv.kvDB = kvDB
		kv.sessions = sessions
	}
}

// kvserver定期检查是否需要快照（主动进行快照）
func (kv *KVServer) checkSnapshotNeed() {
	for !kv.killed() {
		var snapshotData []byte
		var snapshotIndex int

		// 如果该server正在进行被动快照（从接受leader发来的快照到通过channel发给kvserver再到kvserver实际安装快照这期间）
		// 这期间该server不检测是否需要主动快照，避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
		if kv.rf.GetPassiveFlagAndSetActiveFlag() {
			DPrintf("Server %d is passive snapshotting and refuses positive snapshot.\n", kv.me)
			time.Sleep(time.Millisecond * 50) // 检查间隔50ms
			continue
		}

		// 若该server没有开始被动快照，则可以进行检查是否需要主动快照
		// 主动快照的标志在GetPassiveFlagAndSetActiveFlag()方法中已经设置了，这样做是为了保证检查被动快照标志和设置主动快照标志的原子性
		// 比较 maxraftstate 和 persistent.RaftStateSize，如果RaftStateSize超过maxraftstate的90%则认为日志过大需要进行快照
		if kv.maxraftstate != -1 && float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9 {
			kv.mu.Lock()
			// 准备进行主动快照
			DPrintf("KVServer[%d]: The Raft state size is approaching the maxraftstate, Start to snapshot...\n", kv.me)
			snapshotIndex = kv.logLastApplied
			// 将snapshot 信息编码
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvDB)
			e.Encode(kv.sessions)

			snapshotData = w.Bytes()
			kv.mu.Unlock()
		}

		if snapshotData != nil {
			kv.rf.Snapshot(snapshotIndex, snapshotData) // kvserver命令raft进行快照（截止到index）
		}
		kv.rf.SetActiveSnapshottingFlag(false) // 无论检查完需不需要主动快照都要将主动快照标志修改回false

		time.Sleep(time.Millisecond * 50) // 检查间隔50ms
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("(Re)Start KVServer[%d]!\n", me)

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	var mutex sync.Mutex
	kv.mu = mutex
	kv.dead = 0 // 0代表该server还存活，1代表它被kill了
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // kvserver会创建对应的raft
	kv.kvDB = make(map[string]string)
	kv.sessions = make(map[int64]Session)
	kv.notifyMapCh = make(map[int]chan Reply) // 这里里面的channel也要初始化，在该client第一次对kvserver发起请求的handler中初始化
	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false

	// 开启goroutine来apply日志或快照
	go kv.applyMessage()

	// 开启goroutine来定期检查log是否太大需要快照
	go kv.checkSnapshotNeed()

	return kv
}
