package shardkv

// import "../shardmaster"
import (
	"6.824/src/labrpc"
	"6.824/src/shardmaster"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/src/raft"
import "sync"
import "6.824/src/labgob"

const Debug = 0 // 设为1则将打印信息输出到终端

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	RespondTimeout      = 600 // shardkv回复client的超时时间，单位：毫秒
	ConfigCheckInterval = 80  // config变更检查间隔，单位：毫秒
)

// OpType类型
const (
	Get          = "Get"
	Put          = "Put"
	Append       = "Append"
	UpdateConfig = "UpdateConfig" // 更新config版本
	GetShard     = "GetShard"     // group获得了shard数据，需要将其应用到自己的kvDB并更新shard状态	WaitGet -> Exist
	GiveShard    = "GiveShard"    // group已经将shard给出去了，需要更新shard状态 WaitGive -> NoExist
	EmptyOp      = "EmptyOp"      // 空日志
)

// shard在某个group中的状态
type ShardState int

// shard状态类型
const (
	NoExist  ShardState = iota // 该shard已经不归该group管（稳定态）
	Exist                      // 该shard归该group管且已经完成迁移（稳定态）
	WaitGet                    // 该shard归该group管但需要等待迁移完成（迁移过程的中间态）
	WaitGive                   // 该shard已经不归该group管还需要等待将shard迁移走（迁移过程的中间态）
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64  // 标识客户端
	CmdNum      int    // 指令序号
	OpType      string // 操作类型
	Key         string
	Value       string             // 若为Get命令则value可不设
	NewConfig   shardmaster.Config // 新版本的config（仅UpdateConfig指令有效）
	CfgNum      int                // 本次shard迁移的config编号（仅GetShard和GiveShard指令有效）
	ShardNum    int                // 从别的group获取到的shard序号（仅GetShard和GiveShard指令有效）
	ShardData   map[string]string  // shard数据（仅GetShard指令有效）
	SessionData map[int64]Session  // shard前任所有者的Session数据（仅GetShard指令有效）
}

// session跟踪该group为client处理的上一次请求的序列号，以及相关的响应。
// 如果shardkv接收到序列号已经执行过的请求，它会立即响应，而不需要重新执行请求
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

type ShardKV struct {
	mu           sync.Mutex
	me           int // 该server在这一组servers中的序号
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd // make_end()函数将server名称转换为ClientEnd
	gid          int                            // 该ShardKV所属的group的gid
	masters      []*labrpc.ClientEnd            // ShardMaters集群的servers端口
	maxraftstate int                            // snapshot if log grows this big

	// Your definitions here.
	dead                  int32                           // set by Kill()
	mck                   *shardmaster.Clerk              // 关联shardmaster
	kvDB                  map[int]map[string]string       // 该group的服务器保存的键值对，为嵌套的map类型（shardNum -> {key -> value}）
	sessions              map[int64]Session               // 此server的状态机为各个client维护的会话（session）
	notifyMapCh           map[int]chan Reply              // kvserver apply到了等待回复的日志则通过chan通知对应的handler方法回复client，key为日志的index
	logLastApplied        int                             // 此kvserver apply的上一个日志的index
	passiveSnapshotBefore bool                            // 标志着applyMessage上一个从channel中取出的是被动快照并已安装完
	ownedShards           [shardmaster.NShards]ShardState // 该ShardKV负责的shard的状态
	preConfig             shardmaster.Config              // 该ShardKV当前的上一个config（用来找前任shard拥有者）
	curConfig             shardmaster.Config              // 该ShardKV目前所处的config
}

// 为leader在kv.notifyMapCh中初始化一个缓冲为1的channel完成本次与applyMessage()的通信
func (kv *ShardKV) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	notifyCh := make(chan Reply, 1)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

// ShardKV回复client后关闭对应index的notifyCh
func (kv *ShardKV) closeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyMapCh[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

// 检查某个key是否归该group负责且当前可以提供服务，返回结果
// 锁在调用前加
func (kv *ShardKV) checkKeyInGroup(key string) bool {
	shard := key2shard(key)             // 获取key对应的shard序号
	if kv.ownedShards[shard] != Exist { // 查询ShardKV维护的数组来判断
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 这里实现的是group内非leader的server也能回复ErrWrongGroup
	// 先检查该server所属的group是否仍负责请求的key（可能client手里的配置信息未更新）
	kv.mu.Lock()
	if in := kv.checkKeyInGroup(args.Key); !in {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 如果该group确实负责这个key则继续
	getOp := Op{
		ClientId: args.ClientId,
		CmdNum:   args.CmdNum,
		OpType:   Get,
		Key:      args.Key,
	}

	index, _, isLeader := kv.rf.Start(getOp) // 调用rf.Start()，将client的请求操作传给raft进行共识

	// 如果client联系的这个server不是leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Client[%d] Req Group[%d]ShardKV[%d] for Get(key:%v, index:%v).\n", args.ClientId, kv.gid, kv.me, args.Key, index)

	notifyCh := kv.createNotifyCh(index)

	// 等待请求执行完的回复并开始超时计时
	select {
	case res := <-notifyCh:
		// 返回前再次检查该key是否仍由该group负责（防止共识和应用期间config发生变化）
		kv.mu.Lock()
		if in := kv.checkKeyInGroup(args.Key); !in {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		reply.Err = res.Err
		reply.Value = res.Value // Get请求要返回value
		DPrintf("Group[%d]ShardKV[%d] respond client[%v] Get(key:%v, index:%v) req.\n", kv.gid, kv.me, args.ClientId, args.Key, index)
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.closeNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	// 这里实现的是group内非leader的server也能回复ErrWrongGroup
	// 先检查该server所属的group是否仍负责请求的key（可能client手里的配置信息未更新）
	if in := kv.checkKeyInGroup(args.Key); !in {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// ShardKV过滤重复请求
	if args.CmdNum < kv.sessions[args.ClientId].LastCmdNum {
		// 标志这个请求client已经收到正确的回复了，只是这个重发请求到达得太慢了
		// 其实client端并没有在等待这个回复了，直接return
		kv.mu.Unlock()
		return
	} else if args.CmdNum == kv.sessions[args.ClientId].LastCmdNum {
		// 将session中记录的之前执行该请求的结果直接返回，使得不会执行一个请求多次
		// 可能是由于之前的回复在网络中丢失或延迟导致重复请求
		reply.Err = kv.sessions[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	} else { // 如果是未处理的新请求
		kv.mu.Unlock() // 这里一定要先解锁！
		paOp := Op{
			ClientId: args.ClientId,
			CmdNum:   args.CmdNum,
			OpType:   args.Op,
			Key:      args.Key,
			Value:    args.Value,
		}

		index, _, isLeader := kv.rf.Start(paOp)

		// 如果client联系的这个server不是leader
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		DPrintf("Client[%d] Req Group[%d]ShardKV[%d] for Put/Append(key:%v, value:%v, index:%v).\n",
			args.ClientId, kv.gid, kv.me, args.Key, args.Value, index)

		notifyCh := kv.createNotifyCh(index)

		// 等待请求执行完的回复并开始超时计时
		select {
		case res := <-notifyCh:
			// 返回前再次检查该key是否仍由该group负责（防止共识和应用期间config发生变化）
			kv.mu.Lock()
			if in := kv.checkKeyInGroup(args.Key); !in {
				reply.Err = ErrWrongGroup
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			reply.Err = res.Err
			DPrintf("Group[%d]ShardKV[%d] respond client[%v] Put/Append(key:%v, value:%v, index:%v) req.\n",
				kv.gid, kv.me, args.ClientId, args.Key, args.Value, index)
		case <-time.After(RespondTimeout * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go kv.closeNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel
	}
}

// 对map进行深拷贝
func deepCopyMap(originMp map[string]string) map[string]string {
	newMp := map[string]string{}
	for k, v := range originMp {
		newMp[k] = v
	}
	return newMp
}

// 对Session map进行深拷贝
func deepCopySession(originMp map[int64]Session) map[int64]Session {
	newMp := map[int64]Session{}
	for k, v := range originMp {
		newMp[k] = v
	}
	return newMp
}

// 执行client请求的指令（Get/Put/Append）
func (kv *ShardKV) executeClientCmd(op Op, commandIndex int, commandTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 如果该group已经不负责这个key的shard，则立即停止对该key请求的服务
	// 让client等待超时向其他server重新请求
	if in := kv.checkKeyInGroup(op.Key); !in {
		return
	}

	if commandIndex <= kv.logLastApplied {
		return
	}

	// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
	// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
	if kv.passiveSnapshotBefore {
		if commandIndex-kv.logLastApplied != 1 {
			return
		}
		kv.passiveSnapshotBefore = false
	}

	// 否则就将logLastApplied更新为较大值applyMsg.CommandIndex
	kv.logLastApplied = commandIndex
	DPrintf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)

	reply := Reply{}
	sessionRec, exist := kv.sessions[op.ClientId]

	// 如果apply的指令之前已经apply过且不是Get指令则不重复执行，直接返回session中保存的结果
	if exist && op.OpType != Get && op.CmdNum <= sessionRec.LastCmdNum { // 若为重复的Get请求可以重复执行
		reply = kv.sessions[op.ClientId].Response // 返回session中记录的回复
		DPrintf("Group[%d]ShardKV[%d] use the reply(=%v) in sessions for %v.\n", kv.gid, kv.me, reply, op.OpType)
	} else { // 没有执行过的指令则实际执行并记录session
		shardNum := key2shard(op.Key) // 获取key对应的shard序号

		// 没有apply过的指令就在状态机上执行，重复的Get指令可以重新执行
		switch op.OpType {
		case Get:
			v, existKey := kv.kvDB[shardNum][op.Key]
			if !existKey { // 键不存在
				reply.Err = ErrNoKey
				reply.Value = "" // key不存在则Get返回空字符串
			} else {
				reply.Err = OK
				reply.Value = v
			}
			DPrintf("Group[%d]ShardKV[%d] apply Get(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, reply.Value)
		case Put:
			kv.kvDB[shardNum][op.Key] = op.Value
			reply.Err = OK
			DPrintf("Group[%d]ShardKV[%d] apply Put(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, op.Value)
		case Append:
			oldValue, existKey := kv.kvDB[shardNum][op.Key] // 若key不存在则取出的为该类型的零值
			if !existKey {
				reply.Err = ErrNoKey
				kv.kvDB[shardNum][op.Key] = op.Value
			} else {
				reply.Err = OK
				kv.kvDB[shardNum][op.Key] = oldValue + op.Value // 追加到原value的后面
			}
			DPrintf("Group[%d]ShardKV[%d] apply Append(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, op.Value)
		default:
			DPrintf("Not Client CMD OpType!\n")
		}

		// 将最近执行的Put和Append的指令执行结果存放到session中以便后续重复请求直接返回而不重复执行
		if op.OpType != Get { // Get请求的回复就不用放到session了
			session := Session{
				LastCmdNum: op.CmdNum,
				OpType:     op.OpType,
				Response:   reply,
			}
			kv.sessions[op.ClientId] = session
			DPrintf("Group[%d]ShardKV[%d].sessions[%d] = %v\n", kv.gid, kv.me, op.ClientId, session)
		}
	}

	// 如果任期和leader身份没有变则向对应client的notifyMapCh发送reply通知对应的handle回复client
	if _, existCh := kv.notifyMapCh[commandIndex]; existCh {
		if currentTerm, isLeader := kv.rf.GetState(); isLeader && commandTerm == currentTerm {
			kv.notifyMapCh[commandIndex] <- reply
		}
	}
}

// 执行组内config变更相关的指令（UpdateConfig/GetShard/GiveShard）
// 这些指令用于保证一个group内的所有servers对config变更过程的一致认知（它们在操作序列的同一位置进行config版本更新、获取和给出shard）
func (kv *ShardKV) executeConfigCmd(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.logLastApplied {
		return
	}

	// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
	// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
	if kv.passiveSnapshotBefore {
		if commandIndex-kv.logLastApplied != 1 {
			return
		}
		kv.passiveSnapshotBefore = false
	}

	// 否则就将logLastApplied更新为较大值applyMsg.CommandIndex
	kv.logLastApplied = commandIndex
	DPrintf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)

	// 实际执行shard迁移相关指令
	switch op.OpType {
	case UpdateConfig:
		// follower收到后更新的下一个config版本，则更新shard状态（暂不进行shard迁移）
		if kv.curConfig.Num+1 == op.CfgNum {
			kv.updateShardState(op.NewConfig)
			DPrintf("Group[%d]ShardKV[%d] apply UpdateConfig(newCfgNum:%v) Op!\n", kv.gid, kv.me, op.CfgNum)
		}
	case GetShard:
		// 将从前任其他组请求到的shard数据应用到自己的kvDB
		// 重复的GetShard指令由第二个条件过滤
		if kv.curConfig.Num == op.CfgNum && kv.ownedShards[op.ShardNum] == WaitGet { // 只有config.Num相等且处于WaitGet状态时才接受该指令
			kvMap := deepCopyMap(op.ShardData)  // 获取该shard的键值对map的深拷贝副本
			kv.kvDB[op.ShardNum] = kvMap        // 将迁移过来的shard数据的副本存在自己的键值服务器中
			kv.ownedShards[op.ShardNum] = Exist // 更新对应的shard状态 WaitGet -> Exist
			// 将shard前任所有者的session中较新的记录应用到自己的session，用于防止shard迁移后重复执行某些指令导致结果错误
			for clientId, session := range op.SessionData {
				if ownSession, exist := kv.sessions[clientId]; !exist || session.LastCmdNum > ownSession.LastCmdNum {
					kv.sessions[clientId] = session
				}
			}
			DPrintf("Group[%d]ShardKV[%d] apply GetShard(cfgNum:%v, shardNum:%v, shardData:%v) Op!\n",
				kv.gid, kv.me, op.CfgNum, op.ShardNum, op.ShardData)
		}
	case GiveShard:
		// 标志shard已经被迁移到新的所有者，本server真正失去该shard
		// 重复的GiveShard指令由第二个条件过滤
		if kv.curConfig.Num == op.CfgNum && kv.ownedShards[op.ShardNum] == WaitGive { // 只有config.Num相等且处于WaitGive状态时才接受该指令
			kv.ownedShards[op.ShardNum] = NoExist      // 确认对方收到shard后更新自己对应的shard状态 WaitGive -> NoExist
			kv.kvDB[op.ShardNum] = map[string]string{} // 清除不再拥有的shard数据
			DPrintf("Group[%d]ShardKV[%d] apply GiveShard(cfgNum:%v, shardNum:%v) Op!\n",
				kv.gid, kv.me, op.CfgNum, op.ShardNum)
		}
	default:
		DPrintf("Not Config Change CMD OpType!\n")
	}

}

// 空指令虽说没有实际效果，但是还是要更新相关变量
func (kv *ShardKV) executeEmptyCmd(commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.logLastApplied {
		return
	}

	// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
	// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
	if kv.passiveSnapshotBefore {
		if commandIndex-kv.logLastApplied != 1 {
			return
		}
		kv.passiveSnapshotBefore = false
	}

	// 否则就将logLastApplied更新为较大值applyMsg.CommandIndex
	kv.logLastApplied = commandIndex
	DPrintf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)
	DPrintf("Group[%d]ShardKV[%d] apply Empty Op!\n", kv.gid, kv.me)
}

// shardkv执行apply的指令
// 并发请求的一致顺序靠raft达成共识的一致日志来保证
// shardkv执行从applyCh取出来的命令或安装快照
func (kv *ShardKV) applyMessage() {
	DPrintf("Group[%d]ShardKV[%d] (re)start apply message.\n", kv.gid, kv.me)
	for !kv.killed() {
		applyMsg := <-kv.applyCh // 不断从applyCh中取applyMsg

		if applyMsg.CommandValid { // 如果取出的是command则执行
			DPrintf("Group[%d]ShardKV[%d] get a Command(index=%v) applyMsg(= %v) from applyCh.\n", kv.gid, kv.me, applyMsg.CommandIndex, applyMsg)
			op, ok := applyMsg.Command.(Op) // 类型断言
			if !ok {                        // 断言失败也不报panic，继续执行后面的
				DPrintf("convert fail!\n")
			} else { // 断言成功
				// client的指令和组内的config变更相关指令分开处理
				if op.OpType == Get || op.OpType == Put || op.OpType == Append {
					kv.executeClientCmd(op, applyMsg.CommandIndex, applyMsg.CommandTerm)
				} else if op.OpType == UpdateConfig || op.OpType == GetShard || op.OpType == GiveShard {
					kv.executeConfigCmd(op, applyMsg.CommandIndex)
				} else if op.OpType == EmptyOp {
					kv.executeEmptyCmd(applyMsg.CommandIndex)
				} else {
					DPrintf("Unexpected OpType!\n")
				}
			}
		} else if applyMsg.SnapshotValid { // 如果取出的是snapshot
			DPrintf("Group[%d]ShardKV[%d] get a Snapshot applyMsg from applyCh.\n", kv.gid, kv.me)

			// 在raft层已经实现了follower是否安装快照的判断
			// 只有followr接受了快照才会通过applyCh通知状态机，因此这里状态机只需要安装快照即可
			kv.mu.Lock()
			kv.applySnapshotToSM(applyMsg.StateMachineState) // 将快照应用到状态机
			kv.logLastApplied = applyMsg.SnapshotIndex       // 更新logLastApplied避免回滚
			kv.passiveSnapshotBefore = true                  // 刚安装完被动快照，提醒下一个从channel中取出的若是指令则注意是否为“跨快照指令”
			kv.mu.Unlock()
			DPrintf("Group[%d]ShardKV[%d] finish a negative Snapshot, kv.logLastApplied become %v.\n", kv.gid, kv.me, kv.logLastApplied)

			kv.rf.SetPassiveSnapshottingFlag(false) // ShardKV已将被动快照安装完成，修改对应raft的passiveSnapshotting标志
		} else { // 错误的ApplyMsg类型
			DPrintf("Group[%d]ShardKV[%d] get an unexpected ApplyMsg!\n", kv.gid, kv.me)
		}
	}
}

// 将快照应用到state machine
func (kv *ShardKV) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 { // 如果传进来的快照为空或无效则不应用
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[int]map[string]string
	var sessions map[int64]Session
	var preConfig shardmaster.Config
	var curConfig shardmaster.Config
	var ownedShards [shardmaster.NShards]ShardState

	if d.Decode(&kvDB) != nil || d.Decode(&sessions) != nil || d.Decode(&preConfig) != nil ||
		d.Decode(&curConfig) != nil || d.Decode(&ownedShards) != nil {
		DPrintf("Group[%d]ShardKV[%d] applySnapshotToSM ERROR!\n", kv.gid, kv.me)
	} else {
		kv.kvDB = kvDB
		kv.sessions = sessions
		kv.preConfig = preConfig
		kv.curConfig = curConfig
		kv.ownedShards = ownedShards
	}
}

// kvserver定期检查是否需要快照（主动进行快照）
func (kv *ShardKV) checkSnapshotNeed() {
	for !kv.killed() {
		var snapshotData []byte
		var snapshotIndex int

		// 如果该server正在进行被动快照（从接受leader发来的快照到通过channel发给kvserver再到kvserver实际安装快照这期间）
		// 这期间该server不检测是否需要主动快照，避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
		if kv.rf.GetPassiveFlagAndSetActiveFlag() {
			DPrintf("Group[%d]ShardKV[%d] is passive snapshotting and refuses positive snapshot.\n", kv.gid, kv.me)
			time.Sleep(time.Millisecond * 50) // 检查间隔50ms
			continue
		}

		// 若该server没有开始被动快照，则可以进行检查是否需要主动快照
		// 主动快照的标志在GetPassiveFlagAndSetActiveFlag()方法中已经设置了，这样做是为了保证检查被动快照标志和设置主动快照标志的原子性
		// 比较 maxraftstate 和 persistent.RaftStateSize，如果RaftStateSize超过maxraftstate则认为日志过大需要进行快照
		nowStateSize := kv.rf.GetRaftStateSize()
		if kv.maxraftstate != -1 && nowStateSize > kv.maxraftstate {
			kv.mu.Lock()
			// 准备进行主动快照
			snapshotIndex = kv.logLastApplied
			DPrintf("Group[%d]ShardKV[%d]: The Raft state size(%v) is approaching the maxraftstate(%v), Start to snapshot(index:%v)...\n",
				kv.gid, kv.me, nowStateSize, kv.maxraftstate, snapshotIndex)
			// 将snapshot 信息编码
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvDB)
			e.Encode(kv.sessions)
			e.Encode(kv.preConfig)
			e.Encode(kv.curConfig)
			e.Encode(kv.ownedShards)
			snapshotData = w.Bytes()
			kv.mu.Unlock()
		}

		if snapshotData != nil {
			kv.rf.Snapshot(snapshotIndex, snapshotData) // ShardKV命令raft进行快照（截止到index）
		}
		kv.rf.SetActiveSnapshottingFlag(false) // 无论检查完需不需要主动快照都要将主动快照标志修改回false

		time.Sleep(time.Millisecond * 50) // 检查间隔50ms
	}

}

// 检查是否准备好进行config变更（只有当该ShardKV没有迁移中间状态的shard时才能更新config）
// 若有shard处于迁移中间状态则代表上一次config变更还没结束（shard迁移还未完成）
func (kv *ShardKV) updateConfigReady() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, state := range kv.ownedShards {
		if state != NoExist && state != Exist {
			return false
		}
	}
	return true
}

// ShardKV定期询问shardmaster以获取更新的配置信息
// 只更新相关的状态标志位（ownedShards），而不实际迁移shard
func (kv *ShardKV) getLatestConfig() {
	for !kv.killed() {
		// 只有leader才进行检查
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}

		// 为了保证“按照顺序一次处理一个重新配置”（配置变更不重叠、不遗漏）
		// 确保上一次config更新完成后才能开始下一次config更新
		if !kv.updateConfigReady() {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}

		kv.mu.Lock()
		curConfig := kv.curConfig
		kv.mu.Unlock()

		nextConfig := kv.mck.Query(curConfig.Num + 1) // 联系shardmaster获取当前config的下一个config（为了确保每个config变更都执行到）

		// 如果询问到的下一个config确实存在，证明有config需要变更
		if nextConfig.Num == curConfig.Num+1 {
			// 通知同一group内的其他servers在同一位置进入config变更过程
			// 因为非leader的servers是不会定期检查config更新的，因此由leader检查后通过raft共识告诉它们该更新config了
			configUpdateOp := Op{
				OpType:    UpdateConfig,
				NewConfig: nextConfig,
				CfgNum:    nextConfig.Num,
			}
			kv.rf.Start(configUpdateOp)
		}

		time.Sleep(time.Millisecond * ConfigCheckInterval) // 每80ms向shardmaster询问一次最新配置
	}
}

// 检查并更新各shard的状态，在apply UpdateConfig指令更新配置版本时调用
// 调用前加锁
func (kv *ShardKV) updateShardState(nextConfig shardmaster.Config) {
	// 检查本组内shard的变更情况
	for shard, gid := range nextConfig.Shards {
		// 如果曾经拥有该shard但新配置里没有了，则将该shard状态设为等待迁移走
		if kv.ownedShards[shard] == Exist && gid != kv.gid {
			kv.ownedShards[shard] = WaitGive
		}

		// 如果之前没有该shard，新配置里又有了，则将该shard状态设为等待从前任所有者迁移shard过来
		if kv.ownedShards[shard] == NoExist && gid == kv.gid {
			if nextConfig.Num == 1 { // 代表刚初始化集群，不需要去其他group获取shard，直接设为Exist
				kv.ownedShards[shard] = Exist
			} else {
				kv.ownedShards[shard] = WaitGet
			}
		}

		// 之前有现在也有，之前没有现在也没有的情况均不需要迁移shard
	}

	DPrintf("Group[%d]ShardKV[%d] update shard state(%v).\n", kv.gid, kv.me, kv.ownedShards)
	// 初始时preConfig和curConfig的Num均为0，此时只用更新curConfig.Num为1
	if kv.preConfig.Num != 0 || kv.curConfig.Num != 0 {
		kv.preConfig = kv.curConfig
	}
	kv.curConfig = nextConfig

}

// 向上一任持有该shard的servers的leader请求shard
// 定期检查该ShardKV是否有shard等待从其他group迁移shard过来
func (kv *ShardKV) checkAndGetShard() {
	for !kv.killed() {
		// 只有leader才进行检查
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}

		var waitGetShards []int // WaitGet状态的shard序号
		kv.mu.Lock()
		for shard, state := range kv.ownedShards {
			if state == WaitGet {
				waitGetShards = append(waitGetShards, shard)
			}
		}
		// 获取前任所有者的信息要根据上一个config
		preConfig := kv.preConfig
		curConfigNum := kv.curConfig.Num
		kv.mu.Unlock()

		// 使用WaitGroup来保证一轮将所有需要的shard都对应请求一遍再开始下次循环
		var wg sync.WaitGroup

		// 对本轮需要请求的所有shard都向对应的前任group请求
		for _, shard := range waitGetShards {
			wg.Add(1)                              // 每开始一个shard的请求就增加等待计数器
			preGid := preConfig.Shards[shard]      // 获取前任shard所有者的gid
			preServers := preConfig.Groups[preGid] // 根据gid找到该group成员的servers name

			// 向上一任shard所有者的组发送拉取shard请求（依次询问每一个成员，直到找到leader）
			go func(servers []string, configNum int, shardNum int) {
				defer wg.Done() // 在向leader成功请求到该shard或问遍该组成员后仍没有获得shard时减少等待计数器
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si]) // 将server name转换成clientEnd
					args := MigrateArgs{
						ConfigNum: configNum,
						ShardNum:  shardNum,
					}
					reply := MigrateReply{}
					ok := srv.Call("ShardKV.MigrateShard", &args, &reply)

					if !ok || (ok && reply.Err == ErrWrongLeader) { // 若联系不上该server或该server不是leader
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for shard[%v] failed or [ErrWrongLeader]!\n", kv.gid, kv.me, servers[si], shardNum)
						continue // 向该组内下一个server再次请求
					}

					if ok && reply.Err == ErrNotReady { // 如果对方的config还没有更新到请求方的版本
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for shard[%v] but [ErrNotReady]!\n", kv.gid, kv.me, servers[si], shardNum)
						break // 等待下次循环再请求该shard
					}

					if ok && reply.Err == OK { // 成功获取到shard数据
						// 生成一个command下放到raft在自己的集群中共识
						// 保证一个group的所有servers在操作序列的同一点完成shard迁移
						getShardOp := Op{
							OpType:      GetShard,
							CfgNum:      configNum,
							ShardNum:    shardNum,
							ShardData:   reply.ShardData,
							SessionData: reply.SessionData,
						}
						kv.rf.Start(getShardOp)
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for shard[%v] [Successfully]!\n", kv.gid, kv.me, servers[si], shardNum)
						break
					}

				}
			}(preServers, curConfigNum, shard)
		}
		wg.Wait() // 阻塞等待所有协程完成

		time.Sleep(time.Millisecond * ConfigCheckInterval)
	}
}

// shard迁移的handler
// 负责其他组向该组请求shard迁移时作相关处理
func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	// 只有leader才能回复包含shard的数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 如果本身的config不及请求者的config新，则得等到自己更新后才能给它迁移shard
	// 出现的情况：kv.curConfig到args.Config之间不涉及当前两个组之间的shard迁移，因此请求方的config可以自增到前面去
	if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 如果本身的config比请求方的config更新，正常情况下不会出现
	// 因为如果请求方请求比较旧的config的shard迁移未完成，被请求方对应的shard状态将会一直是waitGive，这会阻止被请求方的config继续更新
	// 出现的情况：很久之前此shard迁移请求被发出但是由于网络原因到达得太慢，实际上请求方已经在后续的循环中获得了shard数据（否则被请求方的config不会继续更新）
	if kv.curConfig.Num > args.ConfigNum {
		return // 实际上请求方已经获得了数据，并没有等这个reply，直接return即可
	}

	// 只有双方config.Num相等时才将shard数据回复给请求者
	kvMap := deepCopyMap(kv.kvDB[args.ShardNum]) // 获取该shard的键值对map的深拷贝副本
	reply.ShardData = kvMap                      // 将副本放入reply中，防止出现data race

	// 同时把自己的session也发给对方，防止shard迁移后重复执行某些指令导致结果错误
	sessions := deepCopySession(kv.sessions)
	reply.SessionData = sessions
	reply.Err = OK
}

// leader定期检查自己是否有处于WaitGive的shard，如果有，则向它的接收group发送迁移成功确认RPC
// 若收到对方的肯定回复，代表对方已经成功收到迁移过去的shard数据，则自己这边可以让组内所有servers更新状态WaitGive -> NoExist
// 请求shard方成功收到发过去的shard数据后发来的确认收到RPC的handler
func (kv *ShardKV) checkAndFinishGiveShard() {
	for !kv.killed() {
		// 只有leader才进行检查
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}

		var waitGiveShards []int // WaitGive状态的shard序号
		kv.mu.Lock()
		for shard, state := range kv.ownedShards {
			if state == WaitGive {
				waitGiveShards = append(waitGiveShards, shard)
			}
		}
		// 获取shard新所有者的信息要根据当前的config（因为当前处于config变更阶段，config版本是更新了的，只是没有变更没有完成）
		curConfig := kv.curConfig
		kv.mu.Unlock()

		// 使用WaitGroup来保证一轮将所有需要确认收到的shard都确认一遍再开始下次循环
		var wg sync.WaitGroup

		// 对本轮需要确认收到的所有shard都向对应的现任group发送确认RPC
		for _, shard := range waitGiveShards {
			wg.Add(1)
			curGid := curConfig.Shards[shard]      // 获取现任shard所有者的gid（去向）
			curServers := curConfig.Groups[curGid] // 根据gid找到该group成员的servers name

			// 向现任shard所有者的组发送shard确认收到RPC（依次询问每一个成员，直到找到leader）
			go func(servers []string, configNum int, shardNum int) {
				defer wg.Done()
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si]) // 将server name转换成clientEnd
					args := AckArgs{
						ConfigNum: configNum,
						ShardNum:  shardNum,
					}
					reply := AckReply{}
					ok := srv.Call("ShardKV.AckReceiveShard", &args, &reply)

					if !ok || (ok && reply.Err == ErrWrongLeader) { // 若联系不上该server或该server不是leader
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] failed or [ErrWrongLeader]!\n", kv.gid, kv.me, servers[si], shardNum)
						continue // 向该组内下一个server再次请求
					}

					if ok && reply.Err == ErrNotReady { // 如果对方的config还没有更新到请求方的版本
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] but [ErrNotReady]!\n", kv.gid, kv.me, servers[si], shardNum)
						break // 等待下次循环再来询问
					}

					if ok && reply.Err == OK && !reply.Receive { // 如果对方还没有收到或还没应用shard数据
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] but [Has not received shard]!\n", kv.gid, kv.me, servers[si], shardNum)
						break // 等待下次循环再来询问
					}

					if ok && reply.Err == OK && reply.Receive { // 对方成功收到并应用了shard数据
						// 生成一个GiveShard command下放到raft在自己的集群中共识，通知自己的组员可以更新shard状态为NoExist
						// 保证一个group的所有servers在操作序列的同一点完成shard迁移确认
						giveShardOp := Op{
							OpType:   GiveShard,
							CfgNum:   configNum,
							ShardNum: shardNum,
						}
						kv.rf.Start(giveShardOp)
						DPrintf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] [Successfully]!\n", kv.gid, kv.me, servers[si], shardNum)
						break
					}

				}
			}(curServers, curConfig.Num, shard)
		}
		wg.Wait() // 阻塞等待所有协程完成

		time.Sleep(time.Millisecond * ConfigCheckInterval)
	}
}

// shard迁移成功的确认请求的handler
// 负责处理前任group发来的shard确认收到请求
func (kv *ShardKV) AckReceiveShard(args *AckArgs, reply *AckReply) {
	// 只有leader才能回复包含shard的数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 如果本身的config不及请求者的config新，则得等到自己更新后才能给它迁移成功与否的回复
	// 出现的情况：kv.curConfig到args.Config之间不涉及当前两个组之间的shard迁移，因此请求方的config可以自增到前面去
	if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 如果本身的config比请求方的config更新，说明接收方已经在之前的config成功拿到了shard并成功继续更新配置
	// 出现的情况：接收方成功在自己组安装了shard并将它自己的shard状态改为Exist，若没有其他问题就可以继续更新后面的配置
	if kv.curConfig.Num > args.ConfigNum {
		reply.Receive = true
		reply.Err = OK
		return
	}

	// 双方config.Num相等时则根据自己的shard状态回复
	if kv.ownedShards[args.ShardNum] == Exist {
		reply.Receive = true // 回复自己已经收到且应用了shard数据
		reply.Err = OK
		return
	}

	reply.Receive = false // 自己还没有将shard数据共识后应用，让对方下次再来询问
	reply.Err = OK
	return
}

// leader定期检查当前term是否有日志，如没有就加入一条空日志以便之前的term的日志可以提交
func (kv *ShardKV) addEmptyLog() {
	for !kv.killed() {
		// 只有leader才进行检查
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}

		if !kv.rf.CheckCurrentTermLog() { // 返回false代表没有当前term的日志条目
			emptyOp := Op{
				OpType: EmptyOp,
			}
			kv.rf.Start(emptyOp) // 加入一条空日志
		}

		time.Sleep(time.Millisecond * ConfigCheckInterval)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
// 初始化一个ShardKV，本质是group中的一个server
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid // 每个group中的servers是不会变的
	kv.masters = masters

	// Your initialization code here.
	var mutex sync.Mutex
	kv.mu = mutex
	kv.dead = 0 // 0代表该server还存活，1代表它被kill了

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	// 将masters传给shardmaster.MakeClerk()以便向shardmaster发送rpc
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// 用嵌套map来以shard为单位存储键值对，外层map的key为shard的序号，value为该shard内的键值对map
	// 这样方便ShardKV之间迁移shard
	kv.kvDB = make(map[int]map[string]string)
	// 由于外层map的key最多有NShards个且已知（shard序号），因此直接在这里先初始化10个内层的map
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvDB[i] = make(map[string]string)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	// 一个group中ShardKV servers下层构成一个raft集群
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sessions = make(map[int64]Session)
	kv.notifyMapCh = make(map[int]chan Reply) // 这里里面的channel也要初始化，在该client第一次对kvserver发起请求的handler中初始化
	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false

	kv.preConfig = shardmaster.Config{}
	kv.curConfig = shardmaster.Config{}
	var shardsArr [shardmaster.NShards]ShardState
	kv.ownedShards = shardsArr

	// 开启goroutine来apply日志或快照
	go kv.applyMessage()

	// 开启goroutine来定期检查log是否太大需要快照
	go kv.checkSnapshotNeed()

	// 开启goroutine定期轮询shardmaster以获取新配置
	go kv.getLatestConfig()

	// 开启协程来定期检查是否有shard需要从其他group获取
	go kv.checkAndGetShard()

	// 开启协程来定期检查是否有shard迁移走已经完成
	go kv.checkAndFinishGiveShard()

	// 开启协程定期检查raft层当前term是否有日志，如没有就提交一条空日志帮助之前term的日志提交，防止活锁
	go kv.addEmptyLog()

	return kv
}
