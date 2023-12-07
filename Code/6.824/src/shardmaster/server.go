package shardmaster

import (
	"6.824/src/raft"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/src/labrpc"
import "sync"
import "6.824/src/labgob"

const Debug = 0 // 设为1则将打印信息输出到终端

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// OpType类型
const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

const RespondTimeout = 500 // shardmaster回复client的超时时间，单位：毫秒

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	sessions    map[int64]Session  // 记录shardmaster为每个client处理过的上一次请求的结果
	configs     []Config           // indexed by config num
	notifyMapCh map[int]chan Reply // shardmaster执行完请求操作后通过chan通知对应的handler方法回复client，key为日志的index
}

// session跟踪为client处理的上一次请求的序列号，以及相关的响应。
// 如果shardmaster接收到序列号已经执行过的请求，它会立即响应，而不需要重新执行请求
type Session struct {
	LastSeqNum int    // shardmaster为该client处理的上一次请求的序列号
	OpType     string // 上一次处理的操作请求的类型
	Response   Reply  // 对应的响应
}

// 响应的统一结构，用于shardmaster在session中保存最新请求的响应
type Reply struct {
	Err    Err
	Config Config // 仅Query请求时有效
}

// 统一的操作结构体，依靠OpType区分具体操作
// 其中部分字段仅在特定操作中有效
type Op struct {
	// Your data here.
	ClientId int64            // 标识客户端
	SeqNum   int              // 请求的序号
	OpType   string           // 操作类型，Join/Leave/Move/Query
	Servers  map[int][]string // new GID -> servers mappings，// 仅在Join操作有效
	GIDs     []int            // 仅在Leave操作有效
	Shard    int              // 仅在Move操作有效
	GID      int              // 仅在Move操作有效
	CfgNum   int              // desired config number，仅在Query操作有效
}

// Join handler
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()

	// shardmaster过滤重复请求
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		// 标志这个请求client已经收到正确的回复了，只是这个重发请求到达shardmaster太慢了
		// 其实client端并没有在等待这个回复了，直接return
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		// 将session中记录的之前执行该请求的结果直接返回，使得不会执行一个请求多次
		// 可能是由于之前的回复在网络中丢失或延迟导致重复请求
		reply.Err = sm.sessions[args.ClientId].Response.Err
		sm.mu.Unlock()
		return
	} else { // 如果是未处理的新请求
		sm.mu.Unlock() // 先解锁
		joinOp := Op{
			ClientId: args.ClientId,
			SeqNum:   args.SeqNum,
			OpType:   Join,
			Servers:  args.Servers, // 新增的组中包含的servers
		}

		index, _, isLeader := sm.rf.Start(joinOp) // 调用rf.Start()，将client的请求操作传给raft进行共识

		if !isLeader { // 若client联系的不是leader
			reply.WrongLeader = true
			return
		}

		notifyCh := sm.createNotifyCh(index)

		// 等待请求执行完的回复并开始超时计时
		select {
		case res := <-notifyCh:
			reply.Err = res.Err
		case <-time.After(RespondTimeout * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go sm.closeNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel
	}
}

// Leave handler
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()

	// shardmaster过滤重复请求
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = sm.sessions[args.ClientId].Response.Err
		sm.mu.Unlock()
		return
	} else { // 如果是未处理的新请求
		sm.mu.Unlock() // 先解锁
		leaveOp := Op{
			ClientId: args.ClientId,
			SeqNum:   args.SeqNum,
			OpType:   Leave,
			GIDs:     args.GIDs, // 要离开的组的GID列表
		}

		index, _, isLeader := sm.rf.Start(leaveOp)

		if !isLeader {
			reply.WrongLeader = true
			return
		}

		notifyCh := sm.createNotifyCh(index)

		// 等待请求执行完的回复并开始超时计时
		select {
		case res := <-notifyCh:
			reply.Err = res.Err
		case <-time.After(RespondTimeout * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go sm.closeNotifyCh(index)
	}
}

// Move handler
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()

	// shardmaster过滤重复请求
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = sm.sessions[args.ClientId].Response.Err
		sm.mu.Unlock()
		return
	} else { // 如果是未处理的新请求
		sm.mu.Unlock() // 先解锁
		moveOp := Op{
			ClientId: args.ClientId,
			SeqNum:   args.SeqNum,
			OpType:   Move,
			Shard:    args.Shard, // 要移动的分片序号
			GID:      args.GID,   // 要移动到的组的GID
		}

		index, _, isLeader := sm.rf.Start(moveOp)

		if !isLeader {
			reply.WrongLeader = true
			return
		}

		notifyCh := sm.createNotifyCh(index)

		// 等待请求执行完的回复并开始超时计时
		select {
		case res := <-notifyCh:
			reply.Err = res.Err
		case <-time.After(RespondTimeout * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go sm.closeNotifyCh(index)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 由于Query操作仅查询不会改变配置信息，因此类似于kvraft中的Get请求，重复的Query请求不进行过滤
	queryOp := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum, // 本次请求的序号
		OpType:   Query,
		CfgNum:   args.Num, // 想要查询的Config的序号
	}

	index, _, isLeader := sm.rf.Start(queryOp) // 调用rf.Start()，将client的请求操作传给raft进行共识

	if !isLeader { // 若client联系的不是leader
		reply.WrongLeader = true
		return
	}

	notifyCh := sm.createNotifyCh(index)

	// 等待请求执行完的回复并开始超时计时
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Config = res.Config // Query请求要返回查询到的Config
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go sm.closeNotifyCh(index) // 关闭并删除此临时channel，下个请求到来时重新初始化一个channel
}

// 为leader在sm.notifyMapCh中初始化一个缓冲为1的channel完成本次与applyConfigChange()的通信
func (sm *ShardMaster) createNotifyCh(index int) chan Reply {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	notifyCh := make(chan Reply, 1)
	sm.notifyMapCh[index] = notifyCh
	return notifyCh
}

// ShardMaster回复client后关闭对应index的notifyCh
func (sm *ShardMaster) closeNotifyCh(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.notifyMapCh[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(sm.notifyMapCh[index])
		delete(sm.notifyMapCh, index)
	}
}

// 获取最新的Config
// 调用它的位置需要先持有sm.mu
func (sm *ShardMaster) getLastConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

// ShardMaster从applyCh中取出client的config变更请求并实际执行对应操作
func (sm *ShardMaster) applyConfigChange() {
	for !sm.killed() {
		applyMsg := <-sm.applyCh // 不断从applyCh中取applyMsg

		if applyMsg.CommandValid {
			sm.mu.Lock()

			op, ok := applyMsg.Command.(Op) // 类型断言
			if !ok {                        // 断言失败也不报panic，继续执行后面的
				DPrintf("convert fail!\n")
			} else { // 断言成功，实际执行指令
				reply := Reply{}
				sessionRec, exist := sm.sessions[op.ClientId]

				// 如果apply的指令之前已经apply过且不是Query指令则不重复执行，直接返回session中保存的结果
				if exist && op.OpType != Query && op.SeqNum <= sessionRec.LastSeqNum { // 若为重复的Get请求可以重复执行
					reply = sm.sessions[op.ClientId].Response // 返回session中记录的回复
				} else { // 没有执行过的指令则实际执行并记录session
					switch op.OpType {
					case Join:
						reply.Err = sm.executeJoin(op)
						DPrintf("ShardMaster[%v] Join(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
							sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
					case Leave:
						reply.Err = sm.executeLeave(op)
						DPrintf("ShardMaster[%v] Leave(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
							sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
					case Move:
						reply.Err = sm.executeMove(op)
						DPrintf("ShardMaster[%v] Move(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
							sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
					case Query:
						reply.Err, reply.Config = sm.executeQuery(op)
						DPrintf("ShardMaster[%v] Query(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
							sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
					default:
						DPrintf("Unexpected OpType!\n")
					}
					// 将最近执行的除Query指令外的指令执行结果存放到session中以便后续重复请求直接返回而不重复执行
					if op.OpType != Query { // Query请求的回复就不用放到session了
						session := Session{
							LastSeqNum: op.SeqNum,
							OpType:     op.OpType,
							Response:   reply,
						}
						sm.sessions[op.ClientId] = session
						DPrintf("ShardMaster[%d].sessions[%d] = %v\n", sm.me, op.ClientId, session)
					}
				}

				// 如果任期和leader身份没有变则向对应client的notifyMapCh发送reply通知对应的handle回复client
				if _, existCh := sm.notifyMapCh[applyMsg.CommandIndex]; existCh {
					if currentTerm, isLeader := sm.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
						sm.notifyMapCh[applyMsg.CommandIndex] <- reply
					}
				}
			}

			sm.mu.Unlock()
		} else {
			DPrintf("ShardMaster[%d] get an unexpected ApplyMsg!\n", sm.me)
		}
	}
}

// 对配置中的Groups进行深拷贝
func deepCopyGroups(originG map[int][]string) map[int][]string {
	newGroups := map[int][]string{}
	for gid, servers := range originG {
		// 创建新的servers切片，复制原始切片中的元素到新切片中
		copiedServers := make([]string, len(servers))
		copy(copiedServers, servers)

		// 将复制后的切片放入新的 map 中
		newGroups[gid] = copiedServers
	}
	return newGroups
}

// shard负载均衡
// shard尽可能均匀地分到各group，且移动尽可能少
// 平均下来每个group最后的shard数量最多相差1
// 最后每个group负责的shard的数量为avgShardNum或avgShardNum+1均可
func shardLoadBalance(groups map[int][]string, lastShards [NShards]int) [NShards]int {
	resShards := lastShards // Go 语言中，数组是值类型，因此可以通过简单的赋值操作进行深拷贝
	groupNum := len(groups)
	shardCnt := make(map[int]int, groupNum) // gid -> 负责的shard数量

	// 统计未迁移shard情况下新groups中各group负责的shard数量
	for shard, gid := range lastShards {
		if _, exist := groups[gid]; exist { // 若新配置中仍有这个组
			shardCnt[gid]++
		} else { // 该组已经离开了groups（Leave情况）
			resShards[shard] = 0 // 将该位置的shard释放（即所属gid置为0）
		}
	}

	// 新groups中gid组成的slice
	gidSlice := make([]int, 0, groupNum) // 初始化为容量10的切片
	for gid, _ := range groups {
		gidSlice = append(gidSlice, gid)
		if _, exist := shardCnt[gid]; !exist { // 本次新增的组（Join情况）
			shardCnt[gid] = 0 // 暂时还没分配shard
		}
	}

	// 最后有remainder个组负责(avgShardNum+1)个shard，剩下组负责avgShardNum个shard
	avgShardNum := NShards / groupNum
	remainder := NShards % groupNum

	// 对gidSlice进行排序，规则是：负载大的group排在前面，若负载相同则gid小的排在前面
	// 这样能保证最后得到的负载均衡后的shard分配情况唯一
	for i := 0; i < len(gidSlice)-1; i++ {
		for j := len(gidSlice) - 1; j > i; j-- {
			if shardCnt[gidSlice[j]] > shardCnt[gidSlice[j-1]] ||
				(shardCnt[gidSlice[j]] == shardCnt[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j-1], gidSlice[j] = gidSlice[j], gidSlice[j-1]
			}
		}
	}

	// 由于已经按照shard数从大到小排列好了，因此gidSlice的前remainder个gid就是最后要负责(avgShardNum+1)个shard的
	for i := 0; i < groupNum; i++ {
		var curTar int // 该组最终应负责的shard数
		if i < remainder {
			curTar = avgShardNum + 1
		} else {
			curTar = avgShardNum
		}
		curGid := gidSlice[i]
		delta := shardCnt[curGid] - curTar // 该gid的组要变化的量
		if delta == 0 {                    // 该组已经为最终shard数，不需要变化
			continue
		}

		// 将超载的组先释放掉超出的shard
		if delta > 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 { // 释放了delta个shard就退出
					break
				}
				if resShards[j] == curGid {
					resShards[j] = 0
					delta--
				}
			}
		}
	}

	// 将resShards中待分配的shard分配给不足的group
	for i := 0; i < groupNum; i++ {
		var curTar int // 该组最终应负责的shard数
		if i < remainder {
			curTar = avgShardNum + 1
		} else {
			curTar = avgShardNum
		}
		curGid := gidSlice[i]
		delta := shardCnt[curGid] - curTar // 该gid的组要变化的量
		if delta == 0 {                    // 该组已经为最终shard数，不需要变化
			continue
		}

		// 将待分配的shard迁移到shard不足的组
		if delta < 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if resShards[j] == 0 {
					resShards[j] = curGid
					delta++
				}
			}
		}
	}

	return resShards
}

// 实际执行Join Group操作
func (sm *ShardMaster) executeJoin(op Op) Err {
	lastConfig := sm.getLastConfig()
	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1

	// 先对原来的Groups map进行深拷贝
	newGroups := deepCopyGroups(lastConfig.Groups)

	// 向原配置的Groups中增加新Groups
	// Join可能在配置中已有的组中加入servers，也可能新增整个组
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}

	newConfig.Groups = newGroups

	// shard负载均衡
	newConfig.Shards = shardLoadBalance(newGroups, lastConfig.Shards)

	sm.configs = append(sm.configs, newConfig) // 追加新config
	return OK
}

// 实际执行Leave Group操作
func (sm *ShardMaster) executeLeave(op Op) Err {
	lastConfig := sm.getLastConfig()
	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1

	// 先对原来的Groups map进行深拷贝
	newGroups := deepCopyGroups(lastConfig.Groups)

	// 从原配置的Groups中去除掉指定的Groups
	for _, gid := range op.GIDs {
		delete(newGroups, gid) // 对不存在的键进行删除也是安全的
	}

	newConfig.Groups = newGroups

	var newShards [10]int

	// 如果Leave后一个组都没有，则Shards数组为全0
	// 此种情况单独处理，否则在shardLoadBalance中出现除零报错
	if len(newGroups) != 0 {
		// shard负载均衡
		newShards = shardLoadBalance(newGroups, lastConfig.Shards)
	}
	newConfig.Shards = newShards

	sm.configs = append(sm.configs, newConfig) // 追加新config
	return OK
}

// 实际执行Move shard操作
func (sm *ShardMaster) executeMove(op Op) Err {
	lastConfig := sm.getLastConfig()
	newConfig := Config{}
	newConfig.Num = lastConfig.Num + 1

	// Move操作Groups无变化
	newConfig.Groups = deepCopyGroups(lastConfig.Groups)

	// 强制分配某shard给某个group，不需要负载均衡
	newShards := lastConfig.Shards
	newShards[op.Shard] = op.GID // 直接将该shard的gid修改为对应值
	newConfig.Shards = newShards

	sm.configs = append(sm.configs, newConfig) // 追加新config
	return OK
}

// 实际执行Query Group操作
func (sm *ShardMaster) executeQuery(op Op) (Err, Config) {
	lastConfig := sm.getLastConfig()
	// 如果查询的配置号为-1或大于已知的最大配置号，则shardmaster应回复最新配置
	if op.CfgNum == -1 || op.CfgNum > lastConfig.Num {
		return OK, lastConfig
	}

	// 由于configs[0]占位config的存在，切片下标和config.Num具有对应关系
	return OK, sm.configs[op.CfgNum]
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.dead = 0 // 0代表该server还存活，1代表它被kill了

	sm.configs = make([]Config, 1)
	// 按照实验要求，第一个config的编号应为0，不包含任何组，且所有shard都被分配GID零
	// 在没有对切片内每个元素结构体进行初始化的情况下，直接访问某元素的成员变量得到该类型的零值
	sm.configs[0].Groups = map[int][]string{} // 声明的同时初始化为一个空map

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	var mutex sync.Mutex
	sm.mu = mutex
	sm.sessions = map[int64]Session{}
	sm.notifyMapCh = make(map[int]chan Reply) // 里面的channel使用前记得初始化

	go sm.applyConfigChange()
	return sm
}
