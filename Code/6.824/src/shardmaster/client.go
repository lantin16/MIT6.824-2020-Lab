package shardmaster

//
// Shardmaster clerk.
//

import "6.824/src/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd // server集群的ClientEnd
	// Your data here.
	clientId int64 // client的唯一标识符
	seqNum   int   // 标志client为每个请求分配的序列号到哪了（采用自增，从1开始）
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand() // clientId通过提供的nrand()随机生成，重复概率忽略不计
	ck.seqNum = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	DPrintf("Client[%v] request Query(seqNum = %v, config num = %v)\n",
		ck.clientId, ck.seqNum, num)

	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.seqNum++
				DPrintf("Client[%v] receive Query response and Config=%v. (seqNum -> %v)\n",
					ck.clientId, reply.Config, ck.seqNum)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 添加新的replica group
// 入参： new GID -> servers mappings
func (ck *Clerk) Join(servers map[int][]string) {
	DPrintf("Client[%v] request Join(seqNum = %v, join servers = %v)\n",
		ck.clientId, ck.seqNum, servers)

	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK { // 请求成功
				ck.seqNum++
				DPrintf("Client[%v] receive Join response. (seqNum -> %v)\n",
					ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	DPrintf("Client[%v] request Leave(seqNum = %v, leave gids = %v)\n",
		ck.clientId, ck.seqNum, gids)

	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK { // 请求成功
				ck.seqNum++
				DPrintf("Client[%v] receive Leave response. (seqNum -> %v)\n",
					ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	DPrintf("Client[%v] request Move(seqNum = %v, move shard = %v, moveTO gid = %v)\n",
		ck.clientId, ck.seqNum, shard, gid)

	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK { // 请求成功
				ck.seqNum++
				DPrintf("Client[%v] receive Move response. (seqNum -> %v)\n",
					ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
