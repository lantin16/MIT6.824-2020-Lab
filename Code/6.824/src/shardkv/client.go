package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/src/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/src/shardmaster"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk             // 通过它向shardmaster发起请求
	config   shardmaster.Config             // client所知的配置
	make_end func(string) *labrpc.ClientEnd // 将server name转换成clientEnd的函数
	// You will have to modify this struct.
	clientId   int64 // client的唯一标识符
	commandNum int   // 标志client为每个command分配的序列号到哪了（采用自增，从1开始）

}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters) // 关联shardmaster
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1) // 向shardmaster询问最新的配置
	ck.clientId = nrand()
	ck.commandNum = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	for {
		shard := key2shard(key)        // 获取key对应的分片
		gid := ck.config.Shards[shard] // 获取该shard所在的分组（在client的认知下）
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ { // 向该组内的servers发起请求
				srv := ck.make_end(servers[si]) // 将server name转换成clientEnd
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) { // 请求成功
					ck.commandNum++
					DPrintf("Client[%d] get the [Get] response [key:%v, value:%v].\n", ck.clientId, key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) { // 需要更新配置
					break
				}
				// ... not ok, or ErrWrongLeader, or ErrTimeout
				// 向该组内其他server再次发起请求
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdNum:   ck.commandNum,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.commandNum++
					DPrintf("Client[%d] get the [PutAppend] response [key:%v, value:%v, type:%v].\n", ck.clientId, key, value, op)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader, or ErrTimeout
				// 向该组内其他server再次发起请求
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
