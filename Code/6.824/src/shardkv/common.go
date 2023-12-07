package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdNum   int // client为每个command分配的唯一序列号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CmdNum   int // client为每个command分配的唯一序列号
}

type GetReply struct {
	Err   Err
	Value string
}

// leader向shard的前任所有者请求shard数据使用的RPC参数
type MigrateArgs struct {
	ConfigNum int // 该ShardKV是在哪个config中想请求此shard的
	ShardNum  int // 要请求的shard序号
}

type MigrateReply struct {
	ShardData   map[string]string // 该shard的键值对数据
	SessionData map[int64]Session // 自己的session数据也发给对方
	Err         Err
}

// leader收到对方发来的shard数据后回复对方已收到使用的RPC参数
type AckArgs struct {
	ConfigNum int // 自己所处的config序号
	ShardNum  int // 已收到的shard序号
}

type AckReply struct {
	Receive bool
	Err     Err
}
