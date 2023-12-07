package kvraft

// Err类型
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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
	Err Err // error的字符串
	//LeaderId int // 如果找错了leader则server将自己记录的leaderId附在reply里供client下一次直接定位
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
	//LeaderId int // 如果找错了leader则server将自己记录的leaderId附在reply里供client下一次直接定位
}
