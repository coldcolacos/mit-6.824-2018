package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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

	/*** lab3 part A ***/
	Client_id int64
	Op_id int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	/*** lab4 part A ***/
	Client_id int64
	Op_id int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
