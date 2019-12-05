package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
//import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	/*** lab3 part A ***/
	client_id int64
	leader_id int
	op_id int
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
	// You'll have to add code here.

	/*** lab3 part A ***/
	for ck.client_id = 0; ck.client_id == 0; {
		ck.client_id = nrand()
	}
	ck.leader_id = 0
	ck.op_id = -1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	/*** lab3 part A ***/
	ck.op_id ++
	args := GetArgs{
		Key: key,
		Client_id: ck.client_id,
		Op_id: ck.op_id,
	}
	var reply GetReply
	
	ok := false
	ck.leader_id --
	//time.Sleep(time.Duration(10 + nrand() % 50))

	for !ok || reply.WrongLeader {
		ck.leader_id ++
		if ck.leader_id >= len(ck.servers) {
			ck.leader_id -= len(ck.servers)
		}
		reply = GetReply{}
		ok = ck.servers[ck.leader_id].Call("KVServer.Get", &args, &reply)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	/*** lab3 part A ***/
	ck.op_id ++
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Client_id: ck.client_id,
		Op_id: ck.op_id,
	}
	var reply PutAppendReply

	// fmt.Printf("clerk %s [%s] : [%s]\n", args.Op, args.Key, args.Value)

    ok := false
    ck.leader_id --
	//time.Sleep(time.Duration(10 + nrand() % 50))

    for !ok || reply.WrongLeader {
		ck.leader_id ++
        if ck.leader_id >= len(ck.servers) {
            ck.leader_id -= len(ck.servers)
        }
		reply = PutAppendReply{}
        ok = ck.servers[ck.leader_id].Call("KVServer.PutAppend", &args, &reply)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
