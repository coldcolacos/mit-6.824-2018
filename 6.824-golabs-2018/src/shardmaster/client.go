package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd

	client_id int64
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

	for ck.client_id = 0; ck.client_id == 0; {
		ck.client_id = nrand()
	}
	ck.op_id = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Client_id = ck.client_id
    ck.mu.Lock()
    ck.op_id ++
    args.Op_id = ck.op_id
    ck.mu.Unlock()
	args.Num = num

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
    args.Client_id = ck.client_id
	ck.mu.Lock()
	ck.op_id ++
    args.Op_id = ck.op_id
	ck.mu.Unlock()
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}

    args.Client_id = ck.client_id
	ck.mu.Lock()
    ck.op_id ++
    args.Op_id = ck.op_id
    ck.mu.Unlock()
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}

    args.Client_id = ck.client_id
	ck.mu.Lock()
    ck.op_id ++
    args.Op_id = ck.op_id
    ck.mu.Unlock()
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
