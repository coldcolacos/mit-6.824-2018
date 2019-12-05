package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "bytes"
import "math"
//import "log"

type Op struct {
	Client_id int64
	Op_id int
	Opr string

	// join
	Servers map[int] []string
	// leave
	GIDs []int
	// move
	Shard int
	GID int
	// query
	Num int
}

type SMReply struct {
    Client_id int64
    Op_id int
    Config Config
    Err Err
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	maxraftstate int
	db_op map[int64] int
	db_reply map[int] chan SMReply

	quit chan bool
}

func (sm *ShardMaster) has(client_id int64, op_id int) bool {
    val, ok := sm.db_op[client_id]
    return ok && val >= op_id
}

func (sm *ShardMaster) get(G map[int] int) (int, int) {
	gid := math.MinInt64
	val := math.MaxInt64
	for k, v := range G {
		if v < val {
			gid, val = k, v
		}else if v == val && k < gid {
			gid, val = k, v
		}
	}
	return gid, val
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if _, lead1 := sm.rf.GetState(); lead1 {
        op := Op{
            Client_id: args.Client_id,
            Op_id: args.Op_id,
            Opr: "J",
        }
		op.Servers = make(map[int] []string)
		for k, v := range args.Servers {
			op.Servers[k] = v
		}

        raft_index, _, leader := sm.rf.Start(op)
        if !leader {
            reply.WrongLeader = true
            return
        }
        sm.mu.Lock()
        ch, ok := sm.db_reply[raft_index]
        if !ok {
            ch = make(chan SMReply, 1)
            sm.db_reply[raft_index] = ch
        }
        sm.mu.Unlock()

		select {
            case smreply := <- ch:
                if smreply.Client_id == args.Client_id && smreply.Op_id == args.Op_id {
                    reply.WrongLeader = false
                    reply.Err = smreply.Err
                    //log.Printf("C%d (%d, %d, Get)", kv.me, args.Client_id, args.Op_id)
                    return
                }
            case <- time.After(1000 * time.Millisecond):
        }
    }
    reply.WrongLeader = true
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {

	if _, lead1 := sm.rf.GetState(); lead1 {
        op := Op{
            Client_id: args.Client_id,
            Op_id: args.Op_id,
            Opr: "L",
        }
		op.GIDs = args.GIDs

        raft_index, _, leader := sm.rf.Start(op)
        if !leader {
            reply.WrongLeader = true
            return
        }
        sm.mu.Lock()
        ch, ok := sm.db_reply[raft_index]
        if !ok {
            ch = make(chan SMReply, 1)
            sm.db_reply[raft_index] = ch
        }
        sm.mu.Unlock()

		select {
            case smreply := <- ch:
                if smreply.Client_id == args.Client_id && smreply.Op_id == args.Op_id {
                    reply.WrongLeader = false
                    reply.Err = smreply.Err
                    //log.Printf("C%d (%d, %d, Get)", kv.me, args.Client_id, args.Op_id)
                    return
                }
            case <- time.After(1000 * time.Millisecond):
        }
    }
    reply.WrongLeader = true
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if _, lead1 := sm.rf.GetState(); lead1 {
        op := Op{
            Client_id: args.Client_id,
            Op_id: args.Op_id,
            Opr: "M",
        }
		op.Shard = args.Shard
		op.GID = args.GID

        raft_index, _, leader := sm.rf.Start(op)
        if !leader {
            reply.WrongLeader = true
            return
        }
        sm.mu.Lock()
        ch, ok := sm.db_reply[raft_index]
        if !ok {
            ch = make(chan SMReply, 1)
            sm.db_reply[raft_index] = ch
        }
        sm.mu.Unlock()

		select {
            case smreply := <- ch:
                if smreply.Client_id == args.Client_id && smreply.Op_id == args.Op_id {
                    reply.WrongLeader = false
                    reply.Err = smreply.Err
                    //log.Printf("C%d (%d, %d, Get)", kv.me, args.Client_id, args.Op_id)
                    return
                }
            case <- time.After(1000 * time.Millisecond):
        }
    }
    reply.WrongLeader = true
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if _, lead1 := sm.rf.GetState(); lead1 {
        op := Op{
            Client_id: args.Client_id,
            Op_id: args.Op_id,
            Opr: "Q",
        }
		op.Num = args.Num

        raft_index, _, leader := sm.rf.Start(op)
        if !leader {
            reply.WrongLeader = true
            return
        }
        sm.mu.Lock()
        ch, ok := sm.db_reply[raft_index]
        if !ok {
            ch = make(chan SMReply, 1)
            sm.db_reply[raft_index] = ch
        }
        sm.mu.Unlock()

		select {
            case smreply := <- ch:
                if smreply.Client_id == args.Client_id && smreply.Op_id == args.Op_id {
                    reply.WrongLeader = false
                    reply.Config = smreply.Config
                    reply.Err = smreply.Err
                    //log.Printf("C%d (%d, %d, Get)", kv.me, args.Client_id, args.Op_id)
                    return
                }
            case <- time.After(1000 * time.Millisecond):
        }
    }
    reply.WrongLeader = true
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// sm.quit <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) apply() {

	for ; true; {

        var apply_msg raft.ApplyMsg
        select{
            case apply_msg = <- sm.applyCh:
            case <- sm.quit:
                return
        }

        if !apply_msg.CommandValid{
            var snap_term, snap_index int
            r := bytes.NewBuffer(apply_msg.Snapshot)
            d := labgob.NewDecoder(r)
            d.Decode(&snap_term)
            d.Decode(&snap_index)
            sm.mu.Lock()
			sm.configs = make([]Config, 0)
            sm.db_op = make(map[int64]int)
			d.Decode(&sm.configs)
            d.Decode(&sm.db_op)
            sm.mu.Unlock()
            continue
        }

		sm.mu.Lock()
		op := apply_msg.Command.(Op) // cast type 'interface{}' into 'Op'
        reply := SMReply{
            Client_id: op.Client_id,
            Op_id: op.Op_id,
        }
		//log.Printf("S%d %d %d", sm.me, op.Client_id, op.Op_id)

		if !sm.has(op.Client_id, op.Op_id) {
			if op.Opr == "J" {
				c := Config{}
				c.Groups = make(map[int] []string)
				tail := len(sm.configs) - 1
				for k, v := range sm.configs[tail].Groups {
					c.Groups[k] = v
				}
				for k, v := range op.Servers {
					c.Groups[k] = v
				}
				c.Num = sm.configs[tail].Num + 1
				c.Shards = sm.configs[tail].Shards
				mean := NShards / len(c.Groups)
				M := make(map[int]int)
				for k, v := range c.Shards {
					if v != 0 {
						val, ok := M[v]
						if ok {
							if val < mean {
								M[v] = val + 1
							}else{
								c.Shards[k] = 0
							}
						}else{
							M[v] = 1
						}
					}
				}
				for k := range c.Groups {
					if _, ok := M[k]; !ok {
						M[k] = 0
					}
				}
				for k, v := range c.Shards {
					if v == 0 {
						gid, val := sm.get(M)
						M[gid] = val + 1
						c.Shards[k] = gid
					}
				}
				sm.configs = append(sm.configs, c)
			}else if op.Opr == "L" {
				c := Config{}
				c.Groups = make(map[int] []string)
				del := make(map[int]bool)
				for _, v := range op.GIDs {
					del[v] = true
				}
				tail := len(sm.configs) - 1
				for k, v := range sm.configs[tail].Groups {
					if _, ok := del[k]; !ok {
						c.Groups[k] = v
					}
				}
				c.Num = sm.configs[tail].Num + 1
				c.Shards = sm.configs[tail].Shards
				M := make(map[int]int)
				for k := range c.Groups {
					if _, ok := M[k]; !ok {
						M[k] = 0
					}
				}
				for k, v := range c.Shards {
					if val, ok := M[v]; ok {
						M[v] = val + 1
					}else{
						c.Shards[k] = 0
					}
				}
				for k, v := range c.Shards {
					if v == 0 {
						gid, val := sm.get(M)
						M[gid] = val + 1
						c.Shards[k] = gid
					}
				}
				sm.configs = append(sm.configs, c)
			}else if op.Opr == "M" {
				c := Config{}
				c.Groups = make(map[int] []string)
				tail := len(sm.configs) - 1
				for k, v := range sm.configs[tail].Groups {
					c.Groups[k] = v
				}
				c.Shards = sm.configs[tail].Shards
				c.Shards[op.Shard] = op.GID
				c.Num = sm.configs[tail].Num + 1
				sm.configs = append(sm.configs, c)
			}/*else if op.Opr == "Q" {
				if op.Num < 0 || op.Num >= len(sm.configs) {
					reply.Config = sm.configs[len(sm.configs) - 1]
				}else{
					reply.Config = sm.configs[op.Num]
				}
			}*/
			sm.db_op[op.Client_id] = op.Op_id
		}
		if op.Opr == "Q" {
			if op.Num < 0 || op.Num >= len(sm.configs) {
				reply.Config = sm.configs[len(sm.configs) - 1]
			}else{
				reply.Config = sm.configs[op.Num]
			}
		}

		ch, yes := sm.db_reply[apply_msg.CommandIndex]
        if yes {
            select{
                case <- ch:
                default:
            }
            ch <- reply
            //delete(sm.db_reply, apply_msg.CommandIndex)
        }

        size := sm.rf.RaftStateSize()
        if sm.maxraftstate > 0 && sm.maxraftstate/2 < size {
			//log.Printf("S%d snapshot %d %d", sm.me, op.Client_id, op.Op_id)
            w := new(bytes.Buffer)
            e := labgob.NewEncoder(w)
			e.Encode(sm.configs)
            e.Encode(sm.db_op)
            go sm.rf.RunSnapshot(w.Bytes(), apply_msg.CommandIndex)
        }

        sm.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
    sm.maxraftstate = -1
    sm.rf.SetMaxSize(sm.maxraftstate)

    sm.db_op = make(map[int64]int)
    sm.db_reply = make(map[int]chan SMReply)
    sm.quit = make(chan bool)

    go sm.apply()
	go func(){
		for{
			if sm.rf.Newlead(){sm.rf.Start(Op{})}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return sm
}
