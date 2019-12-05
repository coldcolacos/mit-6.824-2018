package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Client_id int64
	Op_id int
	Opr string
	Key string
	Value string
}

type KVReply struct {
	Client_id int64
	Op_id int
	Value string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	/*** lab3 part A ***/
	db_kv map[string]string
	db_op map[int64]int
	db_reply map[int]chan KVReply

	quit chan bool
}

func (kv *KVServer) has(client_id int64, op_id int) bool {
	val, ok := kv.db_op[client_id]
	return ok && val >= op_id
}

func (kv *KVServer) apply () {

	for ; true; {

		var apply_msg raft.ApplyMsg
		select{
			case apply_msg = <- kv.applyCh:
			case <- kv.quit:
				return
		}

		if !apply_msg.CommandValid{
			var snap_term, snap_index int
			r := bytes.NewBuffer(apply_msg.Snapshot)
			d := labgob.NewDecoder(r)
			d.Decode(&snap_term)
			d.Decode(&snap_index)
			kv.mu.Lock()
			kv.db_kv = make(map[string]string)
			kv.db_op = make(map[int64]int)
			d.Decode(&kv.db_kv)
			d.Decode(&kv.db_op)
			kv.mu.Unlock()
			continue
		}

		kv.mu.Lock()
		op := apply_msg.Command.(Op) // cast type 'interface{}' into 'Op'
		reply := KVReply{
			Client_id: op.Client_id,
			Op_id: op.Op_id,
		}

		if !kv.has(op.Client_id, op.Op_id) {
			if op.Opr == "A" {
				val, ok := kv.db_kv[op.Key]
				if !ok {
					val = ""
				}
				kv.db_kv[op.Key] = val + op.Value
			}else if op.Opr == "P" {
				kv.db_kv[op.Key] = op.Value
			}
			kv.db_op[op.Client_id] = op.Op_id
		}
		val, ok := kv.db_kv[op.Key]
		if ok {
			reply.Err, reply.Value = OK, val
		}else {
			reply.Err, reply.Value = ErrNoKey, ""
		}

		//l := len(reply.Value) - 14
		//if l <0{
		//	l=0
		//}
		//log.Printf("	S%d (%d,%s,%s,%s) %d,%d", kv.me, apply_msg.CommandIndex, op.Opr, op.Key, reply.Value[l:], op.Client_id, op.Op_id)

		ch, yes := kv.db_reply[apply_msg.CommandIndex]
		if yes {
			select{
				case <- ch:
				default:
			}
			ch <- reply
			delete(kv.db_reply, apply_msg.CommandIndex)
		}

		size := kv.rf.RaftStateSize()
		if kv.maxraftstate > 0 && kv.maxraftstate/2 < size {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db_kv)
			e.Encode(kv.db_op)
			go kv.rf.RunSnapshot(w.Bytes(), apply_msg.CommandIndex)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	/*** lab3 part A ***/
	if _, lead1 := kv.rf.GetState(); lead1 {
		op := Op{
			Client_id: args.Client_id,
			Op_id: args.Op_id,
			Opr: "G",
			Key: args.Key,
		}
		raft_index, _, leader := kv.rf.Start(op)
		if !leader {
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		ch, ok := kv.db_reply[raft_index]
		if !ok {
			ch = make(chan KVReply, 1)
			kv.db_reply[raft_index] = ch
		}
		kv.mu.Unlock()

		select {
			case kvreply := <- ch:
				if kvreply.Client_id == args.Client_id && kvreply.Op_id == args.Op_id {
					reply.WrongLeader = false
					reply.Value = kvreply.Value
					reply.Err = kvreply.Err
					//log.Printf("C%d (%d, %d, Get)", kv.me, args.Client_id, args.Op_id)
					return
				}
			case <- time.After(1000 * time.Millisecond):
		}
	}
	reply.WrongLeader = true
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	/*** lab3 part A ***/
	if _, lead1 := kv.rf.GetState(); lead1 {
		op := Op{
			Client_id: args.Client_id,
			Op_id: args.Op_id,
			Opr: args.Op[:1], // "P"ut or "A"ppend
			Key: args.Key,
			Value: args.Value,
		}

		raft_index, _, leader := kv.rf.Start(op)
		if !leader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		ch, ok := kv.db_reply[raft_index]
		if !ok {
			ch = make(chan KVReply, 1)
			kv.db_reply[raft_index] = ch
		}
		kv.mu.Unlock()
		select{
			case kvreply := <- ch:
				if kvreply.Client_id == args.Client_id && kvreply.Op_id == args.Op_id {
					reply.WrongLeader = false
					reply.Err = kvreply.Err
					//log.Printf("C%d (%d, %d, Append)", kv.me, args.Client_id, args.Op_id)
					return
				}
			case <- time.After(1000 * time.Millisecond):
		}
	}
	reply.WrongLeader = true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.quit <- true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetMaxSize(maxraftstate)

	kv.db_kv = make(map[string]string)
	kv.db_op = make(map[int64]int)
	kv.db_reply = make(map[int]chan KVReply)
	kv.quit = make(chan bool)

	go kv.apply()

	return kv
}
