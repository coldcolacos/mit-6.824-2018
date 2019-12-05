package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"bytes"
	"labgob"
	"log"
	"time"
	"strconv"
)

func init() {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(ShardMigrateArgs{})
	labgob.Register(ShardMigrateReply{})
	labgob.Register(ShardDeleteArgs{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateData{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type Info struct {
	Term  int
	Value string
	Err   Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	rf           *raft.Raft

	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	mck      *shardmaster.Clerk
	config   shardmaster.Config // store the latest configuration
	shutdown chan struct{}

	db_shards map[int]struct{}
	db_out map[int]map[int]MigrateData // config number -> shard and migration data
	db_in map[int]int // shards -> config num, waiting to migrate from other group
	db_del  map[int]IntSet                // config number -> shards
	db_config  []shardmaster.Config          // store history configs, so that we don't need to query shard master

	data          map[string]string
	cache         map[int64]string // key -> id of request, value -> key of data
	db_info map[int]chan Info

	cfg_timer  *time.Timer // update to latest config
	in_timer  *time.Timer // leader : pull shard from other group
	del_timer *time.Timer // leader : clean shard in other group
}

// string is used in get
func (kv *ShardKV) start(configNum int, args interface{}) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if configNum != kv.config.Num {
		return ErrWrongGroup, ""
	}
	index, term, ok := kv.rf.Start(args)
	if !ok {
		return ErrWrongLeader, ""
	}
	notifyCh := make(chan Info, 1)
	kv.db_info[index] = notifyCh
	kv.mu.Unlock()
	select {
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.db_info, index)
		return ErrWrongLeader, ""
	case result := <-notifyCh:
		kv.mu.Lock()
		if result.Term != term {
			return ErrWrongLeader, ""
		} else {
			return result.Err, result.Value
		}
	}
	return OK, ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.start(args.ConfigNum, args.copy())
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.start(args.ConfigNum, args.copy())
}

// when shards is moved between groups, the replica in target group use this RPC handler to pull data from source group
func (kv *ShardKV) ShardMigrate(args *ShardMigrateArgs, reply *ShardMigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err, reply.Shard, reply.ConfigNum = OK, args.Shard, args.ConfigNum
	if args.ConfigNum >= kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	reply.MigrateData = MigrateData{Data: make(map[string]string), Cache: make(map[int64]string)}
	if v, ok := kv.db_out[args.ConfigNum]; ok {
		if migrationData, ok := v[args.Shard]; ok {
			for k, v := range migrationData.Data {
				reply.MigrateData.Data[k] = v
			}
			for k, v := range migrationData.Cache {
				reply.MigrateData.Cache[k] = v
			}
		}
	}
}

// when target group finish shard migration, the replica uses this RPC handler to clean up useless shard in source group
func (kv *ShardKV) ShardDelete(args *ShardDeleteArgs, reply *ShardDeleteReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.db_out[args.ConfigNum]; ok {
		if _, ok := kv.db_out[args.ConfigNum][args.Shard]; ok {
			kv.mu.Unlock()
			result, _ := kv.start(kv.getConfigNum(), args.copy())
			reply.Err = result
			kv.mu.Lock()
		}
	}
}

func (kv *ShardKV) getConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.shutdown)
}

func (kv *ShardKV) handle(msg raft.ApplyMsg) {
	result := Info{Term: msg.CommandTerm, Value: "", Err: OK}
	if args, ok := msg.Command.(GetArgs); ok {
//log.Printf("%d%d %d apply Get", kv.gid, kv.me, msg.CommandIndex)
		shard := key2shard(args.Key)
		if args.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
//log.Printf("%d%d %d Get (%d %d)", kv.gid, kv.me, msg.CommandIndex, args.ConfigNum, kv.config.Num)
		} else if _, ok := kv.db_shards[shard]; !ok {
//log.Printf("%d%d %d Get has no", kv.gid, kv.me, msg.CommandIndex)
			result.Err = ErrWrongGroup
		} else {
			result.Value = kv.data[args.Key]
		}
	} else if args, ok := msg.Command.(PutAppendArgs); ok {
//log.Printf("%d%d %d apply PA",	kv.gid,	kv.me, msg.CommandIndex)
		shard := key2shard(args.Key)
		if args.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
//log.Printf("%d%d %d PA (%d %d)", kv.gid, kv.me, msg.CommandIndex, args.ConfigNum, kv.config.Num)
		} else if _, ok := kv.db_shards[shard]; !ok {
			result.Err = ErrWrongGroup
//log.Printf("%d%d %d PA has no", kv.gid, kv.me, msg.CommandIndex)
		} else if _, ok := kv.cache[args.RequestId]; !ok {
			if args.Op == "Put" {
				kv.data[args.Key] = args.Value
			} else {
				kv.data[args.Key] += args.Value
			}
			delete(kv.cache, args.ExpireRequestId)
			kv.cache[args.RequestId] = args.Key
		}
	} else if newConfig, ok := msg.Command.(shardmaster.Config); ok {
//log.Printf("%d%d %d apply NewCfg",	kv.gid,	kv.me, msg.CommandIndex)
		kv.applyNewConf(newConfig)
	} else if args, ok := msg.Command.(ShardMigrateReply); ok {
//log.Printf("%d%d %d apply Mig",	kv.gid,	kv.me, msg.CommandIndex)
		if args.ConfigNum == kv.config.Num-1 {
			delete(kv.db_in, args.Shard)
			if _, ok := kv.db_shards[args.Shard]; !ok {
				if _, ok := kv.db_del[args.ConfigNum]; !ok {
					kv.db_del[args.ConfigNum] = make(IntSet)
				}
				kv.db_del[args.ConfigNum][args.Shard] = struct{}{}
				kv.db_shards[args.Shard] = struct{}{}
				for k, v := range args.MigrateData.Data {
					kv.data[k] = v
				}
				for k, v := range args.MigrateData.Cache {
					kv.cache[k] = v
				}
			}
		}
	} else if args, ok := msg.Command.(ShardDeleteArgs); ok {
//log.Printf("%d%d %d apply Cle",	kv.gid,	kv.me, msg.CommandIndex)
		if _, ok := kv.db_out[args.ConfigNum]; ok {
			delete(kv.db_out[args.ConfigNum], args.Shard)
			if len(kv.db_out[args.ConfigNum]) == 0 {
				delete(kv.db_out, args.ConfigNum)
			}
		}
	}

time.Sleep(10 * time.Millisecond)
s := ""
for i := range kv.db_shards {
	s += strconv.Itoa(i) + "-"
}
//log.Printf("%d%d %d (%s)", kv.gid, kv.me, msg.CommandIndex, s)
    if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate * 3 / 2 {
		w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)

        e.Encode(kv.db_shards)
        e.Encode(kv.db_out)
        e.Encode(kv.db_in)
        e.Encode(kv.db_del)
        e.Encode(kv.db_config)
        e.Encode(kv.config)
        e.Encode(kv.cache)
        e.Encode(kv.data)

        kv.rf.RunSnapshot(w.Bytes(), msg.CommandIndex)
	}
	if ch, ok := kv.db_info[msg.CommandIndex]; ok {
        select {
            case <- ch:
            default:
        }
        ch <- result
        delete(kv.db_info, msg.CommandIndex)
	}
}

func (kv *ShardKV) applyNewConf(newConfig shardmaster.Config) {
	if newConfig.Num <= kv.config.Num {
		return
	}
	oldConfig, oldShards := kv.config, kv.db_shards
	kv.db_config = append(kv.db_config, oldConfig.Copy())
	kv.db_shards, kv.config = make(IntSet), newConfig.Copy()
	for shard, newGID := range newConfig.Shards {
		if newGID == kv.gid {
			if _, ok := oldShards[shard]; ok || oldConfig.Num == 0 {
				kv.db_shards[shard] = struct{}{}
				delete(oldShards, shard)
			} else {
				kv.db_in[shard] = oldConfig.Num
			}
		}
	}
	if len(oldShards) != 0 { // prepare data that needed migration
		v := make(map[int]MigrateData)
		for shard := range oldShards {
			data := MigrateData{Data: make(map[string]string), Cache: make(map[int64]string)}
			for k, v := range kv.data {
				if key2shard(k) == shard {
					data.Data[k] = v
					delete(kv.data, k)
				}
			}
			for k, v := range kv.cache {
				if key2shard(v) == shard {
					data.Cache[k] = v
					delete(kv.cache, k)
				}
			}
			v[shard] = data
		}
		kv.db_out[oldConfig.Num] = v
	}
}

func (kv *ShardKV) to_cfg() {
	kv.mu.Lock()
	defer func(){
		//log.Printf("%d%d poll out", kv.gid, kv.me)
		kv.cfg_timer.Reset(250 * time.Millisecond)
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.db_in) != 0 || len(kv.db_del) != 0 {
		kv.mu.Unlock()
		return
	}
	nextConfigNum := kv.config.Num + 1
	kv.mu.Unlock()
	newConfig := kv.mck.Query(nextConfigNum)
	if newConfig.Num == nextConfigNum {
		kv.rf.Start(newConfig)
	}
}

func (kv *ShardKV) to_in() {
	kv.mu.Lock()
	defer func(){
		//log.Printf("%d%d pull out", kv.gid, kv.me)
		kv.in_timer.Reset(150 * time.Millisecond)
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.db_in) == 0 {
		kv.mu.Unlock()
		return
	}

	var wg sync.WaitGroup
	for shard, configNum := range kv.db_in {
		wg.Add(1)
		go func(shard int, config shardmaster.Config) {
			kv.doPull(shard, config)
			wg.Done()
		}(shard, kv.db_config[configNum].Copy())
	}
	kv.mu.Unlock()
	wg.Wait()
}

// pull shard from other group
func (kv *ShardKV) doPull(shard int, oldConfig shardmaster.Config) {
	configNum := oldConfig.Num
	gid := oldConfig.Shards[shard]
	servers := oldConfig.Groups[gid]
	args := ShardMigrateArgs{Shard: shard, ConfigNum: configNum}
	for _, server := range servers {
		srv := kv.make_end(server)
		var reply ShardMigrateReply
		if srv.Call("ShardKV.ShardMigrate", &args, &reply) && reply.Err == OK {
			kv.start(kv.getConfigNum(), reply)
			return
		}
	}
}

// clean shard in other group
func (kv *ShardKV) to_del() {
	kv.mu.Lock()
	defer func(){
		//log.Printf("%d%d clean out", kv.gid, kv.me)
		kv.del_timer.Reset(150 * time.Millisecond)
	}()
	if len(kv.db_del) == 0 {
		kv.mu.Unlock()
		return
	}

	var wg sync.WaitGroup
	for configNum, shards := range kv.db_del {
		config := kv.db_config[configNum].Copy()
		for shard := range shards {
			wg.Add(1)
			go func(shard int, config shardmaster.Config) {
				kv.doClean(shard, config)
				wg.Done()
			}(shard, config)
		}
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) doClean(shard int, config shardmaster.Config) {
	configNum := config.Num
	args := ShardDeleteArgs{Shard: shard, ConfigNum: configNum}
	gid := config.Shards[shard]
	servers := config.Groups[gid]
	for _, server := range servers {
		srv := kv.make_end(server)
		var reply ShardDeleteReply
		if srv.Call("ShardKV.ShardDelete", &args, &reply) && reply.Err == OK {
			kv.mu.Lock()
			delete(kv.db_del[configNum], shard)
			if len(kv.db_del[configNum]) == 0 {
				delete(kv.db_del, configNum)
			}
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *ShardKV) maintain() {
	for {
		select {
		case <-kv.shutdown:
			return
		case <-kv.cfg_timer.C:
			//log.Printf("%d%d poll", kv.gid, kv.me)
			go kv.to_cfg()
		case <-kv.in_timer.C:
			//log.Printf("%d%d pull", kv.gid, kv.me)
			go kv.to_in()
		case <-kv.del_timer.C:
			//log.Printf("%d%d clean", kv.gid, kv.me)
			go kv.to_del()
		}
	}
}

func (kv *ShardKV) apply() {
	//go kv.rf.Replay(1)
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.CommandValid {
				kv.handle(msg)
			} else if msg.Snapshot != nil && len(msg.Snapshot) > 0 {
				r := bytes.NewBuffer(msg.Snapshot)
    			d := labgob.NewDecoder(r)
			    var sst, ssp int
    			d.Decode(&sst)
    			d.Decode(&ssp)

			    var config shardmaster.Config
    			db_shards := make(IntSet)
    			db_out :=  make(map[int]map[int]MigrateData)
    			db_in := make(map[int]int)
    			db_del := make(map[int]IntSet)
    			db_config := make([]shardmaster.Config, 0)
    			cache := make(map[int64]string)
    			data := make(map[string]string)

			    if d.Decode(&db_shards) != nil ||
        			d.Decode(&db_out) != nil ||
        			d.Decode(&db_in) != nil ||
        			d.Decode(&db_del) != nil ||
        			d.Decode(&db_config) != nil ||
        			d.Decode(&config) != nil ||
        			d.Decode(&cache) != nil ||
        			d.Decode(&data) != nil {
        			log.Fatal("Error in reading snapshot")
    			}
    			kv.config = config
    			kv.db_shards = db_shards
    			kv.db_out = db_out
    			kv.db_in = db_in
    			kv.db_del = db_del
    			kv.db_config = db_config
    			kv.cache = cache
    			kv.data = data
			}
			kv.mu.Unlock()
		case <-kv.shutdown:
			return
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = shardmaster.Config{}
	kv.shutdown = make(chan struct{})

	kv.db_shards = make(IntSet)
	kv.db_out = make(map[int]map[int]MigrateData)
	kv.db_in = make(map[int]int)
	kv.db_del = make(map[int]IntSet)
	kv.db_config = make([]shardmaster.Config, 0)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.db_info = make(map[int]chan Info)

	kv.cfg_timer = time.NewTimer(time.Duration(0))
	kv.in_timer = time.NewTimer(time.Duration(0))
	kv.del_timer = time.NewTimer(time.Duration(0))
	go kv.apply()
	go kv.maintain()
	go func(){
		for{
			if kv.rf.Newlead(){kv.rf.Start("")}
			time.Sleep(100*time.Millisecond)
		}
	}()
	return kv
}
