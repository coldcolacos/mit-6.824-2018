package raft

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

type ROLE string
const(
	LEADER ROLE = "L"
	FOLLOWER ROLE = "F"
	CANDIDATE ROLE = "C"
)

type Entry struct {
	TermId  int
	Content  interface{}
}

type ApplyMsg struct {

	/*** lab2 B ***/
	CommandTerm int
	CommandIndex int
	Command interface{}

	/*** lab3 B ***/
	CommandValid bool
	Snapshot []byte
}

type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]

	/*** lab2 A ***/
	term_id int
	role ROLE
	vote_for int
	timeout *time.Timer

	/*** lab2 B ***/
	logs []Entry
	next_ptr []int
	match_ptr []int
	commit_ptr int
	apply_ptr int
	apply_msg chan ApplyMsg

	apply_remind chan bool

	/*** lab3 B ***/
	SSP int
	maxsize int
	newlead chan bool

	quit chan bool
	killed bool
}

/***
	lab2 A : request vote
***/

func _rand(x, y int) time.Duration {
	z := rand.Intn(y - x)
	return time.Duration(x + z) * time.Millisecond
}
func (rf *Raft) lead_clock() time.Duration {
	t := _rand(100, 101)
	rf.timeout.Reset(t)
	return t
}
func (rf *Raft) follow_clock() time.Duration {
	t := _rand(300, 600)
	rf.timeout.Reset(t)
	return t
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term, lead := rf.term_id, rf.role == LEADER
	rf.mu.Unlock()
	return term, lead
}

type RequestVoteArgs struct {

    /*** lab2 A ***/
    TermId int
    Who int
    LastIndex int
    LastTerm int
}
type RequestVoteReply struct {

    /*** lab2 A ***/
    TermId int
    Who int
    VoteOrNot bool
    OK bool
}

func (rf *Raft) RequestVoteComp(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.OK, reply.Who = true, rf.me
	if rf.term_id == args.TermId && rf.vote_for == args.Who {
		reply.TermId, reply.VoteOrNot = rf.term_id, true
		return
	}
	if rf.term_id > args.TermId ||
		(rf.term_id == args.TermId && rf.vote_for != -1) {
		reply.TermId, reply.VoteOrNot = rf.term_id, false
		return
	}
	if args.TermId > rf.term_id {
		rf.term_id, rf.vote_for = args.TermId, -1
		if rf.role != FOLLOWER {
			rf.role = FOLLOWER
			rf.follow_clock()
		}
	}
	reply.TermId = args.TermId
	if rf.logs[len(rf.logs) - 1].TermId > args.LastTerm ||
		(rf.logs[len(rf.logs) - 1].TermId == args.LastTerm && rf.LL() - 1 > args.LastIndex) {
		reply.VoteOrNot = false
		return
	}
	if rf.vote_for == -1{
		reply.VoteOrNot = true
		//log.Printf("<%d@%d %d@%d>", rf.me, rf.term_id, args.Who, args.TermId)
		rf.vote_for = args.Who
	}else{
		reply.VoteOrNot = false
	}
	rf.follow_clock()
	rf.persist()
}

func (rf *Raft) RequestVoteCallback() {
	rf.mu.Lock()
	if rf.role == LEADER {
		rf.mu.Unlock()
		return
	}
	rf.role = CANDIDATE
	rf.term_id ++
	rf.vote_for = rf.me
	rf.persist()

	args := RequestVoteArgs{
		TermId: rf.term_id,
		Who: rf.me,
		LastIndex: rf.LL() - 1,
		LastTerm: rf.logs[len(rf.logs) - 1].TermId,
	}
	t := rf.follow_clock()
	timer := time.After(t)
	term := rf.term_id
	rf.mu.Unlock()

	vote_ch := make(chan RequestVoteReply, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go func(server int, args RequestVoteArgs, vote_ch chan RequestVoteReply){
                var reply RequestVoteReply
                if !rf.peers[server].Call("Raft.RequestVoteComp", &args, &reply) {
                    reply.OK, reply.Who = false, server
                }
                vote_ch <- reply
            }(i, args, vote_ch)
        }
    }

	votes, N := 1, len(rf.peers)/2+1
	for votes < N {
        select {
        case <- rf.quit:
            rf.quit <- true
            return
        case <-timer:
            return
        case reply := <- vote_ch:
            if !reply.OK {
				go func(server int, args RequestVoteArgs, vote_ch chan RequestVoteReply){
                	var reply RequestVoteReply
                	if !rf.peers[server].Call("Raft.RequestVoteComp", &args, &reply) {
                    	reply.OK, reply.Who = false, server
                	}
                	vote_ch <- reply
            	}(reply.Who, args, vote_ch)
            } else if reply.VoteOrNot {
                votes ++
            } else {
                rf.mu.Lock()
                if rf.term_id < reply.TermId {
                    rf.term_id = reply.TermId
                    rf.role = FOLLOWER
                    rf.vote_for = -1
                    rf.persist()
                    rf.follow_clock()
                }
                rf.mu.Unlock()
            }
        }
    }

	rf.mu.Lock()
    if rf.role == CANDIDATE && rf.term_id == term {
        rf.role = LEADER
        N := len(rf.peers)
        rf.next_ptr = make([]int, N)
        rf.match_ptr = make([]int, N)
        for i := 0; i < N; i++ {
            rf.next_ptr[i] = rf.LL()
        }
        //log.Printf("#################################################################")
        //log.Printf("%d lead now!", rf.me)
		select{
			case <- rf.newlead:
			default:
		}
		rf.newlead <- true
        rf.lead_clock()
    }
    rf.mu.Unlock()
}

func (rf *Raft) Newlead() bool {
	select{
		case <- rf.newlead:
			return true
		default:
			return false
	}
	return false
}

/***
    lab2 B : append entry
***/

type AppendEntryArgs struct {
	TermId int
	Who int
	PrevTerm int
	PrevIndex int
	CommitIndex int
	Entries []Entry
}
type AppendEntryReply struct {
	TermId int
	OK bool
	ConflictIndex int
}

func (rf *Raft)remind(){
	select{
		case <-rf.apply_remind:
		default:
	}
	rf.apply_remind <- true
}

func (rf *Raft) AppendEntryCallback(i int) {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	if rf.next_ptr[i] <= rf.SSP {
		args := SnapshotArgs{
			TermId: rf.term_id,
			LeaderId: rf.me,
			SST: rf.logs[0].TermId,
			SSP: rf.SSP,
			Snapshot: rf.persister.ReadSnapshot(),
		}
		go func(peer_id int, args *SnapshotArgs){
			var reply SnapshotReply
			rf.SnapshotSend(peer_id, args, &reply)
		}(i, &args)
		rf.mu.Unlock()
		return
	}

	args := AppendEntryArgs{
		TermId: rf.term_id,
		Who: rf.me,
		PrevIndex: rf.next_ptr[i] - 1,
		PrevTerm: rf.logs[rf.next_ptr[i] - rf.SSP - 1].TermId,
		CommitIndex: rf.commit_ptr,
	}
	if rf.next_ptr[i] < rf.LL() {
		entries := rf.logs[rf.next_ptr[i] - rf.SSP : ]
		args.Entries = entries
	}
	rf.mu.Unlock()

	var reply AppendEntryReply
	if rf.peers[i].Call("Raft.AppendEntryComp", &args, &reply) {
		rf.mu.Lock()
		if !reply.OK {
			if reply.TermId > rf.term_id {
                rf.term_id = reply.TermId
                rf.role = FOLLOWER
                rf.vote_for = -1
                rf.persist()
                rf.follow_clock()
			} else {
				rf.next_ptr[i] = reply.ConflictIndex
				if rf.next_ptr[i] > rf.LL(){
					rf.next_ptr[i] = rf.LL()
				}
				if rf.next_ptr[i] < 1 {
					rf.next_ptr[i] = 1
				}
				if rf.next_ptr[i] <= rf.SSP {
        			args := SnapshotArgs{
            			TermId: rf.term_id,
            			LeaderId: rf.me,
            			SST: rf.logs[0].TermId,
            			SSP: rf.SSP,
            			Snapshot: rf.persister.ReadSnapshot(),
        			}
        			go func(peer_id int, args *SnapshotArgs){
            			var reply SnapshotReply
            			rf.SnapshotSend(peer_id, args, &reply)
        			}(i, &args)
    			}
			}
		} else {
			x, L := args.PrevIndex, len(args.Entries)
			if x + L >= rf.next_ptr[i] {
				rf.next_ptr[i] = x + L + 1
				rf.match_ptr[i] = x + L
			}

			if x + L < rf.LL() && rf.commit_ptr < x + L && rf.logs[x + L - rf.SSP].TermId == rf.term_id {
				n := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.match_ptr[j] >= x + L {
						n ++
					}
				}
				if n >= len(rf.peers)/2+1 {
					rf.commit_ptr = x + L
					rf.persist()
					rf.remind()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntryComp(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer func(){
		rf.persist()
		rf.mu.Unlock()
	}()

	if rf.term_id > args.TermId {
		reply.TermId, reply.OK = rf.term_id, false
		return
	}

	reply.TermId = args.TermId
	rf.follow_clock()
	if args.TermId > rf.term_id {
		rf.term_id, rf.vote_for = args.TermId, -1
	}
	rf.role = FOLLOWER
	if args.PrevIndex < rf.SSP {
		reply.OK, reply.ConflictIndex = false, rf.SSP+1
		return
	}
	if rf.LL() <= args.PrevIndex || rf.logs[args.PrevIndex - rf.SSP].TermId != args.PrevTerm {
		x := args.PrevIndex
		if x >= rf.LL() {
			x = rf.LL() - 1
		}
		for ; x > rf.commit_ptr && rf.logs[x - 1 - rf.SSP].TermId == rf.logs[x - rf.SSP].TermId; x-- {
		}
		reply.OK, reply.ConflictIndex = false, x
		return
	}

	reply.OK, reply.ConflictIndex = true, -1
	save := append([]Entry{}, rf.logs ...)
	i := 0
	L := len(args.Entries)
	for ; i < L; i++ {
		if args.PrevIndex+1+i >= rf.LL() {
			break
		}
		if rf.logs[args.PrevIndex + 1 + i - rf.SSP].TermId != args.Entries[i].TermId {
			rf.logs = rf.logs[:args.PrevIndex + 1 + i - rf.SSP]
			break
		}
	}

	for ; i < L; i++ {
		rf.logs = append(rf.logs, args.Entries[i])
	}
	if rf.maxsize > 0 && len(rf.encode())*2 > rf.maxsize*3 {
		rf.logs = append([]Entry{}, save ...)
		reply.OK, reply.ConflictIndex = false, 1
		return
	}
	if rf.commit_ptr < args.CommitIndex {
		rf.commit_ptr = args.CommitIndex
		rf.remind()
	}
	rf.follow_clock()
}

/***
    lab2 C : persist
***/

func (rf *Raft) persist () {
	E := rf.encode()
	rf.persister.SaveRaftState(E)
}

func (rf *Raft) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term_id)
	e.Encode(rf.vote_for)
	e.Encode(rf.SSP)
	e.Encode(rf.logs)
	e.Encode(rf.commit_ptr)
	return w.Bytes()
}

func (rf *Raft) readPersistState() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term_id, vote_for, SSP, commit_ptr int
	if d.Decode(&term_id) != nil ||
		d.Decode(&vote_for) != nil ||
		d.Decode(&SSP) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&commit_ptr) != nil {
		log.Fatal("Error in unmarshal raft state")
	}
	rf.term_id = term_id
	rf.vote_for = vote_for
	rf.SSP = SSP
	rf.commit_ptr = commit_ptr
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

/***
    lab3 B : snapshot
***/

func (rf *Raft) LL() int {
	return rf.SSP + len(rf.logs)
}
func (rf *Raft) SetMaxSize(x int){
	rf.maxsize = x
}

func (rf *Raft) RunSnapshot(snapshot []byte, x int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if x <= rf.SSP || x >= rf.LL() {
		return
	}
	rf.logs = rf.logs[x - rf.SSP : ]
	rf.SSP = x
	//log.Printf("%d snapshot at %d", rf.me, x)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs[0].TermId)
	e.Encode(x)

	// (state, snapshot) is atomic !
	rf.persister.SaveStateAndSnapshot(rf.encode(), append(w.Bytes(), snapshot...))
}

func (rf *Raft) UseSnapshot(snapshot []byte){
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var sst, ssp int
	d.Decode(&sst)
	d.Decode(&ssp)

	rf.mu.Lock()
	//log.Printf("%d use snapshot at %d,%d SSP=%d", rf.me, sst, ssp, rf.SSP)
	// ignore cmds before last snapshot
	if rf.apply_ptr < ssp {
		rf.apply_ptr = ssp
	}
	if rf.commit_ptr < ssp {
		rf.commit_ptr = ssp
	}
	rf.trim_logs(sst, ssp)
	rf.mu.Unlock()
	// recover KVServer from snapshot
	rf.apply_msg <- ApplyMsg{
		CommandValid: false,
		Snapshot: snapshot,
	}
}

func (rf *Raft) trim_logs(sst int, ssp int) {
	if ssp < rf.SSP {
		return
	}

	j := -1
	for i := len(rf.logs) - 1; i >= 0; i -- {
		if i + rf.SSP == ssp && rf.logs[i].TermId == sst {
			j = i
			break
		}
	}

	head := Entry{
		TermId: sst,
	}
	if j < 0 {
		rf.logs = []Entry{head}
	}else{
		rf.logs = append([]Entry{head}, rf.logs[j + 1 : ]...)
	}
	rf.SSP = ssp
}

type SnapshotArgs struct{
	TermId int
	LeaderId int
	SST int
	SSP int
	Snapshot []byte
}

type SnapshotReply struct{
	TermId int
}

func (rf *Raft) SnapshotComp(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer func(){
		rf.mu.Unlock()
	}()
	reply.TermId = rf.term_id
	if args.SSP <= rf.commit_ptr{
		return
	}
	if args.TermId < rf.term_id {
		return
	}

	rf.follow_clock()
	if args.TermId > rf.term_id {
		rf.term_id = args.TermId
		rf.role = FOLLOWER
		rf.vote_for = args.LeaderId
	}else if rf.vote_for!=-1 && rf.vote_for!=args.LeaderId {
		//return
	}

	rf.trim_logs(args.SST, args.SSP)
	rf.apply_ptr = args.SSP
	rf.commit_ptr = args.SSP
	rf.persister.SaveStateAndSnapshot(rf.encode(), args.Snapshot)
	rf.apply_msg <- ApplyMsg{
		CommandValid: false,
		Snapshot: args.Snapshot,
	}
}

func (rf *Raft) SnapshotSend(server int, args *SnapshotArgs, reply *SnapshotReply) {
	ok := rf.peers[server].Call("Raft.SnapshotComp", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term_id < reply.TermId {
		rf.term_id = reply.TermId
		rf.role = FOLLOWER
		rf.vote_for = -1
		rf.follow_clock()
		rf.persist()
		return
	}
	if rf.role != LEADER || rf.term_id != args.TermId {
		return
	}
	rf.match_ptr[server] = args.SSP
	rf.next_ptr[server] = args.SSP + 1
}

/***
    Start(), Kill(), Make()
***/

type Op struct {
    Client_id int64
    Op_id int
    Opr string
    Key string
    Value string
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.role != LEADER || rf.killed {
		rf.mu.Unlock()
		return -1, -1, false
	}

	defer func(){
		for i, _ := range rf.peers {
        	if i != rf.me {
            	go rf.AppendEntryCallback(i)
        	}
    	}
		rf.mu.Unlock()
	}()

	if rf.maxsize > 0 && rf.RaftStateSize() > rf.maxsize && rf.logs[len(rf.logs) - 1].TermId == rf.term_id {
		//log.Printf("%d reject", rf.me)
		return -1, -1, false
	}

	//log.Printf("R%d,%d (%d,%d,%s,%s,%s)", rf.me, rf.term_id, cc.Client_id, cc.Op_id, cc.Opr, cc.Key, cc.Value)

	index, term := rf.LL(), rf.term_id
	rf.match_ptr[rf.me] = rf.LL()
	rf.logs = append(rf.logs, Entry{TermId:rf.term_id, Content:command, })
	rf.persist()

	return index, term, true
}

func (rf *Raft) Kill() {

	// change to FOLLOWER to reject 'start(cmd)'
	rf.mu.Lock()
	rf.killed = true
	rf.role = FOLLOWER
	rf.mu.Unlock()

	// goroutine quit only when [return] or [main() exit]
	// use channel to quit goroutine
	rf.quit <- true
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.term_id = 0
	rf.role = FOLLOWER
	rf.vote_for = -1
	rf.timeout = time.NewTimer(_rand(0, 300))

	rf.logs = []Entry{Entry{TermId: 0}}
	rf.commit_ptr = 0
	rf.apply_ptr = 0
	rf.apply_remind = make(chan bool, 1)
	rf.apply_msg = applyCh

	rf.SSP = 0
	rf.newlead = make(chan bool, 1)

	rf.quit = make(chan bool, 1)
	rf.killed = false

	rf.readPersistState()
	rf.UseSnapshot(persister.ReadSnapshot())
	rf.remind()

	go func() {
		for {
        	select {
        		case <- rf.quit:
            		rf.quit <- true
            		return
        		case <- rf.apply_remind:
            		rf.mu.Lock()
            		var x []Entry
            		var y []int
					var z []int
            		l := 0
            		for i := rf.apply_ptr+1; i <= rf.commit_ptr; i ++{
						if i <= rf.SSP || i - rf.SSP >= len(rf.logs) {
							continue
						}
                		x = append(x, rf.logs[i - rf.SSP])
                		y = append(y, i)
						z = append(z, rf.logs[i - rf.SSP].TermId)
                		l ++
            		}
		            rf.apply_ptr = rf.commit_ptr
        		    rf.persist()
            		rf.mu.Unlock()
            		for i := 0; i < l; i ++ {
                		rf.apply_msg <- ApplyMsg{
                    		CommandValid: true,
							CommandTerm: z[i],
                    		CommandIndex: y[i],
                    		Command: x[i].Content,
                		}
            		}
        	}
    	}
	}()
	go func() {
		for {
			select {
				case <-rf.timeout.C:
					if _, lead := rf.GetState(); lead {
						for i, _ := range rf.peers {
        					if i != rf.me {
            					go rf.AppendEntryCallback(i)
        					}
    					}
						rf.lead_clock()
					}else{
						rf.RequestVoteCallback()
					}
				case <-rf.quit:
					rf.quit <- true
					return
			}
		}
	}()
	return rf
}

