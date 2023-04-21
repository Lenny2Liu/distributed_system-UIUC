package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	//"fmt"
	//"fmt"
	//"fmt"
	
	"math/rand"

	//"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"github.com/sasha-s/go-deadlock"
	// "golang.org/x/text/message"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role      int // 0: follower, 1: candidate, 2: leader
	curTerm   int //current term
	votedFor  int //candidateId that received vote from this server in current term, -1 is null
	voteCount int //vote count for current term

	elcTimeout int64 //election timeout, random in [500, 1000], as heartbeat frequence is 10Hz
	// heartbeatTimeout 	int					//heartbeat timeout 100ms fixed
	heartbeatTimer *time.Timer //heartbeat timer
	elcTimer       *time.Timer //election timer
	log            []LogEntry  //log entries kept in current server
	msgch          chan ApplyMsg
	commitedIdx    int   //index of highest log entry known to be committed
	lastApplied    int   //index of highest log entry applied to state machine
	nextIdx        []int //index of the next log entry to send to that server
	matchIdx       []int //index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.role == 2
	term = rf.curTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Currentterm int
	Success     bool
	AgreeIdx    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.curTerm || (args.Term == rf.curTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.curTerm
		rf.elcTimeout = rand.Int63()%300 + 300
		//rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
		return
	}
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = -1
		rf.role = 0 //become follower
	}
	//check more up-to-date: rules:
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//If the logs end with the same term, then whichever log is longer is more up-to-date.
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.CurrentTerm = rf.curTerm
		rf.elcTimeout = rand.Int63()%300 + 300
		rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
	} else {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.curTerm
	}
	//fmt.Println("server ", rf.me, " receive vote request from ", args.CandidateId, " vote granted: ", reply.VoteGranted)
	return
}

func (rf *Raft) initiateElection() {
	rf.mu.Lock()
	if rf.role == 2 {
		rf.mu.Unlock()
		return
	}
	//rf.role = 1
	//fmt.Println("server ", rf.me, " initiate election")
	rf.curTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	args := RequestVoteArgs{
		Term:         rf.curTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		//use go routine to broadcast vote request as it's necessary to initiate a election again without blocking the timer
		//fmt.Println("server ", rf.me, " request vote request to ", peer)
		go func(peer int) {
			
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				if reply.CurrentTerm > rf.curTerm {
					rf.curTerm = reply.CurrentTerm
					rf.votedFor = -1
					rf.role = 0
					rf.elcTimeout = rand.Int63()%300 + 300
					rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2 {
						rf.role = 2
						rf.elcTimer.Stop()
						//fmt.Println("server ", rf.me, " become leader")
						rf.mu.Unlock()
						//fmt.Println("server ", rf.me, " become leader")
						time.Sleep(time.Duration(20) * time.Millisecond)
						for i := range rf.peers {
							rf.nextIdx[i] = len(rf.log)
							// fmt.Println("new leader log length", len(rf.log), rf.log)
							if i == rf.me {
								rf.matchIdx[i] = len(rf.log) - 1
							}
						}
						rf.sendHeartbeat(true)
						rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
					} else {
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
			}
		}(peer)
	}
}

//rf.mu.Unlock()

func (rf *Raft) sendHeartbeat(heartbeat bool) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		Entries:      nil, 
		PrevLogTerm:  rf.log[len(rf.log) - 1].Term, //no log test for 2A, set to -1
		LeaderCommit: rf.commitedIdx,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	//fmt.Println("leader", rf.log)
	for peer := range rf.peers {
		time.Sleep(time.Duration(15) * time.Millisecond)
		if peer == rf.me {
			continue
		}
		//use go routine to broadcast vote request as it's necessary to initiate a election again without blocking the timer
		//go broadcastHeartbeat(peer, rf, &args, &reply)
		//this part is only used to send a heartbeat to all peers, don't care what they reply(2A)
		//now we do care about the reply(2B, modified Apr/4/23)
		//fmt.Println("server ", rf.me, " send heartbeat to ", peer)
		// rf.mu.Unlock()
		go func(peer int) {
			time.Sleep(time.Duration(100) * time.Millisecond)
			if rf.nextIdx[peer] < len(rf.log){ //peer missing some entries
				// fmt.Println("next index", rf.nextIdx[peer], "log length", len(rf.log), peer, rf.me)
				args.Entries = rf.log[rf.nextIdx[peer]:]			//from 
				args.PrevLogIndex = rf.nextIdx[peer] - 1   			//last one we made agreement on
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				
			}
			if rf.sendAppendEntries(peer, &args, &reply){
				rf.mu.Lock()
				if reply.Currentterm > rf.curTerm {
					rf.curTerm = reply.Currentterm
					rf.votedFor = -1
					rf.role = 0
					// reset election timer
					rf.elcTimeout = rand.Int63()%300 + 300
					rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
					rf.mu.Unlock()
					return
				}
				if rf.role != 2 || rf.curTerm != args.Term{	
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					if reply.AgreeIdx > rf.nextIdx[peer] {
						rf.nextIdx[peer] = reply.AgreeIdx
						rf.matchIdx[peer] = reply.AgreeIdx - 1
						// fmt.Println("matching index : ", rf.matchIdx, rf.me)
					}
					if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.curTerm && rf.role == 2{
						rf.updateCommit()
					}
					rf.mu.Unlock()
					return
				}
				// fmt.Println("reply :", reply)
				if reply.AgreeIdx > 0 {
					rf.nextIdx[peer] = reply.AgreeIdx
				}
				rf.mu.Unlock()
				return
			}
		}(peer)
	}
	
}
// no lock for this function
func (rf *Raft) updateCommit() {
	commit := false
	// fmt.Println(len(rf.log))
	// var commited int
	for i := rf.commitedIdx + 1; i <= len(rf.log); i++ {
		NumComitPeer := 0
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			//fmt.Println("matchIdx : ", rf.matchIdx[peer], "Peer : ", peer)
			if rf.matchIdx[peer] >= i {			//number of peers that have log entry at index i
				NumComitPeer ++
				if NumComitPeer + 1 > len(rf.peers)/2 {
					//fmt.Println("commit : ", i)
					commit = true
					rf.commitedIdx = i
					break
				}
			} 
		}
		if  rf.commitedIdx != i {
			break
		}
	}
	if commit {
		// time.Sleep(time.Duration(20) * time.Millisecond)
		newMsg := ApplyMsg{}
		rf.msgch <- newMsg
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("server ", rf.me, " receive heartbeat from ", args.LeaderId)
	if args.Term < rf.curTerm {
		reply.Success = false
		reply.Currentterm = rf.curTerm
		return
	}
	
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = -1
	}
	//become a follower as recieving heartbeat
	rf.role = 0
	rf.elcTimeout = rand.Int63()%300 + 300
	rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
	if args.PrevLogIndex > len(rf.log) - 1 {									//no enough log entries
		reply.Success = false
		reply.Currentterm = rf.curTerm
		reply.AgreeIdx = len(rf.log)
		return
	} else if args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {				// has enough log entries and term matches
		reply.Success = true
		reply.Currentterm = rf.curTerm
		rf.log = append(rf.log[0:args.PrevLogIndex + 1], args.Entries...)
		reply.AgreeIdx = len(rf.log)
		// fmt.Println("follower", rf.log)
	}else{																//has enough log entries but term doesn't match, trace back to the first mismatched entry	
		reply.Success = false
		idx := args.PrevLogIndex
		for idx > rf.commitedIdx && rf.log[idx].Term == rf.log[args.PrevLogIndex].Term {
			idx -= 1
		}
		reply.Currentterm = rf.curTerm
		reply.AgreeIdx = idx + 1
	}
	if reply.Success {
		if args.LeaderCommit > rf.commitedIdx {
			rf.commitedIdx = args.LeaderCommit
			rf.msgch <- ApplyMsg{}
		}
	}
}
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// send an AppendEntry RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()

	// Your code here (2B).
	// this part should be done
	if rf.role != 2{
		rf.mu.Unlock()
		return index, term, false
	}
	NewLog := LogEntry{rf.curTerm, command}
	rf.log = append(rf.log, NewLog)
	// fmt.Println("new entry appending", rf.log, NewLog)
	rf.mu.Unlock()
	go rf.sendHeartbeat(false)
	return len(rf.log) - 1, rf.curTerm, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// all function calls start here
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.elcTimer.C:
			rf.mu.Lock()
			if rf.role == 2 {

				rf.mu.Unlock()
				continue
			} //we should omit the election timeout of leader.
			//fmt.Println("rf.me: ", rf.me, " is leader, reset timer", rf.curTerm)
			rf.role = 1
			rf.elcTimeout = 300 + (rand.Int63() % 300)
			rf.elcTimer.Reset(time.Duration(rf.elcTimeout) * time.Millisecond)
			rf.mu.Unlock()
			//fmt.Println("rf.me: ", rf.me, " start election")
			rf.initiateElection()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.role == 2 {
				rf.mu.Unlock()
				rf.sendHeartbeat(true)
				rf.mu.Lock()
				rf.heartbeatTimer.Reset(time.Duration(120) * time.Millisecond)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) applier(){
	for {
		select {
		case <- rf.msgch:
			rf.mu.Lock()
			var msgs []ApplyMsg
			if rf.commitedIdx <= rf.lastApplied {
				msgs = make([]ApplyMsg, 0)
			} else {
				msgs = make([]ApplyMsg, 0, rf.commitedIdx-rf.lastApplied)
				//fmt.Println("rf.me: ", rf.me, " rf.commitedIdx: ", rf.commitedIdx, " rf.lastApplied: ", rf.lastApplied, len(rf.log))
				for i := rf.lastApplied + 1; i <= rf.commitedIdx; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
					})
				}
			}
			rf.mu.Unlock()
			for _, msg := range msgs {
				// fmt.Println(msg)
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
				}
			}
		}
	}
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.curTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.role = 0
	rf.msgch = make(chan ApplyMsg, 1000)
	rf.log = make([]LogEntry, 1)
	rf.nextIdx = make([]int, len(peers))
	rf.elcTimeout = 300 + (rand.Int63() % 300)
	rf.elcTimer = time.NewTimer(time.Duration(rf.elcTimeout) * time.Millisecond)
	//fmt.Println("election timeout is ", rf.elcTimeout)
	rf.heartbeatTimer = time.NewTimer(time.Duration(120) * time.Millisecond)
	rf.matchIdx = make([]int, len(peers))
	rf.commitedIdx = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	//go rf.AppendEntries()
	return rf
}


