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
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Command interface{}
	Term    int
}
type ServiceState int

const (
	Follower  ServiceState = 1
	Candidate ServiceState = 2
	Leader    ServiceState = 3
	NotVote   int          = -999
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	logger    *log.Logger
	applyChan chan ApplyMsg

	currentTerm     int
	voteFor         int
	lastVoteForTerm int
	log             []LogEntry

	commitIndex     int //已提交的最高的index
	lastApplied     int //收到的最高的index
	lastAppliedTerm int

	nextIndex  []int //下一个server收到log的index
	matchIndex []int //server中已经匹配的log

	status           ServiceState
	lastReceivedTime int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.status == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req *RequestVoteArgs, resp *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("receive requestVote:%+v, currentTerm:%d,commitIndex:%d", req, rf.currentTerm, rf.commitIndex)

	resp.Term = rf.currentTerm
	candidateTerm := req.Term
	if (rf.voteFor == NotVote || rf.lastVoteForTerm < candidateTerm) && candidateTerm > rf.currentTerm && req.LastLogIndex >= rf.commitIndex && req.LastLogTerm >= rf.lastAppliedTerm {
		rf.voteFor = req.CandidateId
		rf.lastVoteForTerm = candidateTerm
		rf.lastReceivedTime = time.Now().UnixMilli()

		resp.Term = candidateTerm
		resp.VoteGranted = true
		rf.logger.Printf("vote for %d,time:%d", req.CandidateId, rf.lastReceivedTime)
	}
}

//
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
//
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) Ticker() {
	for rf.killed() == false {
		periodicalTime := time.Now().UnixMilli()
		time.Sleep(time.Duration(400+rand.Intn(150)) * time.Millisecond)
		if periodicalTime > rf.lastReceivedTime && rf.status == Follower {
			rf.Election(periodicalTime)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		status:    Follower,
		applyChan: applyCh,
		voteFor:   NotVote,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = log.New(os.Stdout, fmt.Sprintf("%d:", rf.me), log.Lmsgprefix|log.Lmicroseconds)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Ticker()
	go rf.LeaderTicker()
	return rf
}
func (rf *Raft) Election(periodicalTime int64) {
	rf.mu.Lock()
	if !(periodicalTime > rf.lastReceivedTime && rf.status == Follower) {
		return
	}
	rf.currentTerm += 1
	rf.status = Candidate
	rf.lastVoteForTerm = rf.currentTerm
	rf.voteFor = rf.me
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  0,
	}
	rf.mu.Unlock()
	rf.logger.Printf("time out, lastReceivedTime:%d,req:%+v", rf.lastReceivedTime, req)
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		resp := &RequestVoteReply{}
		rf.logger.Printf("send vote to %d", i)
		tempI := i
		go func() {
			rf.SendRequestVote(tempI, req, resp)
			respChan <- resp
		}()
	}
	curTime := time.Now().UnixMilli()
	for voteCount <= len(rf.peers)/2 && curTime+50 >= time.Now().UnixMilli() && rf.status == Candidate {
		select {
		case resp := <-respChan:
			rf.logger.Printf("received vote,%+v", resp)
			if resp.VoteGranted {
				voteCount++
			}
			curTime = time.Now().UnixMilli()
		default:
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteCount > len(rf.peers)/2 && rf.status == Candidate {
		rf.logger.Println("become a leader")
		rf.status = Leader
		go rf.SendHeartBeat()
	} else {
		rf.status = Follower
	}
}

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //想传播的log的上一个index
	PrevLogTerm  int //prevLogIndex的term
	Entries      []LogEntry
	LeaderCommit int //leader’s commitIndex
}
type AppendEntriesResp struct {
	Term      int
	Success   bool
	Responder int
}

//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Responder = rf.me
	rf.logger.Printf("receive append entries:%+v,current term:%d", req, rf.currentTerm)
	if req.Term < rf.currentTerm {
		resp.Term = rf.currentTerm
		resp.Success = false
		return
	}
	rf.lastReceivedTime = time.Now().UnixMilli()
	rf.logger.Printf("append entries time:%d", rf.lastReceivedTime)
	if req.Term > rf.currentTerm {
		rf.logger.Printf("become a follower with leader:%d", req.LeaderId)
		rf.currentTerm = req.Term
		rf.voteFor = NotVote
		rf.status = Follower
	}
	resp.Term, resp.Success = rf.currentTerm, true
	return
}

func (rf *Raft) LeaderTicker() {
	for !rf.killed() {
		time.Sleep(200 * time.Millisecond)
		if rf.status == Leader {
			rf.SendHeartBeat()
		}
	}
}

func (rf *Raft) SendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	respChan := make(chan *AppendEntriesResp, len(rf.peers))
	req := &AppendEntriesReq{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastApplied,
		PrevLogTerm:  0, //todo
		Entries:      nil,
		LeaderCommit: 0,
	}
	for i := range rf.peers {
		if i != rf.me {
			resp := &AppendEntriesResp{}
			rf.logger.Printf("send heartbeat to %d,info:%+v", i, req)
			a := i
			go func(b int) {
				rf.SendAppendEntries(b, req, resp)
				respChan <- resp
			}(a)
		}
	}
	curTime := time.Now().UnixMilli()
	for curTime+180 >= time.Now().UnixMilli() {
		select {
		case resp := <-respChan:
			if !resp.Success && resp.Term > rf.currentTerm {
				rf.logger.Printf("become a follower with leader:%d", resp.Responder)
				rf.status = Follower
				rf.currentTerm = resp.Term
			}
		default:

		}
	}
}

func (rf *Raft) SendAppendEntries(to int, req *AppendEntriesReq, resp *AppendEntriesResp) bool {
	ok := rf.peers[to].Call("Raft.AppendEntries", req, resp)
	return ok
}
