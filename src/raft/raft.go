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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const HeartBeatDuration = 200

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

type VoteType int

const (
	PreVote VoteType = iota
	TrueVote
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	logger    *log.Logger
	applyChan chan ApplyMsg
	sendmu    sync.Mutex

	currentTerm     int
	voteFor         int
	lastVoteForTime int64
	log             []LogEntry

	commitIndex      int //已提交的最高的index
	commitTerm       int //已提交的最高的term
	lastAppliedIndex int //收到的最高的index
	lastAppliedTerm  int

	//这两个似乎可以简化成一个，目前nextidx似乎没有用
	nextIndex  []int //下一个server收到log的index
	matchIndex []int //server中已经匹配的log
	online     []int //todo 如果后续Append会经常给失联的发，需要判别

	status           ServiceState
	lastReceivedTime int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term            int
	CandidateId     int
	LastCommitIndex int
	LastCommitTerm  int
	Type            VoteType
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term            int
	VoteGranted     bool
	SelfId          int
	LastCommitIndex int
	LastCommitTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(req *RequestVoteArgs, resp *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("receive requestVote:%+v, currentTerm:%d,commitIndex:%d", req, rf.currentTerm, rf.commitIndex)
	resp.Term = rf.currentTerm
	candidateTerm := req.Term
	resp.SelfId = rf.me
	resp.LastCommitIndex = rf.lastAppliedIndex
	resp.LastCommitTerm = rf.log[rf.lastAppliedIndex].Term
	resp.VoteGranted = false

	if req.Type == PreVote {
		rf.logger.Printf("time:%d, votefor:%d", time.Now().UnixMilli()-rf.lastReceivedTime, rf.voteFor)

		resp.Term = rf.currentTerm
		if time.Now().UnixMilli()-rf.lastReceivedTime > HeartBeatDuration && (rf.voteFor == NotVote || rf.voteFor == req.CandidateId || time.Now().UnixMilli()-rf.lastVoteForTime > HeartBeatDuration) && (rf.log[rf.lastAppliedIndex].Term < req.LastCommitTerm || (rf.log[rf.lastAppliedIndex].Term == req.LastCommitTerm && rf.lastAppliedIndex <= req.LastCommitIndex)) {
			resp.VoteGranted = true
			rf.voteFor = req.CandidateId
			rf.lastVoteForTime = time.Now().UnixMilli()
		}
		return
	}

	if rf.voteFor == req.CandidateId && candidateTerm > rf.currentTerm && (rf.log[rf.lastAppliedIndex].Term < req.LastCommitTerm || (rf.log[rf.lastAppliedIndex].Term == req.LastCommitTerm && rf.lastAppliedIndex <= req.LastCommitIndex)) {
		rf.voteFor = req.CandidateId
		rf.status = Follower
		rf.currentTerm = candidateTerm
		resp.Term = candidateTerm
		resp.VoteGranted = true
		rf.lastVoteForTime = time.Now().UnixMilli()
		rf.logger.Printf("vote for %d,time:%d", req.CandidateId, rf.lastReceivedTime)
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
func (rf *Raft) RPCSendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	rf.mu.Lock()
	isLeader := rf.status == Leader
	// rf.mu.Unlock()

	if !isLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	// rf.mu.Lock()
	rf.lastAppliedIndex++
	rf.lastAppliedTerm = rf.currentTerm
	index := rf.lastAppliedIndex
	term := rf.currentTerm
	entry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.logger.Printf("begin to send command:%+v", entry)
	rf.log = append(rf.log, entry)
	rf.logger.Printf("logs:%+v", rf.log)
	rf.mu.Unlock()
	go func() {
		rf.sendmu.Lock()
		curCommitIndex := rf.commitIndex
		lastApplied := rf.lastAppliedIndex
		rf.logger.Printf("start to append:%v,curCommit:%d,lastApplied:%d", command, curCommitIndex, lastApplied)
		if result := rf.SendAppendEntries(Append); result {
			rf.mu.Lock()
			rf.CommitLog(curCommitIndex, lastApplied)
			rf.commitIndex = lastApplied
			rf.commitTerm = rf.currentTerm
			rf.mu.Unlock()
			rf.SendAppendEntries(Commit)
		}
		rf.sendmu.Unlock()
	}()

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) Ticker() {
	for !rf.killed() {
		periodicalTime := time.Now().UnixMilli()
		time.Sleep(time.Duration(rand.Intn(150)+2*HeartBeatDuration) * time.Millisecond)
		rf.mu.Lock()
		if periodicalTime > rf.lastReceivedTime && rf.status == Follower { //醒了之后发现还没有收到心跳
			rf.voteFor = NotVote
			rf.mu.Unlock()
			if rf.PreVote(periodicalTime) {
				rf.Election(periodicalTime)
			}
		} else {
			rf.mu.Unlock()
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
	// rf.logger.SetOutput(ioutil.Discard)
	rf.logger.Printf("i was born")
	//rf.commitIndex = -1
	//rf.lastAppliedIndex = -1
	rf.log = append(rf.log, LogEntry{})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.online = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Ticker()
	go rf.LeaderTicker()
	return rf
}
func (rf *Raft) PreVote(periodicalTime int64) bool {
	rf.mu.Lock()
	if !(periodicalTime > rf.lastReceivedTime && rf.status == Follower) {
		return false
	}
	rf.status = Candidate
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	req := &RequestVoteArgs{
		Term:            rf.currentTerm + 1,
		CandidateId:     rf.me,
		LastCommitIndex: rf.lastAppliedIndex,
		LastCommitTerm:  rf.log[rf.lastAppliedIndex].Term,
		Type:            PreVote,
	}
	rf.mu.Unlock()
	rf.logger.Printf("prevote, lastReceivedTime:%d,req:%+v", rf.lastReceivedTime, req)
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		resp := &RequestVoteReply{}
		rf.logger.Printf("send vote to %d", i)
		tempI := i
		go func() {
			rf.RPCSendRequestVote(tempI, req, resp)
			respChan <- resp
		}()
	}
	curTime := time.Now().UnixMilli()
	for voteCount <= len(rf.peers)/2 && curTime+50 >= time.Now().UnixMilli() && rf.status == Candidate {
		select {
		case resp := <-respChan:
			rf.logger.Printf("received prevote,%+v", resp)
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
		rf.logger.Println("prevote true")
		return true
	} else {
		rf.logger.Println("prevote false")
		rf.status = Follower
		return false
	}
}
func (rf *Raft) Election(periodicalTime int64) {
	rf.mu.Lock()
	if !(periodicalTime > rf.lastReceivedTime && rf.status == Candidate) {
		return
	}
	rf.currentTerm += 1
	// rf.status = Candidate
	rf.voteFor = rf.me
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	req := &RequestVoteArgs{
		Term:            rf.currentTerm,
		CandidateId:     rf.me,
		LastCommitIndex: rf.lastAppliedIndex,
		LastCommitTerm:  rf.log[rf.lastAppliedIndex].Term,
		Type:            TrueVote,
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
			rf.RPCSendRequestVote(tempI, req, resp)
			respChan <- resp
		}()
	}
	curTime := time.Now().UnixMilli()
	for voteCount < len(rf.peers) && curTime+50 >= time.Now().UnixMilli() && rf.status == Candidate {
		select {
		case resp := <-respChan:
			rf.logger.Printf("received vote,%+v", resp)
			if resp.VoteGranted {
				voteCount++
			}
			rf.matchIndex[resp.SelfId] = min(resp.LastCommitIndex, rf.commitIndex)
			rf.nextIndex[resp.SelfId] = min(rf.matchIndex[resp.SelfId]+1, rf.commitIndex+1)
			curTime = time.Now().UnixMilli()
		default:
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteCount > len(rf.peers)/2 && rf.status == Candidate {
		rf.logger.Println("become a leader")
		rf.status = Leader
		go rf.SendAppendEntries(HeartBeat)
	} else {
		rf.status = Follower
	}
	rf.voteFor = NotVote
}

type AppendEntriesStatus int

const (
	HeartBeat = 0
	Append    = 1
	Commit    = 2
)

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //想传播的log的上一个index
	PrevLogTerm  int //prevLogIndex的term
	Entries      []LogEntry
	LeaderCommit int //leader’s commitIndex
	Status       AppendEntriesStatus
	SelfId       int //接受方在我方的数组中所处的位置
}
type AppendEntriesResp struct {
	Term            int
	Success         bool
	Responder       int
	RequiredTerm    int
	RequiredIndex   int
	SelfId          int //参照req
	LastCommitIndex int
	LastCommitTerm  int
}

// AppendEntries If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) //todo
func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Responder = rf.me
	resp.SelfId = req.SelfId
	rf.logger.Printf("receive append entries:%+v,current term:%d", req, rf.currentTerm)
	if req.Term < rf.currentTerm {
		resp.Term = rf.currentTerm
		resp.LastCommitIndex = rf.commitIndex
		resp.LastCommitTerm = rf.commitTerm
		resp.Success = false
		return
	}
	rf.lastReceivedTime = time.Now().UnixMilli()
	rf.voteFor = NotVote
	//rf.logger.Printf("append entries time:%d", rf.lastReceivedTime)

	//leader校验
	if req.Term > rf.currentTerm && req.LeaderCommit >= rf.commitIndex {
		rf.logger.Printf("become a follower with notifier:%d", req.LeaderId)
		rf.currentTerm = req.Term
		rf.status = Follower
	}
	resp.Term, resp.Success = rf.currentTerm, true

	//不同类型消息变更
	switch req.Status {
	case Append:
		rf.logger.Printf("prevlogidx:%d lastApplied:%d prevlogTerm:%d lastTerm:%d ldcommit:%d commitidx:%d", req.PrevLogIndex, rf.lastAppliedIndex, req.PrevLogTerm, rf.lastAppliedTerm, req.LeaderCommit, rf.commitIndex)
		if req.PrevLogIndex <= rf.lastAppliedIndex && req.PrevLogTerm == rf.log[req.PrevLogIndex].Term && req.LeaderCommit >= rf.commitIndex {
			//if req.PrevLogIndex == rf.lastAppliedIndex+1 ... //todo 这里不知道为啥当初加一了，后续注意
			rf.log = rf.log[:req.PrevLogIndex+1]
			rf.lastAppliedIndex = len(rf.log) - 1
			rf.log = append(rf.log, req.Entries...)
			rf.lastAppliedIndex += len(req.Entries)
			rf.lastAppliedTerm = rf.currentTerm
			rf.logger.Printf("success append entry for req:%+v, self entries:%+v", req, rf.log)
		} else {
			//自己的内容更新
			if req.LeaderCommit < rf.commitIndex { //leader超时后重连，在未变成follower之前有可能会发消息
				resp.Success = false
				rf.logger.Printf("receive command, but false,req:%+v,self applied term:%d,self applied index:%d,self applied commit index:%d", req, rf.lastAppliedTerm, rf.lastAppliedIndex, rf.commitIndex)
			} else { //由于follower个人原因导致log缺失，需要补全
				resp.Success = false
				resp.RequiredIndex = rf.commitIndex
			}
		}
	case Commit:
		rf.logger.Printf("prevlogidx:%d lastApplied:%d prevlogTerm:%d lastTerm:%d ldcommit:%d commitidx:%d", req.PrevLogIndex, rf.lastAppliedIndex, req.PrevLogTerm, rf.lastAppliedTerm, req.LeaderCommit, rf.commitIndex)
		if req.LeaderCommit > rf.lastAppliedIndex { //刚重连就收到了commit的消息
			return
		}
		rf.logger.Printf("self commit index:%d,leader commit:%d", rf.commitIndex, req.LeaderCommit)
		rf.CommitLog(rf.commitIndex, req.LeaderCommit)
		rf.commitTerm = rf.currentTerm
	default:

	}
}

// CommitLog 需要确保外围被锁
func (rf *Raft) CommitLog(from, to int) {
	rf.logger.Printf("commit msg for start:%d,end:%d,commitIndex:%d", from+1, to, rf.commitIndex)
	for i := from + 1; i <= to; i++ {
		if i <= rf.commitIndex {
			continue
		}
		rf.commitIndex++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: rf.commitIndex,
		}
		rf.applyChan <- msg
		rf.logger.Printf("commit msg for command:%+v,i:%d,to:%d", msg, i, to)
	}
}
func (rf *Raft) LeaderTicker() {
	for !rf.killed() {
		time.Sleep(HeartBeatDuration * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			//todo 如果有follower落后，可以在这里做差值校验，对需要补齐的进行append
			//现在思考的方案：通过每次heartbeat不断匹配到matchIdx，1.在下一次append中补足，2.在匹配到之后立刻补足
			rf.SendAppendEntries(HeartBeat)
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) SendOne(status AppendEntriesStatus, id int, respChan chan *AppendEntriesResp) {
	matchIndex := rf.matchIndex[id]
	var entries = make([]LogEntry, 0)
	if status != HeartBeat && matchIndex+1 <= rf.nextIndex[id] {
		entries = rf.log[matchIndex+1:]
	}
	a := id
	req := &AppendEntriesReq{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: matchIndex,
		PrevLogTerm:  rf.log[matchIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
		Status:       status,
		SelfId:       a,
	}
	if id != rf.me {
		resp := &AppendEntriesResp{}
		rf.logger.Printf("send heartbeat to %d,info:%+v", id, req)
		go func(b int) {
			rf.RPCSendAppendEntries(b, req, resp)
			respChan <- resp
		}(a)
	}
}

func (rf *Raft) SendAppendEntries(status AppendEntriesStatus, fixId ...int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("lastApplied:%d,commitIdx:%d", rf.lastAppliedIndex, rf.commitIndex)
	if rf.status != Leader || (status == Append && rf.lastAppliedIndex == rf.commitIndex) {
		rf.logger.Printf("append false")
		return false
	}
	respChan := make(chan *AppendEntriesResp, len(rf.peers))
	for i := range rf.peers {
		rf.SendOne(status, i, respChan)
	}
	startTime := time.Now().UnixMilli()
	count := 0
	successResp := make([]*AppendEntriesResp, 0, len(rf.peers))
	for status != Commit && startTime+100 >= time.Now().UnixMilli() {
		select {
		case resp := <-respChan:
			if resp.Success {
				count++
				rf.logger.Printf("resp1:%v\n", resp)
				successResp = append(successResp, resp)
			}
			if !resp.Success {
				if resp.Term > rf.currentTerm {
					rf.currentTerm = resp.Term
					// if resp.LastCommitIndex >= rf.commitIndex && resp.LastCommitTerm >= rf.commitTerm {
					// 	rf.logger.Printf("become a follower with leader:%d", resp.Responder)
					// 	rf.status = Follower
					// } else {
					// 	rf.logger.Printf("become a candidate by leader:%d", resp.Responder)
					// 	rf.status = Candidate
					// }
					rf.status = Follower
				} else if status == Append {
					rf.matchIndex[resp.SelfId] = resp.RequiredIndex
				}
			}
		default:

		}
	}

	switch status {
	case Append:
		//rf.logger.Printf("len:%d\n", len(successResp))
		for _, resp := range successResp {
			//rf.logger.Printf("resp2:%v\n", resp)
			//rf.logger.Printf("%d\n", resp.SelfId)
			id := resp.SelfId
			entryLen := len(rf.log[rf.matchIndex[id]+1:])
			rf.matchIndex[id] += entryLen
			rf.nextIndex[id] += entryLen
		}
		if count+1 <= len(rf.peers)/2 || rf.status != Leader { //+1是自己
			return false
		}
		//todo 缺少消息对不上的处理
	case Commit:
		return true //todo
	default:

	}
	//close(respChan) //todo 可能会有send to close chan，建议defer+recovery
	return true
}

func (rf *Raft) RPCSendAppendEntries(to int, req *AppendEntriesReq, resp *AppendEntriesResp) bool {
	ok := rf.peers[to].Call("Raft.AppendEntries", req, resp)
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
