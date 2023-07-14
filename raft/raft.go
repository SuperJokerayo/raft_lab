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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"

	"sort"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
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


// hold information about each log entry
type Entry struct {
	Command  interface{} // command for state machine
	Term 	 int 		 // term when entry was received by leader
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	identity int // 0 FOLLOWER 1 candidate 2 leader

	currentTerm int
	votedFor    int

	electionTimer    *time.Ticker // reset election time

	
	log		     []Entry

	commitIndex	 int
	lastApplied  int


	nextIndex	 []int // 均为原始index
	matchIndex	 []int

	applyCh 	 chan ApplyMsg

	snapshot 	 []byte

	lastIncludedIndex 	 int // the highest log entry that's reflected in the snapshot
	lastIncludedTerm	 int
}

const HEARTBEAT_INTERVAL = 100 * time.Millisecond

// Leader send it out
type AppendEntriesRPCsArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []Entry

	LeaderCommit int
}

type AppendEntriesRPCsReply struct {
	Term  	int
	Success bool
	
	// optimize
	ConflictTerm 	   int
	ConflictFirstIndex int
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Reset(getElectionTimeout())
}

func getElectionTimeout() time.Duration {
	return time.Duration(200 + (rand.Int63() % 150)) * time.Millisecond
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log[1:]) + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 1 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) getLogIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) getLogTerm(index int) int {
	return rf.log[rf.getLogIndex(index)].Term
}


func min(a, b int) int {if a > b {return b}; return a}

func max(a, b int) int {if a > b {return a}; return b}

func (rf *Raft) sendAppendEntriesRPCs(server int, args *AppendEntriesRPCsArgs, reply *AppendEntriesRPCsReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
	return ok
}


func (rf *Raft) newAppendEntriesRPSsArgs(i int) *AppendEntriesRPCsArgs {

	argsEntries := make([]Entry, len(rf.log[rf.getLogIndex(rf.nextIndex[i]):]))
	copy(argsEntries, rf.log[rf.getLogIndex(rf.nextIndex[i]):])
	return &AppendEntriesRPCsArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,

		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm: rf.getLogTerm(rf.nextIndex[i] - 1),
		Entries: argsEntries,
		LeaderCommit: rf.commitIndex,
	}

}


func (rf *Raft) newInstallSnapshotRPCsArgs() *InstallSnapshotRPCsArgs {

	return &InstallSnapshotRPCsArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Data: rf.snapshot,
	}

}

func (rf *Raft) updateCommitIndex() {
	if rf.identity != LEADER {
		return 
	}
	n := len(rf.peers)
	copyMatchIndex := make([]int, n)
	copy(copyMatchIndex, rf.matchIndex)
	sort.Slice(copyMatchIndex, func(i, j int) bool {return copyMatchIndex[i] > copyMatchIndex[j]})
	N := copyMatchIndex[n / 2]
	if N > rf.commitIndex && rf.getLogTerm(N) == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

func (rf *Raft) updateLastApplied() {
	
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := &ApplyMsg{
			CommandValid: true,
			Command: rf.log[rf.getLogIndex(rf.lastApplied)].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- *msg
		rf.mu.Lock()
	}
}

func (rf *Raft) sendHeartBeat() {
	heartBeatTimeout := time.NewTicker(HEARTBEAT_INTERVAL)
	for !rf.killed() {
		n := len(rf.peers)
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				if rf.identity != LEADER { // only leader can send AppendEntries
					rf.mu.Unlock()
					return 
				}

				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					args := rf.newInstallSnapshotRPCsArgs()
					reply := &InstallSnapshotRPCsReply{}
					rf.mu.Unlock()
					if ok := rf.sendInstallSnapshotRPCs(i, args, reply); !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.identity != LEADER || rf.currentTerm != args.Term {
						return 
					}
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term, false)
						rf.votedFor = -1
						rf.resetElectionTimeout()
						rf.persist()
						return
					}
					rf.matchIndex[i] = args.LastIncludedIndex
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				} else {
					args := rf.newAppendEntriesRPSsArgs(i)
					reply := &AppendEntriesRPCsReply{}
					rf.mu.Unlock()
					if ok := rf.sendAppendEntriesRPCs(i, args, reply); !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.identity != LEADER || rf.currentTerm != args.Term {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term, false)
						rf.votedFor = -1
						rf.resetElectionTimeout()
						rf.persist()
						return
					}
					
					if reply.Success {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						if rf.commitIndex < rf.getLastLogIndex() {
							rf.updateCommitIndex()
						}
					} else {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[i] = reply.ConflictFirstIndex
						} else {
							for rf.nextIndex[i] > rf.lastIncludedIndex{
								if rf.getLogTerm(rf.nextIndex[i] - 1) == 0 {
									if rf.lastIncludedIndex == 0 {
										rf.nextIndex[i] = 1
									} else {
										rf.nextIndex[i] = rf.lastIncludedIndex
									}		
									break
								}
								if rf.getLogTerm(rf.nextIndex[i] - 1) == reply.ConflictTerm {
									break
								}
								rf.nextIndex[i]--
								if rf.nextIndex[i] == reply.ConflictFirstIndex {
									break
								}
							}
						}
					}
				}
			}(i)
		}
		<- heartBeatTimeout.C
	}
}

func (rf *Raft) HeartBeat(args *AppendEntriesRPCsArgs, reply *AppendEntriesRPCsReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	} ()

	if args.Term < rf.currentTerm {
		return 
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	} 

	rf.toFollower(args.Term, false)
	rf.resetElectionTimeout()
	
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.ConflictFirstIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictTerm = -1
		reply.ConflictFirstIndex = rf.lastIncludedIndex + 1
		return
	} 

	if args.PrevLogIndex != rf.lastIncludedIndex && args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		reply.ConflictFirstIndex = args.PrevLogIndex
		for reply.ConflictFirstIndex > rf.lastIncludedIndex && reply.ConflictFirstIndex > rf.commitIndex{
			if (reply.ConflictTerm != rf.getLogTerm(reply.ConflictFirstIndex)) {
				reply.ConflictFirstIndex++
				break
			}
			reply.ConflictFirstIndex--
			if reply.ConflictFirstIndex == rf.commitIndex || reply.ConflictFirstIndex == rf.lastIncludedIndex {
				reply.ConflictFirstIndex++
				break
			}
		}
		return
	}

	rf.log = rf.log[:rf.getLogIndex(args.PrevLogIndex) + 1]

	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.updateLastApplied()
	}
	reply.Success = true

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == LEADER
	return term, isleader
}


func (rf *Raft) toFollower(leaderTerm int, doLock bool) {
	if doLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.identity = FOLLOWER
	rf.currentTerm = leaderTerm
}

func (rf *Raft) toCandidate(doLock bool) {
	if doLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.identity = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
}

func (rf *Raft) toLeader(doLock bool) {
	if doLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.identity = LEADER
	n := len(rf.peers)

	// reinitialize
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.electionTimer.Stop()
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm  int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		// pass
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}


type InstallSnapshotRPCsArgs struct {
	Term    		  int
	LeaderId 		  int
	LastIncludedIndex int
	LastIncludedTerm  int
	// offset			 int
	Data 			  []byte
	// done			  bool
}


type InstallSnapshotRPCsReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshotRPCs(server int, args *InstallSnapshotRPCsArgs, reply *InstallSnapshotRPCsReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotRPCsArgs, reply *InstallSnapshotRPCsReply) {
	rf.mu.Lock()
	defer func () {
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	} ()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	rf.toFollower(args.Term, false)
	rf.resetElectionTimeout()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	rf.snapshot = args.Data

	log := rf.log[:1]
	if rf.getLastLogIndex() >= args.LastIncludedIndex {
		log = append(log, rf.log[rf.getLogIndex(args.LastIncludedIndex) + 1:]...)
	} else {
		log = append([]Entry{}, log...)
	}
	rf.log = log
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)

	msg := &ApplyMsg {
		SnapshotValid: true,
		Snapshot: rf.snapshot,
		SnapshotTerm: rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex, 
	}	
	rf.mu.Unlock()
	go func() {
		rf.applyCh <- *msg
	}()
	rf.mu.Lock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	} ()
	if index > rf.getLastLogIndex() || index > rf.commitIndex {
		return
	}
	if index <= rf.lastIncludedIndex {
		return
	}

	logIndex := rf.getLogIndex(index)

	rf.lastIncludedTerm = rf.log[logIndex].Term
	rf.lastIncludedIndex = index

	log := rf.log[:1]

	log = append(log, rf.log[logIndex + 1:]...)
	rf.log = log
	rf.snapshot = snapshot
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term  		 int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int // currentTerm, for candidate to update itself
	VoteGranted bool// true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	} ()

	// term less than currentTerm or has voted
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}
	
	if args.Term > rf.currentTerm {
	 	rf.toFollower(args.Term, false)
		rf.votedFor = -1
		rf.resetElectionTimeout()
	} 

	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()){
		return 
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimeout()
	rf.identity = FOLLOWER
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
	defer rf.mu.Unlock()
	index := 0
	term := 0
	isLeader := rf.identity == LEADER

	// Your code here (2B).
	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		rf.log = append(rf.log, Entry{
			Command: command,
			Term: term,
		})
		rf.persist()
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
	}
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

func (rf *Raft) ticker() {
	for rf.killed() == false { 
		// Your code here (2A)
		// Check if a leader election should be started.
		<- rf.electionTimer.C
		rf.leaderElection()
	}
}



func (rf *Raft) leaderElection() {
	defer func() {
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
	} ()
	rf.toCandidate(true)
	votes := 1
	target := len(rf.peers) / 2
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := &RequestVoteArgs {
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm: rf.getLastLogTerm(),
		}
		rf.mu.Unlock()
		go func(i int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.currentTerm {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term, false)
				rf.votedFor = -1
				rf.resetElectionTimeout()
				return
			}
			if rf.identity == CANDIDATE && reply.VoteGranted {
				votes++
				if votes > target {
					rf.toLeader(false)
					go rf.sendHeartBeat()
				}
			}
		}(i)
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
	
	n := len(peers)

	// Your initialization code here (2A, 2B, 2C).

	rf.identity = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1 // vote for none initially
	
	rf.electionTimer = time.NewTicker(getElectionTimeout())
	
	rf.commitIndex = 0
	rf.lastApplied = 0

	// rf.log[0] is a sentry
	rf.log = append([]Entry{}, Entry{
		Command: nil,
		Term: 0,
	})
	// log index start rom 1
	// reinitialized after election
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.snapshot

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

