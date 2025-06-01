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
	"github.com/alioth4j/6.824/src/labgob"
	"github.com/alioth4j/6.824/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	IsSnapshot bool
	Snapshot   []byte
}

// raft states
const (
	LEADER int = iota
	FOLLOWER
	CANDIDATE
)

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

	// state
	// just what it thinks it is
	state int

	// persistent state on all servers
	currentTerm int
	votedFor    int // set to -1 at beginning
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// applyCH in user, such as kvserver
	applyCh chan ApplyMsg

	// each electionTimeout is different, so it is not saved here

	heartbeatInterval      time.Duration
	resetElectionTimerChan chan struct{}
	done                   chan struct{}

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// paper 5.4.1
	// the voter denies its vote if its own log is more up-to-date than that of the candidate.
	lastLogIndex := 0
	lastLogTerm := 0
	if rf.lastIncludedIndex + len(rf.log) > 0 {
		lastLogIndex = rf.lastIncludedIndex + len(rf.log)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	logUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		// reset election timer
		select {
		case rf.resetElectionTimerChan <- struct{}{}:
		default:
		}
	} else {
		reply.VoteGranted = false
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
//
// This is also a wrapper.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == LEADER) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}

	// reset election timeout
	select {
	case rf.resetElectionTimerChan <- struct{}{}:
	default:
	}

	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogIndex != rf.lastIncludedTerm {
			reply.Success = false
			reply.ConflictTerm = rf.lastIncludedTerm
			reply.ConflictIndex = rf.lastIncludedIndex
			return
		}
	} else {
		// if args.PrevLogIndex > rf.lastIncludedIndex + len(rf.log) {
		// 	reply.Success = false
		// 	reply.ConflictTerm = -1
		// 	reply.ConflictIndex = rf.lastIncludedIndex + 1
		// 	return
		// }

		// logIndex := args.PrevLogIndex - rf.lastIncludedIndex

		logIndex := args.PrevLogIndex - rf.lastIncludedIndex - 1
		if logIndex < 0 || logIndex >= len(rf.log) {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log) + 1
			return
		}
		if rf.log[logIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.log[logIndex - 1].Term
			reply.ConflictIndex = logIndex
			// find the first index of this conflicting term
			for i := logIndex; i >= 1; i-- {
				if rf.log[i-1].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	}

	// handle conflicts and append new log entries
	if len(args.Entries) > 0 {
		appendIndex := args.PrevLogIndex + 1
		if appendIndex <= rf.lastIncludedIndex + len(rf.log) {
			for i, entry := range args.Entries {
				logIndex := appendIndex + i
				if logIndex > rf.lastIncludedIndex + len(rf.log) {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				} else if rf.log[logIndex-rf.lastIncludedIndex - 1].Term != entry.Term {
					rf.log = rf.log[:logIndex-rf.lastIncludedIndex - 1]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			}
		} else {
			rf.log = append(rf.log, args.Entries...)
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex + len(rf.log))
		rf.persist()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	if args.Data == nil || len(args.Data) < 1 {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		// as a follower, it still can install snapshot
	}

	offset := args.LastIncludedIndex - rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if offset > 0 {
		if offset <= len(rf.log) {
		    rf.log = rf.log[offset:]
		} else {
			rf.log = []LogEntry{}
		}
	}

	// update commitIndex and lastApplied
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	rf.persister.SaveStateAndSnapshot(rf.persist(), args.Data)

	// let the user know
	applyMsg := ApplyMsg{
		IsSnapshot: true,
		Snapshot:   args.Data,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check whether this server is the leader
	// if not, return false.
	if rf.state != LEADER {
		return -1, -1, false
	}

	// --- start the agreement ---
	// construct LogEntry
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	// append to self's log
	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.currentTerm

	// use goroutine to sync to others, as this method should return immediately
	go rf.broadcastAppendEntries()

	return index, term, true
}

func (rf *Raft) broadcastAppendEntries() {
	// synchronize to check the state
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			// next index
			nextIndex := rf.nextIndex[server]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			// prevLogIndex and prevLogTerm
			prevLogIndex := nextIndex - 1
			prevLogTerm := 0
			if prevLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex-1].Term
			}
			// sync entries
			var entries []LogEntry
			if nextIndex <= len(rf.log) {
				entries = rf.log[nextIndex-rf.lastIncludedIndex - 1:]
			} else {
				entries = nil
			}
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != LEADER || rf.currentTerm != term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.Success {
				rf.matchIndex[server] = prevLogIndex + len(entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.updateCommitIndex()
			} else {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					lastTermIndex := -1
					for i := len(rf.log); i >= 1; i-- {
						if rf.log[i-1].Term == reply.ConflictTerm {
							lastTermIndex = rf.lastIncludedIndex + i
							break
						}
					}
					if lastTermIndex == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						rf.nextIndex[server] = lastTermIndex
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	for N := rf.lastIncludedIndex + len(rf.log); N > rf.commitIndex; N-- {
		if N <= rf.lastIncludedIndex {
			continue
		}
        logIndex := N - rf.lastIncludedIndex - 1
		if logIndex < 0 || logIndex >= len(rf.log) {
			continue
		}
		if rf.log[logIndex].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			break
		}
	}
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
	close(rf.done)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:                  peers,
		persister:              persister,
		me:                     me,
		state:                  FOLLOWER,
		currentTerm:            0,
		votedFor:               -1,
		log:                    make([]LogEntry, 0),
		commitIndex:            0,
		lastApplied:            0,
		nextIndex:              make([]int, len(peers)),
		matchIndex:             make([]int, len(peers)),
		applyCh:                applyCh,
		heartbeatInterval:      100 * time.Millisecond, // in order to pass tests of Figure8 unreliable
		resetElectionTimerChan: make(chan struct{}, 1),
		done:                   make(chan struct{}),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start background goroutines
	go rf.electionTimerGoroutine()
	go rf.heartbeatGoroutine()

	// apply commands to state machine
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				var msgs []ApplyMsg
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					if i <= rf.lastIncludedIndex {
						continue
					}
					logIndex := i - rf.lastIncludedIndex - 1
					if logIndex < 0 || logIndex >= len(rf.log) {
						continue
					}
					msgs = append(msgs, ApplyMsg{
						CommandValid: true,
						Command:      rf.log[logIndex].Command,
						CommandIndex: i,
					})
				}
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()

				for _, msg := range msgs {
					rf.applyCh <- msg
				}
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}

func (rf *Raft) electionTimerGoroutine() {
	for !rf.killed() {
		electionTimeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		time.Sleep(electionTimeout)
		if rf.state != LEADER {
			rf.mu.Lock()
			if rf.state != LEADER {
				select {
				case <-rf.resetElectionTimerChan:
				default:
					rf.state = CANDIDATE
					rf.mu.Unlock()
					rf.startElection()
					continue
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != CANDIDATE {
		rf.state = CANDIDATE
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.lastIncludedIndex + len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex-1].Term
	}
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted && args.Term == rf.currentTerm {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
						rf.state = LEADER
						// align the others with me
						for j := range rf.peers {
							rf.nextIndex[j] = rf.lastIncludedIndex + len(rf.log)
							rf.matchIndex[j] = 0
						}
						// initial heartbeats
						go rf.broadcastAppendEntries()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) heartbeatGoroutine() {
	for !rf.killed() {
		time.Sleep(rf.heartbeatInterval)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.broadcastAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() []byte {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	// TODO remove this?
	rf.persister.SaveRaftState(data)
	return data
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
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// ignore error
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// for kvserver to invoke
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	logIndex := index - rf.lastIncludedIndex - 1
	if logIndex < 0 || logIndex >= len(rf.log) {
		return
	}
	term := rf.log[logIndex].Term

	// truncate log
	rf.log = rf.log[logIndex+1:]

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	rf.persister.SaveStateAndSnapshot(rf.persist(), snapshot)

	go rf.broadcastInstallSnapshot(index, snapshot)
}

func (rf *Raft) compactedIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) broadcastInstallSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              snapshot,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			reply := &InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(i, args, reply)
			if !ok {
				rf.mu.Unlock()
				return
			}
			if reply.Term != rf.currentTerm {
				// TODO do something more?
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) getLogTerm(index int) int {
	if index < rf.lastIncludedIndex {
		panic("index must not less than rf.lastIncludedIndex")
	}
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLogTerm(rf.lastIncludedIndex + len(rf.log))
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) GetSnapshotSize() int {
	return rf.persister.SnapshotSize()
}
