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

	// each electionTimeout is different, so it is not saved here

	heartbeatInterval      time.Duration
	resetElectionTimerChan chan struct{}
	done                   chan struct{}
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
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-1].Term
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

	// 2C page 7-8 gray line optimization
	FirstIndexOfConflictingTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == LEADER) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstIndexOfConflictingTerm = -1
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
	reply.FirstIndexOfConflictingTerm = -1

	// log consistency check, check the last index and term.
	if args.PrevLogIndex > 0 {
		if len(rf.log) < args.PrevLogIndex {
			reply.Success = false
			return
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			rf.log = rf.log[:args.PrevLogIndex]
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.log[i-1].Term != args.PrevLogTerm {
					reply.FirstIndexOfConflictingTerm = i
				} else {
					break
				}
			}
			rf.persist()
			return
		}
	}

	// handle conflicts and append new log entries
	if len(args.Entries) > 0 {
		appendIndex := args.PrevLogIndex + 1
		if appendIndex <= len(rf.log) {
			for i, entry := range args.Entries {
				logIndex := appendIndex + i
				if logIndex > len(rf.log) {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				} else if rf.log[logIndex-1].Term != entry.Term {
					rf.log = rf.log[:logIndex-1]
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
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		rf.persist()
	}

	reply.Success = true
}

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

	index := len(rf.log)
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
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
			// sync entries
			var entries []LogEntry
			if nextIndex <= len(rf.log) {
				entries = rf.log[nextIndex-1:]
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
				if reply.FirstIndexOfConflictingTerm != -1 {
					rf.nextIndex[server] = reply.FirstIndexOfConflictingTerm
				} else {
					if rf.nextIndex[server] > 1 {
						rf.nextIndex[server]--
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log); N > rf.commitIndex; N-- {
		// the leader can only commit entries in self's term
		if rf.log[N-1].Term != rf.currentTerm {
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
		heartbeatInterval:      10 * time.Millisecond, // in order to pass tests of Figure8 unreliable
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
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i-1].Command,
						CommandIndex: i,
					}
					applyCh <- msg
				}
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
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
		lastLogIndex = len(rf.log)
		lastLogTerm = rf.log[lastLogIndex-1].Term
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
							rf.nextIndex[j] = len(rf.log) + 1
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
func (rf *Raft) persist() {
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
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		// ignore error
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}
