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
	"sync"
	"sync/atomic"
	"time"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent On all servers
	// Updated on stable storage before responding to RPCs
	currentTerm               int   // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor                  int   // candidateId that received vote in current Term (or null if none)
	logs                      []Log // log entries; each entry contains command for state machine, and Term when entry was received by leader (first index is 1)
	snapshot                  []byte
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int

	lastValidTime time.Time

	// Volatile state on all servers
	commitIndex    int    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied    int    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	state          string // "follower", "candidate", "leader"
	totalServerNum int
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond

	minElectionTimeoutMilliseconds int
	maxElectionTimeoutMilliseconds int

	// Volatile state on all leaders
	// Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type Log struct {
	Command interface{}
	Term    int
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
	isleader = rf.state == "leader"
	return term, isleader
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
	rf.mu.Lock()
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == "leader"

	// Your code here (2B).
	// append to leader's log, leader sends out AppendEntries RPCs
	// Start() returns w/o waiting for RPC replies
	if isLeader {
		logToAppend := Log{
			Term:    term,
			Command: command,
		}
		Debug(dLog, "S%d %s log %s appended to index %d, try to send rpc entry", rf.me, rf.state, logToAppend, index)
		rf.logs = append(rf.logs, logToAppend)
		rf.persist()
		rf.mu.Unlock()

		for peerId := 0; peerId < len(rf.peers); peerId++ {
			go func(peerId int) {
				rf.sendEntriesToPeer(peerId)
			}(peerId)
		}
	} else {
		rf.mu.Unlock()
	}

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
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []Log{
		Log{
			Term: 0,
		},
	}
	rf.snapshotLastIncludedIndex = 0
	rf.snapshotLastIncludedTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.minElectionTimeoutMilliseconds = 500
	rf.maxElectionTimeoutMilliseconds = 1000

	rf.nextIndex = initSliceWithValueAndCapacity(1, rf.totalServerNum)
	rf.matchIndex = initSliceWithValueAndCapacity(0, rf.totalServerNum)

	rf.totalServerNum = len(peers)
	rf.lastValidTime = time.Now()
	rf.state = "follower"
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(dInfo, "S%d %s, started", rf.me, rf.state)

	// start electionTicker goroutine to start elections
	go rf.electionTicker()
	go rf.applyMsgToStateMachineTicker()

	return rf
}

func initSliceWithValueAndCapacity(initValue int, capacity int) []int {
	s := make([]int, capacity, capacity)
	for index := 0; index < capacity; index++ {
		s[index] = initValue
	}
	return s
}
