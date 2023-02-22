package raft

import (
	"math/rand"
	"sync"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself TODO
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.decideIfConvertToFollower(args.Term)
	Debug(dVote, "S%d %s requestVote RPC from S%d, args.Term %d, lastLogIndex %d, lastLogTerm %d", rf.me, rf.state, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d %s reject vote for candidate S%d, reason args.Term < rf.currentTerm", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	myLastLogTerm := rf.getLastLogTerm()
	myLastLogIndex := rf.getLastLogIndex()
	Debug(dInfo, "S%d Term %d, voted for %d, args.LastLogTerm %d, my lastlogterm, %d ", rf.me, rf.currentTerm, rf.votedFor, args.LastLogTerm, myLastLogTerm)
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > myLastLogTerm) || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)) {
		// grant vote
		Debug(dVote, "S%d %s grant vote for candidate S%d", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimer()
	} else {
		Debug(dVote, "S%d %s reject vote for candidate S%d", rf.me, rf.state, args.CandidateId)
		reply.VoteGranted = false
	}
	rf.persist()
}

// assumes caller holds a lock
func (rf *Raft) decideIfConvertToFollower(term int) {
	if term > rf.currentTerm {
		Debug(dInfo, "S%d %s, converted from T%d to T%d", rf.me, rf.state, rf.currentTerm, term)
		rf.currentTerm = term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetTimer() {
	Debug(dTimer, "S%d %s, reset timer T%d", rf.me, rf.state, rf.currentTerm)
	rf.lastValidTime = time.Now()
}

// The electionTicker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	randomTimeout := rf.getRandomTimeout()
	time.Sleep(randomTimeout)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if time.Since(rf.lastValidTime) > randomTimeout && rf.state != "leader" {
			go func() {
				// kick off election TODO
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = "candidate"
				rf.resetTimer()
				Debug(dLeader, "S%d %s, start a new election on T%d", rf.me, rf.state, rf.currentTerm)
				Term := rf.currentTerm
				CandidateId := rf.me
				LastLogTerm := rf.getLastLogTerm()
				LastLogIndex := rf.getLastLogIndex()
				rf.persist()
				rf.mu.Unlock()
				isElected := rf.getElection(Term, CandidateId, LastLogTerm, LastLogIndex)
				rf.mu.Lock()
				if isElected && rf.state == "candidate" && Term == rf.currentTerm {
					// election success
					Debug(dLeader, "S%d %s, collected enough votes for T%d", rf.me, rf.state, rf.currentTerm)
					rf.state = "leader"
					rf.nextIndex = initSliceWithValueAndCapacity(rf.getLastLogIndex()+1, rf.totalServerNum)
					rf.matchIndex = initSliceWithValueAndCapacity(0, rf.totalServerNum)
					//Debug(dLeader, "S%d %s, re-init nextIndex to %s, matchIndex to %s on T%d", rf.me, rf.state, rf.nextIndex, rf.matchIndex, rf.currentTerm)
					go rf.leaderHeartbeatTicker()
				} else {
					// election fail
					Debug(dLeader, "S%d %s, isElected %t for T%d, current T%d", rf.me, rf.state, isElected, Term, rf.currentTerm)
				}
				rf.mu.Unlock()
			}()
		} else {
			randomTimeout = rf.getRandomTimeout()
			rf.mu.Unlock()
			time.Sleep(randomTimeout)
		}

	}
}

func (rf *Raft) getElection(Term int, CandidateId int, LastLogTerm int, LastLogIndex int) bool {
	// this condition mutex is only for the election broadcast and notify
	voteCount := 1
	finished := 1
	var condMutex sync.Mutex
	cond := sync.NewCond(&condMutex)

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			//Debug(dWarn, "S%d %s send request vote to S%d", rf.me, rf.state, peerId)
			args := &RequestVoteArgs{
				Term:         Term,
				CandidateId:  CandidateId,
				LastLogTerm:  LastLogTerm,
				LastLogIndex: LastLogIndex,
			}
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(peerId, args, reply)

			if !ok {
				Debug(dWarn, "S%d candidate, sendRequestVote RPC failed to S%d for Term %d", rf.me, peerId, Term)
			}
			condMutex.Lock()
			defer condMutex.Unlock()
			if reply.VoteGranted {
				voteCount++
			}
			finished++
			cond.Broadcast()
		}(peerId)
	}

	condMutex.Lock()
	defer condMutex.Unlock()

	for voteCount <= rf.totalServerNum/2 && finished != rf.totalServerNum {
		cond.Wait()
	}
	return voteCount > rf.totalServerNum/2
}

func (rf *Raft) getRandomTimeout() time.Duration {
	minTimeout := rf.minElectionTimeoutMilliseconds
	maxTimeout := rf.maxElectionTimeoutMilliseconds
	return time.Duration(rand.Intn(maxTimeout-minTimeout)+minTimeout) * time.Millisecond
}
