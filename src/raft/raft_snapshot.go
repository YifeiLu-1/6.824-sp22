package raft

type InstallSnapshotArgs struct {
	Term              int // leader’s Term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex

	Data []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for candidate to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshotLastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.resetTimer()
	rf.snapshot = args.Data
	if args.LastIncludedIndex < rf.getLastLogIndex() {
		// throw away logs before the index
		rf.logs = rf.logs[args.LastIncludedIndex-rf.snapshotLastIncludedIndex:]
	} else {
		rf.logs = []Log{
			{
				Term: args.LastIncludedTerm,
			},
		}
	}
	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm

	rf.persist()
	rf.applyCond.Broadcast()
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

// rf.nextIndex[peerId] <= rf.snapshotLastIncludedIndex
// lock is hold
func (rf *Raft) sendInstallSnapshotToPeer(peerId int) {
	Term := rf.currentTerm
	LeaderId := rf.me
	// need to send snapshot
	args := &InstallSnapshotArgs{
		Term:              Term,
		LeaderId:          LeaderId,
		LastIncludedIndex: rf.snapshotLastIncludedIndex,
		LastIncludedTerm:  rf.snapshotLastIncludedTerm,
		Data:              clone(rf.snapshot),
	}
	reply := &InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(peerId, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		// retry
		Debug(dSnap, "S%d %s, InstallSnapshot to S%d for T%d RPC failed", LeaderId, rf.state, peerId, Term)
		return
	}
	Debug(dSnap, "S%d %s, InstallSnapshot to S%d for T%d success, reply T%d", LeaderId, rf.state, peerId, Term, reply.Term)
	rf.decideIfConvertToFollower(reply.Term)
	rf.persist()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d %s, snapshot called", rf.me, rf.state)
	if index <= rf.snapshotLastIncludedIndex {
		Debug(dSnap, "S%d %s, snapshot called, index %d <= rf.snapshotLastIncludedIndex %d, dropping", rf.me, rf.state, index, rf.snapshotLastIncludedIndex)
		return
	}

	rf.snapshotLastIncludedTerm = rf.getLogAtIndex(index).Term
	rf.snapshot = snapshot
	// throw away logs before the index
	rf.logs = rf.logs[index-rf.snapshotLastIncludedIndex:]
	rf.snapshotLastIncludedIndex = index

	rf.persist()
}
