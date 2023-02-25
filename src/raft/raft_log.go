package raft

import (
	"time"
)

func (rf *Raft) getLastLogIndex() int {
	return rf.snapshotLastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLogLength() int {
	return rf.snapshotLastIncludedIndex + len(rf.logs)
}

func (rf *Raft) isLogExistsAtIndex(index int) bool {
	return index >= rf.snapshotLastIncludedIndex
}

func (rf *Raft) getLogAtIndex(index int) Log {
	//Debug(dLog, "S%d %s, getLogAtIndex method, index %d, rf.snapshotLastIncludedIndex %d, actual index %d, logs length %d", rf.me, rf.state, index, rf.snapshotLastIncludedIndex, index-rf.snapshotLastIncludedIndex, len(rf.logs))
	return rf.logs[index-rf.snapshotLastIncludedIndex]
}

func (rf *Raft) getLogTermAtIndex(index int) int {
	actualIndex := index - rf.snapshotLastIncludedIndex
	if actualIndex < 0 {
		Debug(dLog, "S%d %s, getLogTermAtIndex method, index %d, rf.snapshotLastIncludedIndex %d, actual index %d, logs length %d", rf.me, rf.state, index, rf.snapshotLastIncludedIndex, index-rf.snapshotLastIncludedIndex, len(rf.logs))
		return rf.logs[actualIndex].Term
	}
	return rf.logs[actualIndex].Term
}

func (rf *Raft) getLogsSlice(low int, high int) []Log {
	//Debug(dLog, "S%d %s, getLogsSlice method, low %d, high %d, rf.snapshotLastIncludedIndex %d, actual low %d, actual high %d", rf.me, rf.state, low, high, rf.snapshotLastIncludedIndex, low-rf.snapshotLastIncludedIndex, high-rf.snapshotLastIncludedIndex)
	return rf.logs[low-rf.snapshotLastIncludedIndex : high-rf.snapshotLastIncludedIndex]
}

type AppendEntriesArgs struct {
	Term         int // leader’s Term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // Term of prevLogIndex entry

	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for candidate to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	ConflictingEntryTerm               int // term in the conflicting entry (if any), if none then -1
	IndexOfFirstEntryOfConflictingTerm int // index of first entry with that term (if any), if none then -1
	LogLen                             int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.decideIfConvertToFollower(args.Term)
	reply.Term = rf.currentTerm
	reply.ConflictingEntryTerm = -1
	reply.IndexOfFirstEntryOfConflictingTerm = -1
	reply.LogLen = -1
	if args.Term < rf.currentTerm {
		// Reply false if Term < currentTerm
		Debug(dInfo, "S%d %s, reply false for AppendEntry from S%d, reason args.Term T%d < rf.currentTerm %d", rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.resetTimer()
	if args.PrevLogIndex > rf.getLastLogIndex() {
		// Reply false if log is too short
		reply.Success = false
		reply.LogLen = rf.getLogLength()
		return
	}
	Debug(dLog, "S%d %s, AppendEntry from S%d, current T%d, PrevLogIndex %d, PrevLogTerm %d,  LeaderCommit %d", rf.me, rf.state, args.LeaderId, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	if !rf.isLogExistsAtIndex(args.PrevLogIndex) {
		Debug(dError, "S%d %s, AppendEntry from S%d, log at prevLogIndex is already compressed during snapshot, current T%d, PrevLogIndex %d, PrevLogTerm %d,  LeaderCommit %d", rf.me, rf.state, args.LeaderId, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		reply.Success = false
		return
	}
	myLogTermAtPrevLogIndex := rf.getLogAtIndex(args.PrevLogIndex).Term
	if myLogTermAtPrevLogIndex != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose Term matches prevLogTerm
		reply.ConflictingEntryTerm = myLogTermAtPrevLogIndex
		reply.IndexOfFirstEntryOfConflictingTerm = rf.findTheFirstEntryOfTerm(args.PrevLogIndex, myLogTermAtPrevLogIndex)
		reply.Success = false
		return
	}

	reply.Success = true
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it

	for indexInEntries := 0; indexInEntries < len(args.Entries); indexInEntries++ {
		indexToAppend := args.PrevLogIndex + 1 + indexInEntries
		logToAppend := args.Entries[indexInEntries]
		if indexToAppend <= rf.getLastLogIndex() {
			logToCompare := rf.getLogAtIndex(indexToAppend)
			//Debug(dLog, "S%d %s, indexToAppend %d, logToAppend %s, logToCompare %s", rf.me, rf.state, indexToAppend, logToAppend, logToCompare)
			if logToCompare.Term != logToAppend.Term {
				// delete the rest index
				rf.logs = rf.getLogsSlice(rf.snapshotLastIncludedIndex, indexToAppend)
				Debug(dLog, "S%d %s, delete from index %d, log size %d, capacity %d", rf.me, rf.state, indexToAppend, len(rf.logs), cap(rf.logs))
			}

		}
		// Append any new entries not already in the log
		// lastIndex may have changed
		if indexToAppend > rf.getLastLogIndex() {
			rf.logs = append(rf.logs, args.Entries[indexInEntries:len(args.Entries)]...)
			Debug(dLog, "S%d %s, append Entries from args index %d to %d, at self index %d on T%d", rf.me, rf.state, indexInEntries, len(args.Entries), indexToAppend, rf.currentTerm)
			break
		}

	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
	Debug(dCommit, "S%d %s, leaderCommit %d, commitIndex %d, index of last new entry %d", rf.me, rf.state, args.LeaderCommit, rf.commitIndex, indexOfLastNewEntry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, indexOfLastNewEntry)
		rf.applyCond.Broadcast()
		Debug(dLog, "S%d %s, update commitIndex to %d, command %s", rf.me, rf.state, rf.commitIndex, rf.getLogAtIndex(rf.commitIndex).Command)
	}

	rf.persist()
}

func (rf *Raft) findTheFirstEntryOfTerm(maxIndex int, term int) int {
	index := maxIndex

	for index > rf.snapshotLastIncludedIndex+1 && rf.getLogTermAtIndex(index-1) == term {
		index--
	}
	return index
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) applyMsgToStateMachineTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && rf.snapshotLastIncludedIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		Debug(dCommit, "S%d %s, applyLogToStateMachine awakes, commitIndex %d, lastApplied %d, T%d", rf.me, rf.state, rf.commitIndex, rf.lastApplied, rf.currentTerm)

		var applyMsgs []ApplyMsg

		if rf.snapshotLastIncludedIndex > rf.lastApplied {
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      clone(rf.snapshot),
				SnapshotTerm:  rf.snapshotLastIncludedTerm,
				SnapshotIndex: rf.snapshotLastIncludedIndex,
			}
			applyMsgs = append(applyMsgs, applyMsg)
			rf.lastApplied = rf.snapshotLastIncludedIndex
			rf.commitIndex = rf.snapshotLastIncludedIndex
		} else {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.getLogAtIndex(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				}
				applyMsgs = append(applyMsgs, applyMsg)
			}
		}

		rf.persist()
		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			if !rf.killed() {
				if applyMsg.SnapshotValid {
					Debug(dSnap, "S%d, send snapshot up, Term %d, Index %d", rf.me, applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)
				} else {
					Debug(dCommit, "S%d, send command %s up, CommandIndex %d", rf.me, applyMsg.Command, applyMsg.CommandIndex)
				}
				rf.applyCh <- applyMsg
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if len(args.Entries) > 0 && !rf.killed() {
			Debug(dLog, "S%d, send AppendEntries RPC to S%d, args: term %d, prevLogIndex %d, prevLogTerm %d, leaderCommit %d, reply: term %d, success %t, ConflictingEntryTerm %d, IndexOfFirstEntryOfConflictingTerm %d, LogLen %d", args.LeaderId, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, reply.Term, reply.Success, reply.ConflictingEntryTerm, reply.IndexOfFirstEntryOfConflictingTerm, reply.LogLen)
		}
	} else if !rf.killed() {
		//Debug(dLog, "S%d, send AppendEntries RPC to S%d, args: term %d, prevLogIndex %d, prevLogTerm %d, leaderCommit %d, RPC failed", args.LeaderId, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	}
	return ok
}

func (rf *Raft) leaderHeartbeatTicker() {
	isLeader := true
	for rf.killed() == false && isLeader {
		rf.mu.Lock()
		isLeader = rf.state == "leader"
		rf.mu.Unlock()
		for peerId := 0; peerId < len(rf.peers); peerId++ {
			go func(peerId int) {
				rf.sendEntriesToPeer(peerId)
			}(peerId)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendEntriesToPeer(peerId int) {
	rf.mu.Lock()
	if peerId == rf.me || rf.state != "leader" {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peerId] > rf.snapshotLastIncludedIndex {
		rf.sendAppendEntriesToPeer(peerId)
		return
	}
	rf.sendInstallSnapshotToPeer(peerId)
}

// lock is hold
func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	peerNextIndex := rf.nextIndex[peerId]
	Term := rf.currentTerm
	LeaderId := rf.me
	PrevLogIndex := peerNextIndex - 1
	low := peerNextIndex
	high := rf.getLastLogIndex() + 1
	//Debug(dInfo, "S%d %s, AppendEntries RPC slice low %d high %d for S%d", rf.me, rf.state, low, high, peerId)
	entries := rf.getLogsSlice(low, high)

	entriesCopied := make([]Log, len(entries))
	copy(entriesCopied, entries)

	args := &AppendEntriesArgs{
		Term:         Term,
		LeaderId:     LeaderId,
		PrevLogIndex: PrevLogIndex,
		PrevLogTerm:  rf.getLogTermAtIndex(PrevLogIndex),
		Entries:      entriesCopied,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	//Debug(dInfo, "S%d %s, AppendEntries RPC to S%d, args %s", rf.me, rf.state, peerId, args)
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peerId, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.killed() {
		return
	}
	rf.decideIfConvertToFollower(reply.Term)
	if Term != rf.currentTerm || rf.state != "leader" {
		return
	}
	if !reply.Success {
		// update nextIndex and retry
		if reply.LogLen > 0 {
			rf.nextIndex[peerId] = reply.LogLen
		} else {
			ConflictingEntryTerm := reply.ConflictingEntryTerm
			if rf.isLogHasTerm(args.PrevLogIndex, ConflictingEntryTerm) {
				// leader's last entry for XTerm
				rf.nextIndex[peerId] = rf.findTheLastEntryOfTerm(args.PrevLogIndex, ConflictingEntryTerm)
			} else {
				rf.nextIndex[peerId] = reply.IndexOfFirstEntryOfConflictingTerm
			}
		}
		if rf.nextIndex[peerId] <= rf.matchIndex[peerId] {
			rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		}
		Debug(dLog, "S%d %s, AppendEntries RPC PrevLogIndex %d to S%d for T%d, entries length %d, reply fail, decrement nextIndex to %d, matchIndex is %d", rf.me, rf.state, PrevLogIndex, peerId, Term, len(entriesCopied), rf.nextIndex[peerId], rf.matchIndex[peerId])
		return
	}
	//Debug(dLog, "S%d %s, append entry rpc PrevLogIndex %d to S%d for T%d, entries length %d, reply success", rf.me, rf.state, PrevLogIndex, peerId, Term, len(entries))

	// success
	rf.matchIndex[peerId] = max(PrevLogIndex+len(entriesCopied), rf.matchIndex[peerId])
	rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
	rf.updateLeaderCommitIndex(rf.matchIndex[peerId])
}

func (rf *Raft) isLogHasTerm(maxIndex int, term int) bool {
	index := maxIndex
	for index > rf.snapshotLastIncludedIndex && rf.getLogTermAtIndex(index) != term {
		index--
	}
	return index > rf.snapshotLastIncludedIndex
}

func (rf *Raft) findTheLastEntryOfTerm(maxIndex int, term int) int {
	index := maxIndex
	for index > rf.snapshotLastIncludedIndex+1 && rf.getLogTermAtIndex(index-1) != term {
		index--
	}
	return index
}

func (rf *Raft) updateLeaderCommitIndex(proposedNewMatchIndex int) {
	//Debug(dCommit, "S%d %s, propose update index %d, commit index is %d", rf.me, rf.state, proposedNewMatchIndex, rf.commitIndex)
	if proposedNewMatchIndex <= rf.commitIndex {
		return
	}
	// figure 8

	if rf.getLogAtIndex(proposedNewMatchIndex).Term != rf.currentTerm {
		Debug(dCommit, "S%d %s, propose to update leaderCommitIndex to %d, current commitIndex is %d, proposedLogTerm %d, currentTerm %d", rf.me, rf.state, proposedNewMatchIndex, rf.commitIndex, rf.getLogAtIndex(proposedNewMatchIndex).Term, rf.currentTerm)
		return
	}

	num := 1
	for peerId, matchIndex := range rf.matchIndex {
		//Debug(dCommit, "S%d %s, propose update index %d, commit index is %d, matchIndex %d for peer%d", rf.me, rf.state, proposedNewMatchIndex, rf.commitIndex, matchIndex, peerId)
		if peerId == rf.me {
			continue
		}
		if matchIndex >= proposedNewMatchIndex {
			num++
		}
	}
	isMajority := num > rf.totalServerNum/2
	if !isMajority {
		return
	}
	//Debug(dCommit, "S%d %s, propose update index %d, majority collected", rf.me, rf.state, proposedNewMatchIndex)
	if rf.getLogAtIndex(proposedNewMatchIndex).Term == rf.currentTerm {
		rf.commitIndex = proposedNewMatchIndex
		rf.applyCond.Broadcast()
		Debug(dCommit, "S%d %s, update commitIndex to %d", rf.me, rf.state, rf.commitIndex)
	}
}
