package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)
	data := w.Bytes()
	//Debug(dPersist, "raftstate size %d, logs length %d, capacity %d, snapshot size %d", len(data), len(rf.logs), cap(rf.logs), len(rf.snapshot))
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var snapshotLastIncludedIndex int
	var snapshotLastIncludedTerm int
	//var snapshot []byte
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotLastIncludedIndex) != nil ||
		d.Decode(&snapshotLastIncludedTerm) != nil {
		Debug(dPersist, "S%d %s, readPersist fail", rf.me, rf.state)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
		rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
		Debug(dPersist, "S%d %s, readPersist success, logs length %d, capacity %d, snapshot size %d", rf.me, rf.state, len(rf.logs), cap(rf.logs), len(rf.snapshot))
	}
}
