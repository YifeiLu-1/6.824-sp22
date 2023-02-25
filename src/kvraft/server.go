package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GetOp    = "GetOp"
	PutOp    = "PutOp"
	AppendOp = "AppendOp"
)

type OpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVTable     map[string]string
	commitIndex int
	applyCond   *sync.Cond

	requestResults map[int64]map[int64]CommonGetPutAppendReply
	// {
	// 		clientId: {
	//			requestId: response
	//		}
	// }
	highestRequestReceived map[int64]int64
	// { clientId: highestReceivedRequestId }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		op := Op{
			OpType:    GetOp,
			Key:       args.Key,
			ClientId:  clientId,
			RequestId: requestId,
		}
		return op
	}
	response, isLeader := kv.CommonGetPutAppend(getOp, clientId, requestId)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = response.Err
	reply.Value = response.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		var opType OpType
		if args.Op == "Put" {
			opType = PutOp
		} else if args.Op == "Append" {
			opType = AppendOp
		}
		// Your code here.
		op := Op{
			OpType:    opType,
			Key:       args.Key,
			Value:     args.Value,
			ClientId:  clientId,
			RequestId: requestId,
		}
		return op
	}
	response, isLeader := kv.CommonGetPutAppend(getOp, clientId, requestId)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = response.Err

}

// CommonGetPutAppend return the response, isLeader
func (kv *KVServer) CommonGetPutAppend(getOp func() Op, clientId int64, requestId int64) (CommonGetPutAppendReply, bool) {
	response, isLeader, isSuccess := kv.CommonGetPutAppendOnce(getOp, clientId, requestId)
	for {
		if !isLeader || isSuccess {
			break
		}
		response, isLeader, isSuccess = kv.CommonGetPutAppendOnce(getOp, clientId, requestId)
	}

	return response, isLeader
}

// CommonGetPutAppendOnce return the response, isLeader, isSuccess
func (kv *KVServer) CommonGetPutAppendOnce(getOp func() Op, clientId int64, requestId int64) (CommonGetPutAppendReply, bool, bool) {
	op := getOp()

	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		Debug(dClient, "S%d kvraft Start, index %d, isLeader %t, opType %s", kv.me, index, isLeader, GetOp)
	}
	if !isLeader {
		return CommonGetPutAppendReply{}, false, false
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if this request is already finished
	response, isFinished := kv.requestStatus(clientId, requestId)

	if isFinished {
		Debug(dClient, "S%d kvraft CommonGetPutAppend return on cached response", kv.me, response)
		return response, isLeader, true
	}

	startTime := time.Now()

	for kv.commitIndex < index && time.Since(startTime) < time.Second {
		kv.applyCond.Wait()
	}

	response, ok := kv.requestStatus(clientId, requestId)
	Debug(dClient, "S%d kvraft awakes, kv.commitIndex %d, index %d, response %s", kv.me, kv.commitIndex, index, response)
	return response, true, ok
}

// return the response and whether the response is already cached
func (kv *KVServer) requestStatus(clientId int64, requestId int64) (CommonGetPutAppendReply, bool) {
	_, ok := kv.requestResults[clientId]
	if !ok {
		// this is the first time this client send a request
		kv.requestResults[clientId] = map[int64]CommonGetPutAppendReply{}
	}
	response, ok2 := kv.requestResults[clientId][requestId]
	return response, ok2
}

func (kv *KVServer) setHighestRequestReceived(clientId int64, requestId int64) {
	Debug(dClient, "S%d kvraft setHighestRequestReceived, clientId %d, requestId %d", kv.me, clientId, requestId)
	previousHighestRequestId, ok := kv.highestRequestReceived[clientId]
	if !ok {
		kv.highestRequestReceived[clientId] = requestId
	} else {
		if requestId > previousHighestRequestId {
			kv.highestRequestReceived[clientId] = requestId
			delete(kv.requestResults[clientId], previousHighestRequestId)
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(CommonGetPutAppendReply{})

	Debug(dClient, "S%d kvraft starts", me)

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applyCond = sync.NewCond(&kv.mu)
	kv.commitIndex = 0

	kv.KVTable = map[string]string{}
	kv.requestResults = map[int64]map[int64]CommonGetPutAppendReply{}
	kv.highestRequestReceived = map[int64]int64{}

	go kv.applyChListener()

	go func() {
		// wake the cond in case the last log can't commit due to term mismatch leader's term
		for !kv.killed() {
			kv.applyCond.Broadcast()
			time.Sleep(1 * time.Second)
		}
	}()

	return kv
}

func (kv *KVServer) applyChListener() {
	// Your code here.
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()

		if applyMsg.SnapshotValid {
			Debug(dClient, "S%d kvraft received applyMsg snapshot, index %d", kv.me, applyMsg.SnapshotIndex)
			kv.commitIndex = applyMsg.SnapshotIndex
			kv.readSnapshot(applyMsg.Snapshot)
		} else {
			Debug(dClient, "S%d kvraft received applyMsg command, index %d", kv.me, applyMsg.CommandIndex)
			kv.commitIndex = applyMsg.CommandIndex
			if applyMsg.Command != nil {
				op := applyMsg.Command.(Op)
				kv.applyOpAndCacheResponse(&op)
			}
			kv.decideIfSaveStateToSnapshot(applyMsg.CommandIndex)
		}
		kv.applyCond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOpAndCacheResponse(op *Op) {
	clientId := op.ClientId
	requestId := op.RequestId

	if requestId <= kv.highestRequestReceived[clientId] {
		return
	}

	_, isFinished := kv.requestStatus(clientId, requestId)
	if isFinished {
		return
	}
	response := CommonGetPutAppendReply{}
	if op.OpType == GetOp {

		value, ok := kv.KVTable[op.Key]
		if ok {
			response.Err = OK
			response.Value = value
		} else {
			response.Err = ErrNoKey
			response.Value = ""
		}
		Debug(dClient, "S%d process applyMsg command, clientId %d, requestId %d, response %s", kv.me, clientId, requestId, response)
		kv.setHighestRequestReceived(clientId, requestId-1)
		kv.requestResults[clientId][requestId] = response
		return
	} else {
		response.Err = OK
		if op.OpType == PutOp {
			kv.KVTable[op.Key] = op.Value
		} else {
			kv.KVTable[op.Key] = kv.KVTable[op.Key] + op.Value
		}
		Debug(dClient, "S%d process applyMsg command, clientId %d, requestId %d, response %s", kv.me, clientId, requestId, response)
		kv.setHighestRequestReceived(clientId, requestId-1)
		kv.requestResults[clientId][requestId] = response
		return
	}
}

func (kv *KVServer) decideIfSaveStateToSnapshot(commitIndex int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KVTable)
	e.Encode(kv.highestRequestReceived)
	e.Encode(kv.requestResults)

	data := w.Bytes()

	kv.rf.Snapshot(commitIndex, data)
}

func (kv *KVServer) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var KVTable map[string]string
	var requestResults map[int64]map[int64]CommonGetPutAppendReply
	var highestRequestReceived map[int64]int64

	isDecodeKVTableSuccess := d.Decode(&KVTable)
	isDecodehighestRequestReceivedSuccess := d.Decode(&highestRequestReceived)
	isDecodeRequestResultsSuccess := d.Decode(&requestResults)

	if isDecodeKVTableSuccess != nil ||
		isDecodeRequestResultsSuccess != nil ||
		isDecodehighestRequestReceivedSuccess != nil {
		Debug(dClient, "S%d, kvraft read snapshot failed, KVtable success %t, requestresults success %t, highest success %d", kv.me, isDecodeKVTableSuccess, isDecodeRequestResultsSuccess, isDecodehighestRequestReceivedSuccess)
	} else {
		kv.KVTable = KVTable
		kv.requestResults = requestResults
		kv.highestRequestReceived = highestRequestReceived
	}
}
