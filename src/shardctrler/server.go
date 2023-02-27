package shardctrler

import (
	"6.824/raft"
	"6.824/util"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	dead        int32 // set by Kill()
	applyCond   *sync.Cond
	commitIndex int

	// Your data here.

	configs []*Config // indexed by config num

	requestResults map[int64]map[int64]CommonReply
	// {
	// 		clientId: {
	//			requestId: response
	//		}
	// }
	highestRequestReceived map[int64]int64
}

const (
	JoinOp  = "JoinOp"
	LeaveOp = "LeaveOp"
	MoveOp  = "MoveOp"
	QueryOp = "QueryOp"
)

type OpType string

type Op struct {
	OpType    OpType
	ClientId  int64
	RequestId int64

	Servers map[int][]string // join

	GIDs []int // leave

	Shard int // move
	GID   int // move

	Num int // query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		op := Op{
			OpType:    JoinOp,
			ClientId:  clientId,
			RequestId: requestId,
			Servers:   args.Servers,
		}
		return op
	}
	response, isLeader := sc.CommonOp(getOp, clientId, requestId)
	reply.WrongLeader = !isLeader
	if !isLeader {
		reply.Err = OK
		return
	}
	reply.Err = response.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		op := Op{
			OpType:    LeaveOp,
			ClientId:  clientId,
			RequestId: requestId,
			GIDs:      args.GIDs,
		}
		return op
	}
	response, isLeader := sc.CommonOp(getOp, clientId, requestId)
	reply.WrongLeader = !isLeader
	if !isLeader {
		reply.Err = OK
		return
	}
	reply.Err = response.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		op := Op{
			OpType:    MoveOp,
			ClientId:  clientId,
			RequestId: requestId,
			Shard:     args.Shard,
			GID:       args.GID,
		}
		return op
	}
	response, isLeader := sc.CommonOp(getOp, clientId, requestId)
	reply.WrongLeader = !isLeader
	if !isLeader {
		reply.Err = OK
		return
	}
	reply.Err = response.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	clientId := args.ClientId
	requestId := args.RequestId
	getOp := func() Op {
		op := Op{
			OpType:    QueryOp,
			ClientId:  clientId,
			RequestId: requestId,
			Num:       args.Num,
		}
		return op
	}
	response, isLeader := sc.CommonOp(getOp, clientId, requestId)
	reply.WrongLeader = !isLeader
	if !isLeader {
		reply.Err = OK
		return
	}

	reply.Err = response.Err

	sc.mu.Lock()
	defer sc.mu.Unlock()
	newConfig := sc.configs[response.ConfigNum]
	util.Debug(util.DClient, "S%d shardctrler, query result, config before copy, num %d, shards %s, groups %s, sc configs %s", sc.me, newConfig.Num, newConfig.Shards, newConfig.Groups, sc.configs)
	reply.Config = *(newConfig.makeNewCopy())
	util.Debug(util.DClient, "S%d shardctrler, query result, config after copy, num %d, shards %s, groups %s, sc configs %s", sc.me, reply.Config.Num, reply.Config.Shards, reply.Config.Groups, sc.configs)
}

func (sc *ShardCtrler) CommonOp(getOp func() Op, clientId int64, requestId int64) (CommonReply, bool) {
	response, isLeader, isSuccess := sc.CommonOpOnce(getOp, clientId, requestId)
	for {
		if !isLeader || isSuccess {
			break
		}
		response, isLeader, isSuccess = sc.CommonOpOnce(getOp, clientId, requestId)
	}

	return response, isLeader
}

// CommonGetPutAppendOnce return the response, isLeader, isSuccess
func (sc *ShardCtrler) CommonOpOnce(getOp func() Op, clientId int64, requestId int64) (CommonReply, bool, bool) {
	op := getOp()

	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		util.Debug(util.DClient, "S%d kvraft Start, index %d, isLeader %t, opType %s", sc.me, index, isLeader, op.OpType)
	}
	if !isLeader {
		return CommonReply{}, false, false
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// check if this request is already finished
	response, isFinished := sc.requestStatus(clientId, requestId)

	if isFinished {
		util.Debug(util.DClient, "S%d kvraft CommonGetPutAppend return on cached response", sc.me, response)
		return response, isLeader, true
	}

	startTime := time.Now()

	for sc.commitIndex < index && time.Since(startTime) < time.Second {
		sc.applyCond.Wait()
	}

	response, ok := sc.requestStatus(clientId, requestId)
	util.Debug(util.DClient, "S%d kvraft awakes, kv.commitIndex %d, index %d, response %s", sc.me, sc.commitIndex, index, response)
	return response, true, ok
}

// return the response and whether the response is already cached
func (sc *ShardCtrler) requestStatus(clientId int64, requestId int64) (CommonReply, bool) {
	_, ok := sc.requestResults[clientId]
	if !ok {
		// this is the first time this client send a request
		sc.requestResults[clientId] = map[int64]CommonReply{}
	}
	response, ok2 := sc.requestResults[clientId][requestId]
	return response, ok2
}

func (sc *ShardCtrler) setHighestRequestReceived(clientId int64, requestId int64) {
	util.Debug(util.DClient, "S%d kvraft setHighestRequestReceived, clientId %d, requestId %d", sc.me, clientId, requestId)
	previousHighestRequestId, ok := sc.highestRequestReceived[clientId]
	if !ok {
		sc.highestRequestReceived[clientId] = requestId
	} else {
		if requestId > previousHighestRequestId {
			sc.highestRequestReceived[clientId] = requestId
			delete(sc.requestResults[clientId], previousHighestRequestId)
		}
	}

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]*Config, 1)
	sc.configs[0] = initConfig()

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	// You may need initialization code here.
	sc.applyCond = sync.NewCond(&sc.mu)
	sc.commitIndex = 0

	sc.requestResults = map[int64]map[int64]CommonReply{}
	sc.highestRequestReceived = map[int64]int64{}

	go sc.applyChListener()

	go func() {
		// wake the cond in case the last log can't commit due to term mismatch leader's term
		for !sc.killed() {
			sc.applyCond.Broadcast()
			time.Sleep(1 * time.Second)
		}
	}()

	return sc
}

func (sc *ShardCtrler) applyChListener() {
	// Your code here.
	for applyMsg := range sc.applyCh {
		sc.mu.Lock()
		if applyMsg.CommandValid {
			util.Debug(util.DClient, "S%d kvraft received applyMsg command, index %d", sc.me, applyMsg.CommandIndex)
			sc.commitIndex = applyMsg.CommandIndex
			if applyMsg.Command != nil {
				op := applyMsg.Command.(Op)
				sc.applyOpAndCacheResponse(&op)
			}

		}
		sc.applyCond.Broadcast()
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyOpAndCacheResponse(op *Op) {
	clientId := op.ClientId
	requestId := op.RequestId

	if requestId <= sc.highestRequestReceived[clientId] {
		return
	}

	_, isFinished := sc.requestStatus(clientId, requestId)
	if isFinished {
		return
	}
	response := CommonReply{}
	response.Err = OK
	lastConfigIndex := len(sc.configs) - 1
	if op.OpType == QueryOp {
		num := op.Num
		if num == -1 || num > lastConfigIndex {
			response.ConfigNum = lastConfigIndex
		} else {
			response.ConfigNum = num
		}

		util.Debug(util.DClient, "S%d query success, reply configNum %d", sc.me, response.ConfigNum)
	} else {
		newConfig := sc.configs[lastConfigIndex].makeNewCopy()
		newConfig.incrementConfigNum()

		if op.OpType == JoinOp {
			newConfig = newConfig.join(op.Servers)
		} else if op.OpType == LeaveOp {
			newConfig = newConfig.leave(op.GIDs)
		} else if op.OpType == MoveOp {
			newConfig = newConfig.move(op.Shard, op.GID)
		}
		util.Debug(util.DClient, "S%d shardctrler, newConfig after %s, num %d, shards %s, groups %s", sc.me, op.OpType, newConfig.Num, newConfig.Shards, newConfig.Groups)
		sc.configs = append(sc.configs, newConfig)
	}
	sc.setHighestRequestReceived(clientId, requestId-1)
	sc.requestResults[clientId][requestId] = response
}
