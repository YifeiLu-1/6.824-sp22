package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu              sync.Mutex
	clientId        int64
	requestId       int64
	lastValidLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.lastValidLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	peerId := ck.lastValidLeader
	ck.requestId++
	requestId := ck.requestId
	ck.mu.Unlock()

	for {
		args := &GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		reply := &GetReply{}
		Debug(dClient, "Client %d Get to S%d key %s, requestId %d", ck.clientId, peerId, key, requestId)
		ok := ck.servers[peerId].Call("KVServer.Get", args, reply)
		//Debug(dClient, "Client send get %s to S%d", key, peerId)
		if ok && reply.Err == ErrNoKey {
			ck.mu.Lock()
			ck.lastValidLeader = peerId
			Debug(dClient, "Client %d Get to S%d key %s, requestId %d, success, getVal %d", ck.clientId, peerId, key, requestId, reply.Value)
			ck.mu.Unlock()
			return ""
		}
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastValidLeader = peerId
			Debug(dClient, "Client %d Get to S%d key %s, requestId %d, success, getVal %d", ck.clientId, peerId, key, requestId, reply.Value)
			ck.mu.Unlock()
			return reply.Value
		}
		peerId = (peerId + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	peerId := ck.lastValidLeader
	ck.requestId++
	requestId := ck.requestId
	ck.mu.Unlock()
	for {
		args := &PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		reply := &PutAppendReply{}
		ok := ck.servers[peerId].Call("KVServer.PutAppend", args, reply)
		Debug(dClient, "Client %d PutAppend to S%d key %s, value %s, requestId %d", ck.clientId, peerId, key, value, requestId)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastValidLeader = peerId
			Debug(dClient, "Client %d PutAppend to S%d key %s, value %s, requestId %d, success", ck.clientId, peerId, key, value, requestId)
			ck.mu.Unlock()
			return
		}
		peerId = (peerId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
