package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	nextID  int
	leader  int
	id      int64
	// You will have to modify this struct.
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
	ck.nextID = 0
	ck.leader = 0
	ck.id = time.Now().UnixNano()
	// You'll have to add code here.
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
	args := GetArgs{
		Key: key,
		ID:  time.Now().UnixNano(),
		CID: ck.id,
	}

	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrWrongLeader {
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			DPrintf(teal("Get: (%v) --> \"%v\""), key, less(reply.Value))
			if i != ck.leader {
				DPrintf(green("New Leader! %v --> %v"), ck.leader, i)
			}
			ck.leader = i
			return reply.Value
		}
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
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ID:    time.Now().UnixNano(),
		CID:   ck.id,
	}
	for i := ck.leader; ; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrWrongLeader {
			continue
		}
		if reply.Err == OK {
			DPrintf(teal("%v: (%v, \"%v\")"), op, key, value)
			if i != ck.leader {
				DPrintf(green("New Leader! %v --> %v"), ck.leader, i)
			}
			ck.leader = i
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
