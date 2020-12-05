package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type Op struct {
	Op    OpType
	Key   string
	Value string
	ID    int64
}

type LogOp struct {
	Value string
	Error Err
}

type KVServer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	db    map[string]string
	logs  map[int64]bool
	chans map[int64]chan LogOp
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 1. Create Get Op to pass through raft
	msg := Op{
		Op:  OpGet,
		Key: args.Key,
		ID:  args.ID,
	}

	// 2. Send Op and check if current peer is leader
	_, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. Get Channel To Read/Write
	kv.mu.Lock()
	ch, ok := kv.chans[args.ID]
	if !ok {
		ch = make(chan LogOp, 1)
		kv.chans[args.ID] = ch
	}
	kv.mu.Unlock()

	// 4. Wait For Channgel Result
	select {
	case logOp := <-ch:
		reply.Err = logOp.Error
		reply.Value = logOp.Value
	case <-time.After(LogWaitTTL):
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 1. Create PutAppend Op to pass through raft
	msg := Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
		ID:    args.ID,
	}

	// 2. Send Op and check if current peer is leader
	_, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. Get Channel To Read/Write
	kv.mu.Lock()
	ch, ok := kv.chans[args.ID]
	if !ok {
		ch = make(chan LogOp, 1)
		kv.chans[args.ID] = ch
	}
	kv.mu.Unlock()

	// 4. Wait For Channgel Result
	select {
	case logOp := <-ch:
		reply.Err = logOp.Error
	case <-time.After(LogWaitTTL):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applyLogs() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh
		log := msg.Command.(Op)
		logOp := LogOp{Error: OK}

		kv.mu.Lock()
		if kv.chans[log.ID] == nil {
			kv.mu.Unlock()
			continue
		}
		if kv.logs[log.ID] {
			kv.mu.Unlock()
			continue
		}

		if log.Op == OpGet {
			logOp.Value = kv.db[log.Key]
		} else {
			val, ok := kv.db[log.Key]
			if log.Op == OpPut {
				kv.db[log.Key] = log.Value
			} else {
				if ok {
					kv.db[log.Key] = val + log.Value
				} else {
					logOp.Error = ErrNoKey
				}
			}
		}
		ch := kv.chans[log.ID]
		kv.mu.Unlock()
		ch <- logOp
		kv.logs[log.ID] = true
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
	DPrintf(serverDied("Raft Server %v: Killed"), kv.me)
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.cond = sync.NewCond(&kv.mu)
	kv.db = make(map[string]string)
	kv.logs = make(map[int64]bool)
	kv.chans = make(map[int64]chan LogOp)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf(newServer("Server %d Initialized!!!"), me)

	go kv.applyLogs()
	// You may need initialization code here.
	return kv
}
