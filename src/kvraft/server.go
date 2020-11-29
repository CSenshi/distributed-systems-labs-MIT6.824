package kvraft

import (
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

type Op struct {
	Op  OpType
	Key string
	ID  int
}

type KVServer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	db   map[string]string
	logs map[int]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	msg := Op{
		Op:  OpGet,
		Key: args.Key,
		ID:  args.ID,
	}
	_, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !kv.logs[args.ID] {
		kv.cond.Wait()
	}

	reply.Value = kv.db[args.Key]
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	msg := Op{
		Op:  args.Op,
		Key: args.Key,
		ID:  args.ID,
	}
	_, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !kv.logs[args.ID] {
		kv.cond.Wait()
	}

	val, ok := kv.db[args.Key]
	if args.Op == OpPut {
		kv.db[args.Key] = args.Value
	} else {
		if ok {
			kv.db[args.Key] = val + args.Value
		} else {
			reply.Err = ErrNoKey
		}
	}
	DPrintf("KEY: (%v | BEFORE: %v | NOW: %v", args.Key, less(val), less(kv.db[args.Key]))
	reply.Err = OK
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

func (kv *KVServer) readRes() {
	for {
		log := <-kv.applyCh
		op := log.Command.(Op)
		kv.logs[op.ID] = true
		kv.cond.Broadcast()
	}
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
	kv.logs = make(map[int]bool)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf(newServer("Server %d Initialized!!!"), me)

	go kv.readRes()
	// You may need initialization code here.
	return kv
}
