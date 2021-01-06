package kvraft

import (
	"bytes"
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
	CID   int64
}

type LogOp struct {
	Value string
	Error Err
	ID    int64
	CID   int64
}

type KVServer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	db        map[string]string
	logs      map[int64]int64
	chans     map[int]chan LogOp
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 1. Create Get Op to pass through raft
	msg := Op{
		Op:  OpGet,
		Key: args.Key,
		ID:  args.ID,
		CID: args.CID,
	}

	// 2. Send Op and check if current peer is leader
	index, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. Get Channel To Read/Write
	kv.mu.Lock()
	ch, ok := kv.chans[index]
	if !ok {
		ch = make(chan LogOp, 1)
		kv.chans[index] = ch
	}
	kv.mu.Unlock()

	// 4. Wait For Channel Result
	select {
	case logOp := <-ch:
		if logOp.CID == msg.CID && logOp.ID == msg.ID {
			reply.Value = logOp.Value
			reply.Err = logOp.Error
		}
	case <-time.After(LogWaitTTL):
		DPrintf(red("[%v] Log Chanel Time Out!"), kv.me)
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
		CID:   args.CID,
	}

	// 2. Send Op and check if current peer is leader
	index, _, isLeader := kv.rf.Start(msg)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. Get Channel To Read/Write
	kv.mu.Lock()
	ch, ok := kv.chans[index]
	if !ok {
		ch = make(chan LogOp, 1)
		kv.chans[index] = ch
	}
	kv.mu.Unlock()

	// 4. Wait For Channel Result
	select {
	case logOp := <-ch:
		if logOp.CID == msg.CID && logOp.ID == msg.ID {
			reply.Err = logOp.Error
		}
	case <-time.After(LogWaitTTL):
		DPrintf(red("[%v] Log Chanel Time Out!"), kv.me)
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applyLogs() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if !msg.CommandValid {
			kv.decodeSnapshot(msg.Command.([]byte))
			continue
		}

		log := msg.Command.(Op)
		logOp := LogOp{Error: OK, CID: log.CID, ID: log.ID}

		kv.mu.Lock()
		if kv.logs[log.CID] < log.ID {
			if log.Op == OpPut {
				kv.db[log.Key] = log.Value
			}
			if log.Op == OpAppend {
				kv.db[log.Key] += log.Value
			}
			kv.logs[log.CID] = log.ID
		}
		if log.Op == OpGet {
			val, ok := kv.db[log.Key]
			logOp.Value = val
			if !ok {
				logOp.Error = ErrNoKey
			}
		}
		ch, ok := kv.chans[msg.CommandIndex]
		if !ok {
			ch = make(chan LogOp, 1)
			kv.chans[msg.CommandIndex] = ch
		}

		// Check if it's time for compaction and if so take snapshot of current raft state
		if kv.maxraftstate != noMaxState && float32(kv.persister.RaftStateSize()) >= float32(kv.maxraftstate)*0.7 {
			go kv.rf.SnapshotRaftState(msg.CommandIndex, kv.encodeSnapshot())
		}

		kv.mu.Unlock()
		ch <- logOp
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

func (kv *KVServer) encodeSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	encoder.Encode(kv.db)
	encoder.Encode(kv.logs)

	return buffer.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. Decode
	var dbSnap map[string]string
	var logsSnap map[int64]int64

	decoder := labgob.NewDecoder(bytes.NewBuffer(data))

	if decoder.Decode(&dbSnap) != nil || decoder.Decode(&logsSnap) != nil {
		// if we reach this branch, then something went wrong while decoding, we should exit and not write invalid data to memory
		return
	}

	// 2. Write decoded data in the struct memory
	kv.db = dbSnap
	kv.logs = logsSnap
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
	// 1. call labgob.Register on structures you want Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// 2. Init kvServer instance variables
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.cond = sync.NewCond(&kv.mu)
	kv.db = make(map[string]string)
	kv.logs = make(map[int64]int64)
	kv.chans = make(map[int]chan LogOp)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// 3. Read snapshot data
	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	// 4. Run logs applier/commiter go routine
	go kv.applyLogs()

	DPrintf(newServer("Server %d Initialized!!!"), me)
	return kv
}
