package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpName    string
	Record    KV
	RequestID int64
}

type KV struct {
	Key   string
	Value string
}

type KVServer struct {
	mu           sync.Mutex
	cond         *sync.Cond
	requests     map[int]string
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	kvDB         map[string]string
	completed    map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{"Get", KV{args.Key, " "}, args.RequestID})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	for _, ok := kv.requests[index]; ok == false; _, ok = kv.requests[index] {
		kv.cond.Wait()
	}
	ret, ok := kv.kvDB[args.Key]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		reply.Value = ret
	} else {
		reply.Err = ErrNoKey
		reply.Value = ret
	}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	index, _, isLeader := kv.rf.Start(Op{args.Op, KV{args.Key, args.Value}, args.RequestID})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println("KV: Received: Operation: Index: Server:", args.Op, index, kv.me)
	kv.mu.Lock()
	for _, ok := kv.requests[index]; ok == false; _, ok = kv.requests[index] {
		kv.cond.Wait()
	}
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *KVServer) getMessage() {
	for {
		message := <-kv.applyCh
		if message.CommandValid {
			retCommand := message.Command.(Op)
			if retCommand.OpName == "Put" {
				kv.mu.Lock()
				_, ok := kv.completed[retCommand.RequestID]
				if ok {
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				} else {
					kv.completed[retCommand.RequestID] = "Done"
					kv.kvDB[retCommand.Record.Key] = retCommand.Record.Value
					fmt.Println("KV# Put Key: Value: Server:", retCommand.Record.Key, kv.kvDB[retCommand.Record.Key], kv.me)
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				}
			}
			if retCommand.OpName == "Append" {
				kv.mu.Lock()
				_, ok := kv.completed[retCommand.RequestID]
				if ok {
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				} else {
					kv.completed[retCommand.RequestID] = "Done"
					current := kv.kvDB[retCommand.Record.Key]
					kv.kvDB[retCommand.Record.Key] = current + retCommand.Record.Value
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				}

			}
			if retCommand.OpName == "Get" {
				kv.mu.Lock()
				_, ok := kv.completed[retCommand.RequestID]
				if ok {
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				} else {
					kv.completed[retCommand.RequestID] = "Done"
					kv.requests[message.CommandIndex] = "Done"
					kv.cond.Broadcast()
					kv.mu.Unlock()
				}
			}
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.cond = sync.NewCond(&kv.mu)
	kv.requests = make(map[int]string)
	kv.completed = make(map[int64]string)
	go kv.getMessage()
	return kv
}
