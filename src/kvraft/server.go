package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
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

//Only required for this server
type Request struct {
	Term   int
	Index  int
	Status string
}

type KVServer struct {
	mu           sync.Mutex
	cond         *sync.Cond
	requests     map[int64]Request
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	kvDB         map[string]string
	completed    map[int64]string
	persister    *raft.Persister
	peers        []*labrpc.ClientEnd
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{"Get", KV{args.Key, " "}, args.RequestID})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.requests[args.RequestID] = Request{term, index, "running"}

	for request := kv.requests[args.RequestID]; request.Status == "running"; request = kv.requests[args.RequestID] {
		kv.cond.Wait()
	}
	if kv.requests[args.RequestID].Status == "Resubmit" {
		reply.Err = ErrWrongLeader
		delete(kv.requests, args.RequestID)
		kv.mu.Unlock()
		return
	}
	if kv.requests[args.RequestID].Status == "Done" {
		ret, ok := kv.kvDB[args.Key]
		delete(kv.requests, args.RequestID)
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
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	index, term, isLeader := kv.rf.Start(Op{args.Op, KV{args.Key, args.Value}, args.RequestID})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println("KV: Received: Operation: Index: Server:", args.Op, index, kv.me)
	kv.mu.Lock()
	kv.requests[args.RequestID] = Request{term, index, "running"}

	for request := kv.requests[args.RequestID]; request.Status == "running"; request = kv.requests[args.RequestID] {
		kv.cond.Wait()
	}
	if kv.requests[args.RequestID].Status == "Resubmit" {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		delete(kv.requests, args.RequestID)
		return
	}
	if kv.requests[args.RequestID].Status == "Done" {
		delete(kv.requests, args.RequestID)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
}

func (kv *KVServer) getMessage() {
	var oldTerm, newTerm, lastIndex int
	for {
		raftStateSize := kv.persister.RaftStateSize()
		//fmt.Println("KV: Size of snapshot: lastIndex: Server: ", raftStateSize, lastIndex, kv.me)
		if raftStateSize > kv.maxraftstate && kv.maxraftstate != -1 {
			kv.mu.Lock()
			//fmt.Println("KV: Truncate log at: Server: ", lastIndex, kv.me)
			kv.rf.TrimLog(lastIndex, kv.persist()) //TO DO: Find index of last request processed
			kv.mu.Unlock()
		}
		message := <-kv.applyCh
		if message.MessageType == 1 {
			kvBytes := message.Command.([]byte)
			kv.mu.Lock()
			kv.kvDB, kv.completed = kv.readPersist(kvBytes)
			lastIndex = message.CommandIndex
			fmt.Println("Processed Snapshot Message: Command Index: Server:", lastIndex, kv.me)
			kv.mu.Unlock()
		} else if message.MessageType == 2 {
			newTerm = message.CommandTerm
			kv.processOldRequests(oldTerm)
			oldTerm = newTerm
		} else if message.MessageType == 0 {

			if message.CommandValid {
				retCommand := message.Command.(Op)
				lastIndex = message.CommandIndex
				//fmt.Println("Command Index: Server:", lastIndex, kv.me)
				newTerm = message.CommandTerm
				if newTerm != oldTerm {
					kv.processOldRequests(oldTerm)
					oldTerm = newTerm
				}
				if retCommand.OpName == "Put" {
					kv.mu.Lock()
					_, ok := kv.completed[retCommand.RequestID]
					if ok {
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					} else {
						kv.completed[retCommand.RequestID] = "Done"
						kv.kvDB[retCommand.Record.Key] = retCommand.Record.Value
						//fmt.Println("KV# Put Key: Value: Server:", retCommand.Record.Key, kv.kvDB[retCommand.Record.Key], kv.me)
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					}
				}
				if retCommand.OpName == "Append" {
					kv.mu.Lock()
					_, ok := kv.completed[retCommand.RequestID]
					if ok {
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					} else {
						kv.completed[retCommand.RequestID] = "Done"
						current := kv.kvDB[retCommand.Record.Key]
						kv.kvDB[retCommand.Record.Key] = current + retCommand.Record.Value
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					}

				}
				if retCommand.OpName == "Get" {
					kv.mu.Lock()
					_, ok := kv.completed[retCommand.RequestID]
					if ok {
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					} else {
						kv.completed[retCommand.RequestID] = "Done"
						temp := kv.requests[retCommand.RequestID]
						temp.Status = "Done"
						kv.requests[retCommand.RequestID] = temp
						kv.cond.Broadcast()
						kv.mu.Unlock()
					}
				}
			}
		}
	}
}
func (kv *KVServer) processOldRequests(oldTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for requestID, request := range kv.requests {
		if request.Term <= oldTerm && request.Status == "running" {
			request.Status = "Resubmit"
			kv.requests[requestID] = request
			fmt.Println("KV: Request resubmitted: ID: Term:....", requestID, request.Term)
			kv.cond.Broadcast()
		}
	}
}

//Persist snapshot. persist KV and completed requestids for duplicate check
// Assumption is that lock will be provided by executing code
func (kv *KVServer) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.completed)
	data := w.Bytes()
	return data
}
func (kv *KVServer) readPersist(kvBytes []byte) (map[string]string, map[int64]string) {
	var kvDB map[string]string
	var completed map[int64]string
	r := bytes.NewBuffer(kvBytes)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvDB) != nil ||
		d.Decode(&completed) != nil {
		//errors.New("Exit due to null from reading persisted data")
		fmt.Println("Fatal Error in reading persisted KV data")
	}
	return kvDB, completed
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(KV{})
	labgob.Register(Request{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.cond = sync.NewCond(&kv.mu)
	kv.requests = make(map[int64]Request)
	kv.completed = make(map[int64]string)
	kv.persister = persister
	kv.peers = servers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.getMessage()
	return kv
}
