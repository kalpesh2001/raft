package shardmaster

import "../raft"
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu        sync.Mutex
	cond      *sync.Cond
	me        int
	peers     []*labrpc.ClientEnd
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	configNum int
	configs   []Config // indexed by config num
	requests  map[int64]Request
	completed map[int64]string
}

type Op struct {
	// Your data here.
	OpName    string
	Data      Config
	ConfigNum int
	RequestID int64
}
type Request struct {
	Term   int
	Index  int
	Status string
}

//TO DO: Make sure we are using copy of Maps and Arrays
// Decide how to handle update of Config based on consensus from Raft. Should we maintain index for each server.
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var input [NShards]int
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newConfig := merge(sm.getConfig(sm.configNum), args.Servers)
	keys := make([]int, 1)
	for k := range newConfig {
		keys = append(keys, k)
	}
	allocate(input, keys)
	sm.configNum++
	// will not append until received as message from Raft
	//sm.configs = append(sm.configs, Config{sm.configNum, input, newConfig})
	index, term, isLeader := sm.rf.Start(Op{"Join", Config{sm.configNum, input, newConfig}, sm.configNum, args.RequestID})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.mu.Unlock()
		return
	}
	sm.requests[args.RequestID] = Request{term, index, "running"}
	for request := sm.requests[args.RequestID]; request.Status == "running"; request = sm.requests[args.RequestID] {
		sm.cond.Wait()
	}
	if sm.requests[args.RequestID].Status == "Resubmit" {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		delete(sm.requests, args.RequestID)
		sm.mu.Unlock()
		return
	}
	if sm.requests[args.RequestID].Status == "Done" {
		delete(sm.requests, args.RequestID)
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var input [NShards]int //array of shards
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newConfig := remove(sm.getConfig(sm.configNum), args.GIDs)
	keys := make([]int, 1)
	for k := range newConfig {
		keys = append(keys, k)
	}
	allocate(input, keys)
	sm.configNum++
	index, term, isLeader := sm.rf.Start(Op{"Leave", Config{sm.configNum, input, newConfig}, sm.configNum, args.RequestID})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.mu.Unlock()
		return
	}
	sm.requests[args.RequestID] = Request{term, index, "running"}
	for request := sm.requests[args.RequestID]; request.Status == "running"; request = sm.requests[args.RequestID] {
		sm.cond.Wait()
	}
	if sm.requests[args.RequestID].Status == "Resubmit" {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		delete(sm.requests, args.RequestID)
		sm.mu.Unlock()
		return
	}
	if sm.requests[args.RequestID].Status == "Done" {
		delete(sm.requests, args.RequestID)
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var shards [NShards]int
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i := 0; i < len(shards); i++ {
		shards[i] = sm.configs[sm.configNum].Shards[i]
	}
	shards[args.Shard] = args.GID           //TO DO: Does this change affect other parts of config
	newGroups := sm.getConfig(sm.configNum) //TO DO: Create copy of it
	sm.configNum++
	index, term, isLeader := sm.rf.Start(Op{"Move", Config{sm.configNum, shards, newGroups}, sm.configNum, args.RequestID})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sm.mu.Unlock()
		return
	}
	sm.requests[args.RequestID] = Request{term, index, "running"}
	for request := sm.requests[args.RequestID]; request.Status == "running"; request = sm.requests[args.RequestID] {
		sm.cond.Wait()
	}
	if sm.requests[args.RequestID].Status == "Resubmit" {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		delete(sm.requests, args.RequestID)
		sm.mu.Unlock()
		return
	}
	if sm.requests[args.RequestID].Status == "Done" {
		delete(sm.requests, args.RequestID)
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.peers = servers
	sm.configNum = 0
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	//TO DO: implement getMessage loop
	// Your code here.
	return sm
}

//Merge the latest configuration with slice provided by the client.
func merge(source, slice map[int][]string) map[int][]string {
	target := make(map[int][]string)
	for k, v := range source {
		val, ok := slice[k]
		if ok {
			target[k] = val
		} else {
			target[k] = v
		}
	}
	for k, v := range slice {
		_, ok := source[k]
		if !ok {
			target[k] = v
		}
	}
	return target
}

func remove(source map[int][]string, slice []int) map[int][]string {
	target := make(map[int][]string)
	for k, v := range source {
		target[k] = v
	}
	for i := 0; i < len(slice); i++ {
		_, ok := target[i]
		if ok {
			delete(target, i)
		}
	}
	return target
}

//Allocate Shards to groups using round robin
func allocate(input [NShards]int, groups []int) {
	for i := 0; i < len(input); i++ {
		input[i] = groups[i%len(groups)]
	}
}

//TO DO: We may have to create a copy of it????
func (sm *ShardMaster) getConfig(number int) map[int][]string {
	for i := 0; i < len(sm.configs); i++ {
		if sm.configs[i].Num == number {
			return sm.configs[i].Groups
		}
	}
	return nil
}
