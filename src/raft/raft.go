package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	//"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	Follower  State = iota //0
	Candidate State = iota //1
	Leader    State = iota //2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int //added for lab 3A to indicate term of execution
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	currentTerm     int                 //latest term server has been initialized to
	votedFor        int                 // candidateID that received vote in current term (or null if none)
	log             []LogEntry          //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex     int                 // Index of highest log entry known to be commited
	lastApplied     int                 // Index of highest log entry applied to state machine
	peerStatus      State               // 0 = follower; 1 = candidate; 2 = leader
	validBeat       bool
	electionTimeOut int
	nextIndex       []int //for each server, index of the next log entry to send to that server
	matchIndex      []int //for each server, index of the highest log entry known to be replicated on server
	myCh            chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

var serverCount int //Number of servers. Declared to prevent locking
var heartBeat int = 1000 / 10

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.peerStatus == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//Assumption: Persist is called when rf is locked
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
// Your code here (2C).
// Example:
// r := bytes.NewBuffer(data)
// d := labgob.NewDecoder(r)
// var xxx
// var yyy
// if d.Decode(&xxx) != nil ||
//    d.Decode(&yyy) != nil {
//   error...
// } else {
//   rf.xxx = xxx
//   rf.yyy = yyy
// }
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//errors.New("Exit due to null from reading persisted data")
		fmt.Println("Fatal Error in reading persisted data")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // idex of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool //success of rpc

}

//AppendEntries RPC handler. Server response when called by another server (leader?). also used as heartbeat

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("Called AppendEntries:", args.LeaderID)
	if args.Term < rf.currentTerm { //heartbeat scenario
		reply.Success = false
		reply.Term = rf.currentTerm //not resetting clock for reelection
		//fmt.Println("Received Invalid AppendEntry for Server: term: Status:", rf.me, rf.currentTerm, rf.peerStatus)
		//fmt.Println("Leaders ID: Term:", args.LeaderID, args.Term)
		return

	} else if args.Entries == nil && len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		//fmt.Println("Heart beat received: Case 1:", rf.me)
		rf.persist()
		return

	} else if args.Entries == nil && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		//fmt.Println("Heart beat received: Case 2:", rf.me)
		rf.persist()
		return
	} else if args.Entries == nil && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		if args.LeaderCommit > rf.commitIndex {
			//fmt.Println("Heartbeat: Leadercommit: CommitIndex, lastApplied: Length:", args.LeaderCommit, rf.commitIndex, rf.lastApplied, len(rf.log)-1)
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			go rf.commitApplier()
		}
		//fmt.Println("Heart beat received: Case 3:", rf.me)
		rf.persist()
		return

	} else if args.Entries == nil && args.Term >= rf.currentTerm { //This should not happen
		reply.Success = true
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		fmt.Println("Heart beat received: Case 4: This should not happen", rf.me)
		rf.persist()
		return
	} else if args.Entries != nil && len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		//fmt.Println("Get agreement Case 1:", rf.me)
		rf.persist()
		return

	} else if args.Entries != nil && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.log = rf.log[0:args.PrevLogIndex] //truncate the log
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		//fmt.Println("Get agreement Case 2:", rf.me)
		fmt.Println("Truncate logs for server:", rf.me)
		rf.persist()
		return

	} else if args.Entries != nil && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// reason to check length is not replace larger chuck with a smaller one
		len1 := len(rf.log[args.PrevLogIndex+1:])
		len2 := len(args.Entries[:])
		if len2 >= len1 {
			rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries[:]...)
			//fmt.Println("AppendEntries from: Leader: to Server: LeaderEntries: PrevLogIndex:", args.LeaderID, rf.me, args.Entries, args.PrevLogIndex)
			//fmt.Println("ServerLog after append:", rf.log)
		}

		//rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries[:]...)
		reply.Success = true
		rf.currentTerm = args.Term
		rf.peerStatus = Follower
		rf.validBeat = true
		//fmt.Println("Get agreement Case 3:", rf.me)

		if args.LeaderCommit > rf.commitIndex {
			fmt.Println("Leadercommit: CommitIndex, lastApplied: Length:", args.LeaderCommit, rf.commitIndex, rf.lastApplied, len(rf.log)-1)
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			go rf.commitApplier()
		}
		rf.persist()
		return
	}
	fmt.Println("OOPS.....No scenario matched for AppendRPC:", rf.me)
}
func min(arg1, arg2 int) int {
	if arg1 < arg2 {
		return arg1
	}
	return arg2
}

func (rf *Raft) commitFinder() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	startIndex := len(rf.log) - 1
loop:
	for rf.commitIndex < startIndex {
		count := 0
		for i := 0; i < serverCount; i++ {
			if rf.matchIndex[i] >= startIndex && rf.log[startIndex].Term == rf.currentTerm {
				count++
			}
		}
		if wonElection(count, serverCount) {
			rf.commitIndex = startIndex
			if rf.lastApplied < rf.commitIndex {
				go rf.commitApplier()
			}
			break loop
		} else {
			startIndex--
		}
	}
}

func (rf *Raft) commitApplier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		//fmt.Println("Applying Entry: Server: Index:", rf.log[i], rf.me, i)
		rf.myCh <- ApplyMsg{true, rf.log[i].Command, i, rf.currentTerm}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendHeartBeat() {
	go rf.commitFinder()
	//fmt.Println("Applier done")
	for i := 0; i < serverCount; i++ {
		if i != rf.me {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			prevLogIndex := len(rf.log) - 1
			prevLogTerm := rf.log[len(rf.log)-1].Term
			leaderCommit := rf.commitIndex
			rf.mu.Unlock()
			go func(server int) {
				args := AppendEntriesArgs{currentTerm, rf.me, prevLogIndex, prevLogTerm, nil, leaderCommit}
				reply := AppendEntriesReply{}
				result := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				if result { //TO DO: add blurb to reject old request
					rf.mu.Lock()
					if currentTerm != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if !reply.Success {
						if rf.currentTerm < reply.Term {
							fmt.Println("Term will be chaged Leader: From: To:", rf.me, rf.currentTerm, reply.Term)
							rf.peerStatus = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							fmt.Println("Change leader to follower. Stop sending heartbeat: ", rf.me)
						}
					}
					rf.mu.Unlock()
				}

			}(i)
		}
	}
}

func (rf *Raft) initializedLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Remember this include the leader itself
	for i := 0; i < serverCount; i++ {
		rf.nextIndex[i] = len(rf.log) //not including index 0. nextIndex == len
		rf.matchIndex[i] = 0
	}
}

//termIdex is the index of last log entry
func (rf *Raft) getAgreement(termIndex int) {
	//var mu sync.Mutex
	cond := sync.NewCond(&rf.mu)
	var count = 1 //it is already persisted in Leader
	for i := 0; i < serverCount; i++ {
		if i != rf.me && termIndex >= rf.nextIndex[i] {
			go func(server int) {
				var prevLogIndex, prevLogTerm, currentTerm, commitIndex int
				var logChunk []LogEntry
				//fmt.Println("Starting agreement: server: index: nextval:", server, termIndex, rf.nextIndex[server])
				rf.mu.Lock()
				if rf.nextIndex[server] < termIndex+1 {
					prevLogIndex = rf.nextIndex[server] - 1
					prevLogTerm = rf.log[prevLogIndex].Term
					currentTerm = rf.currentTerm
					logChunk = rf.log[rf.nextIndex[server] : termIndex+1]
					commitIndex = rf.commitIndex
					rf.mu.Unlock()
				} else {
					count++
					cond.Broadcast()
					rf.mu.Unlock()
					return
				}

				for {
					newArgs := AppendEntriesArgs{currentTerm, rf.me, prevLogIndex, prevLogTerm, logChunk, commitIndex}
					newReply := AppendEntriesReply{}
					//fmt.Println("Values from agreement Server:, prevLog:, prevTerm, nextIndex:", server, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[server]:termIndex+1])
					result := rf.peers[server].Call("Raft.AppendEntries", &newArgs, &newReply)
					if result {
						rf.mu.Lock()
						if rf.currentTerm != currentTerm || rf.peerStatus != Leader {
							count++
							cond.Broadcast()
							fmt.Println("Return from agreement due to term mismatch: Server: Old term: Current term:", rf.me, currentTerm, rf.currentTerm)
							rf.mu.Unlock()
							return
						} else if !newReply.Success {
							if newReply.Term > currentTerm {
								rf.currentTerm = newReply.Term
								rf.peerStatus = Follower //This may not be sufficient. Need clean escape
								rf.votedFor = -1
								fmt.Println("Converted to Follower in getAgreement:", rf.me)
								count++
								rf.persist()
								cond.Broadcast()
								rf.mu.Unlock()
								return
							} else {
								if rf.nextIndex[server] < termIndex+1 {
									rf.nextIndex[server]--
									prevLogIndex = rf.nextIndex[server] - 1
									prevLogTerm = rf.log[prevLogIndex].Term
									logChunk = rf.log[rf.nextIndex[server] : termIndex+1]
									//newArgs = AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[server] : termIndex+1], rf.commitIndex}
									//newReply = AppendEntriesReply{}
									//rf.persist() -- not required
									rf.mu.Unlock()
								} else {
									cond.Broadcast()
									rf.mu.Unlock()
									//fmt.Println("Prevented update by smaller index: Case 0", rf.me, server)
									return
								}
							}

						} else {
							count++
							if rf.nextIndex[server] < termIndex+1 {
								rf.nextIndex[server] = termIndex + 1
								rf.matchIndex[server] = termIndex
								//rf.persist()
								cond.Broadcast()
								rf.mu.Unlock()
								fmt.Println("Agreement for Leader: Server: Done: nextIndex:", rf.me, server, rf.nextIndex[server])
								//go rf.applier()
								go rf.commitFinder()
								return
							} else {
								cond.Broadcast()
								rf.mu.Unlock()
								//fmt.Println("Prevented update by smaller index: Case 1:", rf.me, server)
								return
							}

						}
					} else {
						time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond) //RPC failed
					}
				}

			}(i)
		}
	}
	rf.mu.Lock()
	for count < serverCount && rf.peerStatus == Leader {
		cond.Wait()
	}
	if rf.peerStatus != Leader {
		//fmt.Println("Returning due to leader change in Agreement:", rf.me)
		//mu.Unlock()
		rf.mu.Unlock()
		return
	} else {
		//mu.Unlock()
		rf.mu.Unlock()
		//fmt.Println("All done agreement entry at Index: Server: ", termIndex, rf.me)
		return
	}
}

//
// example RequestVote RPC handler. Server response when called by another server
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm //To Do: Update term and status of the candidate
		fmt.Println("Case: 1 Vote not granted. Stale term")
		return
	} else if args.Term > rf.currentTerm && len(rf.log) == 1 && args.LastLogIndex == 0 { //Could not have voted for higher term
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.validBeat = true
		rf.peerStatus = Follower
		fmt.Println("Case: 2 Vote granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
		rf.persist()
		return
	} else if len(rf.log) > 1 || args.LastLogIndex > 0 {
		fmt.Println("Vote Asked: Candidate: Term: LastLogIndex: LastLogTerm:", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
		fmt.Println("Vote giver: Server: Term: LastLogIndex: LastLogTerm:", rf.me, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		if args.Term > rf.currentTerm && args.LastLogTerm < rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = false
			reply.Term = args.Term
			rf.peerStatus = Follower
			rf.currentTerm = args.Term
			fmt.Println("Case: 3 Vote Not granted but term change: Server: Term: ", rf.me, rf.currentTerm)
			rf.persist()
			return
		} else if args.Term > rf.currentTerm && args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
			reply.VoteGranted = false
			reply.Term = args.Term
			rf.peerStatus = Follower
			rf.currentTerm = args.Term
			fmt.Println("Case: 3' Vote Not granted but term changed: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
			return
		} else if args.Term > rf.currentTerm && args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.peerStatus = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.validBeat = true
			fmt.Println("Case: 4 Vote granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
			return
		} else if args.Term > rf.currentTerm && args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex > len(rf.log)-1 {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.peerStatus = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.validBeat = true
			fmt.Println("Case: 4' Vote granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
			return
		} else if args.Term == rf.currentTerm && args.LastLogTerm > rf.log[len(rf.log)-1].Term && rf.votedFor == -1 {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			rf.validBeat = true
			fmt.Println("Case: 5 Vote granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
			return
		} else if args.Term == rf.currentTerm && args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex > len(rf.log)-1 && rf.votedFor == -1 {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			rf.validBeat = true
			fmt.Println("Case: 5' Vote granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
			return
			// If all fails, grant vote based on term.
			//TO DO: need to check if it works under all scenarios. Same as term == term and len == len
		} else if args.Term > rf.currentTerm {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.currentTerm = args.Term
			rf.validBeat = true
			rf.peerStatus = Follower
			rf.votedFor = args.CandidateId
			fmt.Println("Case: 6 Vote granted: Server: Term: ", rf.me, rf.currentTerm)
			rf.persist()
			return
		} else {
			reply.VoteGranted = false
			reply.Term = args.Term
			fmt.Println("Case: 7 Default case Vote not granted: Server: Term: To: ", rf.me, rf.currentTerm, args.CandidateId)
			return
		}

	}
}

// Your code here (2A, 2B).

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	var isLeader bool
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.peerStatus == Leader {
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
		rf.nextIndex[rf.me] = index
		rf.matchIndex[rf.me] = index
		fmt.Println("Recevied new entry Server: Index: Value:", rf.me, index, command)
		rf.persist()
		go rf.getAgreement(index)
	} else {
		isLeader = false
	}
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
var electionTimeLow int = 300
var electionRange int = 100

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{nil, 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeOut = electionTimeLow + rand.Intn(electionRange)
	rf.peerStatus = Follower //start as follower
	rf.validBeat = false
	serverCount = len(peers)
	rf.nextIndex = make([]int, serverCount)
	rf.matchIndex = make([]int, serverCount)
	rf.myCh = applyCh
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Println("Launch server: ", rf.me)
	//fmt.Println("Server Count:", serverCount)
	go rf.manageState()
	return rf
}

func (rf *Raft) manageState() {
	for !rf.killed() {
	loop:
		switch rf.mu.Lock(); rf.peerStatus {
		case Follower:
			rf.mu.Unlock()
			time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond)
			//fmt.Println("Sleep timeout: server:", time.Duration(rf.electionTimeOut)*time.Millisecond, rf.me)
			rf.mu.Lock()
			if !rf.validBeat {
				rf.peerStatus = Candidate
				fmt.Println("Didn't recieve valid beat", rf.me)
			} else {
				rf.peerStatus = Follower
				rf.validBeat = false
			}
			rf.mu.Unlock()
		case Candidate:
			rf.mu.Unlock()
			fmt.Println("Started leader election Server:", rf.me)
			rf.mu.Lock()
			rf.currentTerm++
			term := rf.currentTerm
			prevLogIndex := len(rf.log) - 1
			prevLogTerm := rf.log[len(rf.log)-1].Term
			rf.votedFor = rf.me
			rf.validBeat = true
			t0 := time.Now()
			rf.persist()
			rf.mu.Unlock()
			count := 1 //voted for self
			//fmt.Println("Voted for myself:", rf.me)
			finished := 1 // I am alive
			cond := sync.NewCond(&rf.mu)
			for i := 0; i < serverCount; i++ {
				if i != rf.me {
					args := RequestVoteArgs{term, rf.me, prevLogIndex, prevLogTerm}
					reply := RequestVoteReply{}
					go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
						ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if ok {
							finished++
							if reply.VoteGranted {
								if term != rf.currentTerm {
									rf.peerStatus = Follower
									cond.Broadcast()
								} else {
									count++
									//fmt.Println("Vote counted: Server: Term: Count:", rf.me, reply.Term, count)
									cond.Broadcast()
								}
							} else if !(reply.VoteGranted) {
								//rf.mu.Lock()
								if reply.Term > rf.currentTerm {
									rf.currentTerm = reply.Term
									rf.peerStatus = Follower
									rf.persist()
								}
								//rf.mu.Unlock()
								cond.Broadcast()
							}

						} else {
							finished++
							//fmt.Println("No response from server: finished", server, finished)
							cond.Broadcast()
						}
					}(i, &args, &reply)
				}
			}
			rf.mu.Lock()
			//To DO: add explicit check for timer
			for count < (serverCount/2)+1 && finished != serverCount && int(time.Since(t0).Milliseconds()) < rf.electionTimeOut {
				if rf.peerStatus == Follower {
					rf.mu.Unlock()
					break loop
				}
				cond.Wait()
			}
			if int(time.Since(t0).Milliseconds()) > rf.electionTimeOut {
				//rf.mu.Lock()
				rf.peerStatus = Candidate
				rf.mu.Unlock()
				//mu.Unlock()
				fmt.Println("Strated relection after election timeout: Server:", rf.me)
				break loop

			}
			if wonElection(count, serverCount) {
				//rf.mu.Lock()
				rf.peerStatus = Leader
				rf.mu.Unlock()
				fmt.Println("Elected leader: Server. Sending heartbeat", rf.me)
				rf.sendHeartBeat()
				rf.initializedLeaderState()
			} else {
				rf.mu.Unlock()
				time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond)
				//fmt.Println("Start new election after sleep", rf.me)
			}
			//rf.mu.Unlock()

		case Leader:
			rf.mu.Unlock()
			time.Sleep(time.Duration(heartBeat) * time.Millisecond)
			//fmt.Println("Sending heartbeat: Server", rf.me)
			rf.sendHeartBeat()
			fmt.Println("Sending heartbeat done: Server", rf.me)
		}
	}
	//fmt.Println("Server Killed:....", rf.me)
}

func wonElection(count, serverCount int) bool {
	//fmt.Println("Election result:", count, serverCount)
	if serverCount%2 == 0 {
		return count > serverCount/2
	}
	return count >= (serverCount/2)+1
}
