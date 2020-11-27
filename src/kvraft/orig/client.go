package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
	//"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	requestID int64
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
	ck.requestID = nrand()
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
var leader int = -1

func (ck *Clerk) Get(key string) string {
	ck.requestID = nrand()
	args := GetArgs{key, " ", "Get", ck.requestID}
	reply := GetReply{}
	//fmt.Println("Starting Get:", key)
	if leader != -1 {
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				fmt.Println("Get Successful: ", reply.Value)
				return reply.Value
			} else {
				for {
					for i := 0; i < len(ck.servers); i++ {
						ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
						if ok {
							if reply.Err != ErrWrongLeader {
								fmt.Println("Get Successful: ", reply.Value)
								leader = i
								return reply.Value
							}
						}
					}
				}
			}
		} else {
			for {
				for i := 0; i < len(ck.servers); i++ {
					ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
					if ok {
						if reply.Err != ErrWrongLeader {
							fmt.Println("Get Successful: ", reply.Value)
							leader = i
							return reply.Value
						}
					}
				}
			}

		}
	} else {
		for {
			for i := 0; i < len(ck.servers); i++ {
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok {
					if reply.Err != ErrWrongLeader {
						fmt.Println("Get Successful: ", reply.Value)
						leader = i
						return reply.Value
					}
				}
			}
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	fmt.Println("Operation Received: Op: Key: Value", op, key, value)
	ck.requestID = nrand()
	args := PutAppendArgs{key, value, op, ck.requestID}
	reply := PutAppendReply{}
	if leader != -1 {
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				fmt.Println("PutAppend Successful:", op, key)
				return
			} else {
				for {
					for i := 0; i < len(ck.servers); i++ {
						ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
						if ok {
							if reply.Err == OK {
								fmt.Println("PutAppend Successful:", op, key)
								leader = i
								return
							}
						}
					}
				}
			}
		} else {
			for {
				for i := 0; i < len(ck.servers); i++ {
					ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
					if ok {
						if reply.Err == OK {
							fmt.Println("PutAppend Successful:", op, key)
							leader = i
							return
						}
					}
				}
			}
		}
	} else {
		for {
			for i := 0; i < len(ck.servers); i++ {
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						fmt.Println("PutAppend Successful:", op, key)
						leader = i
						return
					}
				}
			}
		}
	}
	/**
	for {

		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					fmt.Println("Operation successful:", op, key)
					return
				}
				//fmt.Println("Operation: Wrong Leader", op, key, i)
			}
		}
	}
	**/
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
