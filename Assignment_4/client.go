package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"strconv"
	"time"
)

// Clerk struct for client
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me       int64
	reqID    int64
	leaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk makes client
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.reqID = 0
	ck.leaderID = -1
	// You'll have to add code here.
	time.Sleep(1000 * time.Millisecond)
	return ck
}

// PrintBool toggles debugging logs
const PrintBool = false

func (ck *Clerk) printMe(msg string, command string, key string, value string, reqID int64) {
	if PrintBool {
		fmt.Printf("CLERK: %d\tRID: %d\tCmd: %s\tKey: %s\tVal: |%s|\t<%s>\n", ck.me, reqID, command, key, value, msg)
	}
}

// Get RPC Call
func (ck *Clerk) Get(key string) string {
	lenServers := len(ck.servers)
	ck.printMe("Call Func Get", "Get", key, "", -1)
	// Make Args for RPC Call
	args := GetArgs{key, ck.reqID, ck.me}

	// Get Last Leader Id
	serverID := ck.leaderID
	// Loop until not successful
	for true {
		for i := 0; i < lenServers; i++ {
			// If no last leader, check sequentially
			if ck.leaderID == -1 {
				serverID = i
			}
			// Construct Reply for RPC Call
			reply := GetReply{false, "", ""}
			ck.printMe("RPC Call on Server "+strconv.Itoa(serverID), "Get", args.Key, "", args.RequestID)
			// Call RPC
			if ck.servers[serverID].Call("RaftKV.Get", &args, &reply) {
				if !reply.WrongLeader {
					// If correct leader return appropriate reply
					ck.leaderID = serverID
					if reply.Err == OK {
						ck.printMe("DONE Server "+strconv.Itoa(serverID), "Get", args.Key, reply.Value, args.RequestID)
						ck.reqID++
						return reply.Value
					}
					ck.printMe("NOT FOUND VAL on Server "+strconv.Itoa(serverID), "Get", args.Key, "", args.RequestID)
					ck.reqID++
					return ""
				}
				// Otherwise set Last Leader Id to -1
				ck.printMe("NOT LEADER Server "+strconv.Itoa(serverID), "Get", args.Key, "", args.RequestID)
				ck.leaderID = -1
			}
		}
	}

	return ""
}

// PutAppend RPC Call
func (ck *Clerk) PutAppend(key string, value string, op string) {
	lenServers := len(ck.servers)
	ck.printMe("Call Func PutAppend", op, key, value, -1)
	// Make Args for RPC Call
	args := PutAppendArgs{key, value, op, ck.reqID, ck.me}

	// Get Last Leader Id
	serverID := ck.leaderID
	// Loop until not successful
	for true {
		for i := 0; i < lenServers; i++ {
			// If no last leader, check sequentially
			if ck.leaderID == -1 {
				serverID = i
			}
			// Construct Reply for RPC Call
			reply := PutAppendReply{false, ""}
			ck.printMe("RPC Call on Server "+strconv.Itoa(serverID), op, args.Key, args.Value, args.RequestID)
			// Call RPC
			if ck.servers[serverID].Call("RaftKV.PutAppend", &args, &reply) {
				// If correct leader return appropriate reply
				if !reply.WrongLeader {
					ck.leaderID = serverID
					ck.printMe("DONE Server "+strconv.Itoa(serverID), op, args.Key, args.Value, args.RequestID)
					ck.reqID++
					return
				}
				// Otherwise set Last Leader Id to -1
				ck.printMe("NOT LEADER Server "+strconv.Itoa(serverID), op, args.Key, args.Value, args.RequestID)
				ck.leaderID = -1
			}
		}
	}
}

// Put calls PutAppend with Put
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append calls PutAppend with Append
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
