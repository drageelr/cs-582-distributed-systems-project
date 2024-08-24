package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

// Command Constants
const (
	Get = iota
	Put
	Append
)

// Debug toggles print logs
const Debug = 0

// DPrintf prints logs for debugging
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *RaftKV) printMe(msg string, command string, key string, value string, err string) {
	DPrintf("Server: %d\tCmd: %s\tKey: %s\tVal: |%s|\tErr: %s\t<%s>", kv.me, command, key, value, err, msg)
}

func (kv *RaftKV) parseOp(t int) string {
	OpStrArr := [...]string{"Get", "Put", "Append"}
	return OpStrArr[t-Get]
}

// Op passed to raft.go for processing
type Op struct {
	Command   int
	Key       string
	Value     string
	RequestID int64
	ClientID  int64
	Result    string
	Err       string
	Index     int
}

// RaftKV struct for server
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int

	db                 map[string]string
	requestChMap       map[int]chan Op
	lastRequestRecvMap map[int64]int64
	lastRequestDoneMap map[int64]int64
}

// Get RPC Call
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Construct Op struct
	data := Op{Get, args.Key, "", args.RequestID, args.ClientID, "", "", -1}

	// Get possible index value and whether server is leader or not
	kv.printMe("Query Request START", kv.parseOp(Get), args.Key, "", "")
	index, success := kv.queryRequest(data)
	data.Index = index
	kv.printMe("Query Request END Index: "+strconv.Itoa(index), kv.parseOp(Get), args.Key, "", "")

	reply.WrongLeader = true

	// If leader then continue
	if success {
		// Get result and send it to client
		kv.printMe("Query Result START", kv.parseOp(Get), args.Key, "", "")
		reply.WrongLeader = false
		value, err := kv.queryResult(data)
		reply.Value = value
		reply.Err = err

		if err == Abort {
			reply.WrongLeader = true
		}
		kv.printMe("Query Result END", kv.parseOp(Get), args.Key, value, err)
	}
}

// PutAppend RPC Call
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Construct Op struct
	data := Op{Put, args.Key, args.Value, args.RequestID, args.ClientID, "", "", -1}
	if args.Op == "Append" {
		data.Command = Append
	}

	duplicateRequest := true

	kv.mu.Lock()
	// Check whether last request received was from the client was with the same requestID or not
	if requestID, ok := kv.lastRequestRecvMap[args.ClientID]; !ok || (ok && requestID != args.RequestID) {
		// If not, then not duplicate request
		duplicateRequest = false
		kv.lastRequestRecvMap[args.ClientID] = args.RequestID
	}
	kv.mu.Unlock()

	if duplicateRequest {
		// If duplicate request then check leaders state and return appropriate reply to the client
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		reply.WrongLeader = true
		reply.Err = OK

		if isLeader {
			reply.WrongLeader = false
		}
	} else {
		// If unique request then get possible index value and whether server is leader or not
		kv.printMe("Query Request START", kv.parseOp(data.Command), args.Key, args.Value, "")
		index, success := kv.queryRequest(data)
		data.Index = index
		kv.printMe("Query Request END Index: "+strconv.Itoa(index), kv.parseOp(data.Command), args.Key, args.Value, "")

		reply.WrongLeader = true

		// If leader then continue
		if success {
			// Get result and send it to client
			kv.printMe("Query Result START", kv.parseOp(data.Command), args.Key, args.Value, "")
			reply.WrongLeader = false
			_, err := kv.queryResult(data)
			reply.Err = err

			if err == Abort {
				reply.WrongLeader = true
			}
			kv.printMe("Query Result START", kv.parseOp(data.Command), args.Key, args.Value, err)
		}
	}
}

// Checks whether to Op structs are equal or not (disregards result, err and index values)
func (kv *RaftKV) isOpEqual(op1 Op, op2 Op) bool {
	if op1.Command == op2.Command && op1.Key == op2.Key && op1.Value == op2.Value && op1.RequestID == op2.RequestID && op1.ClientID == op2.ClientID && op1.Index==op2.Index {
		return true
	}
	return false
}

// Calls Start on raft and returns index and leader state
func (kv *RaftKV) queryRequest(op Op) (int, bool) {
	kv.mu.Lock()
	// kv.printMe("Raft Start CALLED", kv.parseOp(op.command), op.key, op.value, op.err)
	index, _, isLeader := kv.rf.Start(op)
	// kv.printMe("Raft Start RETURNED", kv.parseOp(op.command), op.key, op.value, op.err)
	kv.mu.Unlock()
	return index, isLeader
}

// Returns the result of the request specified by the op struct and the index
func (kv *RaftKV) queryResult(op Op) (string, string) {
	// Make a new channel for this index (hence it will be unique)
	kv.mu.Lock()
	// kv.printMe("MAKING ReqCh", kv.parseOp(op.command), op.key, op.value, op.err)
	reqCh := make(chan Op,1)
	kv.requestChMap[op.Index] = reqCh
	kv.mu.Unlock()
	// kv.printMe("ReqCh MADE", kv.parseOp(op.command), op.key, op.value, op.err)

	for true {
		select {
		// Listen on the specified request's channel
		case data := <-reqCh:
			// If request given to this function (including index) is the same as received by the channel then correct result
			if kv.isOpEqual(data, op) {
				// Return this result
				return data.Result, data.Err
			}

			// Otherwise abort
			break

		// Periodically check whether this server is leader or not. If not then delete the channel and Abort request
		case <-time.After(100 * time.Millisecond):
			kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			kv.printMe("Leader Check: "+strconv.FormatBool(isLeader), kv.parseOp(op.Command), op.Key, op.Value, op.Err)
			kv.mu.Unlock()
			if !isLeader {
				kv.printMe("Req Chan DELETED (ABORT)", kv.parseOp(op.Command), op.Key, op.Value, op.Err)
				break
			}

			}
		}

	return "", Abort
}

func (kv *RaftKV) applyHandler() {
	for true {
		select {
		// Listen on the apply channel
		case applyData := <-kv.applyCh:
			kv.printMe("Apply Received", strconv.Itoa(applyData.Index), "", "", "")
			kv.mu.Lock()
			kv.printMe("Apply LOCK", strconv.Itoa(applyData.Index), "", "", "")
			data := applyData.Command
			if data, ok := data.(Op); ok {
				kv.printMe("Apply Parsed", kv.parseOp(data.Command), data.Key, data.Value, data.Err)

				// If command is Get then return the result + err in "data" struct
				if data.Command == Get {
					if value, ok := kv.db[data.Key]; ok {
						data.Result = value
						data.Err = OK
					} else {
						data.Err = ErrNoKey
					}
				} else {
					// Command is either Put or Append
					// If the request completed for this client does not exist OR if it exists but the requestID is not the same as the last completed one then process command
					if requestID, ok := kv.lastRequestDoneMap[data.ClientID]; !ok || (ok && requestID != data.RequestID) {
						// Apply Put or Append to DB
						if data.Command == Put {
							kv.db[data.Key] = data.Value
						} else if data.Command == Append {
							if _, ok := kv.db[data.Key]; ok {
								kv.db[data.Key] += data.Value
							} else {
								kv.db[data.Key] = data.Value
							}
						}
						// Set Last Request Done for this client to the current requestID
						kv.lastRequestDoneMap[data.ClientID] = requestID
					}
					// In either case, need to return only OK
					data.Err = OK
				}

				kv.printMe("REQ DONE AppIndex: "+strconv.Itoa(applyData.Index), kv.parseOp(data.Command), data.Key, data.Value, data.Err)

				// If channel for this index exists then send data there
				if channel, ok := kv.requestChMap[applyData.Index]; ok {
					data.Index = applyData.Index
					channel <- data
					kv.printMe("REQ SENT", kv.parseOp(data.Command), data.Key, data.Value, data.Err)
				}
			}
			kv.mu.Unlock()
		}
	}
}

// Kill kills server
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// StartKVServer starts server
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.requestChMap = make(map[int]chan Op)
	kv.lastRequestRecvMap = make(map[int64]int64)
	kv.lastRequestDoneMap = make(map[int64]int64)

	go kv.applyHandler()

	return kv
}
