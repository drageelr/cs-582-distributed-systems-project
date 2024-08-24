package raftkv

// Error Strings
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Abort    = "Abort"
)

// PutAppendArgs args for PutAppend RPC Call
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	RequestID int64
	ClientID  int64
}

// PutAppendReply reply for PutAppend RPC Call
type PutAppendReply struct {
	WrongLeader bool
	Err         string
}

// GetArgs args for Get RPC Call
type GetArgs struct {
	Key       string
	RequestID int64
	ClientID  int64
}

// GetReply reply for Get RPC Call
type GetReply struct {
	WrongLeader bool
	Err         string
	Value       string
}
