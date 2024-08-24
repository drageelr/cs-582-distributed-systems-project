package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg struct for Assignment 3
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// Server Roles
const (
	Follower = iota
	Candidate
	Leader
)

// Raft struct for Raft Node
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Persistent State
	role        int
	currentTerm int
	votedFor    int
	logs        []Log

	// Volatile State
	voteCount       int
	ping            bool
	commitIndex     int
	commitVoteCount int
	lastApplied     int
	applyCh         chan ApplyMsg
	tempIndex       int

	// Leader Volatile State
	nextIndex  []int
	matchIndex []int
}

// Log struct for logs
type Log struct {
	Term    int
	Command interface{}
}

// GetState fetches state for tester
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// e.Encode(rf.role)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	// d.Decode(&rf.role)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

// RequestVoteArgs args for Request Vote RPC
type RequestVoteArgs struct {
	CandidateTerm         int
	CandidateID           int
	CandidateLastLogIndex int
	CandidateLastLogTerm  int
}

// RequestVoteReply reply for Request Vote RPC
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote executes Request Vote RPC on Receiver
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	rf.mu.Lock()
	if args.CandidateTerm > rf.currentTerm || (rf.votedFor == -1 && args.CandidateTerm == rf.currentTerm) {
		// If term condition meets -> continue
		logCondition := true

		lenLogs := len(rf.logs)
		if lenLogs > 0 {
			// If logs exists -> continue
			if rf.logs[lenLogs-1].Term > args.CandidateLastLogTerm || ((rf.logs[lenLogs-1].Term == args.CandidateLastLogTerm) && ((lenLogs - 1) > args.CandidateLastLogIndex)) { // Reject
				// If logs are not coherent -> logCondition is false
				logCondition = false
			}
		}

		if logCondition {
			// If log Condition is true -> vote for Candidate
			rf.votedFor = args.CandidateID
			rf.currentTerm = args.CandidateTerm
			rf.role = Follower
			reply.VoteGranted = true
			rf.persist()
		}
	}
	lenLogs := len(rf.logs)
	if lenLogs > 0 {
		if args.CandidateLastLogTerm > rf.logs[lenLogs-1].Term {
			rf.ping = true
		} else if args.CandidateLastLogTerm == rf.logs[lenLogs-1].Term && args.CandidateLastLogIndex > lenLogs-1 {
			rf.ping = true
		}
	} else if args.CandidateLastLogTerm >= 0 {
		rf.ping = true
	}
	// rf.printMe("VOTE TO " + strconv.Itoa(args.CandidateID) + " FOR TERM " + strconv.Itoa(args.CandidateTerm) + " GRANTED: " + strconv.FormatBool(reply.VoteGranted))
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs args for Append Entries RPC
type AppendEntriesArgs struct {
	LeaderTerm         int
	LeaderID           int
	LeaderCommitIndex  int
	LeaderPrevLogIndex int
	LeaderPrevLogTerm  int
	LeaderCurEnrtyLog  []Log
}

// AppendEntriesReply reply for Append Entries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
	Len     int
}

// AppendEntries executes Append Entries RPC on Receiver
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = false
	if args.LeaderTerm >= rf.currentTerm {
		// If Leader's term is greater or equal -> continue

		// Adjust role, term etc according to Leader
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		rf.role = Follower
		rf.ping = true

		lenLogs := len(rf.logs)
		if len(args.LeaderCurEnrtyLog) == 0 {
			// If AE Call is a heartbeat -> continue
			if lenLogs-1 >= args.LeaderPrevLogIndex {
				// If previous log index does exist in follower -> conintue

				// If previous log index is -1 or term at that index matches with the term of leader -> Reply Success
				if args.LeaderPrevLogIndex == -1 {
					reply.Success = true
				} else if args.LeaderPrevLogTerm == rf.logs[args.LeaderPrevLogIndex].Term {
					reply.Success = true
				}
			}
		} else {
			// If AE Call is NOT a heartbeat -> continue
			if lenLogs-1 > args.LeaderPrevLogIndex {
				// If previous log index is greater in follower -> conintue

				// If previous log index is -1 or term at that index matches with the term of leader -> Reply Success
				if args.LeaderPrevLogIndex == -1 {
					reply.Success = true
				} else if rf.logs[args.LeaderPrevLogIndex].Term == args.LeaderPrevLogTerm {
					reply.Success = true
				}
			} else if lenLogs-1 == args.LeaderPrevLogIndex {
				// If previous log index the last entry in follower -> conintue

				// If previous log index is -1 or term at that index matches with the term of leader -> Reply Success
				if args.LeaderPrevLogIndex == -1 {
					reply.Success = true
				} else if rf.logs[args.LeaderPrevLogIndex].Term == args.LeaderPrevLogTerm {
					reply.Success = true
				}
			}
		}

		if reply.Success {
			// If reply is going to be success -> continue
			if len(args.LeaderCurEnrtyLog) != 0 {
				// If AE Call is NOT a heartbeat -> append logs by overriding previous ones if needed
				if lenLogs != 0 {
					rf.logs = rf.logs[0 : args.LeaderPrevLogIndex+1]
				}
				for i := 0; i < len(args.LeaderCurEnrtyLog); i++ {
					rf.logs = append(rf.logs, args.LeaderCurEnrtyLog[i])
				}
				// rf.printLogs("")
			}
			if args.LeaderCommitIndex > rf.commitIndex {
				// If leader's commit index is greater -> update commit index and send committed values on applyCh
				oldCommitIndex := rf.commitIndex
				rf.commitIndex = min(len(rf.logs), args.LeaderCommitIndex)
				for i := oldCommitIndex; i < rf.commitIndex; i++ {
					rf.applyCh <- ApplyMsg{i + 1, rf.logs[i].Command, false, make([]byte, 0)}
				}
				// rf.printMe("Commit Index Update")
			}
		}

		rf.persist()
	}
	reply.Len = len(rf.logs)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Starts new election on timeout
func (rf *Raft) handleElectionTimeout() {
	for true {
		time.Sleep(time.Duration(rand.Intn(400)+350) * time.Millisecond)

		rf.mu.Lock()
		if rf.ping || rf.role == Leader {
			rf.ping = false
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		rf.handleNewElection()
	}
	return
}

// Initializes variables for new election and sends Request Vote RPC Call to all servers
func (rf *Raft) handleNewElection() {
	rf.mu.Lock()
	if rf.role != Leader {
		// Initialize variables for new election
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.voteCount = 1
		rf.role = Candidate
		//stores the states in persistent storage
		rf.persist()
		// rf.printMe("NEW ELECTION")

		// Calls "handleSendRequestVote" for all servers
		argsRV := RequestVoteArgs{rf.currentTerm, rf.me, -1, -1}
		lenLogs := len(rf.logs)
		if lenLogs > 0 {
			argsRV.CandidateLastLogIndex = lenLogs - 1
			argsRV.CandidateLastLogTerm = rf.logs[lenLogs-1].Term
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.handleSendRequestVote(i, argsRV)
			}
		}
		rf.mu.Lock()
	}
	rf.mu.Unlock()
	return
}

// Sends the Request Vote to a particular server specifified by "targetServer" int
// And also handles the response when it comes back from that server
func (rf *Raft) handleSendRequestVote(targetServer int, args RequestVoteArgs) {
	replyRV := RequestVoteReply{Term: -1, VoteGranted: false}
	if rf.sendRequestVote(targetServer, args, &replyRV) {
		// Handle response If Request Vote RPC Call returns true
		rf.mu.Lock()
		if rf.role == Candidate && rf.currentTerm == args.CandidateTerm {
			// If response was on time -> continue
			if replyRV.VoteGranted {
				// If vote granted -> increment vote count
				rf.voteCount++
				if rf.voteCount > (len(rf.peers) / 2) {
					// If majority granted vote -> Convert to leader + start Heartbeat Thread
					rf.role = Leader
					rf.votedFor = -1
					rf.ping = false
					//stores the states in persistent storage
					rf.persist()
					// rf.printMe("WON ELECTION FOR TERM " + strconv.Itoa(args.CandidateTerm))
					go rf.handleNewHeartbeat()
				}
			} else if replyRV.Term >= rf.currentTerm {
				// If targetServer has greater or equal term -> Revert to Follower
				rf.role = Follower
				rf.currentTerm = replyRV.Term
				rf.votedFor = -1
				//stores the states in persistent storage
				rf.persist()
			}
		}
		rf.mu.Unlock()
	}
	return
}

// Sends Heartbeat to all servers in idle periods
// Also starts the thread for "handleBulkSendAppendEntries" for each server ONLY ONCE
func (rf *Raft) handleNewHeartbeat() {
	firstTime := true
	rf.mu.Lock()
	// Initialize matchIndex and nextIndex arrays
	lenLogs := len(rf.logs)
	rf.tempIndex = lenLogs
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lenLogs
		rf.matchIndex[i] = -1
	}
	rf.mu.Unlock()

	// Heartbreat Loop
	for true {
		if !firstTime {
			time.Sleep(140 * time.Millisecond)
		}
		rf.mu.Lock()

		if rf.role != Leader {
			rf.mu.Unlock()
			// If node is no longer the Leader -> terminate loop, which terminates the thread
			return
		}
		argsAE := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, -1, -1, make([]Log, 0, 0)}
		lenLogs := len(rf.logs)
		if lenLogs > 0 {
			argsAE.LeaderPrevLogIndex = lenLogs - 1
			argsAE.LeaderPrevLogTerm = rf.logs[lenLogs-1].Term
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				lenLogs = len(rf.logs)
				if rf.nextIndex[i] > rf.matchIndex[i] && lenLogs > rf.nextIndex[i] {
					numLogs := len(rf.logs) - rf.nextIndex[i]
					thisLogArr := make([]Log, 0, numLogs)
					for j := rf.nextIndex[i]; j < rf.nextIndex[i]+numLogs; j++ {
						thisLogArr = append(thisLogArr, rf.logs[j])
					}
					argsAE = AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, -1, -1, thisLogArr}
					if rf.nextIndex[i] > 0 {
						argsAE.LeaderPrevLogIndex = rf.nextIndex[i] - 1
						argsAE.LeaderPrevLogTerm = rf.logs[argsAE.LeaderPrevLogIndex].Term
					}
				}
				rf.mu.Unlock()
				go rf.handleSendAppendEntries(i, argsAE)
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
		firstTime = false
	}
	return
}

// Sends the Append Entries to a particular server specifified by "targetServer" int
// And also handles the response when it comes back from that server
func (rf *Raft) handleSendAppendEntries(targetServer int, args AppendEntriesArgs) {
	replyAE := AppendEntriesReply{-1, false, 0}
	if rf.sendAppendEntries(targetServer, args, &replyAE) {
		// Handle response If Append Entry RPC Call returns true
		rf.mu.Lock()
		if replyAE.Term > rf.currentTerm {
			// If targetServer has greater term -> Revert to Follower
			rf.role = Follower
			rf.currentTerm = replyAE.Term
			rf.votedFor = -1
			rf.ping = true
			//stores the states in persistent storage
			rf.persist()
		} else {
			// If targetServer has equal term -> continue
			if len(args.LeaderCurEnrtyLog) != 0 {
				// If Append Entry RPC Call was not for heartbeat -> continue
				if replyAE.Success {
					// If reply was Success -> Update matchIndex + nextIndex + commitIndex (If majority agrees on the value)
					rf.matchIndex[targetServer] = args.LeaderPrevLogIndex + len(args.LeaderCurEnrtyLog)
					rf.nextIndex[targetServer] = rf.matchIndex[targetServer] + 1
					for true {
						lenPeers := len(rf.peers)
						countEqual, countGreater := 0, 0
						majority := lenPeers / 2
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me {
								if rf.matchIndex[i] == rf.commitIndex {
									countEqual++
								} else if rf.matchIndex[i] > rf.commitIndex {
									countGreater++
								}
							}
						}
						if countEqual+countGreater >= majority {
							rf.commitIndex++
							// rf.printMe("Commit Index Update")
							rf.applyCh <- ApplyMsg{rf.commitIndex, rf.logs[rf.commitIndex-1].Command, false, make([]byte, 0)}
							if countGreater < majority {
								break
							}
						} else {
							break
						}
					}
				} else {
					// If reply was NOT Sucess -> decrement nextIndex AND if length of logs of the follower is less than previous term index of the Append Entry RPC Call
					// Make nextIndex = length of logs of the follower + 1
					rf.nextIndex[targetServer]--
					if replyAE.Len < args.LeaderPrevLogIndex {
						rf.nextIndex[targetServer] = replyAE.Len + 1
					}
				}
			}
		}
		rf.mu.Unlock()
	}
	return
}

// Utility function for debugging purposes
func (rf *Raft) printMe(msg string) {
	role := "Follower"
	if rf.role == Candidate {
		role = "Candidate"
	} else if rf.role == Leader {
		role = "Leader"
	}
	fmt.Printf("Node: %d Role: %s Term: %d VCount: %d VFor: %d LenLogs: %d CIdx: %d Peers: %d\t<%s>\n", rf.me, role, rf.currentTerm, rf.voteCount, rf.votedFor, len(rf.logs), rf.commitIndex, len(rf.peers), msg)
}

// Utility function for debugging purposes
func (rf *Raft) printLogs(str string) {
	fmt.Printf("Node %d %s Logs: ", rf.me, str)
	lenLogs := len(rf.logs)
	for i := 0; i < lenLogs; i++ {
		fmt.Printf("[%d]{%d, %d} ", i+1, rf.logs[i].Term, rf.logs[i].Command)
	}
	fmt.Printf("\n")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Start for tester to append a command into the logs of the server
// If node is Leader, then send command to append channel for appending into the logs
// Otherwise don't append
// Also returns valid index, term, isLeader values
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.role != Leader {
		isLeader = false
	} else {
		rf.tempIndex++
		index = rf.tempIndex
		term = rf.currentTerm
		// rf.appendCh <- Log{term, command}
		rf.logs = append(rf.logs, Log{term, command})
		//stores the states in persistent storage
		rf.persist()
		// rf.printLogs("LEADER")
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// Kill for removing debug log for dead nodes
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make for tester to make Raft Nodes
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = -1
	rf.ping = false
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.commitVoteCount = 0
	rf.lastApplied = -1
	rf.applyCh = applyCh
	rf.tempIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = -1
		rf.matchIndex[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// rf.printMe("RE-START ME")
	// rf.printLogs("RE-START LOGS")

	// Main thread that runs for every Node infinitely
	go rf.handleElectionTimeout()

	return rf
}
