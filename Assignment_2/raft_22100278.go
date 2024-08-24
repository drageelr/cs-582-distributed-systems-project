package raft

import (
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

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        int
	currentTerm int
	votedFor    int
	voteCount   int
	ping        bool
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
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// RequestVoteArgs args for Request Vote RPC
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateID   int
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
	// // rf.printMe("RV REQ " + strconv.Itoa(args.CandidateID))
	if args.CandidateTerm > rf.currentTerm || (rf.votedFor == -1 && args.CandidateTerm == rf.currentTerm) {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.CandidateTerm
		rf.role = Follower
		reply.VoteGranted = true
		// // rf.printMe("AE REQ " + strconv.Itoa(args.CandidateID) + " voted")
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs args for Append Entries RPC
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderID   int
}

// AppendEntriesReply reply for Append Entries RPC
type AppendEntriesReply struct {
	Term int
}

// AppendEntries executes Append Entries RPC on Receiver
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// // rf.printMe("AE REQ " + strconv.Itoa(args.LeaderID))
	if args.LeaderTerm >= rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		rf.role = Follower
		rf.ping = true
		// // rf.printMe("AE REQ " + strconv.Itoa(args.LeaderID) + " ack")
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleElectionTimeout() {
	for true {
		time.Sleep(time.Duration(rand.Intn(400)+150) * time.Millisecond)

		rf.mu.Lock()
		// rf.printMe("ELEC TIMEOUT")
		if rf.ping || rf.role == Leader {
			rf.ping = false
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go rf.handleNewElection()
	}

}

func (rf *Raft) handleNewElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.role = Candidate

	// rf.printMe("ELEC START")
	argsRV := RequestVoteArgs{rf.currentTerm, rf.me}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.handleSendRequestVote(i, argsRV)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleSendRequestVote(targetServer int, args RequestVoteArgs) {
	replyRV := RequestVoteReply{Term: -1, VoteGranted: false}
	if rf.sendRequestVote(targetServer, args, &replyRV) {
		rf.mu.Lock()
		// // rf.printMe("RV RES" + strconv.Itoa(targetServer))
		if rf.role == Candidate && rf.currentTerm == args.CandidateTerm {
			// // rf.printMe("RV RES " + strconv.Itoa(targetServer) + " on time")
			if replyRV.VoteGranted {
				rf.voteCount++
				// rf.printMe("RV RES " + strconv.Itoa(targetServer) + " vote")
				if rf.voteCount > (len(rf.peers) / 2) {
					// rf.printMe("RV RES " + strconv.Itoa(targetServer) + " win")
					rf.role = Leader
					rf.votedFor = -1
					go rf.handleNewHeartbeat()
				}
			} else if replyRV.Term >= rf.currentTerm {
				// rf.printMe("RV RES " + strconv.Itoa(targetServer) + " cancel")
				rf.role = Follower
				rf.currentTerm = replyRV.Term
				rf.votedFor = -1
				// rf.printMe("CAND TO FOLO")
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleNewHeartbeat() {
	firstTime := true
	// rf.printMe("HB START")
	for true {
		if !firstTime {
			time.Sleep(40 * time.Millisecond)
			firstTime = false
		}
		rf.mu.Lock()
		// // rf.printMe("HB TIMEOUT")
		if rf.role != Leader {
			// rf.printMe("HB CANCEL")
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		argsAE := AppendEntriesArgs{rf.currentTerm, rf.me}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.handleSendAppendEntries(i, argsAE)
			}
		}
		// rf.mu.Unlock()
	}
}

func (rf *Raft) handleSendAppendEntries(targetServer int, args AppendEntriesArgs) {
	replyAE := AppendEntriesReply{-1}
	if rf.sendAppendEntries(targetServer, args, &replyAE) {
		rf.mu.Lock()
		// // rf.printMe("AE RES " + strconv.Itoa(targetServer))
		if replyAE.Term > rf.currentTerm {
			// rf.printMe("AE RES " + strconv.Itoa(targetServer) + " cancel")
			rf.role = Follower
			rf.currentTerm = replyAE.Term
			rf.votedFor = -1
			// rf.printMe("LEAD TO FOLO")
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) printMe(msg string) {
	role := "Follower"
	if rf.role == Candidate {
		role = "Candidate"
	} else if rf.role == Leader {
		role = "Leader"
	}
	fmt.Printf("Node: %d Role: %s Term: %d VCount: %d VFor: %d\t<%s>\n", rf.me, role, rf.currentTerm, rf.voteCount, rf.votedFor, msg)
}

// Start for tester to Start
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// fmt.Printf("STARTED NODE:%d\n", me)
	go rf.handleElectionTimeout()

	return rf
}
