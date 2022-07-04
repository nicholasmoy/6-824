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
	//	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"

	"math"
	"math/rand"
)

const HEARTBEAT_FREQUENCY = time.Duration(150) * time.Millisecond

//
// A Go object implementing a single log entry.
//
type LogEntry struct {
	command string
	term    int
}

//
// Struct to contain configuration commands for AppendEntries RPC
//
type AppendEntriesArgs struct {
	Term     int // Current term
	LeaderId int // Id of term leader

	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int // Index of last commit

	Entries []LogEntry // entries to append
}

//
// Struct to contain configuration commands for AppendEntries RPC
//
type AppendEntriesReply struct {
	Term    int  // Current term
	Success bool // Id of term leader
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// 2A
	currentTerm int        // Latest term server has seen
	votedFor    int        // Who server voted for this Term
	log         []LogEntry // Log entries
	inElection  bool
	lastMsgTime chan time.Time
	commitIndex int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.votedFor == rf.me) && !rf.inElection

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
}

func (rf *Raft) GetLastLogTerm() int {
	var logTerm int
	if len(rf.log) == 0 {
		logTerm = 0
	} else {
		logTerm = rf.log[len(rf.log)-1].term
	}
	return logTerm
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Requester's current term
	CandidateId  int // Requester's Id
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Respondent's term
	VoteGranted bool // Respondent's vote for
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("[%d] Vote request for term %d, candidate %d received. Starting.\n", rf.me, args.Term, args.CandidateId)
	vote := false

	// Refuse vote if all conditions not met
	if args.Term >= rf.currentTerm && // candidate term at least as high
		// candidate log is at least as up to date
		args.LastLogTerm >= rf.GetLastLogTerm() &&
		args.LastLogIndex >= (len(rf.log)-1) {

		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term // Update term to latest
			rf.votedFor = -1           // Reset votedFor
		}

		// If haven't already voted this term or voted for Candidate, grant vote
		if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
			rf.votedFor = args.CandidateId
			vote = true
		}
		fmt.Printf("[%d] Vote granted for %d.\n", rf.me, args.CandidateId)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = vote
	return
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := false

	//  RPC comes in with greater term, then we can process. Otherwise it's definitely rejected.
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		// If in an election, kill election
		if rf.inElection && rf.currentTerm < args.Term {
			rf.lastMsgTime <- time.Now()
			rf.votedFor = -1
			rf.inElection = false
			rf.currentTerm = args.Term
		}
		// If candidate log is at least as up to date, append logs and return success.
		if args.PrevLogTerm == rf.log[args.PrevLogIndex].term {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			success = true
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = success
	return
}

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
// send a AppendEntries RPC to a server.

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1

	// Your code here (2B).

	return index, term, rf.votedFor == rf.me
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(lastMsgTime chan time.Time) {
	lastTs := time.Now()
	quitElectionCh := make(chan bool) // message election goRoutine to quit

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		_, is_leader := rf.GetState()
		if is_leader {
			fmt.Println("Leader Sleep")
			time.Sleep(HEARTBEAT_FREQUENCY)
			// TODO: Update

		} else {
			select {
			// If message received since last awake, reset timer
			case lastTs = <-lastMsgTime:
				fmt.Println("Heartbeat received since last sleep. Resetting timer")

				// Sleep for random amount of time between 0.2 and 0.5 seconds
				timeWindow := time.Duration(200+rand.Intn(1000)) * time.Millisecond
				timeWindow = timeWindow - time.Now().Sub(lastTs)*time.Millisecond

				// Negative timeWindow causes sleep to return immediately
				time.Sleep(timeWindow)

			// Otherwise, request an election
			default:
				fmt.Printf("[%d] Requesting Election\n", rf.me)
				// Kill timed out election thread and start again
				if rf.inElection {
					fmt.Printf("[%d] Killing old Election thread\n", rf.me)
					quitElectionCh <- true
				}
				go RunElection(rf, quitElectionCh)
				rf.inElection = true
				time.Sleep(time.Duration(200+rand.Intn(1000)) * time.Millisecond)
			}
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.inElection = false
	rf.lastMsgTime = make(chan time.Time)
	rf.commitIndex = -1
	// fmt.Println("Starting Ticker")

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	fmt.Println(time.Now())
	go rf.ticker(rf.lastMsgTime)
	rf.lastMsgTime <- time.Now()
	fmt.Println("Starting Ticker")

	return rf
}

//
// timeout is triggered so server wants to start an election
// increments term, votes for itself, resets timer, and requests votes
//
// Waits for votes to return and if so becomes leader
func RunAppendEntries(rf *Raft) {

	appendReplyCh := make(chan AppendEntriesReply)
	// voteReplies = []RequestVoteReply

	rf.lastMsgTime <- time.Now() // Reset election timer

	prevLogIndex := len(rf.log) - 1

	// Set of Vote Request struct
	request := AppendEntriesArgs{
		rf.currentTerm,            // Current term
		rf.me,                     // Id of term leader
		prevLogIndex,              // Index of last log entry leader believes follower should have
		rf.log[prevLogIndex].term, // Term of last log entry
		rf.commitIndex,            // CommitIndex of Leader

		[]LogEntry{}} // Entries to append

	// Spawn AppendEntries manager thread for each other server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else {
			go func(rf *Raft, appendReplyCh chan AppendEntriesReply) {
				var reply AppendEntriesReply
				ok := rf.SendAppendEntries(rf.me, &request, &reply)

				// TODO: Implement retry logic that steps back nextIndex

				// If response OK, send reply to vote aggregation channel for main thread
				if ok {
					appendReplyCh <- reply
				}
			}(rf, appendReplyCh)
		}
	}

	// // TODO Wait for responses to know what to commit.
	// for rf.killed() == false {
	// 	select {

	// 	case reply := <-voteReplyCh:
	// 		if vote.voteGranted {
	// 			voteTally += 1
	// 			// If majority, now the leader. Update state and quit election.
	// 			if voteTally > len(rf.peers)/2 {
	// 				// Election now over.
	// 				rf.inElection = false
	// 				return
	// 			}
	// 		} else {
	// 			// Voter's term is higher than candidacy. Reset term and end election
	// 			if vote.term > rf.currentTerm {
	// 				rf.currentTerm = vote.term
	// 				rf.votedFor = -1 // No longer voting for yourself
	// 				rf.inElection = false
	// 				return
	// 			}
	// 			// Otherwise keep waiting for votes
	// 		}
	// 		return
	// 	}
	// }
}

//
// timeout is triggered so server wants to start an election
// increments term, votes for itself, resets timer, and requests votes
//
// Waits for votes to return and if so becomes leader
func RunElection(rf *Raft, quitElectionCh chan bool) {
	fmt.Println("Running Election now")
	voteReplyCh := make(chan RequestVoteReply)
	// voteReplies = []RequestVoteReply
	rf.currentTerm = rf.currentTerm + 1 // increment term
	rf.votedFor = rf.me                 // vote for self
	voteTally := 1

	// rf.lastMsgTime <- time.Now() // Reset election timer
	logTerm := rf.GetLastLogTerm()
	// Set of Vote Request struct
	request := RequestVoteArgs{
		rf.currentTerm, // incremented term
		rf.me,          // Server's server ID
		len(rf.log),    // Last log index
		logTerm}        // Last log Term

	fmt.Printf("[%d] Starting thread to request: %v\n", rf.me, request)

	// Spawn vote requester for each other server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			fmt.Printf("[%d]It me. skipping vote.\n", i)
			continue
		} else {
			go func(rf *Raft, idx int, voteReplyCh chan RequestVoteReply) {
				fmt.Printf("[%d] Starting thread to request vote for term %d to peer idx: %d\n", rf.me, request.Term, idx)
				var reply RequestVoteReply
				ok := rf.sendRequestVote(idx, &request, &reply)

				// If response OK, send reply to vote aggregation channel for main thread
				if ok {
					fmt.Printf("[%d] Received reply from peer idx: %d\n", rf.me, idx)
					voteReplyCh <- reply
				}
			}(rf, i, voteReplyCh)
		}
	}

	// Listen for quit election message or for replies to come in
	for rf.killed() == false {
		select {

		// Election timed out. Kill this routine and stop listening.
		case <-quitElectionCh:
			fmt.Printf("[%d] Received signal to kill current election\n", rf.me)
			return
		// Received Vote. Tally.
		case vote := <-voteReplyCh:
			fmt.Printf("[%d] Processing vote reply for term %d\n", rf.me, vote.Term)
			if vote.VoteGranted {
				voteTally += 1
				// If majority, now the leader. Update state and quit election.
				if voteTally > len(rf.peers)/2 {
					// Election now over.
					rf.inElection = false
					return
				}
			} else {
				// Voter's term is higher than candidacy. Reset term and end election
				if vote.Term > rf.currentTerm {
					rf.currentTerm = vote.Term
					rf.votedFor = -1 // No longer voting for yourself
					rf.inElection = false
					return
				}
				// Otherwise keep waiting for votes
			}
			return
		}
	}
}
