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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"

	"math"
)

const HEARTBEAT_FREQUENCY = time.Duration(150) * time.Millisecond

//
// A Go object implementing a single log entry.
//
type LogEntry struct {
	Command string
	Term    int
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.votedFor == rf.me) && !rf.inElection

	return term, isleader
}

//
// Check if target term is higher, and if so, update term
// and reset votedFor / election status
// Return true if update was applied, false if ignored
//
func (rf *Raft) SafeUpdateTerm(newTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < newTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1 // No longer voting for yourself
		rf.inElection = false
		return true
	} else {
		return false
	}

}

//
// Generate a random wait period before starting new election

func (rf *Raft) GenElectionTimeout() time.Duration {
	return time.Duration(500+rand.Intn(1000)) * time.Millisecond
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

func (rf *Raft) GetLastLogTerm() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var logTerm int
	if len(rf.log) == 0 {
		logTerm = 0
	} else {
		logTerm = rf.log[len(rf.log)-1].Term
	}
	return logTerm, len(rf.log)
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

	term, _ := rf.GetState()
	lastLogTerm, lastLogIndex := rf.GetLastLogTerm()
	// Refuse vote if all conditions not met
	if args.Term >= term && // candidate term at least as high
		// candidate log is at least as up to date

		args.LastLogTerm >= lastLogTerm &&
		args.LastLogIndex >= lastLogIndex {

		// Update if args.Term > currentTerm
		rf.SafeUpdateTerm(args.Term)

		// If haven't already voted this term or voted for Candidate, grant vote
		rf.mu.Lock()

		if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
			rf.votedFor = args.CandidateId
			vote = true
		}
		rf.mu.Unlock()
		if vote {
			fmt.Printf("[%d] I'm stuck here like a dumbass\n", rf.me)
			rf.lastMsgTime <- time.Now()
			fmt.Printf("[%d] Posted update\n", rf.me)
			fmt.Printf("[%d] Vote granted for %d.\n", rf.me, args.CandidateId)
		}

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
	fmt.Printf("[%d] AppendEntries request for term %d, Leader %d received. Starting.\n", rf.me, args.Term, args.LeaderId)
	//  RPC comes in with greater term, then we can process. Otherwise it's definitely rejected.

	term, _ := rf.GetState()

	if term <= args.Term {
		// If higher term received than my election, kill my election
		updated := rf.SafeUpdateTerm(args.Term)

		// If candidate log is at least as up to date, append logs and return success.
		lastLogTerm, _ := rf.GetLastLogTerm()
		if args.PrevLogTerm == lastLogTerm {
			rf.mu.Lock()
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
			rf.mu.Unlock()
			success = true
		}

		// If leader was ahead in term, or log updated, reset election timer
		if updated || success {
			rf.lastMsgTime <- time.Now()
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
			fmt.Printf("[%d] Leader Sending Heartbeat RPC\n", rf.me)
			// Heartbeat request is an empty list of messages
			lastLogTerm, lastLogIndex := rf.GetLastLogTerm()
			go RunAppendEntries(rf, lastLogIndex, lastLogTerm, &[]LogEntry{})

			time.Sleep(HEARTBEAT_FREQUENCY)
			// TODO: add check to not send heartbeat if you've sent a real AppendEntries request recently

		} else {
			select {
			// If message received since last awake, reset timer
			case lastTs = <-lastMsgTime:
				fmt.Printf("[%d] [%s] Heartbeat [%s] received since last sleep. Resetting timer\n", rf.me, time.Now().Format("20060102150405.000"), lastTs.Format("20060102150405.000"))

				// Sleep for random amount of time between 0.2 and 0.5 seconds
				timeWindow := rf.GenElectionTimeout()

				fmt.Printf("[%d] Desired duration: [%s]\n", rf.me, timeWindow)
				timeWindow = timeWindow - time.Now().Sub(lastTs)
				fmt.Printf("[%d] Truncated duration: [%s]\n", rf.me, timeWindow)

				// Negative timeWindow causes sleep to return immediately
				time.Sleep(timeWindow)

			// Otherwise, request an election
			default:
				fmt.Printf("[%d] [%s] Requesting Election\n", rf.me, time.Now().Format("20060102150405.000"))
				// Kill timed out election thread and start again
				{
					rf.mu.Lock()
					if rf.inElection {
						fmt.Printf("[%d] Killing old Election thread\n", rf.me)
						quitElectionCh <- true
					}
					rf.inElection = true
					go RunElection(rf, rf.currentTerm+1, quitElectionCh)
					rf.mu.Unlock()
				}
				time.Sleep(rf.GenElectionTimeout())
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
func RunAppendEntries(rf *Raft, prevLogIndex, prevLogTerm int, logEntries *[]LogEntry) {
	fmt.Println("Running append entries process")
	appendReplyCh := make(chan AppendEntriesReply)

	// Set of Vote Request struct
	request := AppendEntriesArgs{
		rf.currentTerm, // Current term
		rf.me,          // Id of term leader
		prevLogIndex,   // Index of last log entry leader believes follower should have
		prevLogTerm,    // Term of last log entry
		rf.commitIndex, // CommitIndex of Leader
		*logEntries}    // Entries to append

	// Spawn AppendEntries manager thread for each other server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			fmt.Printf("[%d] it me. skipping append entry\n", rf.me)
			continue
		} else {
			go func(rf *Raft, idx int, appendReplyCh chan AppendEntriesReply) {
				fmt.Printf("[%d] Sending AppendEntries %v to peer %d\n", rf.me, request, idx)
				var reply AppendEntriesReply
				ok := rf.SendAppendEntries(idx, &request, &reply)

				// TODO: Implement retry logic that steps back nextIndex

				// If response OK, send reply to vote aggregation channel for main thread
				if ok {
					appendReplyCh <- reply
				}
			}(rf, i, appendReplyCh)
		}
	}

	// TODO Wait for responses to know what to commit.
	for rf.killed() == false {
		select {
		case reply := <-appendReplyCh:
			fmt.Printf("[%d] Received AppendEntries reply %v\n", rf.me, reply)
		}
	}
}

//
// timeout is triggered so server wants to start an election
// increments term, votes for itself, resets timer, and requests votes
//
// Waits for votes to return and if so becomes leader
func RunElection(rf *Raft, targetTerm int, quitElectionCh chan bool) {
	fmt.Println("Running Election now")
	voteReplyCh := make(chan RequestVoteReply)
	voteTally := 0

	{
		rf.mu.Lock()
		// If targetTerm is not the next one, we got updated term.
		// Need to abandon election.
		if (rf.currentTerm + 1) == targetTerm {
			rf.currentTerm = rf.currentTerm + 1 // increment term
			rf.votedFor = rf.me                 // vote for self
			voteTally += 1
		}
		rf.mu.Unlock()
	}

	// rf.lastMsgTime <- time.Now() // Reset election timer
	lastLogTerm, lastLogIndex := rf.GetLastLogTerm()
	// Set of Vote Request struct
	request := RequestVoteArgs{
		rf.currentTerm, // incremented term
		rf.me,          // Server's server ID
		lastLogIndex,   // Last log index
		lastLogTerm}    // Last log Term

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
					fmt.Printf("[%d] Won election. Switching to leader\n", rf.me)
					rf.inElection = false
					fmt.Printf("[%d]", rf.me)
					fmt.Println(rf.GetState())
					return
				}
			} else {
				// Voter's term is higher than candidacy. Reset term and end election

				rf.SafeUpdateTerm(vote.Term)
				if vote.Term > rf.currentTerm {
					return
				}
				// Otherwise keep waiting for votes
			}
			return
		}
	}
}
