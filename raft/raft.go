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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"

	"math"
)

const HEARTBEAT_FREQUENCY = time.Duration(150) * time.Millisecond
const VERBOSE = true

//
// A Go object implementing a single log entry.
//
type LogEntry struct {
	Command interface{}
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
	Success bool // Whether append successful
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
	currentTerm           int        // Latest term server has seen
	votedFor              int        // Who server voted for this Term
	log                   []LogEntry // Log entries
	inElection            bool
	lastElectionResetTime time.Time // Latest timestamp at which election reset received
	commitIndex           int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2B - Leader State
	nextIndex   []int // Next log entry to try to send to peer
	matchIndex  []int // Last log entry known replicated on peer
	lastApplied int   // Index of last command that was applied
	applyChan   chan ApplyMsg

	appendEntryQuitChan chan bool // Channel to kill outstanding append entry runners
}

// Print log messages if VERBOSE == True
func (rf *Raft) LogMessage(message string) {
	if VERBOSE {
		fmt.Println(fmt.Sprintf("[%d][%s] -", rf.me, time.Now().Format("20060102150405.000")), message)
	}
}

//
// Set lastElectionResetTime to time.Now
func (rf *Raft) ResetElectionTimer() time.Duration {
	rf.LogMessage("Resetting Election Timer")
	delay := rf.GenElectionTimeout()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastElectionResetTime = time.Now().Add(delay)
	rf.LogMessage("Finished resetting Election Timer")
	return delay
}

//
// return timestamp of most recent message to reset election.
func (rf *Raft) GetLatestElectionReset() time.Time {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastElectionResetTime
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
		rf.persist()
		return true
	} else {
		return false
	}

}

//
// Initialize leader state
//
func (rf *Raft) InitializeLeader(fromPersist bool) {
	rf.LogMessage("Initializing leader state")
	_, LastIndex := rf.GetLastLog()
	rf.mu.Lock()
	// If in intervening time I switched to not in election, abandon leadership
	// Unless recovering from crash
	if !rf.inElection && !fromPersist {
		return
	}
	rf.inElection = false
	// Initialize nextIndex to LastIndex + 1, and matchIndex to 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = LastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	// rf.LogMessage(fmt.Sprintln(rf.nextIndex))
	rf.LogMessage("Finished initializing leader state")
}

//
// Generate a random wait period before starting new election

func (rf *Raft) GenElectionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(500)) * time.Millisecond
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// helper function that returns term of a specific log index
//
//

func (rf *Raft) GetLogInfo(logIndex int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var logTerm int
	if logIndex < 1 {
		logTerm = 0
	} else if logIndex > len(rf.log) {
		logTerm = -1
	} else {
		logTerm = rf.log[logIndex-1].Term
	}
	return logTerm
}

//
// helper function that returns copy of peer list
//
//

func (rf *Raft) NumPeers() int {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers)
}

//
// helper function that returns term and index of last log entry
//

func (rf *Raft) GetLastLog() (int, int) {
	rf.mu.Lock()
	logLen := len(rf.log)
	rf.mu.Unlock()
	logTerm := rf.GetLogInfo(logLen)

	return logTerm, logLen
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.LogMessage(fmt.Sprintf("Getting current commit"))
	return rf.commitIndex
}

//
// Checks the largest index has a majority of servers agreeing
// If they agree then return that index
//

func (rf *Raft) CheckCommit() int {
	rf.LogMessage("Checking commit index")
	CommitIndex := -1
	_, is_leader := rf.GetState()
	if !is_leader {
		return -1 // Only leader can check
	} else {

		rf.mu.Lock()
		currentTerm := rf.currentTerm
		tallyThreshold := len(rf.peers) / 2
		matchIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchIndexCopy, rf.matchIndex) // to reduce time we lock rf state
		rf.mu.Unlock()

		var highestLogIndex int
		highestLogTerm, nextHighestLogIndex := rf.GetLastLog()

		// Lower "highestLogIndex" incrementally, counting # of peers that have
		// matchIndex at least as high
		for true {
			highestLogIndex = nextHighestLogIndex
			highestLogTerm = rf.GetLogInfo(highestLogIndex)
			committedTally := 1 // We know at least leader has highestLogIndex
			rf.LogMessage(fmt.Sprintf("Checking how many peers have MatchIndex at least %d, term %d", highestLogIndex, highestLogTerm))
			for i, v := range matchIndexCopy {
				// Skip myself in tally
				if i == rf.me {
					continue
				}
				rf.LogMessage(fmt.Sprintf("Committed tally: %d, Checking peer %d, matchindex %d", committedTally, i, v))
				if v >= highestLogIndex {
					committedTally += 1
				} else {
					// Save the next highest logIndex we find to decrement next
					if nextHighestLogIndex == highestLogIndex {
						nextHighestLogIndex = v
					} else {
						if nextHighestLogIndex < v {
							nextHighestLogIndex = v
						}
					}
				}
			}

			// After one run through all peers, check success criteria
			rf.LogMessage(fmt.Sprintf("Committed tally: %d, Highest Log Term %d, Current Term %d", committedTally, highestLogTerm, currentTerm))
			if committedTally > tallyThreshold && highestLogTerm == currentTerm {
				CommitIndex = highestLogIndex
				rf.LogMessage(fmt.Sprintf("Found commit Index %d", CommitIndex))
				break
			} else if nextHighestLogIndex == highestLogIndex {
				// There are no matches found, and no more lower log indexes
				rf.LogMessage(fmt.Sprintf("Found no valid Commit Indices"))
				break
			}
		}
		rf.LogMessage(fmt.Sprintf("Returning commit Index %d", CommitIndex))
		return CommitIndex
	}
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
	rf.LogMessage("Reading from persisted state.")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}

	// If leader, I might have just been a candidate when I crashed.
	// Just in case, revert to candidate which will force a new election
	if _, isLeader := rf.GetState(); isLeader {

		rf.mu.Lock()
		rf.inElection = true
		rf.mu.Unlock()
		rf.LogMessage("Might have been leader. Resetting to candidate.")
	}

	rf.LogMessage(fmt.Sprintf("Loaded State. currentTerm: %d, votedFor: %d, Log: %v", currentTerm, votedFor, log))
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
	rf.LogMessage(fmt.Sprintf("Vote request for term %d, candidate %d received. Starting.", args.Term, args.CandidateId))
	rf.mu.Lock()
	rf.LogMessage(fmt.Sprintf("My log %v", rf.log))
	rf.mu.Unlock()
	vote := false

	term, _ := rf.GetState()
	lastLogTerm, lastLogIndex := rf.GetLastLog()

	// Terms should be at least equal
	if args.Term >= term {
		// candidate term at higher, reset term and rever tto follower
		rf.SafeUpdateTerm(args.Term)

		rf.LogMessage("Vote request, terms compatible")
		CandidateLogValid := false
		if args.LastLogTerm > lastLogTerm {
			rf.LogMessage(fmt.Sprintf("Candidate log term %d is higher than mine %d.", args.LastLogTerm, lastLogTerm))
			CandidateLogValid = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			rf.LogMessage(fmt.Sprintf("Candidate log term is equal. Log length %d is higher than mine %d.", args.LastLogIndex, lastLogIndex))
			CandidateLogValid = true
		} else {
			rf.LogMessage("Candidate log is not as up to date as mine.")
		}
		// If haven't already voted this term or voted for Candidate, grant vote
		if CandidateLogValid {
			rf.LogMessage("Updating votedFor.")
			rf.mu.Lock()
			votedFor := rf.votedFor
			rf.mu.Unlock()

			if (votedFor == -1) || (votedFor == args.CandidateId) {
				rf.mu.Lock()
				rf.votedFor = args.CandidateId
				rf.LogMessage(fmt.Sprintf("Vote granted for %d.", args.CandidateId))
				rf.mu.Unlock()
				rf.ResetElectionTimer()
				vote = true
			}
		}
	}
	reply.Term = term
	reply.VoteGranted = vote
	return
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := false
	// rf.LogMessage(fmt.Sprintf("AppendEntries request %v for term %d, Leader %d received. My term is %d, log is %v. Starting.", args, args.Term, args.LeaderId, rf.currentTerm, rf.log))
	//  RPC comes in with greater term, then we can process. Otherwise it's definitely rejected.

	term, _ := rf.GetState()
	prevLogTerm := rf.GetLogInfo(args.PrevLogIndex)

	rf.LogMessage(fmt.Sprintf("Attempting Append with args %v. My term %d. My prevLogTerm at index %d", args, term, prevLogTerm))
	// rf.LogMessage(fmt.Sprintf("My log %v", rf.log))
	updated := rf.SafeUpdateTerm(args.Term)

	if term <= args.Term {
		rf.ResetElectionTimer()
		rf.LogMessage(fmt.Sprintf("Current terms match. Checking last log term %d vs. args %d", prevLogTerm, args.PrevLogTerm))
		// If higher term received than my election, kill my election

		// If candidate log is at least as up to date, append logs and return success.
		if args.PrevLogTerm == prevLogTerm {
			rf.LogMessage("Logs match sufficiently. Trying to append")
			rf.mu.Lock()
			logLength := len(rf.log)
			rf.mu.Unlock()

			overlap_ct := logLength - args.PrevLogIndex

			// Log is not up to date with everything before these new appends, it's a fail
			if overlap_ct >= 0 {
				rf.mu.Lock()
				if overlap_ct == 0 { // Append to the end
					rf.log = append(rf.log, args.Entries...)
				} else { // iterate through each overlapping to make sure terms match
					for i, v := range args.Entries {
						// No more entries in log. we can just append the remainder to log
						if i >= overlap_ct {
							rf.log = append(rf.log[:args.PrevLogIndex+i], args.Entries[i:]...)
							continue
						} else {
							// Term mismatch. Overwrite this and all subsequent entries
							if rf.log[args.PrevLogIndex+i].Term != v.Term {
								rf.log = append(rf.log[:args.PrevLogIndex+i], args.Entries[i:]...)
								continue
							} else { // overwrite one at a time
								rf.log[args.PrevLogIndex+i] = args.Entries[i]
							}
						}
					}
				}

				// Update commit index
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
				rf.persist()
				rf.LogMessage(fmt.Sprintf("Append Finish. My log %v. My CommitIndex %d", rf.log, rf.commitIndex))
				rf.mu.Unlock()
				success = true
			}

		}

		// If leader was ahead in term, or log updated, reset election timer
		if updated || success {
			// Set off go routine to apply any newly committed commands
			go rf.ApplyCommands()
		}
	}

	reply.Term = term
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
	_, isLeader := rf.GetState()
	if isLeader {
		rf.LogMessage(fmt.Sprintf("ADDING REAL COMMAND %v", command))
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		rf.LogMessage(fmt.Sprintf("My log %v", rf.log))

		// Guarantee index is correct
		index = len(rf.log)
		term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
		go RunAppendEntries(rf)
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.ResetElectionTimer()
	quitElectionCh := make(chan bool) // message election goRoutine to quit

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.LogMessage(fmt.Sprintf("Checking if Leader"))
		_, is_leader := rf.GetState()
		rf.LogMessage(fmt.Sprintf("Checked if Leader"))
		if is_leader {
			rf.LogMessage(fmt.Sprintf("Leader Sending Heartbeat RPC"))
			// Heartbeat request is an empty list of messages
			go RunAppendEntries(rf)

			// TODO: add check to not send heartbeat if you've sent a real AppendEntries request recently

		} else {
			if time.Now().Before(rf.GetLatestElectionReset()) {
				rf.LogMessage(fmt.Sprintf("[%s] Heartbeat [%s] received since last sleep. Resetting timer", time.Now().Format("20060102150405.000"), rf.GetLatestElectionReset().Format("20060102150405.000")))

			} else // Otherwise, request an election
			{
				rf.LogMessage(fmt.Sprintf("[%s] Requesting Election", time.Now().Format("20060102150405.000")))
				// Kill timed out election thread and start again
				rf.mu.Lock()
				rf.inElection = true

				select {
				case quitElectionCh <- true:
					rf.LogMessage("Killing old Election thread")
				default:
					rf.LogMessage("No thread to kill")
				}
				// rf.mu.Unlock()

				// rf.mu.Lock()
				rf.currentTerm = rf.currentTerm + 1 // increment term
				rf.votedFor = rf.me                 // vote for self
				rf.persist()
				go RunElection(rf, rf.currentTerm, quitElectionCh)
				rf.mu.Unlock()

				// Reset election timer and go back to sleep
				rf.ResetElectionTimer()
			}
		}
		time.Sleep(HEARTBEAT_FREQUENCY)
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
	rf.lastElectionResetTime = time.Now()
	rf.commitIndex = -1
	rf.lastApplied = 0
	rf.applyChan = applyCh
	rf.appendEntryQuitChan = make(chan bool)

	// Initialize nextIndex to 1 and matchIndex to 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.LogMessage(time.Now().String())
	go rf.ticker()
	rf.LogMessage("Starting Ticker")

	return rf
}

type AppendEntriesThreadMessage struct {
	reply        AppendEntriesReply // Reply Payload
	peerId       int                // Id of peer that this thread managed reply from
	numEntries   int                // Num entries appended to peer
	prevLogIndex int                // Previous log index before Entries were Appended
}

//
// leader sends AppendEntries commands to followers to bring up to sync with its log
// At first assumes followers are in sync ("Heartbeat")
// if not in sync it will try to send successively more to each follower until in sync
//
func RunAppendEntries(rf *Raft) {
	rf.LogMessage("Running append entries process")
	select {
	// Try to send cancellation signal to any current appendentries process
	case rf.appendEntryQuitChan <- true:
		rf.LogMessage("Killing outstanding appendentries process")
	default:
		rf.mu.Lock()
		rf.LogMessage(fmt.Sprintf("My log %v", rf.log))
		rf.mu.Unlock()
		appendReplyCh := make(chan AppendEntriesThreadMessage)

		logEntryStub := []LogEntry{}
		lastLogTerm, lastLogIndex := rf.GetLastLog()

		term, _ := rf.GetState()

		// Set of Vote Request struct
		request_stub := AppendEntriesArgs{
			term,                // Current term
			rf.me,               // Id of term leader
			lastLogIndex,        // Index of last log entry leader believes follower should have
			lastLogTerm,         // Term of last log entry
			rf.GetCommitIndex(), // CommitIndex of Leader
			logEntryStub}        // Entries to append

		// Spawn AppendEntries manager thread for each other server

		// Set up context so we can broadcast to all threads to shut down when parent thread shuts down.
		ctx, cancel := context.WithCancel(context.Background())

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				rf.LogMessage("It me. skipping append entry")
				rf.mu.Lock()
				rf.matchIndex[i] = lastLogIndex
				rf.mu.Unlock()
				continue
			} else {
				go func(rf *Raft, idx int, appendReplyCh chan AppendEntriesThreadMessage) {
					var reply AppendEntriesReply
					success := false
					request := request_stub

					rf.mu.Lock()
					initialTerm := rf.currentTerm
					rf.LogMessage(fmt.Sprintf("Append requester for peer %d. NextIndex %v", idx, rf.nextIndex))
					nextIndex := rf.nextIndex[idx]
					logCopy := make([]LogEntry, len(rf.log))
					copy(logCopy, rf.log)
					rf.mu.Unlock()

					AppendExpireTime := time.Now().Add(rf.GenElectionTimeout())

					// If response OK, send reply to aggregation channel for main thread
					for !success {

						// Check if this appendentries thread was killed
						select {
						case <-ctx.Done():
						default:
							// Check if this thread request is out of date and abandon
							currentTerm, isleader := rf.GetState()
							rf.mu.Lock()
							loglen := len(rf.log)
							rf.mu.Unlock()
							if !isleader || currentTerm != initialTerm || loglen > len(logCopy) {
								return
							}
							// Otherwise adjust index and keep retrying
							rf.LogMessage(fmt.Sprintf("NextIndex %d", nextIndex))
							request.PrevLogIndex = nextIndex - 1 //
							request.Entries = logCopy[request.PrevLogIndex:]
							request.PrevLogTerm = rf.GetLogInfo(request.PrevLogIndex)
							rf.LogMessage(fmt.Sprintf("Sending AppendEntries %v to peer %d", request, idx))
							ok := rf.SendAppendEntries(idx, &request, &reply)
							success = reply.Success
							if ok {
								if reply.Success {
									appendReplyCh <- AppendEntriesThreadMessage{reply, idx, len(request.Entries), request.PrevLogIndex}
									return
								} else { // decrement nextIndex for this peer and try again

									// If follower has higher term, abandon AppendEntries
									if reply.Term > currentTerm {
										rf.LogMessage(fmt.Sprintf("AppendEntries response from %d has higher term. Abandoning and forfeiting leader", idx))
										rf.SafeUpdateTerm(reply.Term)
										return
									} else if request.PrevLogIndex < 1 {
										// Failed even after going back to zero log entry.
										// Must be some other issue.
										rf.mu.Lock()
										rf.LogMessage(fmt.Sprintf("Append response from %d unsuccessful even decrementing to 0. Returning", idx))
										rf.mu.Unlock()
										appendReplyCh <- AppendEntriesThreadMessage{reply, idx, 0, request.PrevLogIndex}
										return
									} else {
										nextIndex += -1
										rf.mu.Lock()
										rf.LogMessage(fmt.Sprintf("Failed to append message for peer %d. Decrementing next Index to %d", idx, nextIndex))
										rf.mu.Unlock()
									}
								}
							} else { // Unsuccessful RPC
								rf.LogMessage(fmt.Sprintf("No OK reply for peer %d.", idx))
								// If we've been trying for a long time, end loop. Otherwise will retry again with the same index
								if time.Now().After(AppendExpireTime) {
									rf.LogMessage(fmt.Sprintf("%d Timed out. Giving up.", idx))
									appendReplyCh <- AppendEntriesThreadMessage{reply, idx, 0, request.PrevLogIndex}
									return
								}
							}
						}
					}
				}(rf, i, appendReplyCh)
			}
		}

		replyTally := 1 // Start with one including yourself
		for rf.killed() == false {
			select {
			// Check if got signal to quit
			case <-rf.appendEntryQuitChan:
				cancel() // Kill outstanding threads
				rf.LogMessage(fmt.Sprintf("Received order to kill myself. Appending log of length %v", lastLogIndex))
				return
			case ThreadMessage := <-appendReplyCh:
				rf.LogMessage(fmt.Sprintf("Received AppendEntries reply %v\n", ThreadMessage))

				if ThreadMessage.reply.Success {
					rf.LogMessage(fmt.Sprintf("Append response from %d successful", ThreadMessage.peerId))
					rf.mu.Lock()
					rf.nextIndex[ThreadMessage.peerId] = (ThreadMessage.prevLogIndex + 1) + ThreadMessage.numEntries
					rf.matchIndex[ThreadMessage.peerId] = rf.nextIndex[ThreadMessage.peerId] - 1
					rf.LogMessage(fmt.Sprintf("Peer %d - NextIndex: %d, MatchIndex: %d", ThreadMessage.peerId, rf.nextIndex[ThreadMessage.peerId], rf.matchIndex[ThreadMessage.peerId]))
					rf.mu.Unlock()

					CommitIndex := rf.CheckCommit()
					currentCommitIndex := rf.GetCommitIndex()

					if CommitIndex > currentCommitIndex {
						rf.LogMessage(fmt.Sprintf("Leader updating commit index from %d to %d", currentCommitIndex, CommitIndex))
						rf.mu.Lock()
						rf.commitIndex = CommitIndex
						rf.mu.Unlock()
						go rf.ApplyCommands()
					}
				} else {
					// Update NextIndex to the last one we tried before failing (should be 1)
					rf.mu.Lock()
					rf.nextIndex[ThreadMessage.peerId] = (ThreadMessage.prevLogIndex + 1) + ThreadMessage.numEntries
					rf.mu.Unlock()
					rf.LogMessage(fmt.Sprintf("Append response from %d unsuccessful even after retries.", ThreadMessage.peerId))
				}
				rf.LogMessage(fmt.Sprintf("Done processing append reply"))

				// Check if all threads replied and if so, close everything
				replyTally += 1
				if replyTally >= rf.NumPeers() {
					rf.LogMessage(fmt.Sprintf("All threads replied. Ending"))
					cancel() // Kill outstanding threads
					return
				}
			}
		}
	}
}

//
// timeout is triggered so server wants to start an election
// increments term, votes for itself, resets timer, and requests votes
//
// Waits for votes to return and if so becomes leader
func RunElection(rf *Raft, targetTerm int, quitElectionCh chan bool) {
	voteReplyCh := make(chan RequestVoteReply)
	voteTally := 1

	rf.LogMessage("Running Election now")
	currentTerm, _ := rf.GetState()

	// If targetTerm is not the next one, we got updated term.
	// Need to abandon election.
	if (currentTerm) != targetTerm {
		rf.LogMessage("Term changed since started. Ending election thread")
		return
	}
	lastLogTerm, lastLogIndex := rf.GetLastLog()
	// Set of Vote Request struct
	request := RequestVoteArgs{
		currentTerm,  // incremented term
		rf.me,        // Server's server ID
		lastLogIndex, // Last log index
		lastLogTerm}  // Last log Term

	// Spawn vote requester for each other server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.LogMessage("It me. skipping append vote")
			continue
		} else {
			go func(rf *Raft, idx int, voteReplyCh chan RequestVoteReply) {
				rf.LogMessage(fmt.Sprintf("Starting thread to request vote for term %d to peer idx: %d", request.Term, idx))
				var reply RequestVoteReply
				ok := rf.sendRequestVote(idx, &request, &reply)

				// If response OK, send reply to vote aggregation channel for main thread
				if ok {
					rf.LogMessage(fmt.Sprintf("Received reply from peer idx: %d", idx))
					voteReplyCh <- reply
					rf.LogMessage(fmt.Sprintf("Successfully pushed vote to channel"))
				}
			}(rf, i, voteReplyCh)
		}
	}

	// Listen for quit election message or for replies to come in
	for rf.killed() == false {
		rf.LogMessage(fmt.Sprintf("Vote listener thread is active and listening for more"))
		select {
		// Election timed out. Kill this routine and stop listening.
		case <-quitElectionCh:
			rf.LogMessage("Kill current election thread")
			return
		// Received Vote. Tally.
		case vote := <-voteReplyCh:
			rf.LogMessage(fmt.Sprintf("Processing vote reply for term %v", vote))
			if vote.VoteGranted {
				voteTally += 1
				// If majority, now the leader. Update state and quit election.
				rf.LogMessage(fmt.Sprintf("Vote tally for term %d: %d", vote.Term, voteTally))
				if voteTally > len(rf.peers)/2 {
					// Election now over.
					rf.InitializeLeader(false)
					// Immediately run append entries to assert control
					go RunAppendEntries(rf)
					return
				}
			} else {
				// Voter's term is higher than candidacy. Reset term and end election
				updated := rf.SafeUpdateTerm(vote.Term)
				if updated {
					return
				}
				// Otherwise keep waiting for votes
			}
		}
	}
}

//
// Apply all remaining committed yet unapplied commands on a given server
//

func (rf *Raft) ApplyCommands() {
	rf.mu.Lock()
	LastApplied := rf.lastApplied
	CommitIndex := rf.commitIndex
	rf.mu.Unlock()

	rf.LogMessage(fmt.Sprintf("Applying Commands from %d to %d", LastApplied, CommitIndex))
	for LastApplied < CommitIndex {
		// Todo: actually apply command
		LastApplied += 1
		rf.mu.Lock()
		command := rf.log[LastApplied-1].Command
		// Send applied message to applyChan
		applymsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: LastApplied,
		}
		rf.lastApplied = LastApplied
		rf.LogMessage(fmt.Sprintf("Updated lastApplied to %d", rf.lastApplied))
		// Need to lock applychan so we don't apply out of order
		rf.applyChan <- applymsg
		rf.mu.Unlock()
	}
	rf.LogMessage(fmt.Sprintf("Done Applying Commands"))
}
