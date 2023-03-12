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
	"context"
	"log"
	"math"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type machineIdentity int
type voteResult int

const (
	minElectionTimeout      = 250 * time.Millisecond
	electionTimeoutInterval = 150

	minNewLeaderElectionTimeout      = 4 * time.Second
	newLeaderElectionTimeoutInterval = 500

	appendEntriesInterval = 200 * time.Millisecond

	pollCollectClientCmdInterval = 50 * time.Millisecond

	minSleepInterval = 5 * time.Millisecond
)

const (
	follower machineIdentity = iota
	candidate
	leader
)

const (
	voteSuccess voteResult = iota
	voteFail
	voteTimeout
	// 如果在candidate状态下收到了当前选举任期或者更新任期的leader发来的心跳RPC，则是下面的状态
	voteNewLeaderFound
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type LogEntry struct {
	LogTerm           int
	Command           interface{}
	Index             int
	CommitCh          chan bool
	ReplyCh           chan int
	ReplyChStopSignal *bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currTerm int
	votedFor int
	log      []*LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leader
	nextIndex  []int
	matchIndex []int
	// lab required
	applyCh                       chan ApplyMsg
	meIdentity                    machineIdentity
	appendCh                      chan int
	lastReceivedAppendEntriesDate time.Time
	lastReceiveFollowerDate       time.Time //用于leader判断自己是否脱离集群，失去leader地位了
	processedIndex                int       // 记录已经经过sendHeartbeat方法处理的最大日志编号

	//term                          int

	//lastCommitLogIndex  int
	//lastReceiveLogIndex int
	//nextLogIndex        int
	//logCh               chan *LogEntry

	//leader                        int
	//notCommitCmdMap map[int]interface{}
	//logPersister    map[int]interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currTerm
	isleader = rf.meIdentity == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	// Lab required
	// network may fail
	OK bool
}

type AppendEntriesArgs struct {
	LeaderId     int
	LeaderTerm   int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	// Lab required
	ReplyCh           chan int
	ReplyChStopSignal *bool
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 卫语句形式
	// 比较当前任期和candidate的任期
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currTerm > args.Term {
		// 不是比当前更新的任期，投反对票
		log.Printf("任期落后 rf:%v term:%v vote down to candidate:%v candidateTerm:%v", rf.me, rf.currTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		return
	}

	if rf.currTerm == args.Term {
		if rf.votedFor != -1 {
			log.Printf("本任期已经投给其他candidate了 rf:%v term:%v vote down to candidate:%v candidateTerm:%v", rf.me, rf.currTerm, args.CandidateId, args.Term)
			reply.VoteGranted = false
			reply.Term = rf.currTerm
			return
		}
	}

	// 论文5.4.1 检查日志完整性，candidate完整性低的话投反对票
	if rf.lastApplied > args.LastLogIndex {
		log.Printf("日志不完整 rf:%v term:%v vote down to candidate:%v candidateTerm:%v", rf.me, rf.currTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		// 收到的candidate日志完整性更加低，但是term>=自己，针对>的情况，自身需要更新任期才能赶上选举，让自己凭着更高的完整性当上leader
		rf.currTerm = args.Term
		return
	}

	// 日志完整性校验通过，投赞成票
	log.Printf("rf:%v term:%v vote up to candidate:%v candidateTerm:%v", rf.me, rf.currTerm, args.CandidateId, args.Term)
	if rf.meIdentity == leader {
		// 剥夺leader权力
		rf.meIdentity = follower
	}
	reply.VoteGranted = true
	rf.currTerm = args.Term
	rf.votedFor = args.CandidateId
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if args.LogIndex != -1 {
	log.Printf("rf:%v follower receive prevLogIdx:%v prevLogTerm:%v status:%v, lastApplied:%v, argsCommitIdx:%v, rfCommitIdx:%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.meIdentity, rf.lastApplied, args.LeaderCommit, rf.commitIndex)
	//}

	// 如果当前处于candidate状态
	if rf.meIdentity == candidate {
		// 检查任期
		if args.LeaderTerm < rf.currTerm {
			reply.Success = false
			reply.Term = rf.currTerm
			return
		}
		// 更新任期
		rf.currTerm = args.LeaderTerm
		// 通知正在发送voteRequest的goroutine停止
		rf.appendCh <- 2
	} else if rf.meIdentity == follower {
		// 检查任期（针对可能两个leader同时存在的情况，旧leader发的append不理会）
		if args.LeaderTerm < rf.currTerm {
			// 没有理会过期leader的append消息
			log.Printf("true from 3")
			reply.Success = false
			reply.Term = rf.currTerm
			return
		}
		// 如果换了新的leader，与新leader同步
		if rf.votedFor != args.LeaderId {
			rf.votedFor = args.LeaderId
		}
		//// 判断前一条日志的任期是否符合
		//if args.PrevLogIndex > len(rf.log)+1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].LogTerm {
		//	log.Printf("rf:%v 日志错序 args.PrevLogIdx:%v args.PrevLogTerm:%v rf.log[%v].LogTerm:%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].LogTerm)
		//	log.Printf("%v", len(rf.log))
		//	reply.Success = false
		//	reply.Term = rf.currTerm
		//	rf.lastReceivedAppendEntriesDate = time.Now()
		//	return
		//}
		// 更新任期
		rf.currTerm = args.LeaderTerm
	} else if rf.meIdentity == leader {
		if args.LeaderTerm < rf.currTerm {
			// reply置为nil，说明没有理会过期的append消息
			reply.Success = false
			//reply.Term = -1
			return
		}
		// 更新任期
		rf.currTerm = args.LeaderTerm
		// 转回follower
		rf.meIdentity = follower
		//log.Fatalf("mutiple leaders exist")
	}
	// 判断前一条日志的任期是否符合
	followerLastReceivedIdx := rf.log[len(rf.log)-1].Index
	if args.PrevLogIndex > followerLastReceivedIdx || args.PrevLogTerm != rf.log[args.PrevLogIndex].LogTerm {
		// 仅仅是打log的一个判断
		if args.PrevLogIndex <= followerLastReceivedIdx {
			log.Printf("rf:%v 日志错序 args.PrevLogIdx:%v args.PrevLogTerm:%v rf.log[%v].LogTerm:%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].LogTerm)
		}
		log.Printf("%v", len(rf.log))
		reply.Success = false
		reply.Term = rf.currTerm
		rf.lastReceivedAppendEntriesDate = time.Now()
		return
	}
	// 能走到这里说明reply.Success = true
	reply.Success = true
	reply.Term = rf.currTerm
	rf.lastReceivedAppendEntriesDate = time.Now()
	// 更新日志状态
	newEntries := []*LogEntry{}
	for i := 0; i < len(args.Entries); i++ {
		entryFromLeader := args.Entries[i]
		log.Printf("rf:%v 收到append idx:%v cmd:%v", rf.me, entryFromLeader.Index, entryFromLeader.Command)
		entry := &LogEntry{
			LogTerm: entryFromLeader.LogTerm,
			Command: entryFromLeader.Command,
			Index:   entryFromLeader.Index,
		}

		if entry.Index < len(rf.log) {
			// 如果存在，则直接覆盖，无论是否相同
			rf.log[entry.Index] = entry
		} else {
			// 不存在则先添加到临时切片newEntries
			newEntries = append(newEntries, entry)
		}
	}
	rf.log = append(rf.log, newEntries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log[len(rf.log)-1].Index)))
	}
	// commit并且持久化
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: rf.log[i].Index,
		}
		rf.applyCh <- msg
	}
	// 更新lastApplied
	rf.lastApplied = rf.commitIndex
	// todo persist
	rf.appendCh <- 1
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, replyCh chan RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	reply.OK = ok
	log.Printf("rf:%v candidate received server:%v vote reply ok:%v voteGranted:%v", rf.me, server, ok, reply.VoteGranted)
	replyCh <- *reply
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	replyCh := args.ReplyCh
	stopSignal := args.ReplyChStopSignal

	log.Printf("rf:%v ready send server:%v prevLogIndex:%v prevLogTerm:%v", rf.me, server, args.PrevLogIndex, args.PrevLogTerm)
	if len(args.Entries) == 0 {
		log.Printf("normal hearbeat")
	} else {
		for i := 0; i < len(args.Entries); i++ {
			log.Printf("entry idx:%v command:%v logTerm:%v", args.Entries[i].Index, args.Entries[i].Command, args.Entries[i].LogTerm)
		}
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		rf.lastReceiveFollowerDate = time.Now()
		if reply.Success {
			if stopSignal != nil && *stopSignal != true {
				log.Printf("rf:%v 这里发送replyCh，来自server:%v ok:%v accept:%v", rf.me, server, ok, reply.Success)
				replyCh <- 1
				log.Printf("没有卡主")
			}
		}
	}

	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return 0, 0, false
	}

	rf.mu.Lock()
	index = rf.nextIndex[rf.me]
	log.Printf("rf:%v 收到命令 cmd:%v idx:%v rfIdentity:%v %v", rf.me, command, index, rf.meIdentity, time.Now())
	rf.nextIndex[rf.me]++
	signal := false
	logEntry := LogEntry{
		LogTerm:           rf.currTerm,
		Command:           command,
		Index:             index,
		CommitCh:          make(chan bool, 1),
		ReplyCh:           make(chan int, len(rf.peers)),
		ReplyChStopSignal: &signal,
	}
	rf.log = append(rf.log, &logEntry)
	rf.mu.Unlock()

	select {
	case success := <-logEntry.CommitCh:
		if success {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: index,
			}
			rf.applyCh <- msg
			// todo persist
		} else {
			rf.mu.Lock()
			// 同时更新follower的matchIndex和nextIndex
			log.Printf("rf:%v 修复已经接受到valid=false命令的nextIndex和matchIndexIndex", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				// 有一部分节点已经接收到valid=true的包了，我们把他们的matchIndex和nextIndex减少
				// 下次给他们发心跳的时候就会发valid=false版本的RPC，他们就能覆盖掉valid=true的版本
				log.Printf("rf:%v server:%v matchidx:%v invalidIndex:%v", rf.me, i, rf.matchIndex[i], index)
				if rf.matchIndex[i] >= index {
					rf.matchIndex[i] = index - 1
					rf.nextIndex[i] = index
				}
			}
			// 删除这条log，并且更新log数组元素的index
			rf.log = append(rf.log[:index], rf.log[index+1:]...)
			for idx := index; idx < len(rf.log); idx++ {
				log.Printf("rf:%v 超时后更新索引 log[%v].Index=%v 更新为Index=%v", rf.me, idx, rf.log[idx].Index, idx)
				rf.log[idx].Index = idx
			}
			rf.nextIndex[rf.me]--
			rf.processedIndex--
			rf.mu.Unlock()
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	// 第一次调用的时候初始化raft.lastReceivedAppendEntriesDate
	rf.lastReceivedAppendEntriesDate = time.Now()

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()

		if rf.meIdentity == follower {
			rf.checkAppendEntries()
		} else if rf.meIdentity == candidate {
			rf.startNewElection()
		} else if rf.meIdentity == leader {
			rf.sendHeartbeat()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.meIdentity = follower
	rf.appendCh = make(chan int, 100*len(rf.peers))
	rf.applyCh = applyCh
	rf.currTerm = 0
	rf.log = []*LogEntry{}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.lastApplied = 0
	rf.commitIndex = 0
	// 初始化，防止第一个任期vote的时候读到rf.log[0]=nil
	rf.log = append(rf.log, &LogEntry{})
	rf.votedFor = -1

	//file := "./" + "log" + ".txt"
	//logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	//if err != nil {
	//	panic(err)
	//}
	//log.SetOutput(logFile) // 将文件设置为log输出的文件

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) checkAppendEntries() {
	// 随机一个数字
	randomOffset := rand.Int63n(electionTimeoutInterval)
	randomElectionTimeout := time.Duration(randomOffset)*time.Millisecond + minElectionTimeout
	realElectionTimeout := randomElectionTimeout - time.Since(rf.lastReceivedAppendEntriesDate)
	prevTerm := rf.currTerm
	rf.mu.Unlock()
	// 睡眠
	time.Sleep(realElectionTimeout)
	// 与上一次记录的接收心跳时间作比较
	rf.mu.Lock()
	//log.Printf("rf:%v follower check append", rf.me)
	if time.Since(rf.lastReceivedAppendEntriesDate) > realElectionTimeout {
		// 超时未收到心跳，开始新的选举
		// 判断是不是已经处于选举阶段了，如果是因为选举新leader引起的超时，则再睡一段时间
		if rf.currTerm != prevTerm {
			ctx, cancel := context.WithTimeout(context.Background(), minNewLeaderElectionTimeout+newLeaderElectionTimeoutInterval*time.Millisecond)
			defer cancel()
			// 记住！不能带锁睡觉，不然receive append那里无法拿到锁去唤醒这里的select
			rf.mu.Unlock()
			select {
			case <-rf.appendCh:
				// 新的leader已经发消息来了，解除睡眠
				//因为appendCh带缓冲区，所以要清空可能囤积的消息
				for _ = range rf.appendCh {
				}
				//log.Printf("rf:%v 消费appendCh 新任期超时唤醒", rf.me)
				return
			case <-ctx.Done():
				// 超时还没有新leader的消息，则再开一轮选举
				rf.mu.Lock()
				log.Printf("rf:%v term:%v 选举中超时", rf.me, rf.currTerm)
				rf.meIdentity = candidate
				rf.mu.Unlock()
			}
		} else {
			log.Printf("rf:%v term:%v 普通超时", rf.me, rf.currTerm)
			rf.meIdentity = candidate
			rf.mu.Unlock()
		}
	} else {
		// 如果没有超时，这里直接解锁
		// 消费来自follower状态下，正常接收一条append日志的appendCh
		//for _ = range rf.appendCh {
		//}
		//log.Printf("rf:%v lastIdx:%v 没有超时", rf.me, rf.lastCommitLogIndex)
		rf.mu.Unlock()
		<-rf.appendCh
		//for _ = range rf.appendCh {
		//}
		//log.Printf("rf:%v 消费appendCh follower没有超时", rf.me)
	}
}

func (rf *Raft) sendHeartbeat() {
	//log.Printf("rf:%v 判断%v %v", rf.me, time.Since(rf.lastReceiveFollowerDate), time.Duration(electionTimeoutInterval)*time.Millisecond+minElectionTimeout)
	if time.Since(rf.lastReceiveFollowerDate) > time.Duration(electionTimeoutInterval)*time.Millisecond+minElectionTimeout {
		log.Printf("rf:%v 超越最长的选举timeout，不会再是leader了", rf.me)
		// 清空未处理的任务，一起返回客户端失败
		for idx := rf.processedIndex + 1; idx < len(rf.log); idx++ {
			rf.log[idx].CommitCh <- false
		}
		rf.meIdentity = follower
		rf.mu.Unlock()
		return
	}
	prevNow := time.Now()

	//log.Printf("rf:%v term:%v send heartbeat", rf.me, rf.term)

	// append rpc内容
	var logIndex int
	var command interface{}
	var replyClientCh chan bool
	var replyCh chan int
	var stopSignal *bool

	// logIndex是已收到客户端但是还没进行发送的命令号（并发情况下logIndex!=rf.lastApplied+1，也logIndex!=rf.nextIndex[rf.me]-1）
	logIndex = rf.processedIndex + 1
	needWaitForReplies := false
	// 本来rf.processedIndex + 1期待是比现有log数组最大的日志编号大一号的，如果这个位置有log了，说明收到client的命令了
	if logIndex <= len(rf.log)-1 {
		needWaitForReplies = true
		// 填写收集reply方法需要的参数
		command = rf.log[logIndex].Command
		replyClientCh = rf.log[logIndex].CommitCh
		// 本轮日志发送的reply收集channel
		replyCh = rf.log[logIndex].ReplyCh
		// stopSignal解决"多个sender一个receiver"同步关闭replyCh的问题
		stopSignal = rf.log[logIndex].ReplyChStopSignal
		// 这条命令已经被处理了，自增，这样processQueue能够拿到这条命令
		rf.processedIndex++
		log.Printf("组装带命令心跳包的收集内容 idx:%v len:%v replyCh:%v stopSignal:%v", logIndex, len(rf.log), replyCh, stopSignal)
	} else {
		needWaitForReplies = false
	}
	rf.mu.Unlock()

	if needWaitForReplies {
		rf.collectAppendReply(replyClientCh, replyCh, logIndex, stopSignal, command)
	}

	// 计算真实的interval
	realInterval := pollCollectClientCmdInterval - time.Since(prevNow)
	if realInterval > 0 {
		time.Sleep(realInterval)
	} else {
		time.Sleep(minSleepInterval)
	}
}

func (rf *Raft) collectAppendReply(commitCh chan bool, replyCh chan int, logIndex int, replyChStopSignal *bool, command interface{}) {
	currReplies := 0
	minElectionTimeoutCh := time.After(minElectionTimeout)
CollectLogReply:
	for {
		select {
		case <-replyCh:
			// 收集
			log.Printf("rf:%v 收到回复replyCh idx:%v cmd:%v", rf.me, logIndex, command)
			currReplies++
			if currReplies*2+1 >= len(rf.peers) {
				rf.mu.Lock()
				if replyChStopSignal != nil {
					*replyChStopSignal = true
					close(replyCh)
				}
				// commit这条LogEntry
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: logIndex,
				}
				rf.applyCh <- applyMsg
				rf.lastApplied = logIndex
				rf.commitIndex = rf.lastApplied
				// 把nextIndex变回大于leader现有日志大一号的状态
				rf.nextIndex[rf.me] = rf.lastApplied + 1
				log.Printf("rf:%v leader apply idx:%v cmd:%v", rf.me, logIndex, command)
				// todo persist

				commitCh <- true
				rf.mu.Unlock()
				break CollectLogReply
			}
		case <-minElectionTimeoutCh:
			log.Printf("等待append回复超时，idx:%v 命令无效", logIndex)
			//rf.mu.Lock()
			// 删除logIndex日志
			// rf.log = append(rf.log[:logIndex], rf.log[logIndex+1:]...)
			if replyChStopSignal != nil {
				*replyChStopSignal = true
				close(replyCh)
			}
			commitCh <- false
			//rf.mu.Unlock()
			break CollectLogReply
		}
	}
}

func (rf *Raft) startNewElection() {
	// 选举定时器
	randomOffset := rand.Intn(newLeaderElectionTimeoutInterval)
	randomNewLeaderElectionTimeout := time.Duration(randomOffset)*time.Millisecond + minNewLeaderElectionTimeout
	ctx, cancel := context.WithTimeout(context.Background(), randomNewLeaderElectionTimeout)

	rf.meIdentity = candidate
	rf.currTerm++
	electionTerm := rf.currTerm
	//log.Printf("rf:%v term:%d", &rf, rf.term)

	// 自己也有一票
	voteUpNumbers := 1
	rf.votedFor = rf.me
	// 总共收到的票数
	voteAllReplies := 0
	voteRes := voteFail
	voteReplyCh := make(chan RequestVoteReply)
	// 发送选票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         electionTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastApplied,
			LastLogTerm:  rf.log[rf.lastApplied].LogTerm,
		}
		reply := RequestVoteReply{}
		// 投票RPC新开一个goroutine去处理，当那些goroutine接收到reply时，通知这个goroutine
		go rf.sendRequestVote(i, &args, &reply, voteReplyCh)
	}
	rf.mu.Unlock()

CollectVotes:
	for {
		select {
		case reply := <-voteReplyCh:
			//log.Printf("rf:%v term:%v timeout:%v up:%v", &rf, rf.term, reply.Timeout, reply.VoteUp)
			if reply.OK {
				voteAllReplies++
				if reply.VoteGranted {
					// 收到赞成票
					voteUpNumbers++
					// 检查是否过半
					if 2*voteUpNumbers > len(rf.peers) {
						voteRes = voteSuccess
						break CollectVotes
					}
				} else {
					// 收到反对票
					// 如果已经有半数或以上节点投了反对票，选举失败
					if 2*(voteAllReplies-voteUpNumbers) >= len(rf.peers) {
						log.Printf("rf:%v electionTerm:%v 收到一半以上的反对票", rf.me, electionTerm)
						voteRes = voteFail
						break CollectVotes
					}
				}
			}

		case <-ctx.Done():
			// 超时还没有跳出循环，需要再次新开一轮选举
			voteRes = voteTimeout
			break CollectVotes
		// 如果在此期间收到了新leader的RPC，说明从这里变回follower状态
		case fromAppendCh := <-rf.appendCh:
			//log.Printf("rf:%v 消费appendCh", rf.me)
			// 判断appendCh里面的消息，读出来==2的时候才是voteFail的情况，其他都是堆积的无效消息
			if fromAppendCh == 2 {
				log.Printf("rf:%v electionTerm:%v append ch", rf.me, electionTerm)
				voteRes = voteFail
				break CollectVotes
			}
		}
	}
	// 释放ctx资源
	cancel()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteRes == voteSuccess {
		// 选举成功
		log.Printf("rf:%v elect success", rf.me)
		// 初始化lastReceiveFollowerDate
		rf.lastReceiveFollowerDate = time.Now()
		// 初始化发送列表
		for i := 0; i < len(rf.peers); i++ {
			// 更新nextIndex
			rf.nextIndex[i] = rf.commitIndex + 1
			rf.processedIndex = rf.commitIndex
			if i == rf.me {
				continue
			}
			// matchIndex
			rf.matchIndex[i] = 0
			// 启动发送队列
			go rf.processSendQueue(i)
		}

		rf.meIdentity = leader
	} else if voteRes == voteFail {
		// 选举失败
		log.Printf("rf:%v 选举失败", rf.me)
		rf.votedFor = -1
		rf.meIdentity = follower
	} else if voteRes == voteTimeout {
		// 超时，重新选举
		rf.votedFor = -1
		rf.meIdentity = candidate
	}
}

//hahahehe
// todo 从appendRPC收到新leader的消息，恢复成follower
// todo If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)

func (rf *Raft) processSendQueue(server int) {
	for true {
		// 每次循环都检查自己的leader身份是否结束
		rf.mu.Lock()
		isLeader := rf.meIdentity == leader
		if !isLeader {
			rf.mu.Unlock()
			log.Printf("rf:%v found myself not leader", rf.me)
			return
		}
		prevNow := time.Now()
		// 记录当前leader期待客户端发来的最新命令编号，如果一套请求响应流程走完后，这个编号更新了
		// 说明又有新命令到来了，不sleep直接continue
		leaderNextIndex := rf.nextIndex[rf.me]
		// 组装args参数
		// 拿到"可以发送部分"的最大日志编号
		lastProcessedLogIndex := rf.processedIndex
		// "前一条日志"应该是rf.nextIndex[server] - 1，这样follower才可以判断日志是否错序
		// 但是在刚刚开始的时候，一条客户端的命令都没有，rf.nextIndex和rf.matchIndex都是0
		// 这时候rf.nextIndex[server] - 1就会取到负数-1，这种情况换成matchIndex的0
		var prevLogIdx int
		if rf.nextIndex[server] > rf.matchIndex[server] {
			prevLogIdx = rf.nextIndex[server] - 1
		} else {
			prevLogIdx = rf.matchIndex[server]
		}
		log.Printf("rf:%v lastProcessedLogIndex:%v nextIdx[%v]:%v", rf.me, lastProcessedLogIndex, server, rf.nextIndex[server])
		var prevLogTerm int
		prevLogTerm = rf.log[prevLogIdx].LogTerm

		var args AppendEntriesArgs
		var reply AppendEntriesReply

		// 判断要发的entries数组最后一个元素（也就是已处理的最新的logIndex）
		if lastProcessedLogIndex >= rf.nextIndex[server] {
			startIdx := rf.nextIndex[server]
			// 因为只有最后一个logIndex有可能还在等待reply，所以下面两个参数选择的是最后一个log
			replyChOfLastLog := rf.log[lastProcessedLogIndex].ReplyCh
			stopSignalOfLastLog := rf.log[lastProcessedLogIndex].ReplyChStopSignal
			log.Printf("server:%v的nextidx是%v，lastProcessedLogIndex:%v, replyCh:%v stopSignal:%v", server, rf.nextIndex[server], lastProcessedLogIndex, replyChOfLastLog, stopSignalOfLastLog)
			entries := rf.log[startIdx : lastProcessedLogIndex+1]
			args = AppendEntriesArgs{
				LeaderId:          rf.me,
				LeaderTerm:        rf.currTerm,
				LeaderCommit:      rf.commitIndex,
				PrevLogIndex:      prevLogIdx,
				PrevLogTerm:       prevLogTerm,
				Entries:           entries,
				ReplyCh:           replyChOfLastLog,
				ReplyChStopSignal: stopSignalOfLastLog,
			}
			reply = AppendEntriesReply{}

		} else {
			// 发送空心跳
			args = AppendEntriesArgs{
				LeaderId:          rf.me,
				LeaderTerm:        rf.currTerm,
				LeaderCommit:      rf.commitIndex,
				PrevLogIndex:      prevLogIdx,
				PrevLogTerm:       prevLogTerm,
				Entries:           nil,
				ReplyCh:           nil,
				ReplyChStopSignal: nil,
			}
			reply = AppendEntriesReply{}
		}
		rf.mu.Unlock()
		// 发送请求
		ok := rf.sendAppendEntries(server, &args, &reply)
		// 处理回复
		rf.mu.Lock()
		retryImmediately := false
		if ok {
			if reply.Success {
				// ok && reply.Success
				// 一切正常，更新nextIndex和matchIndex
				if len(args.Entries) != 0 {
					rf.matchIndex[server] = lastProcessedLogIndex
					rf.nextIndex[server] = lastProcessedLogIndex + 1
				}
			} else {
				// ok && !reply.Success
				if reply.Term > rf.currTerm {
					// 如果follower是因为发现自己任期落后，则要检查自己是不是收到voteRPC、appendRPC变回follower了
					if rf.meIdentity != leader {
						// 已经失去leader身份
						rf.mu.Unlock()
						return
					}
					// 没有收到voteRPC、appendRPC消息，自己还是leader，则更新term，并立即重试
					rf.currTerm = reply.Term
					retryImmediately = true
				} else {
					// 如果follower不是因为任期落后而拒绝的，说明是日志错序
					// nextIndex[server]-1并立即重试
					rf.nextIndex[server]--
					log.Printf("------")
					retryImmediately = true
				}
			}
		} else {
			// !ok
			// 网络原因，需立即重试
			retryImmediately = true
		}
		// 一轮请求/响应结束，判断是否需要睡眠以及睡眠的时间
		if retryImmediately {
			rf.mu.Unlock()
			time.Sleep(minSleepInterval)
			continue
		}
		if leaderNextIndex != rf.nextIndex[rf.me] && leaderNextIndex != 0 {
			log.Printf("rf:%v 因为收到新命令，需要立即重试 原nextIndex:%v 现nextIndex:%v", rf.me, leaderNextIndex, rf.nextIndex[rf.me])
			rf.mu.Unlock()
			time.Sleep(minSleepInterval)
			continue
		}
		rf.mu.Unlock()
		// 计算真实的睡眠时间
		realInterval := appendEntriesInterval - time.Since(prevNow)
		if realInterval > 0 {
			time.Sleep(realInterval)
		} else {
			time.Sleep(minSleepInterval)
		}
	}
}
