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
	"6.824/labgob"
	"bytes"
	"container/list"
	"context"
	"log"
	"math/rand"
	"os"

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

	pollCollectAppendReplyInterval = 50 * time.Millisecond
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
	command  interface{}
	index    int
	commitCh chan int
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

	lastReceivedAppendEntriesDate time.Time
	lastReceiveFollowerDate       time.Time //用于leader判断自己是否脱离集群，失去leader地位了
	meIdentity                    machineIdentity
	term                          int
	appendCh                      chan int
	lastCommitLogIndex            int
	lastReceiveLogIndex           int
	nextLogIndex                  int
	logCh                         chan *LogEntry
	applyCh                       chan ApplyMsg
	sendQueue                     []*list.List
	leader                        int
	notCommitCmdMap               map[int]interface{}
	logPersister                  map[int]interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.meIdentity == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastCommitLogIndex)
	e.Encode(rf.term)
	e.Encode(rf.leader)
	e.Encode(rf.logPersister)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastCommitLogIndex int
	var term int
	var leader int
	var logPersister map[int]interface{}
	if d.Decode(&lastCommitLogIndex) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&leader) != nil ||
		d.Decode(&logPersister) != nil {
		log.Fatalf("error read persist")
	} else {
		rf.lastCommitLogIndex = lastCommitLogIndex
		rf.term = term
		rf.leader = leader
		rf.logPersister = logPersister
	}
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
	ElectionTerm       int
	LastCommitLogIndex int
	Leader             int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteUp  bool
	Timeout bool
}

type AppendEntriesArgs struct {
	Leader                int
	LeaderTerm            int
	LogIndex              int
	Command               interface{}
	ReplyCh               chan int
	ReplyChStopSignal     *bool
	NeedWaitApply         bool
	LeaderLastCommitIndex int
}

type AppendEntriesReply struct {
	Accept         bool
	ExpectLogIndex int
}

type CommitEntriesArgs struct {
	LogIndex int
	Command  interface{}
}

type CommitEntriesReply struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 卫语句形式
	// 比较当前任期和candidate的任期
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term >= args.ElectionTerm {
		// 不是比当前更新的任期，投反对票
		log.Printf("任期落后 rf:%v term:%v vote down to candidate:%v candidateTerm:%v", rf.me, rf.term, args.Leader, args.ElectionTerm)
		reply.VoteUp = false
		return
	}

	// 论文5.4.1 检查日志完整性，candidate完整性低的话投反对票
	if rf.lastCommitLogIndex > args.LastCommitLogIndex {
		log.Printf("日志不完整 rf:%v term:%v vote down to candidate:%v candidateTerm:%v", rf.me, rf.term, args.Leader, args.ElectionTerm)
		reply.VoteUp = false
		// 如果自己有更完整的日志，本应成为新的leader，但是因为没有跟上最新的选举任期节奏，接下来也一直跟不上
		// 所以这种情况需要停掉本次选举，更新任期
		if rf.meIdentity == candidate {
			rf.term = args.ElectionTerm
			rf.appendCh <- 2
		}
		return
	}

	// 日志完整性校验通过，投赞成票
	log.Printf("rf:%v term:%v vote up to candidate:%v candidateTerm:%v", rf.me, rf.term, args.Leader, args.ElectionTerm)
	if rf.meIdentity == leader {
		// 剥夺leader权力
		rf.meIdentity = follower
	}
	reply.VoteUp = true
	rf.term = args.ElectionTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//if args.LogIndex != -1 {
	log.Printf("rf:%v follower receive idx:%v status:%v, lsatIdx:%v", rf.me, args.LogIndex, rf.meIdentity, rf.lastCommitLogIndex)
	//}

	// 如果当前处于candidate状态
	if rf.meIdentity == candidate {
		// 检查任期
		if args.LeaderTerm < rf.term {
			if time.Since(rf.lastReceivedAppendEntriesDate) > time.Duration(newLeaderElectionTimeoutInterval)*time.Millisecond+minNewLeaderElectionTimeout {
				// 走到了这里，说明是自己出了问题开启了一次错误的选举，leader本身没有问题，需要恢复到原来的状态
				// 恢复任期
				log.Printf("rf:%v fount myself error", rf.me)
				rf.term = args.LeaderTerm
				rf.meIdentity = follower
				// 通知正在发送voteRequest的goroutine停止
				//log.Printf("rf:%v 生产appendCh candidate", rf.me)
				rf.appendCh <- 2
				rf.lastReceivedAppendEntriesDate = time.Now()
				// 走到这里虽然发现是自己的网络问题了，但是依然要无视，因为这条append RPC的logIndex > rf.lastCommitLogIndex+1
				// 是不连续的日志，我们期望的是rf.lastCommitLogIndex+1的日志，所以我们现在还不能接受
				// 我们把任期和identity恢复后，下一次收到leader重试的rf.lastCommitLogIndex+1后就能恢复正常了
				// rf.checkNeedAutoCommitLastEntry(args)
				log.Printf("true from 2")
				reply.Accept = false
				reply.ExpectLogIndex = rf.lastCommitLogIndex + 1
				return
			}
			// 考虑一种情况，某个follower自己网络出了问题，他开启了一次新的选举，但是因为是网络问题所以选票没有传出去
			// 他也收不到其他节点的发对票，所以他一直重启选举
			// 当他恢复网络时，收到了很多来自leader的append消息，如果收到了比自己lastCommitLogIndex+1还要大的append消息，那说明
			// 在他开启选举期间，整个集群都认为leader是有效的（因为lastCommitLogIndex+1被leader commit了，才会发下一个append）
			// 这样这个follower就能发现之前是自己出了问题
			if args.LogIndex == -1 || args.LogIndex <= rf.lastCommitLogIndex+1 {
				// 即使args.LogIndex == rf.lastCommitLogIndex+1，也有可能是leader真的超时了，我们依然认为自己的candidate是有效的
				log.Printf("true from 1")
				reply.Accept = false
				reply.ExpectLogIndex = rf.lastCommitLogIndex + 2
				return
			} else {
				// 走到了这里，说明是自己出了问题开启了一次错误的选举，leader本身没有问题，需要恢复到原来的状态
				// 恢复任期
				log.Printf("rf:%v fount myself error", rf.me)
				rf.term = args.LeaderTerm
				rf.meIdentity = follower
				// 通知正在发送voteRequest的goroutine停止
				//log.Printf("rf:%v 生产appendCh candidate", rf.me)
				rf.appendCh <- 2
				rf.lastReceivedAppendEntriesDate = time.Now()
				// 走到这里虽然发现是自己的网络问题了，但是依然要无视，因为这条append RPC的logIndex > rf.lastCommitLogIndex+1
				// 是不连续的日志，我们期望的是rf.lastCommitLogIndex+1的日志，所以我们现在还不能接受
				// 我们把任期和identity恢复后，下一次收到leader重试的rf.lastCommitLogIndex+1后就能恢复正常了
				log.Printf("true from 2")
				reply.Accept = false
				reply.ExpectLogIndex = rf.lastCommitLogIndex + 1

				return
			}
		}
		// 走到了这里，说明原来的leader出了问题，但是这轮选举中已经有了结果，有别的节点成功当上了leader
		// 更新任期
		rf.term = args.LeaderTerm
		// 通知正在发送voteRequest的goroutine停止
		rf.lastReceivedAppendEntriesDate = time.Now()
		// rf.checkNeedAutoCommitLastEntry(args)
		// 把notCommitMap删除，上一个leader未commit的数据轮到下一个leader的时候，可能会出现同样的index不同command的情况
		// 我们需要以新的leader为准，所以把未commit的部分删除
		rf.lastReceiveLogIndex = rf.lastCommitLogIndex
		for k, _ := range rf.notCommitCmdMap {
			delete(rf.notCommitCmdMap, k)
		}
		// 更新日志状态
		if args.LogIndex != -1 {
			log.Printf("(2)rf:%v 更新lastIdx%v为%v", rf.me, rf.lastReceiveLogIndex, args.LogIndex)
			// rf.lastCommitLogIndex = args.LogIndex
			rf.lastReceiveLogIndex = args.LogIndex
		}
		reply.Accept = true
		reply.ExpectLogIndex = rf.lastReceiveLogIndex + 1
		rf.notCommitCmdMap[args.LogIndex] = args.Command

		//log.Printf("rf:%v 生产appendCh candidate", rf.me)
		rf.appendCh <- 2
	} else if rf.meIdentity == follower {
		// 检查任期（针对可能两个leader同时存在的情况，旧leader发的append不理会）
		if args.LeaderTerm < rf.term {
			// 没有理会过期leader的append消息
			log.Printf("true from 3")
			reply.Accept = false
			reply.ExpectLogIndex = -1
			return
		}
		// 如果换了新的leader，与新leader同步
		hasChangedLeader := false
		if rf.leader != args.Leader {
			hasChangedLeader = true
			if rf.lastCommitLogIndex > args.LeaderLastCommitIndex {
				// 如果follower比leader的lastIndex大，需要丢弃不相同的部分。如果follower较小，则不用动
				log.Printf("(3)rf:%v 更新lastIdx%v为%v", rf.me, rf.lastReceiveLogIndex, args.LogIndex)
				rf.lastCommitLogIndex = args.LeaderLastCommitIndex
				rf.lastReceiveLogIndex = rf.lastCommitLogIndex
			}
			// 把notCommitMap删除，上一个leader未commit的数据轮到下一个leader的时候，可能会出现同样的index不同command的情况
			// 我们需要以新的leader为准，所以把未commit的部分删除
			rf.lastReceiveLogIndex = rf.lastCommitLogIndex
			for k, _ := range rf.notCommitCmdMap {
				delete(rf.notCommitCmdMap, k)
			}
			rf.leader = args.Leader
		}
		if !hasChangedLeader {
			rf.checkNeedAutoCommitLastEntry(args)
		}

		// 论文5.5 一个 follower 如果接收了一个 AppendEntries 请求
		// 但是这个请求里面的这些日志条目在它日志中已经有了，它就会直接忽略这个新的请求中的这些日志条目。
		// 当args.logIndex == -1的时候是一条空心跳包，接受它
		if args.LogIndex != -1 && rf.lastCommitLogIndex >= args.LogIndex && !hasChangedLeader {
			reply.Accept = true
			if rf.lastReceiveLogIndex < rf.lastCommitLogIndex {
				rf.lastReceiveLogIndex = rf.lastCommitLogIndex
			}
			reply.ExpectLogIndex = rf.lastReceiveLogIndex + 1
			rf.lastReceivedAppendEntriesDate = time.Now()
			return
		}
		// 日志错序，先不接受，等待rf.lastCommitLogIndex + 1日志到达
		if args.LogIndex != -1 && args.LogIndex != rf.lastCommitLogIndex+1 {
			rf.lastReceivedAppendEntriesDate = time.Now()
			log.Printf("rf:%v log index:%v expect:%v, true from 4", rf.me, args.LogIndex, rf.lastCommitLogIndex+1)
			reply.Accept = false
			reply.ExpectLogIndex = rf.lastCommitLogIndex + 1
			return
		}
		// 更新日志状态
		if args.LogIndex != -1 {
			log.Printf("(4)rf:%v 更新lastIdx%v为%v", rf.me, rf.lastReceiveLogIndex, args.LogIndex)
			rf.lastReceiveLogIndex = args.LogIndex
		}
		rf.notCommitCmdMap[args.LogIndex] = args.Command
		rf.term = args.LeaderTerm
		rf.lastReceivedAppendEntriesDate = time.Now()
		reply.Accept = true
		if rf.lastReceiveLogIndex < rf.lastCommitLogIndex {
			rf.lastReceiveLogIndex = rf.lastCommitLogIndex
		}
		reply.ExpectLogIndex = rf.lastReceiveLogIndex + 1

		log.Printf("rf:%v 收到append idx:%v cmd:%v", rf.me, args.LogIndex, args.Command)
		rf.appendCh <- 1
	} else if rf.meIdentity == leader {
		if args.LeaderTerm < rf.term {
			// reply置为nil，说明没有理会过期的append消息
			reply.Accept = false
			reply.ExpectLogIndex = -1
			return
		}
		// 更新日志状态
		//rf.lastCommitLogIndex = args.LogIndex
		//rf.term = args.LeaderTerm
		//rf.lastReceivedAppendEntriesDate = time.Now()
		//rf.meIdentity = follower
		log.Fatalf("mutiple leaders exist")
	}
	if args.NeedWaitApply != true && args.LogIndex != -1 {
		log.Printf("rf:%v ready to send applych idx:%v cmd:%v", rf.me, args.LogIndex, args.Command)
		applyMsg := ApplyMsg{
			CommandValid: args.Command != -1,
			Command:      args.Command,
			CommandIndex: args.LogIndex,
		}
		delete(rf.notCommitCmdMap, args.LogIndex)
		rf.lastCommitLogIndex = args.LogIndex
		rf.applyCh <- applyMsg
		rf.logPersister[args.LogIndex] = args.Command
	}
}

func (rf *Raft) checkNeedAutoCommitLastEntry(args *AppendEntriesArgs) {
	if args.LeaderLastCommitIndex >= rf.lastReceiveLogIndex && rf.lastReceiveLogIndex > rf.lastCommitLogIndex {
		log.Printf("rf:%v auto applych idx:%v cmd:%v currLogIdx:%v currLogCmd:%v currLeaderLastCommit:%v", rf.me, rf.lastReceiveLogIndex, rf.notCommitCmdMap[rf.lastReceiveLogIndex], args.LogIndex, args.Command, args.LeaderLastCommitIndex)
		applyMsg := ApplyMsg{
			CommandValid: rf.notCommitCmdMap[rf.lastReceiveLogIndex] != -1,
			Command:      rf.notCommitCmdMap[rf.lastReceiveLogIndex],
			CommandIndex: rf.lastReceiveLogIndex,
		}
		delete(rf.notCommitCmdMap, args.LogIndex)
		rf.lastCommitLogIndex = rf.lastReceiveLogIndex
		rf.applyCh <- applyMsg
		rf.logPersister[applyMsg.CommandIndex] = applyMsg.Command
	}
}

func (rf *Raft) CommitEntries(args *CommitEntriesArgs, reply *CommitEntriesReply) {
	rf.mu.Lock()
	lastCommitLogIndex := rf.lastCommitLogIndex
	// 收到了错序的commit消息，无视
	if args.LogIndex != lastCommitLogIndex+1 {
		rf.mu.Unlock()
		return
	}
	rf.lastCommitLogIndex++
	rf.mu.Unlock()
	log.Printf("ready to send applych %v", rf.me)
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      args.Command,
		CommandIndex: args.LogIndex,
	}
	rf.applyCh <- applyMsg
	delete(rf.notCommitCmdMap, args.LogIndex)
	rf.logPersister[args.LogIndex] = args.Command
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
	reply.Timeout = !ok
	replyCh <- *reply
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	replyCh := args.ReplyCh
	stopSignal := args.ReplyChStopSignal

	log.Printf("rf:%v ready send server:%v idx:%v", rf.me, server, args.LogIndex)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Accept {
		if *stopSignal != true {
			//log.Printf("rf:%v 这里发送replyCh，来自server:%v idx:%v ok:%v accept:%v", rf.me, server, args.LogIndex, ok, reply.Accept)
			replyCh <- 1
			//log.Printf("没有卡主")
		}
	}
	if ok {
		rf.lastReceiveFollowerDate = time.Now()
		log.Printf("rf:%v lastReceiveFollower:%v", rf.me, rf.lastReceiveFollowerDate)
	}

	return ok
}

func (rf *Raft) sendCommitEntries(server int, args *CommitEntriesArgs, reply *CommitEntriesReply) {
	rf.peers[server].Call("Raft.CommitEntries", args, reply)
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

	log.Printf("rf:%v 收到命令 cmd:%v rfIdentity:%v %v", rf.me, command, rf.meIdentity, time.Now())
	rf.mu.Lock()
	rf.nextLogIndex++
	index = rf.nextLogIndex
	logEntry := LogEntry{
		command:  command,
		index:    index,
		commitCh: make(chan int, 1),
	}
	rf.notCommitCmdMap[index] = command
	rf.mu.Unlock()

	rf.logCh <- &logEntry

	select {
	case <-logEntry.commitCh:
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
		if rf.lastCommitLogIndex < index {
			log.Printf("(1)rf:%v 更新lastIdx%v为%v", rf.me, rf.lastCommitLogIndex, index)
			rf.lastCommitLogIndex = index
		}
		rf.applyCh <- applyMsg
		rf.logPersister[index] = command
		log.Printf("rf:%v receive reply client idx:%v cmd:%v", rf.me, index, command)
	case <-time.After(2 * (minElectionTimeout + electionTimeoutInterval*time.Millisecond)):
		// 关闭logEntry.commitCh，防止收集follower回复的goroutine阻塞
		rf.mu.Lock()
		// 放一个1进去，是为了让收集的goroutine能够通过 _ := <- replyClientCh判断是否关闭了，因为这个判断是读channel的操作
		// 所以要放一个1进去，那边才能读出来，不然那边会阻塞住
		logEntry.commitCh <- 1
		close(logEntry.commitCh)
		// 这个index会被废弃，
		rf.notCommitCmdMap[index] = -1
		rf.mu.Unlock()
		applyMsg := ApplyMsg{
			CommandValid: false,
			Command:      command,
			CommandIndex: index,
		}
		rf.applyCh <- applyMsg
		rf.logPersister[index] = -1
		log.Printf("等待apply回复超时，通知客户端本次命令执行失败 idx:%v cmd:%v", index, command)
	}
	rf.persist()
	log.Printf("rf:%v leader persist 成功", rf.me)

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
	rf.appendCh = make(chan int, 1000*len(rf.peers))
	rf.logCh = make(chan *LogEntry, len(rf.peers))
	rf.lastCommitLogIndex = 0
	rf.applyCh = applyCh
	rf.sendQueue = make([]*list.List, len(rf.peers))
	rf.notCommitCmdMap = map[int]interface{}{}
	rf.logPersister = map[int]interface{}{}
	rf.leader = -1

	file := "./" + "log" + ".txt"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastReceiveLogIndex = rf.lastCommitLogIndex
	rf.nextLogIndex = rf.lastCommitLogIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) checkAppendEntries() {
	// 随机一个数字
	randomOffset := rand.Int63n(electionTimeoutInterval)
	randomElectionTimeout := time.Duration(randomOffset)*time.Millisecond + minElectionTimeout
	realElectionTimeout := randomElectionTimeout - time.Since(rf.lastReceivedAppendEntriesDate)
	prevTerm := rf.term
	rf.mu.Unlock()
	// 睡眠
	//log.Printf("timeout:%v, randomOffset:%v", realElectionTimeout, randomOffset)
	time.Sleep(realElectionTimeout)
	// 与上一次记录的接收心跳时间作比较
	rf.mu.Lock()
	//log.Printf("rf:%v follower check append", rf.me)
	if time.Since(rf.lastReceivedAppendEntriesDate) > realElectionTimeout {
		// 超时未收到心跳，开始新的选举
		// 判断是不是已经处于选举阶段了，如果是因为选举新leader引起的超时，则再睡一段时间
		if rf.term != prevTerm {
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
				log.Printf("rf:%v term:%v 选举中超时", rf.me, rf.term)
				rf.meIdentity = candidate
				rf.mu.Unlock()
			}
		} else {
			log.Printf("rf:%v term:%v 普通超时", rf.me, rf.term)
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
	log.Printf("rf:%v 判断%v %v", rf.me, time.Since(rf.lastReceiveFollowerDate), time.Duration(electionTimeoutInterval)*time.Millisecond+minElectionTimeout)
	if time.Since(rf.lastReceiveFollowerDate) > time.Duration(electionTimeoutInterval)*time.Millisecond+minElectionTimeout {
		log.Printf("rf:%v 超越最长的选举timeout，不会再是leader了", rf.me)
		rf.meIdentity = follower
		rf.mu.Unlock()
		return
	}
	prevNow := time.Now()

	// 本轮日志发送的reply收集channel
	replyCh := make(chan int, len(rf.peers))
	// stopSignal解决"多个sender一个receiver"同步关闭replyCh的问题
	stopSignal := false
	//log.Printf("rf:%v term:%v send heartbeat", rf.me, rf.term)

	// append rpc内容
	var logIndex int
	var command interface{}
	var replyClientCh chan int

	// 如果有新的命令，则发送命令包，否则发送空心跳包
	select {
	case logEntry := <-rf.logCh:
		command = logEntry.command
		logIndex = logEntry.index
		replyClientCh = logEntry.commitCh
	default:
		logIndex = -1
		command = nil
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Leader:                rf.me,
			LeaderTerm:            rf.term,
			LogIndex:              logIndex,
			Command:               command,
			ReplyCh:               replyCh,
			ReplyChStopSignal:     &stopSignal,
			LeaderLastCommitIndex: rf.lastCommitLogIndex,
		}
		// 添加到发送队列
		rf.sendQueue[i].PushBack(args)
		log.Printf("rf:%v add to send queue server:%v idx:%v cmd:%v", rf.me, i, logIndex, command)
	}

	rf.mu.Unlock()

	rf.collectAppendReply(replyClientCh, replyCh, &stopSignal, logIndex, command)

	// 计算真实的interval
	realInterval := appendEntriesInterval - time.Since(prevNow)
	if realInterval > 0 {
		time.Sleep(realInterval)
	}
}

func (rf *Raft) collectAppendReply(commitCh chan int, replyCh chan int, replyChStopSignal *bool, logIndex int, command interface{}) {
	currReplies := 0
	// todo collect部分需不需要阻塞，这条日志没完成的话，可以继续发下一条日志吗？
	// todo answer 暂时设计为不可继续发送，原因同下
	minElectionTimeoutCh := time.After(appendEntriesInterval)
CollectLogReply:
	for {
		select {
		case <-replyCh:
			// 收集
			currReplies++
			if currReplies*2+1 >= len(rf.peers) {
				// todo 现在无限重试到一半节点成功接收的情况，是否要增加超时处理
				// todo 增加超时处理，本条命令超时后给客户端返回commandValid=false
				rf.mu.Lock()
				*replyChStopSignal = true
				// 如果本次发送的不是空的心跳包，则需要commit这条LogEntry
				if logIndex != -1 {
					select {
					case <-commitCh:
						// 能读出数据，说明logEntry.commitCh那边超时了，放了一个1进去，并且把channel关闭了，不能commit了
						log.Printf("等待append回复超时，idx:%v 命令无效", logIndex)
					default:
						//log.Printf("rf:%v send commit idx:%v currReplies:%v len(peers):%v", rf.me, logIndex, currReplies, len(rf.peers))
						commitCh <- 1
						//rf.sendCommitRPC(logIndex, command)
					}
				}
				close(replyCh)
				rf.mu.Unlock()
				break CollectLogReply
			}

			//每一小段时间起来看一下start方法那边是否已经通知超时了
		case <-time.After(pollCollectAppendReplyInterval):
			select {
			case <-commitCh:
				// 能读出数据，说明logEntry.commitCh那边超时了，放了一个1进去，并且把channel关闭了，不能commit了
				log.Printf("等待append回复超时，idx:%v 命令无效", logIndex)
				rf.mu.Lock()
				*replyChStopSignal = true
				close(replyCh)
				rf.mu.Unlock()
				break CollectLogReply

			default:
				// 没有超时，重复for-select
				log.Printf("rf:%v leader等待append回复没有超时 idx:%v cmd:%v", rf.me, logIndex, command)
			}

			// 因为这里collectReply是阻塞的，这里阻塞住就会读不到start方法传来的命令
			// 如果logIndex=-1，就没必要继续卡住收集reply了
		case <-minElectionTimeoutCh:
			if logIndex == -1 {
				log.Printf("等待append回复超时，idx:%v 命令无效", logIndex)
				rf.mu.Lock()
				*replyChStopSignal = true
				close(replyCh)
				rf.mu.Unlock()
				break CollectLogReply
			}
		}
	}
}

func (rf *Raft) startNewElection() {
	// 选举定时器
	randomOffset := rand.Intn(newLeaderElectionTimeoutInterval)
	randomNewLeaderElectionTimeout := time.Duration(randomOffset)*time.Millisecond + minNewLeaderElectionTimeout
	ctx, cancel := context.WithTimeout(context.Background(), randomNewLeaderElectionTimeout)

	rf.meIdentity = candidate
	rf.term++
	electionTerm := rf.term
	//log.Printf("rf:%v term:%d", &rf, rf.term)

	// 自己也有一票
	voteUpNumbers := 1
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
			ElectionTerm:       electionTerm,
			LastCommitLogIndex: rf.lastCommitLogIndex,
			Leader:             rf.me,
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
			if !reply.Timeout {
				voteAllReplies++
				if reply.VoteUp {
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
			if i == rf.me {
				continue
			}
			rf.sendQueue[i] = list.New()
			// 启动发送队列
			go rf.processSendQueue(i)
			// 更新下一个要发送的index
			rf.nextLogIndex = rf.lastCommitLogIndex
			// 清空map
			rf.notCommitCmdMap = map[int]interface{}{}
		}

		rf.meIdentity = leader
	} else if voteRes == voteFail {
		// 选举失败
		log.Printf("rf:%v 选举失败", rf.me)
		rf.meIdentity = follower
	} else if voteRes == voteTimeout {
		// 超时，重新选举
		rf.meIdentity = candidate
	}
}

func (rf *Raft) sendCommitRPC(logIndex int, command interface{}) {
	for server := 0; server < len(rf.peers); server++ {
		log.Printf("rf:%v send commit to server:%v, idx:%v", rf.me, server, logIndex)
		if server == rf.me {
			continue
		}
		args := CommitEntriesArgs{
			LogIndex: logIndex,
			Command:  command,
		}
		reply := CommitEntriesReply{}
		go rf.sendCommitEntries(server, &args, &reply)
	}
}

func (rf *Raft) processSendQueue(server int) {
	for true {
		// 每次循环都检查自己的leader身份是否结束
		rf.mu.Lock()
		isLeader := rf.meIdentity == leader
		rf.mu.Unlock()
		if !isLeader {
			// 清空sendQueue
			log.Printf("rf:%v found myself not leader, remove all elements in send queue", rf.me)
			for node := rf.sendQueue[server].Front(); node != nil; node = node.Next() {
				rf.sendQueue[server].Remove(node)
			}
			return
		}
		if rf.sendQueue[server].Len() > 0 {
			front := rf.sendQueue[server].Front()
			args := front.Value.(*AppendEntriesArgs)
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			if args.LogIndex > rf.lastCommitLogIndex {
				args.NeedWaitApply = true
			} else {
				args.NeedWaitApply = false
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, args, &reply)
			log.Printf("leader receive append reply server:%v ok:%v, accept:%v ,expect:%v", server, ok, reply.Accept, reply.ExpectLogIndex)
			if !ok {
				// !ok 直接重发
			} else {
				//ok
				if reply.Accept == false {
					// ok但是不接受，查看是不是因为错序了
					if reply.ExpectLogIndex != -1 {
						// 遍历这个server的queue，找到expect后放到头部第一个重发
						found := false
						for node := rf.sendQueue[server].Front(); node != nil; node = node.Next() {
							if node.Value.(*AppendEntriesArgs).LogIndex == reply.ExpectLogIndex {
								found = true
								rf.sendQueue[server].MoveToFront(node)
							}
						}
						// 发送队列没找到，看看是不是notCommitMap里面
						if found != true {
							rf.mu.Lock()
							if cmd, ok1 := rf.notCommitCmdMap[reply.ExpectLogIndex]; ok1 {
								stopSigal := true
								argsFromMap := AppendEntriesArgs{
									Leader:                rf.me,
									LeaderTerm:            args.LeaderTerm,
									LogIndex:              reply.ExpectLogIndex,
									Command:               cmd,
									ReplyCh:               nil,
									ReplyChStopSignal:     &stopSigal,
									NeedWaitApply:         false,
									LeaderLastCommitIndex: rf.lastCommitLogIndex,
								}
								rf.mu.Unlock()
								rf.sendQueue[server].PushFront(&argsFromMap)
								//replyFromMap := AppendEntriesReply{}
								//rf.sendAppendEntries(server, &argsFromMap, &replyFromMap)
							} else {
								log.Printf("not found idx:%v need to find from persist", reply.ExpectLogIndex)
								expectCmd := rf.logPersister[reply.ExpectLogIndex]
								stopSigal := true
								argsFromPersist := AppendEntriesArgs{
									Leader:                rf.me,
									LeaderTerm:            args.LeaderTerm,
									LogIndex:              reply.ExpectLogIndex,
									Command:               expectCmd,
									ReplyCh:               nil,
									ReplyChStopSignal:     &stopSigal,
									NeedWaitApply:         false,
									LeaderLastCommitIndex: rf.lastCommitLogIndex,
								}
								rf.mu.Unlock()
								rf.sendQueue[server].PushFront(&argsFromPersist)
							}
						}
					}
				} else {
					// 一切正常
					rf.sendQueue[server].Remove(front)
					// 查看是否需要调整队列优先级
					rf.mu.Lock()
					lastCommitLogIndex := rf.lastCommitLogIndex
					rf.mu.Unlock()
					if reply.ExpectLogIndex != -1 && reply.ExpectLogIndex <= lastCommitLogIndex {
						// 遍历这个server的queue，找到expect后放到头部第一个重发
						found := false
						for node := rf.sendQueue[server].Front(); node != nil; node = node.Next() {
							if node.Value.(*AppendEntriesArgs).LogIndex == reply.ExpectLogIndex {
								found = true
								rf.sendQueue[server].MoveToFront(node)
							}
						}
						if found != true {
							rf.mu.Lock()
							if cmd, ok1 := rf.notCommitCmdMap[reply.ExpectLogIndex]; ok1 {
								stopSigal := true
								argsFromMap := AppendEntriesArgs{
									Leader:            rf.me,
									LeaderTerm:        args.LeaderTerm,
									LogIndex:          reply.ExpectLogIndex,
									Command:           cmd,
									ReplyCh:           nil,
									ReplyChStopSignal: &stopSigal,
									NeedWaitApply:     false,
								}
								rf.mu.Unlock()
								rf.sendQueue[server].PushFront(&argsFromMap)
								//replyFromMap := AppendEntriesReply{}
								//rf.sendAppendEntries(server, &argsFromMap, &replyFromMap)
							} else {
								log.Printf("not found idx:%v need to find from persist", reply.ExpectLogIndex)
								expectCmd := rf.logPersister[reply.ExpectLogIndex]
								stopSigal := true
								argsFromPersist := AppendEntriesArgs{
									Leader:                rf.me,
									LeaderTerm:            args.LeaderTerm,
									LogIndex:              reply.ExpectLogIndex,
									Command:               expectCmd,
									ReplyCh:               nil,
									ReplyChStopSignal:     &stopSigal,
									NeedWaitApply:         false,
									LeaderLastCommitIndex: rf.lastCommitLogIndex,
								}
								rf.mu.Unlock()
								rf.sendQueue[server].PushFront(&argsFromPersist)
							}
						}
					}
				}
			}
		} else {
			// 没有发送任务
			time.Sleep(appendEntriesInterval / 5)
		}
	}
}
