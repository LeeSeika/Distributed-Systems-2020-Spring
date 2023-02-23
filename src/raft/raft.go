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
	meIdentity                    machineIdentity
	term                          int
	appendCh                      chan int
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
	ElectionTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteUp  bool
	Timeout bool
}

type AppendEntriesArgs struct {
	LeaderTerm int
}

type AppendEntriesReply struct {
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
		log.Printf("rf:%v term:%v vote down", rf.me, rf.term)
		reply.VoteUp = false
		return
	}

	// todo 论文5.4.1 检查日志完整性，candidate完整性低的话投反对票

	// 日志完整性校验通过，投赞成票
	log.Printf("rf:%v term:%v vote up", rf.me, rf.term)
	reply.VoteUp = true
	rf.term++
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("rf:%v term:%v leaderTearm:%v status:%v receive append", rf.me, rf.term, args.LeaderTerm, rf.meIdentity)
	// 如果当前处于candidate状态
	if rf.meIdentity == candidate {
		// 检查任期
		if args.LeaderTerm < rf.term {
			// reply置为nil，说明没有理会过期的append消息
			reply = nil
			return
		}
		// 通知正在发送voteRequest的goroutine停止
		rf.appendCh <- 1
		rf.term = args.LeaderTerm
		rf.lastReceivedAppendEntriesDate = time.Now()
	} else if rf.meIdentity == follower {
		// 检查任期（针对可能两个leader同时存在的情况，旧leader发的append不理会）
		if args.LeaderTerm < rf.term {
			// reply置为nil，说明没有理会过期的append消息
			reply = nil
			return
		}
		rf.term = args.LeaderTerm
		rf.lastReceivedAppendEntriesDate = time.Now()
	} else if rf.meIdentity == leader {
		if args.LeaderTerm < rf.term {
			// reply置为nil，说明没有理会过期的append消息
			reply = nil
			return
		}
		rf.term = args.LeaderTerm
		rf.lastReceivedAppendEntriesDate = time.Now()
		rf.meIdentity = follower
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, replyCh chan int, stopSignal *int, wg *sync.WaitGroup) bool {
	ok := false
	hasSent := false
	for ok != true {
		// 有些节点暂时crash了，但是不影响raft集群工作，raft如果发现大部分节点应该接收了上一条日志，就会进行下一条日志的同步
		// 这就对应replyCh被关闭的情况，这时候就不对crash节点进行重复的sendAppend操作了，节省资源
		rf.mu.Lock()
		if *stopSignal == 1 {
			rf.mu.Unlock()
			return ok
		} else {
			rf.mu.Unlock()
			if !hasSent {
				log.Printf("rf:%v term:%v send heartbeat to %v", rf.me, rf.term, server)
				wg.Done()
				hasSent = true
			}
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if *stopSignal != 1 {
		replyCh <- 1
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
	rf.appendCh = make(chan int, len(rf.peers))

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
	prevTerm := rf.term
	rf.mu.Unlock()
	// 睡眠
	//log.Printf("timeout:%v, randomOffset:%v", realElectionTimeout, randomOffset)
	time.Sleep(realElectionTimeout)
	// 与上一次记录的接收心跳时间作比较
	rf.mu.Lock()
	if time.Since(rf.lastReceivedAppendEntriesDate) > realElectionTimeout {
		// 超时未收到心跳，开始新的选举
		// 判断是不是已经处于选举阶段了，如果是因为选举新leader引起的超时，则再睡一段时间
		if rf.term != prevTerm {
			log.Printf("1")
			ctx, cancel := context.WithTimeout(context.Background(), minNewLeaderElectionTimeout+newLeaderElectionTimeoutInterval*time.Millisecond)
			defer cancel()
			// 记住！不能带锁睡觉，不然receive append那里无法拿到锁去唤醒这里的select
			rf.mu.Unlock()
			select {
			case <-rf.appendCh:
				// 新的leader已经发消息来了，解除睡眠
				// 因为appendCh带缓冲区，所以要清空可能囤积的消息
				for _ = range rf.appendCh {
				}
				return
			case <-ctx.Done():
				// 超时还没有新leader的消息，则再开一轮选举
				rf.mu.Lock()
				log.Printf("rf:%v term:%v 选举中", rf.me, rf.term)
				rf.meIdentity = candidate
				rf.mu.Unlock()
			}
		} else {
			log.Printf("rf:%v term:%v", rf.me, rf.term)
			rf.meIdentity = candidate
			rf.mu.Unlock()
		}
	} else {
		// 如果没有超时，这里直接解锁
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat() {
	// todo 加上日志逻辑后，如果每次interval后有日志就发送带日志信息的appendRPC，无日志就发送维持leader地位的心跳appendRPC
	replyCh := make(chan int, len(rf.peers))
	// stopSignal解决"多个sender一个receiver"同步关闭replyCh的问题
	stopSignal := 0
	log.Printf("rf:%v term:%v send heartbeat", rf.me, rf.term)
	// wait group 防止一种特殊的情况，leader至少尝试一次对每一个follower发出RPC，才会走下面的逻辑，否则可能一轮RPC都还没发完
	// 就已经接收到超过一半的append成功回复了，就会造成集群中一些节点一次都没有接收到append RPC
	wg := sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		args := AppendEntriesArgs{
			LeaderTerm: rf.term,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply, replyCh, &stopSignal, &wg)
	}

	rf.mu.Unlock()

	wg.Wait()
	prevNow := time.Now()

	currReplies := 0
	// todo collect部分需不需要阻塞，这条日志没完成的话，可以继续发下一条日志吗？
	// todo answer 暂时设计为不可继续发送，原因同下
CollectLogReply:
	for {
		select {
		case <-replyCh:
			// 收集
			currReplies++
			if currReplies*2+1 >= len(rf.peers) {
				// todo 现在无限重试到一半节点成功接收的情况，是否要增加超时处理
				// todo answer 暂时不增加超时处理，因为如果没有一半以上的节点正常工作的话，Raft集群是处于不可用状态，这里就代表这种状态
				rf.mu.Lock()
				stopSignal = 1
				close(replyCh)
				rf.mu.Unlock()
				break CollectLogReply
			}
		}
	}

	// 计算真实的interval
	realInterval := appendEntriesInterval - time.Since(prevNow)
	if realInterval > 0 {
		time.Sleep(realInterval)
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
			ElectionTerm: electionTerm,
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
		case <-rf.appendCh:
			voteRes = voteFail
			break CollectVotes
		}
	}
	// 释放ctx资源
	cancel()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteRes == voteSuccess {
		// 选举成功
		rf.meIdentity = leader
	} else if voteRes == voteFail {
		// 选举失败
		rf.meIdentity = follower
	} else if voteRes == voteTimeout {
		// 超时，重新选举
		rf.meIdentity = candidate
	}
}
