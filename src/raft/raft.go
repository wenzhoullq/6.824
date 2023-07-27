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
	"6.5840/labrpc"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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

//节点状态枚举值

const (
	Leader = iota + 1
	Candidates
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int                 // 当前任期
	voteFor     int                 // 当前任期内投递的候选人的Id
	leader      int                 // 它的领导
	voteNum     int                 // 累计获得票数
	beatCancel  context.CancelFunc  // 用于接受心跳的cancel,
	beatCtx     context.Context     // 用于接受心跳的ctx
	status      int                 // 状态枚举值,leader,candidates,follower,
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
	Term         int //候选者的任期
	CandidateId  int //候选者的Id
	LastLogIndex int //上一个日志的索引下标
	LastLogTerm  int //上一个日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //follower的任期,如果candidate的任期晚于follower,更新自己的状态
	VoteGranted bool //是否获得了这个选票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果候选者的任期小于follower的任期,则拒绝这个请求
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		//返回follower的任期
		reply.Term = rf.currentTerm
		return
	}
	// 比较日志,如果候选者的日志落后于follower,则拒绝本次请求

	// 如果这个任期内已投过票,则拒绝
	if args.Term == rf.currentTerm {
		if rf.voteFor != -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}
	rf.changeStatus(args.Term, args.CandidateId, true, Follower)
	//返回follower的任期
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// 更新follower的日志
	rf.beatCancel()
	//fmt.Println(rf.me, args.CandidateId, rf.currentTerm)
}

type AppendEntryArgs struct {
	LeaderId     int        //领导编号
	Term         int        // 当前任期
	PrevLogIndex int        //上一个需要提交的logIndex
	PrevLogTerm  int        //上一个需要提交的任期
	Entries      []ApplyMsg //log日志,如果长度为0则表明是心跳
	LeaderCommit int        //Leader的下表
}

type AppendEntryReply struct {
	Term    int  // Follower的任期,如果Follower>Leader,则更新Leader的任期
	Success bool //是否接受了日志
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.currentTerm > args.Term {
		//本次请求失败
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//更新状态
	rf.changeStatus(args.Term, args.LeaderId, false, Follower)
	//无论是心跳还是日志复制都要重置过期时间
	rf.beatCancel()
	if len(args.Entries) > 0 {
		//非心跳包,进行更新

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
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	return rf.peers[server].Call("Raft.AppendEntry", args, reply)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) beta() {
	for !rf.killed() {
		//Follower的过期时间为50 + (rand.Int63() % 300),leader定时传心跳包的时间25
		//The tester requires that the leader send heartbeat RPCs no more than ten times per second.但是和默认冲突
		ms := 25
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if _, leader := rf.GetState(); !leader {
			continue
		}
		//对所有的节点发送心跳包
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			req := AppendEntryArgs{
				LeaderId: rf.me,
				Term:     rf.currentTerm,
			}
			reply := AppendEntryReply{}
			go func(i int) {
				//开启协程防止阻塞
				rf.sendAppendEntry(i, &req, &reply)
			}(i)
		}
	}
}

func (rf *Raft) changeStatus(term int, leaderId int, voteFor bool, status int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch status {
	case Leader:
		rf.status = Leader
	case Follower:
		//确定领导
		if leaderId != -1 {
			//投票记为发送给新leader,因为存在将票已经投给其他的候选者的情况
			rf.leader = leaderId
		}
		if voteFor {
			//如果是投票则投票
			rf.voteFor = leaderId
		}
		//改变状态
		rf.status = Follower
		//改变任期
		rf.currentTerm = term
		//累计得票数归零
		rf.voteNum = 0
	case Candidates:
		//改变状态
		rf.status = Candidates
		//当前任期自增
		rf.currentTerm = term
		//给自己投一票
		rf.voteFor = rf.me
		//累计票数增加
		rf.voteNum = 1
	}

}

func (rf *Raft) ticker() {
o1:
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//如果是leader,则不进行检测
		if _, leader := rf.GetState(); leader {
			continue o1
		}
		select {
		case <-rf.beatCtx.Done():
			//收到心跳包,重置ctx
			beatCtx, beatCancel := context.WithCancel(context.Background())
			rf.mu.Lock()
			rf.beatCtx = beatCtx
			rf.beatCancel = beatCancel
			rf.mu.Unlock()
			continue o1
		default:
			//没有收到心跳包成为candidate开始选举为leader
			//选举失败则立即开始选举
			for rf.killed() == false {
				rf.changeStatus(rf.currentTerm+1, rf.me, true, Candidates)
				for i := 0; i < len(rf.peers); i++ {
					// 对其他节点发起投票请求
					if i == rf.me {
						continue
					}
					go func(server int) {
						//开启协程,防止单个节点失联导致程序阻塞
						reqArgs := &RequestVoteArgs{
							Term:        rf.currentTerm,
							CandidateId: rf.me,
						}
						replyArgs := &RequestVoteReply{}
						if ok := rf.sendRequestVote(server, reqArgs, replyArgs); ok {
							//接受投票且任期相同则票数+1
							if replyArgs.VoteGranted && replyArgs.Term == rf.currentTerm && rf.status == Candidates {
								rf.mu.Lock()
								rf.voteNum++
								rf.mu.Unlock()
								if rf.voteNum <= len(rf.peers)/2 {
									return
								}
								//如果获得大部分选票,则成为leader
								rf.changeStatus(-1, -1, false, Leader)
								//对其他节点发起自己的日志复制
								for i := 0; i < len(rf.peers); i++ {
									if i == rf.me {
										continue
									}
									go func(i int) {
										//开启协程防止阻塞
										args := &AppendEntryArgs{
											LeaderId: rf.me,
											Term:     rf.currentTerm,
										}
										reply := &AppendEntryReply{}
										//如果还是Leader则发起心跳
										if _, ok := rf.GetState(); ok {
											rf.sendAppendEntry(i, args, reply)
										}
									}(i)
								}
							} else if replyArgs.Term > rf.currentTerm {
								//如果follower任期大于candidates,则candidates变为follower并且退出选举
								rf.changeStatus(replyArgs.Term, -1, false, Follower)
								return
							}

						}
					}(i)
				}
				time.Sleep(time.Duration(10) * time.Millisecond)
				if _, leader := rf.GetState(); leader {
					continue o1
				}
			}
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
	//raft初始化
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	ctx, cancel := context.WithCancel(context.Background())
	rf.beatCancel = cancel
	rf.beatCtx = ctx
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beta()
	return rf
}
