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
	"fmt"
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

const (
	Replicate = iota + 1
	Beat
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
	log         []Entry             //本地的日志
	commitIndex int                 //待提交的日志
	lastApplied int                 //日志的高位下标,用于状态机,前提是commit下标大于lastApplied
	applyChan   chan ApplyMsg       //用于提交commit的
	//一般来说 nextIndex = matchIndex + 1, nextIndex是一个乐观的位置,默认每一个follower都是从nextIndex的位置开始复制;而match是一个保守的位置,代表着所有已经被复制好的位置
	nextIndex  []int //下一个需要送到指定的日志的下标,默认的是leader的log的最后一个位置+1,是一个乐观的位置,需要不断的匹配进行减少
	matchIndex []int //follower和leader匹配的最高索引,是一个保守的位置,代表已经合并了的

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Entry struct {
	Command interface{} //具体的指令
	Term    int         //日志所在的任期
	Index   int         //这个任期下的下标
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
	LastLogIndex int //candidate最后一个日志的索引下标
	LastLogTerm  int //candidate最后一个日志的任期
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
	// 检查任期
	fmt.Println(args, rf.currentTerm, rf.commitIndex, "haha", rf.me)
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.VoteGranted = false
		//返回follower的任期
		reply.Term = currentTerm
		return
	}
	// 检查日志的完整性
	// 先比较日志的任期号
	rf.mu.Lock()
	rf.currentTerm = args.Term
	//currentTerm = args.Term
	if rf.commitIndex > 0 && (rf.log[rf.commitIndex-1].Term > args.LastLogTerm || rf.commitIndex > args.LastLogIndex) {
		//上一条日志和发送过来的index所对应的任期不能匹配
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 如果这个任期内已投过票,则拒绝
	if args.Term == currentTerm {
		if rf.voteFor != -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}
	rf.changeStatus(args.Term, args.CandidateId, true, Follower)
	//返回follower的任期
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = true
	fmt.Println(args, rf.currentTerm, rf.commitIndex, "gaga", rf.me)
	rf.beatCancel()
	//fmt.Println(rf.me, args.CandidateId, rf.currentTerm)
}

//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	fmt.Println(args, rf.currentTerm, rf.commitIndex, "haha")
//	rf.beatCancel()
//	//if rf.commitIndex > 0 {
//	//	fmt.Println(rf.log[rf.commitIndex-1].Term, "haha")
//	//}
//	// Your code here (2A, 2B).
//	// 检查任期
//	rf.mu.Lock()
//
//	currentTerm := rf.currentTerm
//	rf.mu.Unlock()
//	if args.Term < currentTerm {
//		reply.VoteGranted = false
//		//返回follower的任期
//		reply.Term = currentTerm
//		return
//	}
//	rf.mu.Lock()
//	reply.Term = rf.currentTerm
//	rf.mu.Unlock()
//	// 任期相同检查日志的完整性
//	rf.mu.Lock()
//	if rf.commitIndex > 0 && (rf.log[rf.commitIndex-1].Term > args.Term || (rf.log[rf.commitIndex-1].Term == args.Term && rf.commitIndex > args.LastLogIndex)) {
//		//上一条日志和发送过来的index所对应的任期不能匹配
//		reply.VoteGranted = false
//		reply.Term = currentTerm
//		rf.mu.Unlock()
//		return
//	}
//	rf.mu.Unlock()
//	// 如果这个任期内已投过票,则拒绝
//	if args.Term == currentTerm {
//		if rf.voteFor != -1 {
//			reply.Term = currentTerm
//			reply.VoteGranted = false
//			return
//		}
//	}
//	rf.changeStatus(args.Term, args.CandidateId, true, Follower)
//	//返回follower的任期
//	reply.VoteGranted = true
//	fmt.Println(args, rf.currentTerm, rf.me, "gaga")
//}

type AppendEntryArgs struct {
	LeaderId     int     //领导编号
	Term         int     // 当前任期
	PrevLogIndex int     //上一个需要提交的logIndex
	PrevLogTerm  int     //上一个需要提交的任期
	Entries      []Entry //log日志,如果长度为0则表明是心跳
	LeaderCommit int     //Leader的下标,leader节点的commitIndex
}

type AppendEntryReply struct {
	Term       int  // Follower的任期,如果Follower>Leader,则更新Leader的任期
	Success    bool //是否接受了日志
	MatchIndex int  //匹配的日志
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//matchIndex的返回值是log的长度
	//fmt.Println(args)
	//无论是心跳还是日志复制都要重置过期时间
	rf.beatCancel()
	rf.mu.Lock()
	//先检查任期
	if rf.currentTerm > args.Term {
		//本次请求失败
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.MatchIndex = 0
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//更新状态
	rf.changeStatus(args.Term, args.LeaderId, false, Follower)
	//更新commit
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		//非心跳包,进行比较和更新
		logLen := len(rf.log)
		if logLen > 0 {
			if args.PrevLogIndex > 0 && args.PrevLogTerm > rf.log[args.PrevLogIndex-1].Term {
				//上一条日志和发送过来的index所对应的任期不能匹配
				reply.Success = false
				reply.Term = rf.currentTerm
				rf.mu.Unlock()
				return
			}
		}
		//更新日志
		for _, v := range args.Entries {
			rf.log = append(rf.log, v)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.MatchIndex = min(args.LeaderCommit, len(rf.log))

		//fmt.Println(rf.me, rf.log, rf.commitIndex, rf.currentTerm)
	}
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Println(args)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		//fmt.Println(rf.me, rf.status, rf.log, rf.currentTerm)
	}
	for i := args.PrevLogIndex; i < rf.commitIndex; i++ {
		//fmt.Println(len(rf.log), i, args.LeaderCommit)
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.log[i].Index,
			Command:      rf.log[i].Command,
		}
	}

	//fmt.Println(rf.me, rf.log, rf.commitIndex, rf.currentTerm)
}
func min(i, j int) int {
	if i < j {
		return i
	}
	return j
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
// leader用于接收日志并且持久化,但是不提交
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	//如果不是leader,则f
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//本地添加日志
	logLen := len(rf.log)
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   logLen + 1,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	return logLen + 1, term, isLeader
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

func (rf *Raft) changeStatus(term int, leaderId int, voteFor bool, status int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch status {
	case Leader:
		rf.status = Leader
		//维护nextIndex为自己的最后一条日志的Index+1
		//fmt.Println("gaga")
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
		}
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

func (rf *Raft) newAppendEntryArgs(i int, status int) *AppendEntryArgs {
	//defer func() {
	//	if r := recover(); r != nil {
	//		fmt.Println(len(rf.log), rf.nextIndex[i])
	//	}
	//}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntryArgs{
		LeaderId:     rf.me,
		Term:         rf.currentTerm,
		PrevLogTerm:  0,
		PrevLogIndex: 0,
	}
	//如果log存在,则选取日志中的term和index,反之则为0
	nextIndex := rf.nextIndex[i]
	matchIndex := rf.matchIndex[i]
	switch status {
	case Beat:
		args.LeaderCommit = matchIndex
		if matchIndex > 0 {
			args.PrevLogTerm = rf.log[matchIndex-1].Term
			args.PrevLogIndex = rf.log[matchIndex-1].Index
		}
	case Replicate:
		args.LeaderCommit = len(rf.log)
		//要复制的日志的前一个,因此是-2
		if nextIndex-2 >= 0 {
			//fmt.Println(nextIndex-1, rf.log, rf.me)
			args.PrevLogTerm = rf.log[nextIndex-2].Term
			args.PrevLogIndex = rf.log[nextIndex-2].Index
		}
		//从要复制的地方开始
		for j := rf.nextIndex[i] - 1; j < len(rf.log); j++ {
			//fmt.Println(rf.me, j, rf.matchIndex[i], rf.nextIndex[i], len(rf.log), rf.nextIndex, rf.matchIndex)
			args.Entries = append(args.Entries, rf.log[j])
		}
	}
	return args
}

func (rf *Raft) logReplication(i int) {
	for {
		//开启协程防止阻塞

		//创建请求参数
		//fmt.Println(rf.me, rf.nextIndex, rf.log, rf.matchIndex)
		args := rf.newAppendEntryArgs(i, Replicate)
		reply := &AppendEntryReply{}
		//如果是Leader则发起复制
		//fmt.Println(ok, "baba")
		if ok := rf.sendAppendEntry(i, args, reply); !ok {
			//如果发送失败,如follower宕机,则不发日志复制了，等心跳有反应后再发日志
			return
		}
		if !reply.Success && reply.Term == rf.currentTerm {
			//如果因为日志不一致导致的匹配失败(任期相同),修改nextIndex,继续
			rf.mu.Lock()
			if rf.nextIndex[i] > 1 {
				rf.nextIndex[i]--
			}
			rf.mu.Unlock()
			continue
		} else {
			//接收本次的日志,leader本地提交,matchIndex改变,nextIndex改变
			rf.mu.Lock()
			rf.nextIndex[i] = reply.MatchIndex + 1
			//fmt.Println(reply.MatchIndex)
			rf.matchIndex[i] = reply.MatchIndex
			//fmt.Println(reply.MatchIndex, rf.matchIndex[i], "ds", rf.nextIndex[i])
			//统计这个d
			n := reply.MatchIndex
			if rf.commitIndex < n {
				cnt := 1
				for j := 0; j < len(rf.peers); j++ {
					if rf.me == j {
						continue
					}
					if rf.matchIndex[j] >= n && rf.log[n-1].Term == rf.currentTerm {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					//fmt.Println(rf.me, rf.status, rf.log, rf.currentTerm)
					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.log[rf.commitIndex].Index,
						Command:      rf.log[rf.commitIndex].Command,
					}
					rf.commitIndex = n
				}
			}
			//fmt.Println(rf.me, rf.log, rf.commitIndex, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//如果是leader,则不进行检测
		if _, leader := rf.GetState(); leader {
			continue
		}
		select {
		case <-rf.beatCtx.Done():
			//收到心跳包,重置ctx
			beatCtx, beatCancel := context.WithCancel(context.Background())
			rf.mu.Lock()
			rf.beatCtx = beatCtx
			rf.beatCancel = beatCancel
			rf.mu.Unlock()
		default:
			//没有收到心跳包成为candidate开始选举为leader
			//选举失败则立即开始选举
			//fmt.Println(rf.me, rf.currentTerm)
			for rf.killed() == false {

				rf.changeStatus(rf.currentTerm+1, rf.me, true, Candidates)
				for i := 0; i < len(rf.peers); i++ {
					// 对其他节点发起投票请求
					if i == rf.me {
						continue
					}
					go func(server int) {
						//开启协程,防止单个节点失联导致程序阻塞
						rf.mu.Lock()
						lastLogTerm := 0
						logLen := len(rf.log)
						if logLen > 0 {
							lastLogTerm = rf.log[logLen-1].Term
						}
						reqArgs := &RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: len(rf.log),
							LastLogTerm:  lastLogTerm,
						}
						rf.mu.Unlock()
						replyArgs := &RequestVoteReply{}
						fmt.Println(reqArgs, rf.me)
						if ok := rf.sendRequestVote(server, reqArgs, replyArgs); ok {
							//接受投票且任期相同则票数+1
							if replyArgs.VoteGranted {
								rf.mu.Lock()
								rf.voteNum++
								rf.mu.Unlock()
								if rf.voteNum <= len(rf.peers)/2 {
									return
								}
								//如果获得大部分选票,则成为leader
								if _, leader := rf.GetState(); !leader {
									rf.changeStatus(-1, -1, false, Leader)
								}
								////对其他节点发起自己的日志复制,可能会发起多次复制
								//rf.logReplication()
							} else if replyArgs.Term > rf.currentTerm {
								//如果follower任期大于candidates,则candidates变为follower并且退出选举
								if _, leader := rf.GetState(); leader {
									rf.changeStatus(replyArgs.Term, -1, false, Follower)
								}
								return
							}
						}
					}(i)
				}
				time.Sleep(50 * time.Millisecond)
				if _, leader := rf.GetState(); leader {
					break
				}
			}
		}
	}
}

func (rf *Raft) beat() {
	for !rf.killed() {
		//Follower的过期时间为50 + (rand.Int63() % 300),leader定时传心跳包的时间25
		//The tester requires that the leader send heartbeat RPCs no more than ten times per second.但是和默认冲突
		ms := 40
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if _, leader := rf.GetState(); !leader {
			continue
		}
		//对所有的节点发送心跳包
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			//fmt.Println(rf.me, rf.nextIndex, rf.log, rf.matchIndex)
			reply := &AppendEntryReply{}
			go func(i int) {
				//开启协程防止阻塞
				//fmt.Println(len(rf.log), rf.matchIndex[i])
				rf.mu.Lock()
				logLen := len(rf.log)
				rf.mu.Unlock()
				if logLen < rf.nextIndex[i] {
					//如果长度相同,则为心跳
					req := rf.newAppendEntryArgs(i, Beat)
					rf.sendAppendEntry(i, req, reply)
				} else {
					//长度不相同,则是日志复制
					//fmt.Println(reply.MatchIndex, rf.commitIndex, i, rf.me, rf.currentTerm)
					rf.logReplication(i)
				}
			}(i)
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
	persister *Persister, applyChan chan ApplyMsg) *Raft {
	rf := &Raft{}
	//raft初始化
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 1
	rf.voteFor = -1
	ctx, cancel := context.WithCancel(context.Background())
	rf.beatCancel = cancel
	rf.beatCtx = ctx
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyChan = applyChan
	//初始化nextIndex
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.log = make([]Entry, 0)
	rf.lastApplied = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beat()
	return rf
}
