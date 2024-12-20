package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	FOLLOWER  = 0 // 跟随者状态
	CANDIDATE = 1 // 候选者状态
	LEADER    = 2 // 领导者状态
)

const (
	electionTimeoutMin = 300 * time.Millisecond // 选举超时最小值
	electionTimeoutMax = 600 * time.Millisecond // 选举超时最大值
	heartbeatTimeout   = 50 * time.Millisecond  // 心跳超时时间
	checkTimeout       = 5 * time.Millisecond   // 检查超时时间
)

// 生成随机选举超时时间
func randTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Intn(int(electionTimeoutMax-electionTimeoutMin)))*time.Millisecond
}

// ApplyMsg 用于向上层应用传递的消息
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// LogItem 表示日志条目
type LogItem struct {
	Term    int         // 任期
	Command interface{} // 命令
}

// Raft 结构体表示一个 Raft 节点
type Raft struct {
	mu        sync.Mutex           // 互斥锁，保护共享状态
	peers     []*labrpc.ClientEnd  // 其他节点的 RPC 客户端
	persister *Persister           // 持久化存储
	me        int                  // 当前节点的 ID

	currentTerm int       // 当前任期
	votedFor    int       // 当前任期内投票给的候选者 ID
	log         []LogItem // 日志条目

	commitIndex int // 已提交的日志索引
	lastApplied int // 已应用到状态机的日志索引

	nextIndex  []int // 每个节点的下一个日志条目索引
	matchIndex []int // 每个节点已复制的日志条目索引

	role  int         // 当前角色 (FOLLOWER, CANDIDATE, LEADER)
	votes int         // 获得的选票数
	timer *time.Timer // 定时器，用于选举或心跳
}

// 锁定方法
func (rf *Raft) lock()   { rf.mu.Lock() }
// 解锁方法
func (rf *Raft) unlock() { rf.mu.Unlock() }

// GetState 获取当前节点的任期和是否为领导者
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()
	return rf.currentTerm, rf.role == LEADER
}

// persist 持久化当前节点的状态
func (rf *Raft) persist() {
	var w bytes.Buffer
	e := gob.NewEncoder(&w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// readPersist 从持久化存储中恢复状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	var r bytes.Buffer
	r.Write(data)
	d := gob.NewDecoder(&r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// resetToFollower 将当前节点重置为跟随者状态
func (rf *Raft) resetToFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.persist()
		rf.timer.Reset(randTimeout())
	}
}

// RequestVoteArgs 请求投票的参数
type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人的 ID
	LastLogIndex int // 候选人最后日志条目的索引
	LastLogTerm  int // 候选人最后日志条目的任期
}

// RequestVoteReply 请求投票的回复
type RequestVoteReply struct {
	Term        int  // 当前任期
	VoteGranted bool // 是否授予投票
}

// RequestVote 处理投票请求
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.resetToFollower(args.Term)
	lastLog := rf.log[len(rf.log)-1]
	logUpToDate := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= len(rf.log)-1)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.timer.Reset(randTimeout())
		reply.VoteGranted = true
	}
}

// AppendEntriesArgs 附加日志请求的参数
type AppendEntriesArgs struct {
	Term         int       // 领导者的任期
	LeaderId     int       // 领导者的 ID
	PrevLogIndex int       // 前一个日志条目的索引
	PrevLogTerm  int       // 前一个日志条目的任期
	Entries      []LogItem // 要附加的日志条目
	LeaderCommit int       // 领导者的已提交日志索引
}

// AppendEntriesReply 附加日志请求的回复
type AppendEntriesReply struct {
	Term    int  // 当前任期
	Success bool // 是否成功
}

// AppendEntries 处理附加日志请求
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	defer rf.unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetToFollower(args.Term)

	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		return
	}

	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Start 开始一个新的命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock()
	defer rf.unlock()

	if rf.role != LEADER {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogItem{Term: term, Command: command})
	rf.persist()

	return index, term, true
}

// Kill 停止当前节点的运行
func (rf *Raft) Kill() {
	rf.lock()
	defer rf.unlock()
	rf.me = -1
	rf.timer.Stop()
}

// Make 创建一个新的 Raft 实例
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		log:       []LogItem{{}},
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		votedFor:  -1,
		role:      FOLLOWER,
		timer:     time.NewTimer(randTimeout()),
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()
	go rf.applyCommands(applyCh)

	return rf
}

// runElectionTimer 运行选举计时器，处理超时逻辑
func (rf *Raft) runElectionTimer() {
	for rf.me != -1 {
		<-rf.timer.C
		rf.lock()
		if rf.role != LEADER {
			rf.startElection()
		}
		rf.unlock()
	}
}

// startElection 发起选举
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	rf.votes = 1
	rf.persist()
	rf.timer.Reset(randTimeout())

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
		}
	}
}

// sendRequestVote 发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	var reply RequestVoteReply
	if rf.peers[server].Call("Raft.RequestVote", args, &reply) {
		rf.lock()
		defer rf.unlock()

		if rf.role != CANDIDATE || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.resetToFollower(reply.Term)
			return
		}

		if reply.VoteGranted {
			rf.votes++
			if rf.votes > len(rf.peers)/2 {
				rf.role = LEADER
				rf.timer.Reset(heartbeatTimeout)
				rf.initializeLeaderState()
			}
		}
	}
}

// initializeLeaderState 初始化领导者状态
func (rf *Raft) initializeLeaderState() {
	lastLogIndex := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex
		rf.matchIndex[i] = 0
	}
	go rf.sendHeartbeats()
}

// sendHeartbeats 发送心跳
func (rf *Raft) sendHeartbeats() {
	for rf.me != -1 && rf.role == LEADER {
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppendEntries(i, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				})
			}
		}
		time.Sleep(heartbeatTimeout)
	}
}

// sendAppendEntries 发送附加日志请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if rf.peers[server].Call("Raft.AppendEntries", args, &reply) {
		rf.lock()
		defer rf.unlock()

		if rf.role != LEADER || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.resetToFollower(reply.Term)
			return
		}

		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		} else {
			rf.nextIndex[server]--
		}
	}
}

// applyCommands 应用已提交的日志到状态机
func (rf *Raft) applyCommands(applyCh chan ApplyMsg) {
	for rf.me != -1 {
		time.Sleep(checkTimeout)
		rf.lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyCh <- ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command}
		}
		rf.unlock()
	}
}
