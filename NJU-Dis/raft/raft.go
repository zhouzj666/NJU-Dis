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
	FOLLOWER  = 0 
	CANDIDATE = 1 
	LEADER    = 2 
)

const (
	electionTimeoutMin = 300 * time.Millisecond 
	electionTimeoutMax = 600 * time.Millisecond 
	heartbeatTimeout   = 50 * time.Millisecond  
	checkTimeout       = 5 * time.Millisecond   
)

// 生成随机选举超时时间
func randTimeout() time.Duration {
	diff := (electionTimeoutMax - electionTimeoutMin).Milliseconds()
	return electionTimeoutMin + time.Duration(rand.Intn(int(diff)))*time.Millisecond
}

// ApplyMsg 用于向上层应用传递的消息
// 包括日志索引、命令和快照数据

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}


type LogItem struct {
	Term    int
	Command interface{}
}


type Raft struct {
	mu        sync.Mutex           
	peers     []*labrpc.ClientEnd  
	persister *Persister           
	me        int                  

	currentTerm int   
	votedFor    int   
	log         []LogItem

	commitIndex int 
	lastApplied int 

	nextIndex  []int 
	matchIndex []int 

	role  int  // 当前角色 (FOLLOWER, CANDIDATE, LEADER)
	votes int  // 获得的选票数
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
// 并更新任期信息
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
// 包括候选人的任期、ID、最后日志索引和任期
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 请求投票的回复
// 包括当前任期和是否授予投票
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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
// 包括任期、领导者信息、前日志索引和条目
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogItem
	LeaderCommit int
}

// AppendEntriesReply 附加日志请求的回复
// 包括当前任期和是否成功
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries 处理附加日志请求
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	defer rf.unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.resetToFollower(args.Term)

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
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
// 返回命令的索引、任期和是否为领导者
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
		// 选举逻辑简化处理
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
