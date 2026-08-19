package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RaftNode 单个 raft 成员节点（仅 master 节点持有）。
//
// 简化决策：MemoryStorage 内存存储，无 WAL/快照持久化；
// 节点重启后 raft 状态清零，以新 ID 重新加入集群重建（5b/后续阶段再补持久化）。
type RaftNode struct {
	id      uint64
	node    raft.Node
	storage *raft.MemoryStorage
	sm      *StateMachine

	// send 由上层注入：把 raft 消息投递给目标节点（生产走 gRPC，测试可内存直连）
	send func(to uint64, data []byte) error
	// onApply 已提交命令的回调（节点装配层用它创建本地分片等）
	onApply func(Command)

	proposeC chan []byte               // 普通命令提案
	confC    chan *raftpb.ConfChangeV2 // 成员变更提案
	stopc    chan struct{}
	done     chan struct{}

	mu      sync.RWMutex
	leader  uint64 // 当前已知 leader
	started bool
}

// raftConfig 统一的 raft 参数（测试可调小选举间隔）
var raftConfig = struct {
	electionTick  int
	heartbeatTick int
}{electionTick: 10, heartbeatTick: 1}

// newRaftNode 创建 raft 节点（内部共用）
func newRaftNode(id uint64, sm *StateMachine, send func(to uint64, data []byte) error, onApply func(Command), peers []raft.Peer) *RaftNode {
	storage := raft.NewMemoryStorage()
	cfg := &raft.Config{
		ID:              id,
		Storage:         storage,
		ElectionTick:    raftConfig.electionTick,
		HeartbeatTick:   raftConfig.heartbeatTick,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
	}
	var n raft.Node
	if len(peers) > 0 {
		n = raft.StartNode(cfg, peers) // 自举：初始成员
	} else {
		n = raft.RestartNode(cfg) // 空成员启动，等待 leader 的 ConfChange 加入
	}
	r := &RaftNode{
		id: id, node: n, storage: storage, sm: sm,
		send: send, onApply: onApply,
		proposeC: make(chan []byte, 64),
		confC:    make(chan *raftpb.ConfChangeV2, 16),
		stopc:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	go r.run()
	return r
}

// RaftMemberID 节点的 raft 成员 ID（RaftID 优先，回退节点身份 ID）
func RaftMemberID(meta NodeMeta) uint64 {
	if meta.RaftID != 0 {
		return meta.RaftID
	}
	return meta.ID
}

// BootstrapMaster 自举单 master 集群（集群第一个 master 节点）。
// 当选 leader 后自动把自身元信息作为普通命令提案复制给后续加入的成员
// （自举成员不经 ConfChange，否则其他成员学不到它的元信息）。
func BootstrapMaster(meta NodeMeta, sm *StateMachine, send func(to uint64, data []byte) error, onApply func(Command)) *RaftNode {
	id := RaftMemberID(meta)
	r := newRaftNode(id, sm, send, onApply, []raft.Peer{{ID: id}})
	go func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if r.IsLeader() {
				r.Propose(Command{Type: CmdAddNode, Node: &meta})
				return
			}
			select {
			case <-r.stopc:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	return r
}

// JoinRaft 以空成员身份启动，等待集群 leader 通过 ConfChange 把本节点加进来
func JoinRaft(id uint64, sm *StateMachine, send func(to uint64, data []byte) error, onApply func(Command)) *RaftNode {
	return newRaftNode(id, sm, send, onApply, nil)
}

// run raft Ready 主循环
func (r *RaftNode) run() {
	defer close(r.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.node.Tick()
		case data := <-r.proposeC:
			// 提案失败（非 leader）直接丢弃，调用方通过轮询状态感知
			r.node.Propose(context.Background(), data)
		case cc := <-r.confC:
			// raft 同一时刻只允许一个未应用的成员变更；
			// 异步重试直至被接受（不能阻塞 Ready 循环，否则待应用的变更永远无法推进）
			go func(cc *raftpb.ConfChangeV2) {
				for {
					if err := r.node.ProposeConfChange(context.Background(), cc); err == nil {
						return
					}
					select {
					case <-r.stopc:
						return
					case <-time.After(200 * time.Millisecond):
					}
				}
			}(cc)
		case rd := <-r.node.Ready():
			if rd.SoftState != nil {
				r.mu.Lock()
				r.leader = rd.SoftState.Lead
				r.mu.Unlock()
			}
			r.storage.Append(rd.Entries)
			// 先发送消息（含响应），再应用已提交日志
			for _, m := range rd.Messages {
				data, err := proto.Marshal(m)
				if err != nil {
					continue
				}
				// 发送失败（目标下线等）容忍：raft 会重发
				r.send(m.GetTo(), data)
			}
			for _, ent := range rd.CommittedEntries {
				r.applyEntry(ent)
			}
			r.node.Advance()
		case <-r.stopc:
			return
		}
	}
}

// applyEntry 应用一条已提交日志
func (r *RaftNode) applyEntry(ent *raftpb.Entry) {
	switch ent.GetType() {
	case raftpb.EntryNormal:
		if len(ent.Data) == 0 {
			break
		}
		var cmd Command
		if err := json.Unmarshal(ent.Data, &cmd); err != nil {
			break
		}
		r.sm.Apply(cmd)
		if r.onApply != nil {
			r.onApply(cmd)
		}
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := proto.Unmarshal(ent.Data, &cc); err != nil {
			break
		}
		r.node.ApplyConfChange(&cc)
		// 成员变更上下文携带节点元信息，写入节点表
		if len(cc.Context) > 0 {
			var meta NodeMeta
			if err := json.Unmarshal(cc.Context, &meta); err == nil {
				r.sm.Apply(Command{Type: CmdAddNode, Node: &meta})
				if r.onApply != nil {
					r.onApply(Command{Type: CmdAddNode, Node: &meta})
				}
			}
		}
	}
}

// Step 投递一条来自远端节点的 raft 消息
func (r *RaftNode) Step(data []byte) error {
	var m raftpb.Message
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("cluster: raft 消息解码失败: %w", err)
	}
	return r.node.Step(context.Background(), &m)
}

// IsLeader 本节点是否为当前 leader
func (r *RaftNode) IsLeader() bool {
	return r.node.Status().RaftState == raft.StateLeader
}

// LeaderID 当前已知 leader（0 表示未知）
func (r *RaftNode) LeaderID() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leader
}

// Propose 提交一条控制面命令（仅 leader 有效）
func (r *RaftNode) Propose(cmd Command) error {
	if !r.IsLeader() {
		return ErrNotLeader
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	select {
	case r.proposeC <- data:
		return nil
	default:
		return fmt.Errorf("cluster: 提案队列已满")
	}
}

// AddMaster 把新 master 节点加入 raft group（仅 leader 有效）。
// meta 经 ConfChange 上下文随日志复制到各成员。
func (r *RaftNode) AddMaster(meta NodeMeta) error {
	if !r.IsLeader() {
		return ErrNotLeader
	}
	ctxData, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	memberID := RaftMemberID(meta)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{
			Type:   confChangeTypePtr(raftpb.ConfChangeAddNode),
			NodeId: &memberID,
		}},
		Context: ctxData,
	}
	select {
	case r.confC <- cc:
		return nil
	default:
		return fmt.Errorf("cluster: 成员变更队列已满")
	}
}

// Stop 停止 raft 循环
func (r *RaftNode) Stop() {
	close(r.stopc)
	<-r.done
	r.node.Stop()
}

// WaitLeader 等待集群选出 leader（测试与启动等待用），超时返回 false
func (r *RaftNode) WaitLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.LeaderID() != 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// confChangeTypePtr 取枚举指针（raftpb 为指针字段）
func confChangeTypePtr(t raftpb.ConfChangeType) *raftpb.ConfChangeType { return &t }
