package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/FalconEngine/falcon/pkg/mlog"
)

// RaftNode 单个 raft 成员节点（仅 master 节点持有）。
//
// 持久化（P0-1）：store 非 nil 时 raft 日志/HardState 写 WAL、定期快照压缩，
// 重启后从磁盘灌回 MemoryStorage 以 RestartNode 恢复，CSM 经快照+日志重放还原；
// store 为 nil 时退化为纯内存（旧行为，兼容既有测试）。
type RaftNode struct {
	id      uint64
	node    raft.Node
	storage *raft.MemoryStorage
	store   *raftStore // nil = 纯内存
	sm      *StateMachine

	// send 由上层注入：把 raft 消息投递给目标节点（生产走 gRPC，测试可内存直连）
	send func(to uint64, data []byte) error
	// onApply 已提交命令的回调（节点装配层用它创建本地分片等）
	onApply func(Command)

	proposeC chan []byte               // 普通命令提案
	confC    chan *raftpb.ConfChangeV2 // 成员变更提案
	stopc    chan struct{}
	done     chan struct{}

	mu         sync.RWMutex
	leader     uint64 // 当前已知 leader
	onSnapshot func() // 应用了 leader 快照后的回调（node 层补齐本地分片用）
	started    bool

	// 持久化恢复/快照状态（仅构造器与 run 循环访问）
	restored      bool              // 本次启动是否从磁盘恢复
	appliedIndex  uint64            // 已应用的日志 index
	snapshotIndex uint64            // 当前快照覆盖到的 index
	confState     *raftpb.ConfState // 最近一次成员配置（CreateSnapshot 用）
	lastHardState *raftpb.HardState // 最近一次硬状态（快照轮替时写入新 WAL）
}

// raftConfig 统一的 raft 参数（测试可调小选举间隔与快照阈值）
var raftConfig = struct {
	electionTick      int
	heartbeatTick     int
	snapshotThreshold int // 应用进度领先快照多少条后触发快照压缩（WAL 轮替）
}{electionTick: 10, heartbeatTick: 1, snapshotThreshold: 512}

// newRaftNode 创建 raft 节点（内部共用）。
// store 非 nil 且磁盘已有状态时：快照灌回 MemoryStorage + 恢复 CSM，
// SetHardState + Append 重放 WAL，然后 raft.RestartNode 恢复运行。
func newRaftNode(id uint64, sm *StateMachine, store *raftStore, send func(to uint64, data []byte) error, onApply func(Command), peers []raft.Peer) (*RaftNode, error) {
	storage := raft.NewMemoryStorage()
	r := &RaftNode{
		id: id, storage: storage, store: store, sm: sm,
		send: send, onApply: onApply,
		proposeC: make(chan []byte, 64),
		confC:    make(chan *raftpb.ConfChangeV2, 16),
		stopc:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	restarting := false
	if store != nil && store.hasState() {
		snap, hs, entries, err := store.load()
		if err != nil {
			return nil, fmt.Errorf("cluster: 加载 raft 持久化状态失败: %w", err)
		}
		if snap != nil {
			if err := storage.ApplySnapshot(snap); err != nil {
				return nil, fmt.Errorf("cluster: 灌回 raft 快照失败: %w", err)
			}
			if err := sm.Restore(snap.Data); err != nil {
				return nil, err
			}
			r.confState = snap.Metadata.GetConfState()
			r.snapshotIndex = snap.Metadata.GetIndex()
			r.appliedIndex = r.snapshotIndex
		}
		if hs != nil {
			if err := storage.SetHardState(hs); err != nil {
				return nil, fmt.Errorf("cluster: 灌回 HardState 失败: %w", err)
			}
			r.lastHardState = hs
		}
		if err := storage.Append(entries); err != nil {
			return nil, fmt.Errorf("cluster: 重放 WAL 失败: %w", err)
		}
		restarting = true
	}
	cfg := &raft.Config{
		ID:              id,
		Storage:         storage,
		ElectionTick:    raftConfig.electionTick,
		HeartbeatTick:   raftConfig.heartbeatTick,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
	}
	switch {
	case restarting:
		r.node = raft.RestartNode(cfg)
		r.restored = true
	case len(peers) > 0:
		r.node = raft.StartNode(cfg, peers) // 自举：初始成员
		r.confState = &raftpb.ConfState{Voters: []uint64{id}}
	default:
		r.node = raft.RestartNode(cfg) // 空成员启动，等待 leader 的 ConfChange 加入
	}
	go r.run()
	return r, nil
}

// RaftMemberID 节点的 raft 成员 ID（RaftID 优先，回退节点身份 ID）
func RaftMemberID(meta NodeMeta) uint64 {
	if meta.RaftID != 0 {
		return meta.RaftID
	}
	return meta.ID
}

// proposeSelfWhenLeader 当选 leader 后把自身元信息作为普通命令提案复制给后续
// 加入的成员（自举成员不经 ConfChange，否则其他成员学不到它的元信息）。
// 持久化重启后 CSM 已有自身记录，重复提案为幂等覆盖。
func (r *RaftNode) proposeSelfWhenLeader(meta NodeMeta) {
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
}

// BootstrapMaster 自举单 master 集群（纯内存，无持久化，兼容旧测试）
func BootstrapMaster(meta NodeMeta, sm *StateMachine, send func(to uint64, data []byte) error, onApply func(Command)) *RaftNode {
	r, _ := newRaftNode(RaftMemberID(meta), sm, nil, send, onApply, []raft.Peer{{ID: RaftMemberID(meta)}}) // store=nil 不会出错
	r.proposeSelfWhenLeader(meta)
	return r
}

// JoinRaft 以空成员身份启动（纯内存），等待集群 leader 通过 ConfChange 把本节点加进来
func JoinRaft(id uint64, sm *StateMachine, send func(to uint64, data []byte) error, onApply func(Command)) *RaftNode {
	r, _ := newRaftNode(id, sm, nil, send, onApply, nil)
	return r
}

// BootstrapMasterPersistent 自举（带持久化）：dir 下已有状态时从磁盘恢复，
// 否则自举为新集群。
func BootstrapMasterPersistent(meta NodeMeta, sm *StateMachine, dir string, send func(to uint64, data []byte) error, onApply func(Command)) (*RaftNode, error) {
	store, err := openRaftStore(dir)
	if err != nil {
		return nil, err
	}
	r, err := newRaftNode(RaftMemberID(meta), sm, store, send, onApply, []raft.Peer{{ID: RaftMemberID(meta)}})
	if err != nil {
		store.close()
		return nil, err
	}
	r.proposeSelfWhenLeader(meta)
	return r, nil
}

// JoinRaftPersistent 空成员启动（带持久化）：dir 下已有状态时从磁盘恢复
// （恢复后仍是集群成员，无需重新 join），否则等待 leader 的 ConfChange。
func JoinRaftPersistent(meta NodeMeta, sm *StateMachine, dir string, send func(to uint64, data []byte) error, onApply func(Command)) (*RaftNode, error) {
	store, err := openRaftStore(dir)
	if err != nil {
		return nil, err
	}
	r, err := newRaftNode(RaftMemberID(meta), sm, store, send, onApply, nil)
	if err != nil {
		store.close()
		return nil, err
	}
	return r, nil
}

// RestoredFromStore 本次启动是否从持久化状态恢复（node 层据此跳过 joinViaSeeds）
func (r *RaftNode) RestoredFromStore() bool { return r.restored }

// SetOnSnapshot 设置"应用了 leader 快照"回调（创建后立即设置，早于 raft 消息到达）
func (r *RaftNode) SetOnSnapshot(fn func()) {
	r.mu.Lock()
	r.onSnapshot = fn
	r.mu.Unlock()
}

// run raft Ready 主循环。
// Ready 处理按 etcd 标准顺序：快照落盘→ApplySnapshot→恢复 CSM →
// WAL 追加→MemoryStorage 追加 → HardState 落盘 → 发送消息 → 应用已提交 →
// Advance → 应用进度领先快照超阈值时自快照压缩。
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
			// 1. 收到 leader 的追赶快照：先落盘（含 WAL 轮替）再应用再恢复 CSM
			if !raft.IsEmptySnap(rd.Snapshot) {
				r.applyLeaderSnapshot(rd)
			}
			// 2. WAL 追加先于 MemoryStorage（崩溃后 WAL 是权威）
			if len(rd.Entries) > 0 {
				if r.store != nil {
					if err := r.store.append(rd.Entries); err != nil {
						mlog.Error("cluster: raft WAL 追加失败: %v", err)
					}
				}
				r.storage.Append(rd.Entries)
			}
			// 3. HardState 落盘
			if !raft.IsEmptyHardState(rd.HardState) {
				if r.store != nil {
					if err := r.store.saveHardState(rd.HardState); err != nil {
						mlog.Error("cluster: raft HardState 落盘失败: %v", err)
					}
				}
				r.lastHardState = rd.HardState
			}
			// 4. 先发送消息（含响应），再应用已提交日志
			for _, m := range rd.Messages {
				data, err := proto.Marshal(m)
				if err != nil {
					continue
				}
				// 发送失败（目标下线等）容忍：raft 会重发
				r.send(m.GetTo(), data)
			}
			// 5. 应用已提交日志
			for _, ent := range rd.CommittedEntries {
				r.applyEntry(ent)
			}
			r.node.Advance()
			// 6. 自快照压缩（仅持久化模式）
			r.maybeSnapshot()
		case <-r.stopc:
			return
		}
	}
}

// applyLeaderSnapshot 处理 leader 发来的快照：落盘（WAL 轮替为空，
// 快照之后无本地未提交条目）→ MemoryStorage.ApplySnapshot → 恢复 CSM → 回调
func (r *RaftNode) applyLeaderSnapshot(rd raft.Ready) {
	idx := rd.Snapshot.Metadata.GetIndex()
	if r.store != nil {
		hs := rd.HardState
		if raft.IsEmptyHardState(hs) {
			hs = r.lastHardState
		}
		if hs == nil {
			hs = &raftpb.HardState{}
		}
		if err := r.store.saveSnapshot(rd.Snapshot, hs, nil); err != nil {
			mlog.Error("cluster: leader 快照落盘失败: %v", err)
		}
	}
	if err := r.storage.ApplySnapshot(rd.Snapshot); err != nil {
		mlog.Warn("cluster: 应用 raft 快照失败 (index=%d): %v", idx, err)
		return
	}
	r.snapshotIndex = idx
	r.appliedIndex = idx
	r.confState = rd.Snapshot.Metadata.GetConfState()
	if err := r.sm.Restore(rd.Snapshot.Data); err != nil {
		mlog.Error("cluster: CSM 快照恢复失败: %v", err)
		return
	}
	r.mu.RLock()
	fn := r.onSnapshot
	r.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

// maybeSnapshot 应用进度领先快照超过阈值时：CSM 快照 + CreateSnapshot +
// 落盘（WAL 轮替，保留快照之后的未提交尾部条目）+ MemoryStorage Compact
func (r *RaftNode) maybeSnapshot() {
	if r.store == nil {
		return
	}
	threshold := uint64(raftConfig.snapshotThreshold)
	if threshold == 0 || r.appliedIndex <= r.snapshotIndex+threshold {
		return
	}
	if r.confState == nil || len(r.confState.Voters) == 0 {
		return // 尚无有效成员配置（未加入集群前不产生快照）
	}
	data, err := r.sm.Snapshot()
	if err != nil {
		return
	}
	snap, err := r.storage.CreateSnapshot(r.appliedIndex, r.confState, data)
	if err != nil {
		return
	}
	// WAL 轮替时保留快照之后的未提交条目（丢失未提交尾部在 raft 语义下安全，
	// 但保留可避免无谓的 leader 重发）
	last, err := r.storage.LastIndex()
	if err != nil {
		return
	}
	var tail []*raftpb.Entry
	if last > r.appliedIndex {
		tail, err = r.storage.Entries(r.appliedIndex+1, last+1, math.MaxUint64)
		if err != nil {
			return
		}
	}
	hs := r.lastHardState
	if hs == nil {
		hs = &raftpb.HardState{}
	}
	if err := r.store.saveSnapshot(snap, hs, tail); err != nil {
		mlog.Error("cluster: raft 快照落盘失败: %v", err)
		return
	}
	r.snapshotIndex = r.appliedIndex
	if err := r.storage.Compact(r.appliedIndex); err != nil {
		mlog.Warn("cluster: raft 日志压缩失败: %v", err)
	}
}

// applyEntry 应用一条已提交日志
func (r *RaftNode) applyEntry(ent *raftpb.Entry) {
	if ent.GetIndex() > r.appliedIndex {
		r.appliedIndex = ent.GetIndex()
	}
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
	case raftpb.EntryConfChange:
		// V1 成员变更条目（StartNode 自举写入的首条日志即 V1 格式）：
		// 必须应用进成员表，否则 follower 重放后 tracker 缺失自举成员
		var cc raftpb.ConfChange
		if err := proto.Unmarshal(ent.Data, &cc); err != nil {
			break
		}
		r.applyConfChangeV2(cc.AsV2())
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := proto.Unmarshal(ent.Data, &cc); err != nil {
			break
		}
		r.applyConfChangeV2(&cc)
	}
}

// applyConfChangeV2 应用一条成员变更：更新 raft 成员配置（跟踪最新 ConfState
// 供快照使用），变更上下文携带的节点元信息写入 CSM 节点表
func (r *RaftNode) applyConfChangeV2(cc *raftpb.ConfChangeV2) {
	if cs := r.node.ApplyConfChange(cc); cs != nil {
		r.confState = cs
	}
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

// Step 投递一条来自远端节点的 raft 消息。
// 收件人校验：地址复用场景下（旧成员磁盘清空、同地址新成员尚未完成替换），
// 网络层可能把发给旧成员 ID 的消息投递到本节点——直接丢弃，避免污染 raft 日志
// （raft 库自身不校验 m.To，投递正确性是传输层职责）。
func (r *RaftNode) Step(data []byte) error {
	var m raftpb.Message
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("cluster: raft 消息解码失败: %w", err)
	}
	if m.GetTo() != r.id {
		return fmt.Errorf("cluster: raft 消息收件人不匹配 (to=%d, self=%d)，已丢弃", m.GetTo(), r.id)
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

// ReplaceMaster 陈旧成员清理：以一条 joint consensus 成员变更原子完成
// 移除旧 raft 成员 + 加入新成员（仅 leader 有效）。
// 用于节点磁盘清空后以同地址（同 NodeID）新 RaftID 重新加入的场景。
func (r *RaftNode) ReplaceMaster(oldRaftID uint64, meta NodeMeta) error {
	if !r.IsLeader() {
		return ErrNotLeader
	}
	ctxData, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	newID := RaftMemberID(meta)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{
			{Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: &oldRaftID},
			{Type: confChangeTypePtr(raftpb.ConfChangeAddNode), NodeId: &newID},
		},
		Context: ctxData,
	}
	select {
	case r.confC <- cc:
		return nil
	default:
		return fmt.Errorf("cluster: 成员变更队列已满")
	}
}

// Stop 停止 raft 循环并关闭持久化存储
func (r *RaftNode) Stop() {
	close(r.stopc)
	<-r.done
	r.node.Stop()
	if r.store != nil {
		if err := r.store.close(); err != nil {
			mlog.Warn("cluster: raft store 关闭失败: %v", err)
		}
	}
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
