package cluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// waitUntil 轮询条件直至超时
func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// TestRaftThreeNodeCluster 3 节点 raft：自举 + ConfChange 加入 + 选主 + 命令复制
func TestRaftThreeNodeCluster(t *testing.T) {
	nodes := map[uint64]*RaftNode{}
	sms := map[uint64]*StateMachine{}
	// 内存直连传输（生产环境走 gRPC，见 node 包）
	send := func(to uint64, data []byte) error {
		if n, ok := nodes[to]; ok {
			return n.Step(data)
		}
		return nil // 目标未就绪，容忍（raft 会重发）
	}

	n1 := BootstrapMaster(NodeMeta{ID: 1, Name: "n1", GRPCAddr: "127.0.0.1:1", Master: true, Data: true}, NewStateMachine(), send, nil)
	nodes[1] = n1
	n2 := JoinRaft(2, NewStateMachine(), send, nil)
	nodes[2] = n2
	n3 := JoinRaft(3, NewStateMachine(), send, nil)
	nodes[3] = n3
	for id, n := range nodes {
		sms[id] = n.sm
		defer n.Stop()
	}

	// 选主
	waitUntil(t, "node1 选主", 10*time.Second, func() bool { return n1.IsLeader() })

	// ConfChange 加入 node2/node3
	// 注意：raft 同一时刻只允许一个待提交的成员变更，串行加入
	if err := n1.AddMaster(NodeMeta{ID: 2, Name: "n2", GRPCAddr: "127.0.0.1:2", Master: true, Data: true}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "node2 加入", 10*time.Second, func() bool {
		_, ok := sms[1].Node(2)
		return ok
	})
	if err := n1.AddMaster(NodeMeta{ID: 3, Name: "n3", GRPCAddr: "127.0.0.1:3", Master: true, Data: true}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "三节点成员表收敛", 10*time.Second, func() bool {
		for _, sm := range sms {
			if len(sm.AllNodes()) != 3 {
				return false
			}
		}
		return true
	})

	// 命令复制：注册一个 data-only 节点 + 建索引（含分配路由）
	if err := n1.Propose(Command{Type: CmdAddNode, Node: &NodeMeta{ID: 4, Name: "d4", Data: true}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "data 节点注册复制", 10*time.Second, func() bool {
		_, ok := sms[3].Node(4)
		return ok
	})

	routes, err := AllocateRoutes(sms[1], 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	meta := IndexMeta{Name: "logs", NumShards: 2, NumReplicas: 1, Mapping: json.RawMessage(`{"fields":[{"name":"content","type":"text"}]}`)}
	if err := n1.Propose(Command{Type: CmdAddIndex, Index: &meta, Routes: routes}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "索引与路由表复制到全部节点", 10*time.Second, func() bool {
		for _, sm := range sms {
			r, ok := sm.Route("logs")
			if !ok || len(r) != 2 {
				return false
			}
		}
		return true
	})
	// 各节点路由表一致
	for id, sm := range sms {
		r, _ := sm.Route("logs")
		for i, route := range r {
			if route.Primary != routes[i].Primary {
				t.Fatalf("node %d 路由表不一致: %+v vs %+v", id, r, routes)
			}
		}
	}

	// 非 leader 提案应被拒绝
	if err := n2.Propose(Command{Type: CmdAddNode, Node: &NodeMeta{ID: 9}}); err != ErrNotLeader {
		t.Fatalf("非 leader 提案应返回 ErrNotLeader, got %v", err)
	}
}

// TestAllocatorBalance 分配器均衡性与 primary/replica 不同节点约束
func TestAllocatorBalance(t *testing.T) {
	sm := NewStateMachine()
	for i := uint64(1); i <= 3; i++ {
		n := NodeMeta{ID: i, Data: true}
		sm.Apply(Command{Type: CmdAddNode, Node: &n})
	}

	routes, err := AllocateRoutes(sm, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	// 3 分片 × (1主+1副) = 6 个分配位，3 节点应各 2 个
	load := map[uint64]int{}
	for _, r := range routes {
		if contains(r.Replicas, r.Primary) {
			t.Fatal("primary 与 replica 同节点")
		}
		load[r.Primary]++
		for _, id := range r.Replicas {
			load[id]++
		}
	}
	for id, n := range load {
		if n != 2 {
			t.Fatalf("节点 %d 持有 %d 个分片, want 2（不均衡）", id, n)
		}
	}

	// 无 data 节点应报错
	empty := NewStateMachine()
	if _, err := AllocateRoutes(empty, 1, 0); err == nil {
		t.Fatal("无 data 节点应报错")
	}

	// 副本数超过节点数时降级（不报错）
	single := NewStateMachine()
	n := NodeMeta{ID: 1, Data: true}
	single.Apply(Command{Type: CmdAddNode, Node: &n})
	r2, err := AllocateRoutes(single, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range r2 {
		if len(r.Replicas) != 0 {
			t.Fatalf("单节点不应分配副本, got %+v", r)
		}
	}

	// 已有负载参与均衡：第二次分配应与第一次错开
	sm2 := NewStateMachine()
	for i := uint64(1); i <= 3; i++ {
		n := NodeMeta{ID: i, Data: true}
		sm2.Apply(Command{Type: CmdAddNode, Node: &n})
	}
	first, _ := AllocateRoutes(sm2, 3, 0)
	sm2.Apply(Command{Type: CmdAddIndex, Index: &IndexMeta{Name: "a", NumShards: 3}, Routes: first})
	second, _ := AllocateRoutes(sm2, 3, 0)
	// 两次分配后每节点恰持 2 个 primary
	load = map[uint64]int{}
	for _, r := range append(first, second...) {
		load[r.Primary]++
	}
	for id, n := range load {
		if n != 2 {
			t.Fatalf("跨索引不均衡: 节点 %d 持 %d 个 primary", id, n)
		}
	}
}

// memTransport 进程内直连传输（按 raft 成员 ID 寻址，支持节点重建后替换）
type memTransport struct {
	mu sync.RWMutex
	m  map[uint64]*RaftNode
}

func newMemTransport() *memTransport { return &memTransport{m: map[uint64]*RaftNode{}} }

func (t *memTransport) set(id uint64, n *RaftNode) {
	t.mu.Lock()
	t.m[id] = n
	t.mu.Unlock()
}

func (t *memTransport) del(id uint64) {
	t.mu.Lock()
	delete(t.m, id)
	t.mu.Unlock()
}

// send 目标未就绪时容忍（raft 会重发）
func (t *memTransport) send(to uint64, data []byte) error {
	t.mu.RLock()
	n := t.m[to]
	t.mu.RUnlock()
	if n == nil {
		return nil
	}
	return n.Step(data)
}

// smEqual 比较两个 CSM 快照是否逐项相等（JSON map key 有序，字节可比）
func smEqual(a, b *StateMachine) bool {
	ba, err := a.Snapshot()
	if err != nil {
		return false
	}
	bb, err := b.Snapshot()
	if err != nil {
		return false
	}
	return string(ba) == string(bb)
}

// TestRaftPersistentFullRestart 持久化全停全启：
// 3 节点（tempdir store）propose 后全停，同目录重建——
// CSM 无需重注册即恢复、三节点逐项相等、选主成功、新提案继续复制。
// 快照阈值调低，顺带覆盖 sm.Snapshot/Restore + WAL 轮替路径。
func TestRaftPersistentFullRestart(t *testing.T) {
	old := raftConfig.snapshotThreshold
	raftConfig.snapshotThreshold = 5
	defer func() { raftConfig.snapshotThreshold = old }()

	dirs := map[uint64]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}
	metas := map[uint64]NodeMeta{
		1: {ID: 1, RaftID: 11, Name: "n1", GRPCAddr: "127.0.0.1:1", Master: true, Data: true},
		2: {ID: 2, RaftID: 22, Name: "n2", GRPCAddr: "127.0.0.1:2", Master: true, Data: true},
		3: {ID: 3, RaftID: 33, Name: "n3", GRPCAddr: "127.0.0.1:3", Master: true, Data: true},
	}
	tp := newMemTransport()

	startAll := func() map[uint64]*RaftNode {
		nodes := map[uint64]*RaftNode{}
		n1, err := BootstrapMasterPersistent(metas[1], NewStateMachine(), dirs[1], tp.send, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes[11] = n1
		tp.set(11, n1)
		for _, id := range []uint64{2, 3} {
			n, err := JoinRaftPersistent(metas[id], NewStateMachine(), dirs[id], tp.send, nil)
			if err != nil {
				t.Fatal(err)
			}
			nodes[RaftMemberID(metas[id])] = n
			tp.set(RaftMemberID(metas[id]), n)
		}
		return nodes
	}
	stopAll := func(nodes map[uint64]*RaftNode) {
		for id, n := range nodes {
			n.Stop()
			tp.del(id)
		}
	}

	nodes := startAll()
	n1 := nodes[11]
	waitUntil(t, "首轮选主", 10*time.Second, func() bool { return n1.IsLeader() })
	if err := n1.AddMaster(metas[2]); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "n2 加入", 10*time.Second, func() bool {
		_, ok := nodes[11].sm.Node(2)
		return ok
	})
	if err := n1.AddMaster(metas[3]); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "三节点收敛", 10*time.Second, func() bool {
		for _, n := range nodes {
			if len(n.sm.AllNodes()) != 3 {
				return false
			}
		}
		return true
	})

	// 提案建索引（推进 applied 越过快照阈值）
	routes, err := AllocateRoutes(nodes[11].sm, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	meta := IndexMeta{Name: "logs", NumShards: 2, NumReplicas: 1, Mapping: json.RawMessage(`{"fields":[{"name":"content","type":"text"}]}`)}
	if err := n1.Propose(Command{Type: CmdAddIndex, Index: &meta, Routes: routes}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "索引复制到全部节点", 10*time.Second, func() bool {
		for _, n := range nodes {
			if _, ok := n.sm.Route("logs"); !ok {
				return false
			}
		}
		return true
	})
	// 快照应已触发（WAL 轮替 + Compact 路径覆盖；以快照文件落盘为准，避免读 run 循环私有字段）
	waitUntil(t, "快照触发", 10*time.Second, func() bool {
		files, _ := filepath.Glob(filepath.Join(dirs[1], "snap-*.json"))
		return len(files) > 0
	})

	stopAll(nodes)

	// 同目录重建：CSM 从磁盘恢复，无需任何重注册
	nodes = startAll()
	defer stopAll(nodes)
	waitUntil(t, "重启后索引元数据在各节点就位", 10*time.Second, func() bool {
		for _, n := range nodes {
			if _, ok := n.sm.Index("logs"); !ok {
				return false
			}
			if len(n.sm.AllNodes()) != 3 {
				return false
			}
		}
		return true
	})
	waitUntil(t, "重启后选主", 10*time.Second, func() bool {
		for _, n := range nodes {
			if n.LeaderID() == 0 {
				return false
			}
		}
		return true
	})
	// 三节点 CSM 逐项相等
	waitUntil(t, "三节点 CSM 逐项相等", 10*time.Second, func() bool {
		return smEqual(nodes[11].sm, nodes[22].sm) && smEqual(nodes[11].sm, nodes[33].sm)
	})

	// 新提案继续复制
	var leader *RaftNode
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
		}
	}
	if leader == nil {
		t.Fatal("重启后无 leader")
	}
	if err := leader.Propose(Command{Type: CmdAddNode, Node: &NodeMeta{ID: 9, Name: "d9", Data: true}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "重启后新提案复制", 10*time.Second, func() bool {
		for _, n := range nodes {
			if _, ok := n.sm.Node(9); !ok {
				return false
			}
		}
		return true
	})
}

// TestRaftRemoveStaleNode remove_node 彻底清理：
// n2 模拟磁盘清空后以同 NodeID 新 RaftID 重建——
// CmdRemoveNode 摘除节点表/路由表引用并确定性补建，
// joint 成员变更原子移除旧 raft 成员并加入新成员，新成员追平全量状态。
func TestRaftRemoveStaleNode(t *testing.T) {
	dirs := map[uint64]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()}
	metas := map[uint64]NodeMeta{
		1: {ID: 1, RaftID: 11, Name: "n1", GRPCAddr: "127.0.0.1:1", Master: true, Data: true},
		2: {ID: 2, RaftID: 22, Name: "n2", GRPCAddr: "127.0.0.1:2", Master: true, Data: true},
		3: {ID: 3, RaftID: 33, Name: "n3", GRPCAddr: "127.0.0.1:3", Master: true, Data: true},
	}
	tp := newMemTransport()

	n1, err := BootstrapMasterPersistent(metas[1], NewStateMachine(), dirs[1], tp.send, nil)
	if err != nil {
		t.Fatal(err)
	}
	tp.set(11, n1)
	defer n1.Stop()
	n2, err := JoinRaftPersistent(metas[2], NewStateMachine(), dirs[2], tp.send, nil)
	if err != nil {
		t.Fatal(err)
	}
	tp.set(22, n2)
	n3, err := JoinRaftPersistent(metas[3], NewStateMachine(), dirs[3], tp.send, nil)
	if err != nil {
		t.Fatal(err)
	}
	tp.set(33, n3)
	defer n3.Stop()

	waitUntil(t, "选主", 10*time.Second, func() bool { return n1.IsLeader() })
	if err := n1.AddMaster(metas[2]); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "n2 加入", 10*time.Second, func() bool { _, ok := n1.sm.Node(2); return ok })
	if err := n1.AddMaster(metas[3]); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "三节点收敛", 10*time.Second, func() bool { return len(n3.sm.AllNodes()) == 3 })

	// 建索引：3 分片 1 副本，路由覆盖 1/2/3（n2 必持有分片）
	routes, err := AllocateRoutes(n1.sm, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	meta := IndexMeta{Name: "logs", NumShards: 3, NumReplicas: 1}
	if err := n1.Propose(Command{Type: CmdAddIndex, Index: &meta, Routes: routes}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "索引复制", 10*time.Second, func() bool {
		_, ok := n3.sm.Route("logs")
		return ok
	})
	holdsShard := func(sm *StateMachine, id uint64) bool {
		routes, _ := sm.Route("logs")
		for _, r := range routes {
			if r.Primary == id || contains(r.Replicas, id) {
				return true
			}
		}
		return false
	}
	if !holdsShard(n1.sm, 2) {
		t.Fatal("前置条件：n2 应持有 logs 分片")
	}

	// n2 停止并清空磁盘，以同 NodeID=2 新 RaftID=220 重建
	n2.Stop()
	tp.del(22)
	if err := os.RemoveAll(dirs[2]); err != nil {
		t.Fatal(err)
	}
	meta2New := NodeMeta{ID: 2, RaftID: 220, Name: "n2", GRPCAddr: "127.0.0.1:2", Master: true, Data: true}
	n2new, err := JoinRaftPersistent(meta2New, NewStateMachine(), dirs[2], tp.send, nil)
	if err != nil {
		t.Fatal(err)
	}
	tp.set(220, n2new)
	defer n2new.Stop()

	// leader 执行清理（node.JoinNode 的检测职责在 cluster 层直接复现）
	if err := n1.Propose(Command{Type: CmdRemoveNode, NodeID: 2}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "旧节点记录摘除", 10*time.Second, func() bool {
		_, ok1 := n1.sm.Node(2)
		_, ok3 := n3.sm.Node(2)
		return !ok1 && !ok3
	})
	// 路由表不再引用节点 2，且无空 primary（已确定性重分配）
	routesAfter, _ := n1.sm.Route("logs")
	for i, r := range routesAfter {
		if r.Primary == 0 || r.Primary == 2 || contains(r.Replicas, 2) {
			t.Fatalf("分片 %d 路由仍引用已摘除节点: %+v", i, r)
		}
	}

	// joint 成员变更：移除旧 raft 成员 22，加入新成员 220
	if err := n1.ReplaceMaster(22, meta2New); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "新节点记录复制到各成员", 10*time.Second, func() bool {
		for _, sm := range []*StateMachine{n1.sm, n3.sm} {
			nd, ok := sm.Node(2)
			if !ok || nd.RaftID != 220 {
				return false
			}
		}
		return true
	})
	// 新成员经全量日志重放追平：CSM 与 leader 逐项相等
	waitUntil(t, "新成员 CSM 追平", 10*time.Second, func() bool { return smEqual(n1.sm, n2new.sm) })

	// 新提案继续复制到新成员
	if err := n1.Propose(Command{Type: CmdAddNode, Node: &NodeMeta{ID: 9, Name: "d9", Data: true}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "清理后新提案复制到新成员", 10*time.Second, func() bool {
		_, ok := n2new.sm.Node(9)
		return ok
	})
}
