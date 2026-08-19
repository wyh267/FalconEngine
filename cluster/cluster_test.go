package cluster

import (
	"encoding/json"
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
		_, ok := sms[1].Nodes[2]
		return ok
	})
	if err := n1.AddMaster(NodeMeta{ID: 3, Name: "n3", GRPCAddr: "127.0.0.1:3", Master: true, Data: true}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "三节点成员表收敛", 10*time.Second, func() bool {
		for _, sm := range sms {
			if len(sm.Nodes) != 3 {
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
		_, ok := sms[3].Nodes[4]
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
