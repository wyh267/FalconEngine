package node

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/FalconEngine/falcon/config"
	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/transport"
)

// clusterNode 进程内集群测试节点（真实 gRPC，可调故障参数）
type clusterNode struct {
	*Node
	grpc     *transport.Server
	dir      string
	grpcAddr string
	httpAddr string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// startClusterNode 启动一个集群节点（master+data）。
// grpcAddr 由调用方预分配：重启同一节点时复用同地址，节点 ID 由地址派生，
// 模拟真实部署中"同配置重启即同节点"。
func startClusterNode(t *testing.T, name, dir, grpcAddr string, seeds []string) *clusterNode {
	t.Helper()
	httpAddr := freeAddr(t)

	cfg := config.Default()
	cfg.Node.Name = name
	cfg.Data.Path = filepath.Join(dir, "data")
	cfg.Cluster.Seeds = seeds

	mgr, err := index.OpenManager(cfg.Data.Path)
	if err != nil {
		t.Fatal(err)
	}
	nd := New(cfg, mgr, transport.NewClient())
	// 用分配到的真实地址覆盖（NodeID 随之确定）
	nd.Meta.GRPCAddr = grpcAddr
	nd.Meta.HTTPAddr = httpAddr
	nd.Meta.ID = NodeID(grpcAddr)
	// 加速故障检测
	nd.HeartbeatInterval = 200 * time.Millisecond
	nd.DeadTimeout = 800 * time.Millisecond
	nd.PullInterval = 200 * time.Millisecond

	gs := transport.NewServer(nd)
	go gs.Start(grpcAddr)
	deadline := time.Now().Add(5 * time.Second)
	for gs.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := nd.Start(); err != nil {
		t.Fatal(err)
	}
	cn := &clusterNode{Node: nd, grpc: gs, dir: dir, grpcAddr: grpcAddr, httpAddr: httpAddr}
	return cn
}

func splitAddr(addr string) (string, string) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:]
		}
	}
	return addr, ""
}

func (cn *clusterNode) stop(t *testing.T) {
	cn.grpc.GracefulStop()
	cn.Node.Stop()
	cn.Mgr.Close()
}

// waitFor 轮询条件直至超时
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// TestClusterReplicationAndFailover 三节点复制 + 故障转移 + 重启追赶
func TestClusterReplicationAndFailover(t *testing.T) {
	base := t.TempDir()
	// 预分配地址（重启复用）；n1 自举，n2/n3 经 seed 加入
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n3.stop(t)

	// 成员收敛
	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().Nodes) == 3 && len(n2.state().Nodes) == 3 && len(n3.state().Nodes) == 3
	})

	// 建索引：3 分片 1 副本，wait_for_active_shards=all
	sch, err := schema.New([]schema.Field{
		{Name: "content", Type: "text"},
		{Name: "level", Type: "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("logs", index.IndexSettings{NumShards: 3, NumReplicas: 1, WaitAll: "all"}, sch); err != nil {
		t.Fatal(err)
	}
	// 等路由表复制到 n2/n3
	waitFor(t, "路由表复制", 10*time.Second, func() bool {
		_, ok1 := n2.state().Route("logs")
		_, ok2 := n3.state().Route("logs")
		return ok1 && ok2
	})
	// 等分片落地
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("logs")
			if !ok || len(ix.LocalShards()) != 2 {
				return false
			}
		}
		return true
	})

	// 协调写入 12 篇（n2 作为协调节点，验证转发 + waitAll 同步复制）
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("w-%d", i)
		if err := n2.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"分布式 文档 %d","level":%d}`, i, i))); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}

	// 等待副本追平（心跳上报 + 拉取）
	defer func() {
		if t.Failed() {
			raw, _ := n1.state().Snapshot()
			t.Logf("追平超时 n1 sm: %s", raw)
			for _, nd := range []*clusterNode{n1, n2, n3} {
				if ix, ok := nd.Mgr.Get("logs"); ok {
					for _, sh := range ix.LocalShards() {
						lsn, _ := ix.ShardLSN(sh)
						t.Logf("%s shard%d lsn=%d", nd.Meta.Name, sh, lsn)
					}
				}
			}
		}
	}()
	waitFor(t, "副本 LSN 追平", 10*time.Second, func() bool {
		routes, _ := n1.state().Route("logs")
		for shardID, r := range routes {
			stats := n1.state().ShardStatusOf("logs")[shardID]
			pLSN := stats[r.Primary].LSN
			for _, rep := range r.Replicas {
				if stats[rep].LSN != pLSN {
					return false
				}
			}
		}
		return true
	})

	// 跨节点查询：n3 上 scatter-gather 全量
	raw, err := n3.Search("logs", []byte(`{"query":{"match_all":{}},"size":50}`))
	if err != nil {
		t.Fatal(err)
	}
	var res index.Result
	if err := jsonUnmarshalX(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Total != 12 {
		t.Fatalf("跨节点查询 total = %d, want 12", res.Total)
	}

	// 找出一个 primary 持有者杀掉：选持有某 primary 的 n2 或 n3（非 leader 也可）
	// 这里直接杀 n2（3 分片 3 节点均衡分配，n2 必持有至少一个 primary 或 replica）
	routes, _ := n1.state().Route("logs")
	killedPrimaryShards := 0
	for _, r := range routes {
		if r.Primary == n2.Meta.ID {
			killedPrimaryShards++
		}
	}
	t.Logf("n2 持有 %d 个 primary 分片", killedPrimaryShards)

	// 模拟崩溃：停止 gRPC 与 raft
	n2.grpc.GracefulStop()
	n2.Node.Stop()
	n2.Mgr.Close()

	// 等待 leader 判下线并完成故障转移（受影响分片 primary 全部切走）
	waitFor(t, "n2 判下线", 15*time.Second, func() bool {
		return n1.state().Dead[n2.Meta.ID]
	})
	waitFor(t, "primary 全部转移", 15*time.Second, func() bool {
		routes, _ := n1.state().Route("logs")
		for _, r := range routes {
			if r.Primary == n2.Meta.ID {
				return false
			}
			if !n1.state().NodeAlive(r.Primary) {
				return false
			}
		}
		return true
	})

	// 故障转移后写入继续成功
	for i := 12; i < 18; i++ {
		id := fmt.Sprintf("w-%d", i)
		waitFor(t, "转移后可写 "+id, 15*time.Second, func() bool {
			return n1.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"转移后 文档 %d","level":%d}`, i, i))) == nil
		})
	}

	// 已 ack 的文档全部可查（waitAll=all 保证数据不丢）
	waitFor(t, "ack 文档可查", 10*time.Second, func() bool {
		raw, err := n1.Search("logs", []byte(`{"query":{"match_all":{}},"size":100}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 18
	})

	// 重启 n2（同数据目录同地址 = 同节点 ID）：重新加入、降级为副本、拉取追平
	n2 = startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	defer n2.stop(t)

	waitFor(t, "n2 重新上线", 15*time.Second, func() bool {
		return !n1.state().Dead[n2.Meta.ID]
	})
	defer func() {
		if t.Failed() {
			raw, _ := n1.state().Snapshot()
			t.Logf("n1 sm: %s", raw)
			if ix, ok := n2.Mgr.Get("logs"); ok {
				for _, sh := range ix.LocalShards() {
					lsn, _ := ix.ShardLSN(sh)
					t.Logf("n2 shard %d lsn=%d", sh, lsn)
				}
			} else {
				t.Log("n2 不持有 logs")
			}
		}
	}()
	waitFor(t, "n2 副本追平", 20*time.Second, func() bool {
		routes, ok := n2.state().Route("logs")
		if !ok {
			return false
		}
		stats := n1.state().ShardStatusOf("logs")
		ix, ok := n2.Mgr.Get("logs")
		if !ok {
			return false
		}
		for _, shardID := range ix.LocalShards() {
			pLSN := stats[shardID][routes[shardID].Primary].LSN
			local, err := ix.ShardLSN(shardID)
			if err != nil || local != pLSN {
				return false
			}
		}
		return true
	})
}

// jsonUnmarshalX 测试辅助
func jsonUnmarshalX(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
