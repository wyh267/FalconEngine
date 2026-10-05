package node

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FalconEngine/falcon/cluster"
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

// countIndexQueues 统计本节点指定索引的拉取器与推送队列数量（删索引清理断言用）
func countIndexQueues(n *Node, indexName string) (pullers, pushQs int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for key := range n.pullers {
		if key.index == indexName {
			pullers++
		}
	}
	for key := range n.pushQ {
		if key.index == indexName {
			pushQs++
		}
	}
	return pullers, pushQs
}

// TestClusterDeleteIndex 删索引集群路由：经 leader 删除后，
// 三节点的本地索引、数据目录、CSM 元数据全部清除，重建同名索引可用
func TestClusterDeleteIndex(t *testing.T) {
	base := t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n2.stop(t)
	defer n3.stop(t)

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().Nodes) == 3 && len(n2.state().Nodes) == 3 && len(n3.state().Nodes) == 3
	})

	sch, err := schema.New([]schema.Field{{Name: "content", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("logs", index.IndexSettings{NumShards: 3, NumReplicas: 1}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("logs")
			if !ok || len(ix.LocalShards()) != 2 {
				return false
			}
		}
		return true
	})

	// 写入数据并等复制追平，保证删除发生在"有数据、有副本"的状态下
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("d-%d", i)
		if err := n2.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"文档 %d"}`, i))); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}
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

	// 经 leader 删除索引
	found, err := n1.DeleteIndex("logs")
	if err != nil || !found {
		t.Fatalf("DeleteIndex = %v, %v", found, err)
	}
	// 再删应返回 found=false
	if found, err = n1.DeleteIndex("logs"); err != nil || found {
		t.Fatalf("重复 DeleteIndex = %v, %v, want found=false", found, err)
	}

	// 三节点：本地索引、数据目录、复制状态全部清除
	waitFor(t, "三节点本地索引与数据目录清除", 15*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			if _, ok := nd.Mgr.Get("logs"); ok {
				return false
			}
			if _, err := os.Stat(filepath.Join(nd.dir, "data", "logs")); !os.IsNotExist(err) {
				return false
			}
			if p, q := countIndexQueues(nd.Node, "logs"); p != 0 || q != 0 {
				return false
			}
		}
		return true
	})
	// CSM 元数据（索引表/路由表/分片状态）已清
	if _, ok := n1.state().Index("logs"); ok {
		t.Fatal("CSM 索引表仍含 logs")
	}
	if _, ok := n1.state().Route("logs"); ok {
		t.Fatal("CSM 路由表仍含 logs")
	}
	if stats := n1.state().ShardStatusOf("logs"); len(stats) != 0 {
		t.Fatalf("CSM 分片状态仍含 logs: %v", stats)
	}

	// 重建同名索引：可用且数据为空（1 分片 2 副本，三节点各持一份，
	// 使 leader 本机也能等到分片落地——CreateIndex 的既有等待语义）
	if err := n1.CreateIndex("logs", index.IndexSettings{NumShards: 1, NumReplicas: 2}, sch); err != nil {
		t.Fatalf("重建同名索引失败: %v", err)
	}
	// 重建后首个写入：若路由的 primary 落在 follower 上，其 apply/分片落地
	// 可能略滞后于本节点 CreateIndex 返回，转发会打空——轮询重试至成功
	// （与下方"重建后查询命中"的既有等待语义一致；同 id 重写幂等）
	waitFor(t, "重建后写入成功", 10*time.Second, func() bool {
		return n1.WriteDoc("logs", "new-1", []byte(`{"content":"重建后的文档"}`)) == nil
	})
	// 默认 wait_all=1（异步复制），查询本地优先可能先命中未追平的副本，轮询等待
	waitFor(t, "重建后查询命中", 10*time.Second, func() bool {
		raw, err := n1.Search("logs", []byte(`{"query":{"match":{"content":"重建"}}}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 1
	})
}

// TestClusterDeleteDocReplication 删文档复制链路回归：
// 协调节点删除 → primary 应用并复制 → 全部副本本地均不可见
func TestClusterDeleteDocReplication(t *testing.T) {
	base := t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n2.stop(t)
	defer n3.stop(t)

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().Nodes) == 3 && len(n2.state().Nodes) == 3 && len(n3.state().Nodes) == 3
	})

	// 1 分片 2 副本：三节点各持一份
	sch, err := schema.New([]schema.Field{{Name: "content", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("docs", index.IndexSettings{NumShards: 1, NumReplicas: 2}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok || len(ix.LocalShards()) != 1 {
				return false
			}
		}
		return true
	})

	// n2 作协调节点写入 3 篇
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("d-%d", i)
		if err := n2.WriteDoc("docs", id, []byte(fmt.Sprintf(`{"content":"复制 文档 %d"}`, i))); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}
	waitFor(t, "写入复制到全部副本", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok || ix.DocCount() != 3 {
				return false
			}
		}
		return true
	})

	// 经协调节点删除一篇
	found, err := n2.DeleteDoc("docs", "d-1")
	if err != nil || !found {
		t.Fatalf("DeleteDoc = %v, %v", found, err)
	}
	// 删除不存在文档返回 found=false
	if found, err = n2.DeleteDoc("docs", "d-1"); err != nil || found {
		t.Fatalf("重复 DeleteDoc = %v, %v, want found=false", found, err)
	}

	// 全部副本本地均不可见（删除经复制链路到达）
	waitFor(t, "删除复制到全部副本", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok || ix.DocCount() != 2 {
				return false
			}
			if _, found, err := ix.Get("d-1"); err != nil || found {
				return false
			}
		}
		return true
	})
}

// TestClusterFullRestart 全集群重启（P0-1 raft 元数据持久化）：
// 3 节点建索引写数据 → 全部停止 → 同目录同地址全部重启 ——
// 元数据无需重注册即在、路由表不变、已 ack 文档可查、新写入可复制追平。
func TestClusterFullRestart(t *testing.T) {
	base := t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().AllNodes()) == 3 && len(n2.state().AllNodes()) == 3 && len(n3.state().AllNodes()) == 3
	})

	sch, err := schema.New([]schema.Field{{Name: "content", Type: "text"}, {Name: "level", Type: "number"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("logs", index.IndexSettings{NumShards: 3, NumReplicas: 1, WaitAll: "all"}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("logs")
			if !ok || len(ix.LocalShards()) != 2 {
				return false
			}
		}
		return true
	})

	// waitAll=all：已 ack 数据全部副本落盘
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("w-%d", i)
		if err := n2.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"重启 文档 %d","level":%d}`, i, i))); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}
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
	routesBefore, _ := json.Marshal(mustRoute(t, n1, "logs"))

	// 全部停止（模拟全集群有计划停机）
	n1.stop(t)
	n2.stop(t)
	n3.stop(t)

	// 全部重启（同数据目录同地址 = 同节点身份 + 同 RaftID）
	n1 = startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 = startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 = startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n2.stop(t)
	defer n3.stop(t)

	// RaftID 持久化复用（每数据目录唯一）
	for _, nd := range []*clusterNode{n1, n2, n3} {
		nd := nd
		if nd.Meta.RaftID == 0 {
			t.Fatalf("node %s RaftID 为空", nd.Meta.Name)
		}
	}

	// 元数据无需重注册即在（CSM 从磁盘恢复）
	waitFor(t, "重启后元数据恢复", 15*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			if _, ok := nd.state().Index("logs"); !ok {
				return false
			}
			if _, ok := nd.state().Route("logs"); !ok {
				return false
			}
			if len(nd.state().AllNodes()) != 3 {
				return false
			}
		}
		return true
	})
	// 路由表与重启前一致
	waitFor(t, "重启后路由表一致", 10*time.Second, func() bool {
		routesAfter, _ := json.Marshal(mustRoute(t, n1, "logs"))
		return string(routesAfter) == string(routesBefore)
	})

	// 已 ack 文档可查（数据从各节点磁盘回放）
	waitFor(t, "重启后文档可查", 15*time.Second, func() bool {
		raw, err := n3.Search("logs", []byte(`{"query":{"match_all":{}},"size":50}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 12
	})

	// 新写入继续走复制链路并追平
	for i := 12; i < 18; i++ {
		id := fmt.Sprintf("w-%d", i)
		waitFor(t, "重启后可写 "+id, 15*time.Second, func() bool {
			return n1.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"重启后 文档 %d","level":%d}`, i, i))) == nil
		})
	}
	waitFor(t, "重启后副本追平", 15*time.Second, func() bool {
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
	waitFor(t, "重启后全量可查", 10*time.Second, func() bool {
		raw, err := n2.Search("logs", []byte(`{"query":{"match_all":{}},"size":50}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 18
	})
}

// mustRoute 取路由表（测试中必须存在）
func mustRoute(t *testing.T, n *clusterNode, indexName string) []cluster.ShardRoute {
	t.Helper()
	r, ok := n.state().Route(indexName)
	if !ok {
		t.Fatalf("索引 %q 无路由", indexName)
	}
	return r
}

// TestClusterWipedNodeRejoin 磁盘清空重加入（remove_node 陈旧成员清理）：
// n2 被杀且数据目录清空 → 以同地址（同 NodeID）新数据目录（新 RaftID）重加入 ——
// leader 检测出陈旧成员，摘除其节点记录与路由引用（确定性补建到其他节点）、
// 移除旧 raft 成员并接纳新成员；集群继续可写可查。
func TestClusterWipedNodeRejoin(t *testing.T) {
	base := t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n3.stop(t)

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().AllNodes()) == 3 && len(n3.state().AllNodes()) == 3
	})
	if !n1.RaftNode().IsLeader() {
		t.Skipf("n1 不是 leader，跳过（本用例依赖 n1 处理 JoinNode）")
	}

	sch, err := schema.New([]schema.Field{{Name: "content", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("logs", index.IndexSettings{NumShards: 3, NumReplicas: 1, WaitAll: "all"}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("logs")
			if !ok || len(ix.LocalShards()) != 2 {
				return false
			}
		}
		return true
	})
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("d-%d", i)
		if err := n1.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"清理前 文档 %d"}`, i))); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}

	// 杀掉 n2 并清空其数据目录（模拟磁盘丢失）
	oldRaftID := n2.Meta.RaftID
	n2ID := n2.Meta.ID
	n2.grpc.GracefulStop()
	n2.Node.Stop()
	n2.Mgr.Close()
	if err := os.RemoveAll(filepath.Join(base, "n2")); err != nil {
		t.Fatal(err)
	}

	// n2 以同地址、全新空数据目录重加入（新 RaftID）
	n2 = startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	defer n2.stop(t)
	if n2.Meta.RaftID == oldRaftID {
		t.Fatal("磁盘清空后 RaftID 应重新生成")
	}

	// 陈旧成员清理落地：三节点成员表中 n2 的记录更新为新 RaftID（旧 raft 成员已摘除）
	waitFor(t, "成员记录更新为新 RaftID", 20*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			meta, ok := nd.state().Node(n2ID)
			if !ok || meta.RaftID != n2.Meta.RaftID {
				return false
			}
			if len(nd.state().AllNodes()) != 3 {
				return false
			}
		}
		return true
	})

	// 路由表：不再引用 n2（其分片已确定性补建到 n1/n3），且无空 primary
	waitFor(t, "路由表清理并补建", 15*time.Second, func() bool {
		routes, ok := n1.state().Route("logs")
		if !ok {
			return false
		}
		for _, r := range routes {
			if r.Primary == 0 || r.Primary == n2ID || containsNode(r.Replicas, n2ID) {
				return false
			}
			if !n1.state().NodeAlive(r.Primary) {
				return false
			}
			if len(r.Replicas) != 1 {
				return false
			}
		}
		return true
	})

	// 新写入成功（waitAll=all，副本在 n1/n3 上 ack）
	for i := 6; i < 12; i++ {
		id := fmt.Sprintf("d-%d", i)
		waitFor(t, "清理后可写 "+id, 15*time.Second, func() bool {
			return n1.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"清理后 文档 %d"}`, i))) == nil
		})
	}
	// 经重加入的 n2 查询（其本地无分片，全部走远端）数据完整
	waitFor(t, "重加入节点查询数据完整", 15*time.Second, func() bool {
		raw, err := n2.Search("logs", []byte(`{"query":{"match_all":{}},"size":50}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 12
	})
	// 新成员已追平控制面：其 CSM 路由表与 leader 一致
	waitFor(t, "新成员 CSM 追平", 10*time.Second, func() bool {
		r1, _ := json.Marshal(mustRoute(t, n1, "logs"))
		r2, _ := json.Marshal(mustRoute(t, n2, "logs"))
		return string(r1) == string(r2)
	})
}

// TestClusterMappingBroadcast mapping 变更经 raft 广播到各节点生效：
// 显式 UpdateMapping 与 primary 动态推断的新字段，都应在各节点本地分片与 CSM 中可见
func TestClusterMappingBroadcast(t *testing.T) {
	base := t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	defer n1.stop(t)
	defer n2.stop(t)
	defer n3.stop(t)

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().Nodes) == 3 && len(n2.state().Nodes) == 3 && len(n3.state().Nodes) == 3
	})

	sch, err := schema.New([]schema.Field{
		{Name: "content", Type: "text"},
		{Name: "level", Type: "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex("docs", index.IndexSettings{NumShards: 2, NumReplicas: 1, WaitAll: "all"}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok || len(ix.LocalShards()) == 0 {
				return false
			}
		}
		return true
	})

	// 显式 mapping 更新（经 n2 提交——非 leader 节点经 leaderAddrs 转发）
	if err := n2.UpdateMapping("docs", []schema.Field{{Name: "tag", Type: "keyword"}}); err != nil {
		t.Fatalf("UpdateMapping error: %v", err)
	}
	// 与现有字段冲突的更新应报错
	if err := n1.UpdateMapping("docs", []schema.Field{{Name: "level", Type: "keyword"}}); err == nil {
		t.Fatal("修改已有字段类型应报错")
	}
	// 各节点本地分片可见新字段（raft apply 回调落地）
	waitFor(t, "显式 mapping 落地各节点", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok {
				return false
			}
			if _, ok := ix.UnionSchema().Field("tag"); !ok {
				return false
			}
		}
		return true
	})
	// CSM 中 mapping 为权威
	if meta, _ := n1.state().Index("docs"); !mappingCovers(meta.Mapping, []schema.Field{{Name: "tag", Type: "keyword"}}) {
		t.Fatal("CSM mapping 应包含 tag")
	}

	// 写入含新字段文档（waitAll=all，副本同步 ack）→ 新字段可查
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("t-%d", i)
		if err := n1.WriteDoc("docs", id, []byte(`{"content":"映射 广播","tag":"x","level":1}`)); err != nil {
			t.Fatalf("WriteDoc(%s) error: %v", id, err)
		}
	}
	raw, err := n3.Search("docs", []byte(`{"query":{"term":{"tag":"x"}},"size":10}`))
	if err != nil {
		t.Fatal(err)
	}
	var res index.Result
	if err := jsonUnmarshalX(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Total != 4 {
		t.Fatalf("term tag 命中 = %d, want 4", res.Total)
	}

	// 动态推断广播：写入含未声明字段的文档，新字段（含 keyword 子字段）应
	// 经 primary 异步提案进入 CSM 并落地各节点本地分片
	if err := n1.WriteDoc("docs", "dyn-1", []byte(`{"content":"动态 字段","dynfield":"xyz","level":9}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "动态字段广播到 CSM 与各节点", 10*time.Second, func() bool {
		meta, ok := n1.state().Index("docs")
		if !ok || !mappingCovers(meta.Mapping, []schema.Field{
			{Name: "dynfield", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
		}) {
			return false
		}
		for _, nd := range []*clusterNode{n1, n2, n3} {
			ix, ok := nd.Mgr.Get("docs")
			if !ok {
				return false
			}
			if _, ok := ix.UnionSchema().Field("dynfield.keyword"); !ok {
				return false
			}
		}
		return true
	})
	// 新字段的 keyword 子字段整串精确查询命中
	waitFor(t, "动态字段可查", 10*time.Second, func() bool {
		raw, err := n2.Search("docs", []byte(`{"query":{"term":{"dynfield.keyword":"xyz"}}}`))
		if err != nil {
			return false
		}
		var res index.Result
		if err := jsonUnmarshalX(raw, &res); err != nil {
			return false
		}
		return res.Total == 1
	})
}
