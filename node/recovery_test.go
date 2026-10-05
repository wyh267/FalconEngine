package node

// P0-2 副本恢复（peer recovery）集群级验证：
//   - 场景 A 落后出窗：副本死亡期间 primary 持续写 + 多次 Flush 轮替，
//     保留窗口越过副本位点；副本重启后靠"段拷贝 + translog 补差"追平，
//     文档逐条一致。（在无恢复机制的代码上此用例必失败：拉取出窗后
//     永远空转，LSN 无法追平。）
//   - 场景 B 角色降级分叉：未复制写入后 kill primary → 故障转移 →
//     新 primary 继续写（使拉取无法靠位点发现分叉）→ 旧 primary 重启，
//     由 primary-role 标记识别降级，强制段拷贝恢复重置，无幻影文档。

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/schema"
)

// setupRecoveryCluster 启动 3 节点集群并建 1 分片 2 副本索引（三节点各持一份）
func setupRecoveryCluster(t *testing.T, indexName string) (base string, nodes map[string]*clusterNode, addrs map[string]string) {
	t.Helper()
	base = t.TempDir()
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)
	addrs = map[string]string{"n1": addr1, "n2": addr2, "n3": addr3}
	n1 := startClusterNode(t, "n1", filepath.Join(base, "n1"), addr1, nil)
	n2 := startClusterNode(t, "n2", filepath.Join(base, "n2"), addr2, []string{addr1})
	n3 := startClusterNode(t, "n3", filepath.Join(base, "n3"), addr3, []string{addr1})
	nodes = map[string]*clusterNode{"n1": n1, "n2": n2, "n3": n3}

	waitFor(t, "三节点成员表收敛", 15*time.Second, func() bool {
		return len(n1.state().Nodes) == 3 && len(n2.state().Nodes) == 3 && len(n3.state().Nodes) == 3
	})
	sch, err := schema.New([]schema.Field{{Name: "content", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.CreateIndex(indexName, index.IndexSettings{NumShards: 1, NumReplicas: 2}, sch); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "分片落地", 10*time.Second, func() bool {
		for _, nd := range nodes {
			ix, ok := nd.Mgr.Get(indexName)
			if !ok || len(ix.LocalShards()) != 1 {
				return false
			}
		}
		return true
	})
	return base, nodes, addrs
}

// primaryOf 返回分片 0 的 primary 节点与其余节点名
func primaryOf(t *testing.T, n *Node, nodes map[string]*clusterNode, indexName string) (primary *clusterNode, others []string) {
	t.Helper()
	routes, ok := n.state().Route(indexName)
	if !ok {
		t.Fatalf("索引 %q 无路由表", indexName)
	}
	for name, nd := range nodes {
		if nd.Meta.ID == routes[0].Primary {
			primary = nd
		} else {
			others = append(others, name)
		}
	}
	if primary == nil {
		t.Fatal("primary 不在节点集中")
	}
	return primary, others
}

// killNode 模拟崩溃（同既有集成测试：停 gRPC + raft + 关引擎）
func killNode(t *testing.T, nd *clusterNode) {
	t.Helper()
	nd.grpc.GracefulStop()
	nd.Node.Stop()
	nd.Mgr.Close()
}

// assertDocsEqual 逐条断言两个节点上指定文档的 _source 一致
func assertDocsEqual(t *testing.T, indexName string, a, b *clusterNode, ids []string) {
	t.Helper()
	aix, ok := a.Mgr.Get(indexName)
	if !ok {
		t.Fatalf("%s 不持有索引 %s", a.Meta.Name, indexName)
	}
	bix, ok := b.Mgr.Get(indexName)
	if !ok {
		t.Fatalf("%s 不持有索引 %s", b.Meta.Name, indexName)
	}
	for _, id := range ids {
		araw, afound, err := aix.Get(id)
		if err != nil {
			t.Fatalf("%s Get(%s) error: %v", a.Meta.Name, id, err)
		}
		braw, bfound, err := bix.Get(id)
		if err != nil {
			t.Fatalf("%s Get(%s) error: %v", b.Meta.Name, id, err)
		}
		if afound != bfound || (afound && !bytes.Equal(araw, braw)) {
			t.Fatalf("文档 %s 不一致: %s(found=%v)=%s vs %s(found=%v)=%s",
				id, a.Meta.Name, afound, araw, b.Meta.Name, bfound, braw)
		}
	}
}

// TestPeerRecoveryOutOfWindow 场景 A：落后出窗 → 段拷贝恢复追平
func TestPeerRecoveryOutOfWindow(t *testing.T) {
	base, nodes, addrs := setupRecoveryCluster(t, "logs")

	primary, others := primaryOf(t, nodes["n1"].Node, nodes, "logs")
	victimName, survivorName := others[0], others[1]
	victim, survivor := nodes[victimName], nodes[survivorName]
	t.Logf("primary=%s victim=%s survivor=%s", primary.Meta.Name, victimName, survivorName)
	// 收尾只停始终存活（或重启后存活）的节点，避免与 killNode 重复 stop
	defer primary.stop(t)
	defer survivor.stop(t)

	// 初始写 5 篇并等三副本追平
	var ids []string
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("init-%d", i)
		ids = append(ids, id)
		if err := primary.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"初始 文档 %d"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	key := shardKey{"logs", 0}
	waitFor(t, "三副本追平", 10*time.Second, func() bool {
		for _, nd := range []*clusterNode{primary, victim, survivor} {
			ix, _ := nd.Mgr.Get("logs")
			if lsn, err := ix.ShardLSN(0); err != nil || lsn != 4 {
				return false
			}
		}
		return true
	})
	frozenLSN := int64(4)

	// kill 副本
	killNode(t, victim)
	waitFor(t, "victim 判下线", 15*time.Second, func() bool {
		return primary.state().Dead[victim.Meta.ID]
	})

	// primary 持续写 + 强制 _flush 轮替；每轮先等存活副本 ack 推进保留窗口
	pix, _ := primary.Mgr.Get("logs")
	for round := 0; round < 6; round++ {
		for w := 0; w < 3; w++ {
			id := fmt.Sprintf("post-%d-%d", round, w)
			ids = append(ids, id)
			if err := primary.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"故障期 文档 %d-%d"}`, round, w))); err != nil {
				t.Fatal(err)
			}
		}
		lastLSN, err := pix.ShardLSN(0)
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, "存活副本 ack 推进", 10*time.Second, func() bool {
			primary.mu.RLock()
			ack := primary.replAcks[key][survivor.Meta.ID]
			primary.mu.RUnlock()
			return ack >= lastLSN
		})
		if err := pix.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// 断言已出窗：victim 需要的起点已被保留窗口清除
	oldest, err := pix.ShardOldestLSN(0)
	if err != nil {
		t.Fatal(err)
	}
	if oldest <= frozenLSN+1 {
		t.Fatalf("保留窗口未越过 victim 位点: oldest=%d frozen=%d", oldest, frozenLSN)
	}
	t.Logf("victim 已出窗: oldest=%d frozen=%d", oldest, frozenLSN)

	// 重启 victim：落后出窗 → 段拷贝恢复 → 追平
	victim = startClusterNode(t, victimName, filepath.Join(base, victimName), addrs[victimName], []string{addrs["n1"]})
	defer victim.stop(t)
	waitFor(t, "victim 重新上线", 15*time.Second, func() bool {
		return !primary.state().Dead[victim.Meta.ID]
	})
	waitFor(t, "victim 追平 primary", 20*time.Second, func() bool {
		vix, ok := victim.Mgr.Get("logs")
		if !ok {
			return false
		}
		vlsn, err := vix.ShardLSN(0)
		if err != nil {
			return false
		}
		plsn, err := pix.ShardLSN(0)
		return err == nil && vlsn == plsn
	})

	// 文档逐条一致（含故障前与故障期写入的全部 23 篇）
	assertDocsEqual(t, "logs", primary, victim, ids)

	// 段拷贝发生的旁证：victim 分片目录出现与 primary 一致的段目录
	entries, err := os.ReadDir(filepath.Join(base, victimName, "data", "logs", "shard-0"))
	if err != nil {
		t.Fatal(err)
	}
	hasSeg := false
	for _, ent := range entries {
		if ent.IsDir() && len(ent.Name()) >= 4 && ent.Name()[:4] == "seg-" {
			hasSeg = true
			break
		}
	}
	if !hasSeg {
		t.Fatal("victim 分片目录无段目录（段拷贝未发生？）")
	}
}

// TestPeerRecoveryDemotionReset 场景 B：角色降级分叉 → 强制恢复重置，无幻影文档
func TestPeerRecoveryDemotionReset(t *testing.T) {
	base, nodes, addrs := setupRecoveryCluster(t, "logs")

	primary, others := primaryOf(t, nodes["n1"].Node, nodes, "logs")
	primaryName := primary.Meta.Name
	t.Logf("primary=%s", primaryName)
	// 收尾只停始终存活（或重启后存活）的节点，避免与 killNode 重复 stop
	for _, name := range others {
		defer nodes[name].stop(t)
	}

	// 等 reconcile 打上 primary-role 标记
	pix, _ := primary.Mgr.Get("logs")
	waitFor(t, "primary-role 标记", 10*time.Second, func() bool {
		return pix.ShardWasPrimaryRole(0)
	})

	// 幻影写：引擎级直写（等价于"非 waitAll 写后立即 kill primary、推送未到达"）
	if _, err := pix.Index("phantom", []byte(`{"content":"幻影 文档"}`)); err != nil {
		t.Fatal(err)
	}
	if lsn, _ := pix.ShardLSN(0); lsn != 0 {
		t.Fatalf("幻影写 LSN = %d, want 0", lsn)
	}

	// 立即 kill primary
	killNode(t, primary)
	// 注意：此后读集群状态必须用存活节点（被 kill 的节点 sm 已冻结）
	survivor := nodes[others[0]]
	waitFor(t, "primary 转移", 15*time.Second, func() bool {
		routes, _ := survivor.state().Route("logs")
		return routes[0].Primary != primary.Meta.ID && survivor.state().NodeAlive(routes[0].Primary)
	})

	newPrimary, _ := primaryOf(t, survivor.Node, nodes, "logs")
	t.Logf("new primary=%s", newPrimary.Meta.Name)
	// "primary 转移"等的是 survivor 的视图；newPrimary 本节点的路由 apply
	// 可能略滞后，直接写会按旧视图转发给已下线的旧 primary——等其本地就绪
	waitFor(t, "new primary 本地路由就绪", 10*time.Second, func() bool {
		routes, _ := newPrimary.state().Route("logs")
		return len(routes) > 0 && routes[0].Primary == newPrimary.Meta.ID
	})
	nix, _ := newPrimary.Mgr.Get("logs")

	// 故障转移后继续写 3 篇：nextLSN 越过幻影位点，
	// 副本按位点拉取无法发现分叉（只有降级检测能兜底）
	var ids []string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("keep-%d", i)
		ids = append(ids, id)
		if err := newPrimary.WriteDoc("logs", id, []byte(fmt.Sprintf(`{"content":"保留 文档 %d"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if lsn, _ := nix.ShardLSN(0); lsn != 2 {
		t.Fatalf("新 primary LSN = %d, want 2", lsn)
	}

	// 重启旧 primary：降级识别 → 强制段拷贝恢复 → 重置到新 primary 状态
	primary = startClusterNode(t, primaryName, filepath.Join(base, primaryName), addrs[primaryName], []string{addrs["n1"]})
	defer primary.stop(t)
	waitFor(t, "旧 primary 重新上线", 15*time.Second, func() bool {
		return !newPrimary.state().Dead[primary.Meta.ID]
	})
	waitFor(t, "旧 primary 被重置追平", 20*time.Second, func() bool {
		rix, ok := primary.Mgr.Get("logs")
		if !ok {
			return false
		}
		rlsn, err := rix.ShardLSN(0)
		if err != nil {
			return false
		}
		nlsn, err := nix.ShardLSN(0)
		return err == nil && rlsn == nlsn
	})

	// 无幻影文档，保留文档逐条一致
	rix, _ := primary.Mgr.Get("logs")
	if _, found, err := rix.Get("phantom"); err != nil || found {
		t.Fatalf("幻影文档应被恢复重置抹掉: found=%v err=%v", found, err)
	}
	assertDocsEqual(t, "logs", newPrimary, primary, ids)
	if n, m := nix.DocCount(), rix.DocCount(); n != m || n != 3 {
		t.Fatalf("DocCount 不一致: newPrimary=%d restored=%d, want 3", n, m)
	}
}
