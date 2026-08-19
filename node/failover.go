package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/FalconEngine/falcon/cluster"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/transport"
)

// 故障检测与转移：
//   - 所有 data 节点每 HeartbeatInterval 向 master leader 上报分片状态（ShardStatusReport）
//   - leader 本地记录各节点 lastSeen；超过 DeadTimeout 未上报 → 提案 CmdNodeDead
//     → 对每个受影响分片从 in-sync 副本（LSN 追平 primary 最后已知值）选新 primary
//     → 提案 CmdUpdateRoute；并在存活节点上补建副本保持副本数
//   - 节点重新上线：上报时 leader 发现其在 Dead 表中 → 提案 CmdNodeAlive；
//     旧 primary 通过路由表发现自己已降级为 replica，reconcile 自动启动拉取追平

// ---------- 上报（所有 data 节点） ----------

// reportLoop 周期向 master leader 上报心跳与分片状态
func (n *Node) reportLoop() {
	ticker := time.NewTicker(n.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.reportOnce()
		case <-n.stopc:
			return
		}
	}
}

// collectShardStatus 汇总本地分片状态
func (n *Node) collectShardStatus() []transport.ShardStatusItem {
	var out []transport.ShardStatusItem
	for _, indexName := range n.Mgr.Names() {
		ix, ok := n.Mgr.Get(indexName)
		if !ok {
			continue
		}
		routes, _ := n.state().Route(indexName)
		for _, shardID := range ix.LocalShards() {
			lsn, err := ix.ShardLSN(shardID)
			if err != nil {
				continue
			}
			primary := shardID < len(routes) && routes[shardID].Primary == n.Meta.ID
			out = append(out, transport.ShardStatusItem{
				Index:   indexName,
				Shard:   int32(shardID),
				Primary: primary,
				Lsn:     lsn,
			})
		}
	}
	return out
}

// leaderAddrs 返回候选的 master leader 地址。
// 注意 LeaderID 是 raft 成员 ID（重启即变），需经节点表 RaftID 映射解析；
// 本节点即 leader 时直接返回自身（bootstrap 节点无 seeds 可用）。
func (n *Node) leaderAddrs() []string {
	if rn := n.RaftNode(); rn != nil {
		if rn.IsLeader() {
			return []string{n.Meta.GRPCAddr}
		}
		if id := rn.LeaderID(); id != 0 {
			for _, meta := range n.state().AllNodes() {
				if cluster.RaftMemberID(meta) == id {
					return []string{meta.GRPCAddr}
				}
			}
		}
	}
	return n.seeds
}

// reportOnce 执行一次上报
func (n *Node) reportOnce() {
	items := n.collectShardStatus()
	for _, addr := range n.leaderAddrs() {
		if err := n.Client.ShardStatusReport(addr, n.Meta, items); err == nil {
			return
		} else {
			mlog.Debug("node %s 上报失败 %s: %v", n.Meta.Name, addr, err)
		}
	}
}

// ---------- 故障检测与转移（master leader） ----------

// failoverLoop leader 周期扫描节点存活，触发故障转移与副本补建
func (n *Node) failoverLoop() {
	ticker := time.NewTicker(n.DeadTimeout / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.failoverOnce()
		case <-n.stopc:
			return
		}
	}
}

// failoverOnce 一轮故障检测：判下线 → 转移 primary → 补建副本
func (n *Node) failoverOnce() {
	rn := n.RaftNode()
	if rn == nil || !rn.IsLeader() {
		return
	}
	sm := n.state()
	deadline := time.Now().Add(-n.DeadTimeout)

	n.mu.Lock()
	lastSeen := make(map[uint64]time.Time, len(n.lastSeen))
	for id, t := range n.lastSeen {
		lastSeen[id] = t
	}
	n.mu.Unlock()

	for _, meta := range sm.DataNodes() {
		if meta.ID == n.Meta.ID {
			continue
		}
		if sm.Dead[meta.ID] {
			continue
		}
		// 从未上报过的节点用加入时间兜底未知——保守起见只判上报过的节点下线
		last, reported := lastSeen[meta.ID]
		if reported && last.Before(deadline) {
			mlog.Warn("node %s (id=%d) 超过 %v 未心跳，判下线", meta.Name, meta.ID, n.DeadTimeout)
			if err := rn.Propose(cluster.Command{Type: cluster.CmdNodeDead, NodeID: meta.ID}); err != nil {
				continue
			}
			n.failoverNode(meta.ID)
		}
	}
}

// failoverNode 对下线节点的全部受影响分片执行转移与副本补建。
// 策略（与 ES 的"旧主分片回归后降级为副本"一致）：
//   - primary 下线 → 从 in-sync 存活副本（LSN 追平 primary 最后已知值）提升新 primary
//   - 下线节点保留在路由表副本位中：重新上线后自动以副本身份拉取追平
//   - 若仍有空闲存活 data 节点，额外补建副本，保证存活副本数达标
func (n *Node) failoverNode(deadID uint64) {
	rn := n.RaftNode()
	sm := n.state()
	// CmdNodeDead 刚提案、尚未 apply，本地判定存活时先把 deadID 排除
	alive := func(id uint64) bool { return id != deadID && sm.NodeAlive(id) }
	raw, err := sm.Snapshot()
	if err != nil {
		return
	}
	var full struct {
		Indices map[string]cluster.IndexMeta    `json:"indices"`
		Routes  map[string][]cluster.ShardRoute `json:"routes"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		return
	}

	for indexName, routes := range full.Routes {
		stats := sm.ShardStatusOf(indexName)
		meta := full.Indices[indexName]
		for shardID, r := range routes {
			newRoute := cluster.ShardRoute{Primary: r.Primary, Replicas: append([]uint64(nil), r.Replicas...)}
			changed := false

			// primary 转移：优先 in-sync 副本（LSN 追平 primary 最后已知值）
			if r.Primary == deadID {
				deadLSN := int64(-1)
				if st, ok := stats[shardID][deadID]; ok {
					deadLSN = st.LSN
				}
				best, bestLSN := uint64(0), int64(-2)
				for _, id := range newRoute.Replicas {
					if !alive(id) {
						continue
					}
					lsn := int64(-1)
					if st, ok := stats[shardID][id]; ok {
						lsn = st.LSN
					}
					if lsn > bestLSN {
						best, bestLSN = id, lsn
					}
				}
				if best == 0 {
					mlog.Error("分片 %s[%d] 无存活副本可提升，等待节点恢复", indexName, shardID)
					continue
				}
				if bestLSN < deadLSN {
					mlog.Warn("分片 %s[%d] 提升的副本未完全追平 (replica lsn=%d, primary 最后已知 lsn=%d)，可能存在未 ack 数据丢失",
						indexName, shardID, bestLSN, deadLSN)
				}
				newRoute.Primary = best
				// 副本列表：摘除新 primary，保留下线节点（回归后作为副本追平）
				reps := newRoute.Replicas[:0]
				for _, id := range newRoute.Replicas {
					if id != best {
						reps = append(reps, id)
					}
				}
				newRoute.Replicas = append(reps, deadID)
				changed = true
			}

			// 副本补建：存活副本数不足时，选存活且不持有的 data 节点补上
			// （下线节点占位的副本位保留，允许暂时超额，回归后自然追平）
			aliveReplicas := 0
			for _, id := range newRoute.Replicas {
				if alive(id) {
					aliveReplicas++
				}
			}
			for aliveReplicas < meta.NumReplicas {
				pick := uint64(0)
				for _, nd := range sm.DataNodes() {
					if !alive(nd.ID) || nd.ID == newRoute.Primary || containsNode(newRoute.Replicas, nd.ID) {
						continue
					}
					pick = nd.ID
					break
				}
				if pick == 0 {
					break // 没有更多存活节点
				}
				newRoute.Replicas = append(newRoute.Replicas, pick)
				aliveReplicas++
				changed = true
			}

			if changed {
				mlog.Info("分片 %s[%d] 路由变更: primary=%d replicas=%v", indexName, shardID, newRoute.Primary, newRoute.Replicas)
				rn.Propose(cluster.Command{
					Type: cluster.CmdUpdateRoute, IndexName: indexName,
					ShardID: shardID, Route: newRoute,
				})
			}
		}
	}
}

// ShardStatusReport 心跳与状态上报（仅 master leader 处理）
func (n *Node) ShardStatusReport(from transport.NodeMeta, shards []transport.ShardStatusItem) error {
	rn := n.RaftNode()
	if rn == nil {
		return fmt.Errorf("node: 本节点不是 master")
	}
	if !rn.IsLeader() {
		return cluster.ErrNotLeader
	}
	n.mu.Lock()
	n.lastSeen[from.ID] = time.Now()
	n.mu.Unlock()

	// 下线节点重新上报 → 恢复为存活
	if n.state().Dead[from.ID] {
		rn.Propose(cluster.Command{Type: cluster.CmdNodeAlive, NodeID: from.ID})
	}
	items := make([]cluster.ShardStatus, 0, len(shards))
	for _, s := range shards {
		items = append(items, cluster.ShardStatus{
			Index: s.Index, Shard: int(s.Shard), Primary: s.Primary, LSN: s.Lsn, Docs: s.Docs,
		})
	}
	return rn.Propose(cluster.Command{Type: cluster.CmdShardStatus, NodeID: from.ID, Shards: items})
}

// ---------- 集群健康与分片列表 ----------

// ClusterHealth 集群健康：green 全部主副齐备存活；yellow primary 全活但副本缺失；red 有 primary 缺失
func (n *Node) ClusterHealth() ([]byte, error) {
	sm := n.state()
	status := "green"
	raw, err := sm.Snapshot()
	if err != nil {
		return nil, err
	}
	var snap struct {
		Indices map[string]cluster.IndexMeta    `json:"indices"`
		Routes  map[string][]cluster.ShardRoute `json:"routes"`
	}
	json.Unmarshal(raw, &snap)

	alive := 0
	for _, nd := range sm.DataNodes() {
		if sm.NodeAlive(nd.ID) {
			alive++
		}
	}
	for name, meta := range snap.Indices {
		routes := snap.Routes[name]
		for _, r := range routes {
			if !sm.NodeAlive(r.Primary) {
				status = "red"
				continue
			}
			aliveReplicas := 0
			for _, id := range r.Replicas {
				if sm.NodeAlive(id) {
					aliveReplicas++
				}
			}
			if aliveReplicas < meta.NumReplicas && status == "green" {
				status = "yellow"
			}
		}
	}
	return json.Marshal(map[string]any{"status": status, "nodes": alive})
}

// CatShards 分片列表：primary/replica 节点、状态、文档数（来自心跳上报）
func (n *Node) CatShards() ([]byte, error) {
	sm := n.state()
	raw, err := sm.Snapshot()
	if err != nil {
		return nil, err
	}
	var snap struct {
		Routes map[string][]cluster.ShardRoute `json:"routes"`
	}
	json.Unmarshal(raw, &snap)

	type row struct {
		Index  string `json:"index"`
		Shard  int    `json:"shard"`
		PriRep string `json:"prirep"` // p / r
		Node   string `json:"node"`
		NodeID uint64 `json:"node_id"`
		LSN    int64  `json:"lsn"`
		Docs   int64  `json:"docs"`
		State  string `json:"state"` // STARTED / OFFLINE
	}
	var rows []row
	for indexName, routes := range snap.Routes {
		stats := sm.ShardStatusOf(indexName)
		add := func(shardID int, prirep string, nodeID uint64) {
			meta, _ := sm.Node(nodeID)
			r := row{Index: indexName, Shard: shardID, PriRep: prirep, Node: meta.Name, NodeID: nodeID, State: "STARTED"}
			if st, ok := stats[shardID][nodeID]; ok {
				r.LSN, r.Docs = st.LSN, st.Docs
			}
			if !sm.NodeAlive(nodeID) {
				r.State = "OFFLINE"
			}
			rows = append(rows, r)
		}
		for shardID, r := range routes {
			add(shardID, "p", r.Primary)
			for _, id := range r.Replicas {
				add(shardID, "r", id)
			}
		}
	}
	// 排序：索引名 -> 分片 -> primary 优先
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			a, b := rows[i], rows[j]
			if a.Index != b.Index {
				if a.Index > b.Index {
					rows[i], rows[j] = b, a
				}
			} else if a.Shard != b.Shard {
				if a.Shard > b.Shard {
					rows[i], rows[j] = b, a
				}
			} else if a.PriRep > b.PriRep {
				rows[i], rows[j] = b, a
			}
		}
	}
	if rows == nil {
		rows = []row{}
	}
	return json.Marshal(rows)
}
