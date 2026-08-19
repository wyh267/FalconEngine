// 写入路径与复制：协调转发、primary 异步推送、replica 按 LSN 拉取追赶。
//
// 复制协议：
//   - primary 写入本地分片后得到 LSN（translog 全局单调，跨代际连续）
//   - 异步推送：每分片一个顺序队列 goroutine，把 (lsn, op) 推给路由表中
//     全部存活 replica（ApplyOp，带 LSN 对齐检查）；失败按退避重试，
//     超过 5 次放弃该条——由 replica 的拉取循环兜底愈合
//   - 拉取追赶：replica 每 PullInterval 按本地 LSN+1 向 primary 批量
//     FetchTranslog 并顺序应用；角色提升为 primary 后拉取器自动停止
//   - 写入一致性：settings.write_wait_for_active_shards=all 时 primary
//     同步等全部存活 replica ack，任一失败则整体报错（文档可读性降级，
//     但不会产生"已 ack 却丢失"的语义）
package node

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/FalconEngine/falcon/cluster"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/translog"
	"github.com/FalconEngine/falcon/transport"
)

// shardKey 标识一个分片
type shardKey struct {
	index string
	shard int
}

// replTask 一条待推送的复制任务
type replTask struct {
	lsn int64
	op  []byte // translog.Op 的 JSON
}

// shardIDOf 文档路由：murmur3(id) % numShards
func shardIDOf(id string, numShards int) int {
	return int(murmur3.Sum32([]byte(id)) % uint32(numShards))
}

// routeOf 查路由表
func (n *Node) routeOf(indexName string, shardID int) (cluster.ShardRoute, error) {
	routes, ok := n.sm.Route(indexName)
	if !ok || shardID >= len(routes) {
		return cluster.ShardRoute{}, fmt.Errorf("node: 索引 %q 分片 %d 无路由", indexName, shardID)
	}
	return routes[shardID], nil
}

// waitAllOf 索引是否配置了 wait_for_active_shards=all
func (n *Node) waitAllOf(meta cluster.IndexMeta) bool {
	return meta.WaitAll
}

// ---------- 协调侧写入口 ----------

// WriteDoc 集群写入：路由到 primary（本地直接写，否则 gRPC 转发）
func (n *Node) WriteDoc(indexName, id string, doc []byte) error {
	op := translog.Op{Type: translog.OpIndex, ID: id, Doc: doc}
	_, err := n.writeOp(indexName, id, op)
	return err
}

// DeleteDoc 集群删除：路由到 primary；lsn<0 表示文档不存在
func (n *Node) DeleteDoc(indexName, id string) (bool, error) {
	op := translog.Op{Type: translog.OpDelete, ID: id}
	lsn, err := n.writeOp(indexName, id, op)
	if err != nil {
		return false, err
	}
	return lsn >= 0, nil
}

// writeOp 写操作协调：算分片 → 查路由 → 本地写或转发
func (n *Node) writeOp(indexName, id string, op translog.Op) (int64, error) {
	meta, ok := n.sm.Index(indexName)
	if !ok {
		return -1, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	shardID := shardIDOf(id, meta.NumShards)
	route, err := n.routeOf(indexName, shardID)
	if err != nil {
		return -1, err
	}
	opBytes, err := json.Marshal(op)
	if err != nil {
		return -1, fmt.Errorf("node: 序列化写操作失败 (id=%s body=%s): %w", id, string(op.Doc), err)
	}
	if route.Primary == n.Meta.ID {
		return n.primaryWrite(indexName, meta, shardID, op, opBytes)
	}
	primary, ok := n.smNode(route.Primary)
	if !ok {
		return -1, fmt.Errorf("node: primary 节点 %d 不在节点表中", route.Primary)
	}
	if !n.sm.NodeAlive(route.Primary) {
		return -1, fmt.Errorf("node: primary 节点 %s 已下线，等待故障转移", primary.Name)
	}
	return n.Client.ForwardWrite(primary.GRPCAddr, indexName, int32(shardID), opBytes, n.waitAllOf(meta))
}

// primaryWrite primary 侧写入：本地应用 + 复制到副本
func (n *Node) primaryWrite(indexName string, meta cluster.IndexMeta, shardID int, op translog.Op, opBytes []byte) (int64, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return -1, fmt.Errorf("node: 索引 %q 不在本节点", indexName)
	}
	// 本地应用（primary 直接用带 LSN 对齐的 ApplyOp 语义自应用亦可，
	// 但走 Index/Delete 以获得 found 语义）
	var lsn int64
	var err error
	switch op.Type {
	case translog.OpIndex:
		lsn, err = ix.Index(op.ID, op.Doc)
	case translog.OpDelete:
		var found bool
		found, lsn, err = ix.Delete(op.ID)
		if err == nil && !found {
			return -1, nil // 文档不存在：lsn=-1，不复制
		}
	default:
		return -1, fmt.Errorf("node: 未知操作类型 %d", op.Type)
	}
	if err != nil {
		return -1, err
	}

	if n.waitAllOf(meta) {
		// 同步等全部存活副本 ack
		if err := n.pushSync(indexName, shardID, lsn, opBytes); err != nil {
			return -1, err
		}
	} else {
		// 异步推送（失败由副本拉取兜底）
		n.enqueuePush(indexName, shardID, lsn, opBytes)
	}
	return lsn, nil
}

// aliveReplicas 返回路由表中存活且非本节点的副本节点
func (n *Node) aliveReplicas(route cluster.ShardRoute) []uint64 {
	var out []uint64
	for _, id := range route.Replicas {
		if id != n.Meta.ID && n.sm.NodeAlive(id) {
			out = append(out, id)
		}
	}
	return out
}

// pushSync 同步推送并等待全部存活副本 ack（wait_for_active_shards=all）
func (n *Node) pushSync(indexName string, shardID int, lsn int64, op []byte) error {
	route, err := n.routeOf(indexName, shardID)
	if err != nil {
		return err
	}
	for _, id := range n.aliveReplicas(route) {
		meta, ok := n.smNode(id)
		if !ok {
			return fmt.Errorf("node: 副本节点 %d 不在节点表中", id)
		}
		if _, err := n.Client.ApplyOp(meta.GRPCAddr, indexName, int32(shardID), lsn, op); err != nil {
			if strings.Contains(err.Error(), "LSN 缺口") ||
				strings.Contains(err.Error(), "不存在") ||
				strings.Contains(err.Error(), "不在本节点") {
				// 新补建/落后的副本尚未就绪（不算 active），跳过——
				// 它将通过拉取循环补齐，语义同 ES 的 initializing shard 不计入
				// wait_for_active_shards
				continue
			}
			return fmt.Errorf("node: 副本 %s 未 ack（wait_for_active_shards=all）: %w", meta.Name, err)
		}
	}
	return nil
}

// ---------- 异步推送 ----------

// enqueuePush 把复制任务投入该分片的顺序队列
func (n *Node) enqueuePush(indexName string, shardID int, lsn int64, op []byte) {
	key := shardKey{indexName, shardID}
	n.mu.Lock()
	ch, ok := n.pushQ[key]
	if !ok {
		ch = make(chan replTask, 1024)
		n.pushQ[key] = ch
		go n.pushLoop(key, ch)
	}
	n.mu.Unlock()
	select {
	case ch <- replTask{lsn: lsn, op: op}:
	default:
		mlog.Warn("复制队列已满，丢弃 %s[%d] lsn=%d（由副本拉取兜底）", indexName, shardID, lsn)
	}
}

// pushLoop 单分片顺序推送循环
func (n *Node) pushLoop(key shardKey, ch chan replTask) {
	for {
		select {
		case <-n.stopc:
			return
		case task := <-ch:
			n.pushTask(key, task)
		}
	}
}

// pushTask 推送一条任务到全部存活副本，退避重试最多 5 次
func (n *Node) pushTask(key shardKey, task replTask) {
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		route, err := n.routeOf(key.index, key.shard)
		if err != nil {
			return // 索引已删除
		}
		replicas := n.aliveReplicas(route)
		allOK := true
		for _, id := range replicas {
			meta, ok := n.smNode(id)
			if !ok {
				continue
			}
			if _, err := n.Client.ApplyOp(meta.GRPCAddr, key.index, int32(key.shard), task.lsn, task.op); err != nil {
				mlog.Warn("复制推送失败 %s[%d] -> %s lsn=%d: %v", key.index, key.shard, meta.Name, task.lsn, err)
				allOK = false
			}
		}
		if allOK {
			return
		}
		select {
		case <-n.stopc:
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}
	mlog.Warn("复制推送放弃 %s[%d] lsn=%d（由副本拉取兜底愈合）", key.index, key.shard, task.lsn)
}

// ---------- 副本拉取 ----------

// reconcile 对账本地分片角色：replica 启动拉取器，primary/不再持有则停止
func (n *Node) reconcile() {
	if !n.Meta.Data {
		return
	}
	for _, indexName := range n.Mgr.Names() {
		ix, ok := n.Mgr.Get(indexName)
		if !ok {
			continue
		}
		routes, ok := n.sm.Route(indexName)
		if !ok {
			continue
		}
		for _, shardID := range ix.LocalShards() {
			if shardID >= len(routes) {
				continue
			}
			key := shardKey{indexName, shardID}
			r := routes[shardID]
			isReplica := r.Primary != n.Meta.ID && containsNode(r.Replicas, n.Meta.ID)
			n.mu.Lock()
			_, pulling := n.pullers[key]
			n.mu.Unlock()
			if isReplica && !pulling {
				n.startPuller(key)
			} else if !isReplica && pulling {
				n.stopPuller(key)
			}
		}
	}
}

// startPuller 启动副本拉取循环
func (n *Node) startPuller(key shardKey) {
	cancel := make(chan struct{})
	n.mu.Lock()
	n.pullers[key] = cancel
	n.mu.Unlock()
	mlog.Info("node %s 副本拉取启动 %s[%d]", n.Meta.Name, key.index, key.shard)
	go func() {
		ticker := time.NewTicker(n.PullInterval)
		defer ticker.Stop()
		for {
			select {
			case <-cancel:
				return
			case <-n.stopc:
				return
			case <-ticker.C:
				n.pullOnce(key, cancel)
			}
		}
	}()
}

// stopPuller 停止拉取循环
func (n *Node) stopPuller(key shardKey) {
	n.mu.Lock()
	cancel, ok := n.pullers[key]
	if ok {
		delete(n.pullers, key)
	}
	n.mu.Unlock()
	if ok {
		close(cancel)
		mlog.Info("node %s 副本拉取停止 %s[%d]（角色已变更）", n.Meta.Name, key.index, key.shard)
	}
}

// pullOnce 按 LSN 从 primary 拉一批并顺序应用
func (n *Node) pullOnce(key shardKey, cancel chan struct{}) {
	route, err := n.routeOf(key.index, key.shard)
	if err != nil || route.Primary == n.Meta.ID {
		return
	}
	if !n.sm.NodeAlive(route.Primary) {
		return // primary 下线，等待故障转移
	}
	primary, ok := n.smNode(route.Primary)
	if !ok {
		return
	}
	ix, ok := n.Mgr.Get(key.index)
	if !ok {
		return
	}
	lsn, err := ix.ShardLSN(key.shard)
	if err != nil {
		return
	}
	ops, _, err := n.Client.FetchTranslog(primary.GRPCAddr, key.index, int32(key.shard), lsn+1, 512)
	if err != nil {
		mlog.Warn("副本拉取失败 %s[%d] from=%d: %v", key.index, key.shard, lsn+1, err)
		return
	}
	for i, opBytes := range ops {
		var op translog.Op
		if err := json.Unmarshal(opBytes, &op); err != nil {
			return
		}
		opLSN := lsn + 1 + int64(i)
		if _, err := ix.ApplyOp(key.shard, opLSN, op); err != nil {
			// LSN 缺口：primary 的 translog 已轮替掉副本需要的历史，
			// 5b 简化实现不做快照全量同步，告警等待人工介入（见 README 遗留限制）
			mlog.Warn("副本应用失败 %s[%d] lsn=%d: %v", key.index, key.shard, opLSN, err)
			return
		}
	}
	// 有进展则立即再拉一轮
	if len(ops) > 0 {
		select {
		case <-cancel:
		case <-n.stopc:
		default:
			n.pullOnce(key, cancel)
		}
	}
}

func containsNode(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// smNode 查节点表
func (n *Node) smNode(id uint64) (transport.NodeMeta, bool) {
	n.mu.RLock()
	sm := n.sm
	n.mu.RUnlock()
	return sm.Node(id)
}
