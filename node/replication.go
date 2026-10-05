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
	"github.com/FalconEngine/falcon/index"
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
		if err == nil {
			// 动态 mapping：推断出新字段后异步广播到 CSM（见 mapping.go）
			n.maybeBroadcastMapping(indexName, ix)
		}
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
		ack, err := n.Client.ApplyOp(meta.GRPCAddr, indexName, int32(shardID), lsn, op)
		if err != nil {
			if strings.Contains(err.Error(), "LSN 缺口") ||
				strings.Contains(err.Error(), "恢复中") ||
				strings.Contains(err.Error(), "不存在") ||
				strings.Contains(err.Error(), "不在本节点") {
				// 新补建/落后/恢复中的副本尚未就绪（不算 active），跳过——
				// 它将通过拉取循环或段拷贝恢复补齐，语义同 ES 的
				// initializing shard 不计入 wait_for_active_shards
				continue
			}
			return fmt.Errorf("node: 副本 %s 未 ack（wait_for_active_shards=all）: %w", meta.Name, err)
		}
		n.recordReplAck(shardKey{indexName, shardID}, id, ack, false)
	}
	return nil
}

// ---------- 异步推送 ----------

// enqueuePush 把复制任务投入该分片的顺序队列。
// 发送在锁内完成（select+default 不会阻塞）：deleteLocalIndex 删除并关闭
// channel 同样在锁内进行，保证不会向已关闭的 channel 发送。
func (n *Node) enqueuePush(indexName string, shardID int, lsn int64, op []byte) {
	key := shardKey{indexName, shardID}
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.pushQ[key]
	if !ok {
		ch = make(chan replTask, 1024)
		n.pushQ[key] = ch
		go n.pushLoop(key, ch)
	}
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
		case task, ok := <-ch:
			if !ok {
				return // 索引已删除，队列随之关闭
			}
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
			ack, err := n.Client.ApplyOp(meta.GRPCAddr, key.index, int32(key.shard), task.lsn, task.op)
			if err != nil {
				mlog.Warn("复制推送失败 %s[%d] -> %s lsn=%d: %v", key.index, key.shard, meta.Name, task.lsn, err)
				allOK = false
				continue
			}
			n.recordReplAck(key, id, ack, false)
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

// ---------- 副本 ack 记账与 translog 保留窗口（retention lease 最小形态） ----------

// recordReplAck 记录副本确认到的 LSN 并推进保留窗口。
// authoritative=true（FetchTranslog 拉取位点）时直接覆盖——拉取位点反映副本
// 真实位置，段拷贝恢复重建后可能回退；false（ApplyOp 响应）时单调取大。
func (n *Node) recordReplAck(key shardKey, nodeID uint64, lsn int64, authoritative bool) {
	n.mu.Lock()
	m := n.replAcks[key]
	if m == nil {
		m = map[uint64]int64{}
		n.replAcks[key] = m
	}
	if authoritative || lsn > m[nodeID] {
		m[nodeID] = lsn
	}
	n.mu.Unlock()
	n.updateRetentionLSN(key)
}

// updateRetentionLSN primary 侧推进分片 translog 保留窗口：
// 全局检查点 = min(存活副本的 ack)；未见过的存活副本保守按 -1（不推进）；
// 死亡副本的租约直接释放（对齐 ES 摘除死分片的 retention lease——落后过多
// 的副本回归时由段拷贝恢复兜底）。无存活副本时回到默认（只留当前代际）。
// 内存态即可：primary 重启后检查点丢失退化为默认保留，副本大不了走段拷贝，安全。
func (n *Node) updateRetentionLSN(key shardKey) {
	route, err := n.routeOf(key.index, key.shard)
	if err != nil || route.Primary != n.Meta.ID {
		return // 只有 primary 维护本分片的保留窗口
	}
	checkpoint := index.NoRetentionFloor
	n.mu.RLock()
	acks := n.replAcks[key]
	for _, id := range route.Replicas {
		if id == n.Meta.ID || !n.sm.NodeAlive(id) {
			continue // 死亡副本：租约释放
		}
		ack, seen := acks[id]
		if !seen {
			checkpoint = -1 // 有存活副本从未联系：保守不推进
			break
		}
		if ack < checkpoint {
			checkpoint = ack
		}
	}
	n.mu.RUnlock()
	if ix, ok := n.Mgr.Get(key.index); ok {
		ix.SetRetentionLSN(key.shard, checkpoint)
	}
}

// ---------- 副本拉取 ----------

// reconcile 对账本地分片角色：replica 启动拉取器，primary/不再持有则停止；
// 顺带维护 primary 角色标记（降级检测用）与 translog 保留窗口推进
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
			isPrimary := r.Primary == n.Meta.ID
			isReplica := !isPrimary && containsNode(r.Replicas, n.Meta.ID)
			switch {
			case isPrimary:
				// 持久化 primary 角色标记：供（含重启后的）降级识别
				ix.MarkShardPrimaryRole(shardID)
				// 周期推进保留窗口：感知副本死亡（释放租约）与路由变化
				n.updateRetentionLSN(key)
			case isReplica:
				if ix.ShardWasPrimaryRole(shardID) {
					// primary→replica 角色降级：本地历史可能与新 primary 分叉
					// （故障转移时未复制的写入可丢，对齐 ES 语义），强制段拷贝
					// 恢复重置，规避"同 LSN 不同内容"的静默分叉
					if primary, ok := n.smNode(r.Primary); ok && n.sm.NodeAlive(r.Primary) {
						mlog.Warn("node %s 分片 %s[%d] 由 primary 降级为副本，强制段拷贝恢复",
							n.Meta.Name, indexName, shardID)
						n.startRecovery(key, primary)
					}
				}
			}
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

// pullOnce 按 LSN 从 primary 拉一批并顺序应用；
// 落后出窗/历史分叉时触发段拷贝恢复（见 recovery.go）
func (n *Node) pullOnce(key shardKey, cancel chan struct{}) {
	if n.isRecovering(key) {
		return // 段拷贝恢复中，等完成后从新基线继续拉取
	}
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
	from := lsn + 1
	ops, next, oldest, err := n.Client.FetchTranslog(primary.GRPCAddr, key.index, int32(key.shard), from, 512, n.Meta.ID)
	if err != nil {
		mlog.Warn("副本拉取失败 %s[%d] from=%d: %v", key.index, key.shard, from, err)
		return
	}
	if len(ops) == 0 {
		switch {
		case from < oldest:
			// 落后出窗：primary 的保留窗口已清除副本需要的历史，
			// 靠段拷贝恢复追平（替换原"告警等人工介入"）
			mlog.Warn("副本 %s[%d] 落后出窗 (from=%d oldest=%d)，触发段拷贝恢复", key.index, key.shard, from, oldest)
			n.startRecovery(key, primary)
		case next < from:
			// 副本超前于 primary：历史分叉（如旧 primary 降级），段拷贝恢复重置
			mlog.Warn("副本 %s[%d] 历史分叉 (from=%d next=%d)，触发段拷贝恢复", key.index, key.shard, from, next)
			n.startRecovery(key, primary)
		}
		return // 空 ops 且未出窗：已追平
	}
	for i, opBytes := range ops {
		var op translog.Op
		if err := json.Unmarshal(opBytes, &op); err != nil {
			return
		}
		opLSN := from + int64(i)
		if _, err := ix.ApplyOp(key.shard, opLSN, op); err != nil {
			// LSN 缺口等应用失败：拉取循环会持续重试；
			// 若因历史分叉无法愈合，由出窗/分叉/降级检测触发段拷贝恢复
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
