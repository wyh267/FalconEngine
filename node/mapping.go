// mapping 体系（P1-7）的集群衔接：
//   - CSM 中 mapping 为权威（只增不改，合并安全）：显式 PUT _mapping 与 primary
//     动态推断出的新字段都经 leader 提案 CmdUpdateMapping 广播
//   - 落地：master 节点在 raft apply 回调、data-only 节点在轮询对账中，
//     把 CSM mapping 合并进本节点全部本地分片（幂等）
//   - 窗口期（遗留限制，见 README）：primary 推断 → raft apply/轮询到达之间，
//     未收到含新字段文档的分片暂时没有该字段（收到文档的分片会自行推断）；
//     老段没有新字段的索引文件，视为字段缺失，待 merge 重建后自愈
package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/FalconEngine/falcon/cluster"
	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/schema"
)

// mappingPayload CmdUpdateMapping 的载荷结构（{"fields":[...]} 增量）
type mappingPayload struct {
	Fields []schema.Field `json:"fields"`
}

// applyLocalMapping 把 mapping（增量或全量，均幂等）合并进本节点该索引的全部本地分片。
// 整体合并遇冲突（本地推断类型与 CSM 不一致）时降级为逐字段尽力合并，
// 跳过冲突项——冲突属动态推断的类型分歧，各分片保留本地定义（见 README 遗留限制）。
func (n *Node) applyLocalMapping(indexName string, mapping json.RawMessage) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok || len(mapping) == 0 {
		return
	}
	var p mappingPayload
	if err := json.Unmarshal(mapping, &p); err != nil || len(p.Fields) == 0 {
		return
	}
	if err := ix.UpdateMapping(p.Fields); err == nil {
		return
	}
	for _, f := range p.Fields {
		if err := ix.UpdateMapping([]schema.Field{f}); err != nil {
			mlog.Warn("node %s 合并 mapping 字段 %s.%s 失败（与本地定义冲突，保留本地）: %v",
				n.Meta.Name, indexName, f.Name, err)
		}
	}
}

// ProposeMapping 处理 mapping 更新请求（transport.Handler 实现，仅 master leader 受理）：
// 与 CSM 现有 mapping 做合并预检（新字段合法、已有字段不冲突）后提案 CmdUpdateMapping。
func (n *Node) ProposeMapping(indexName string, mapping []byte) error {
	rn := n.RaftNode()
	if rn == nil {
		return fmt.Errorf("node: 本节点不是 master，无法处理 mapping 更新")
	}
	if !rn.IsLeader() {
		return cluster.ErrNotLeader
	}
	meta, ok := n.state().Index(indexName)
	if !ok {
		return fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	var p mappingPayload
	if err := json.Unmarshal(mapping, &p); err != nil || len(p.Fields) == 0 {
		return fmt.Errorf("node: mapping 载荷非法（应为 {\"fields\":[...]} 且非空）")
	}
	// 提案前校验：在 CSM 现有 mapping 副本上试合并（校验新字段类型/分词器、
	// 已有字段 Type/Analyzer 冲突）；CSM 本体只在 raft apply 时修改
	csm, err := csmSchemaOf(meta)
	if err != nil {
		return err
	}
	if err := csm.MergeFields(p.Fields); err != nil {
		return err
	}
	return rn.Propose(cluster.Command{Type: cluster.CmdUpdateMapping, IndexName: indexName, Mapping: mapping})
}

// UpdateMapping 集群感知 mapping 更新（api.ClusterProvider 实现）：
// 把增量字段发给 leader 校验并提案，等待本地集群视图可见后返回。
// 与现有字段定义冲突返回错误；幂等。
func (n *Node) UpdateMapping(name string, fields []schema.Field) error {
	if len(fields) == 0 {
		return fmt.Errorf("node: mapping 更新字段列表为空")
	}
	if _, ok := n.state().Index(name); !ok {
		return fmt.Errorf("node: 索引 %q 不存在", name)
	}
	payload, err := json.Marshal(mappingPayload{Fields: fields})
	if err != nil {
		return err
	}
	var lastErr error
	sent := false
	for _, addr := range n.leaderAddrs() {
		if err := n.Client.UpdateMapping(addr, name, payload); err != nil {
			lastErr = err
			continue
		}
		sent = true
		break
	}
	if !sent {
		return fmt.Errorf("node: mapping 更新未送达 leader: %w", lastErr)
	}
	// 等待本地视图收敛（master 为 raft apply；data-only 为周期轮询）
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if meta, ok := n.state().Index(name); ok && mappingCovers(meta.Mapping, fields) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node: mapping 更新 %q 等待落地超时", name)
}

// maybeBroadcastMapping primary 写入后检查动态推断出的新字段：
// 本地字段并集相对 CSM 有增量时，异步发给 leader 提案 CmdUpdateMapping。
// 尽力而为：失败仅推迟 CSM 收敛（本地分片 schema 已是新值），
// 后续写入或显式 PUT _mapping 会再次触发。
func (n *Node) maybeBroadcastMapping(indexName string, ix *index.Index) {
	union := ix.UnionSchema()
	if union == nil {
		return
	}
	flatLen := len(union.Flattened())
	n.mu.RLock()
	done := n.mappingBcast[indexName] == flatLen
	n.mu.RUnlock()
	if done {
		return
	}
	meta, ok := n.state().Index(indexName)
	if !ok {
		return
	}
	csm, err := csmSchemaOf(meta)
	if err != nil {
		return
	}
	delta := union.DiffFields(csm)
	n.mu.Lock()
	n.mappingBcast[indexName] = flatLen
	n.mu.Unlock()
	if len(delta) == 0 {
		return
	}
	payload, err := json.Marshal(mappingPayload{Fields: delta})
	if err != nil {
		return
	}
	go func() {
		for _, addr := range n.leaderAddrs() {
			if err := n.Client.UpdateMapping(addr, indexName, payload); err == nil {
				return
			}
		}
		// 未送达：清除标记，下一次写入重试
		n.mu.Lock()
		if n.mappingBcast[indexName] == flatLen {
			delete(n.mappingBcast, indexName)
		}
		n.mu.Unlock()
		mlog.Debug("node %s mapping 增量广播 %s 暂未送达 leader", n.Meta.Name, indexName)
	}()
}

// csmSchemaOf 把 CSM 索引元数据中的 mapping 解析为 Schema（空 mapping 返回空 Schema）
func csmSchemaOf(meta cluster.IndexMeta) (*schema.Schema, error) {
	if len(meta.Mapping) == 0 {
		return schema.New(nil)
	}
	var sch schema.Schema
	if err := json.Unmarshal(meta.Mapping, &sch); err != nil {
		return nil, fmt.Errorf("node: 索引 %q CSM mapping 解析失败: %w", meta.Name, err)
	}
	return &sch, nil
}

// mappingCovers 判断 CSM mapping 是否已包含全部给定字段
func mappingCovers(csmMapping json.RawMessage, fields []schema.Field) bool {
	if len(csmMapping) == 0 {
		return false
	}
	var csm schema.Schema
	if err := json.Unmarshal(csmMapping, &csm); err != nil {
		return false
	}
	req := &schema.Schema{Fields: fields}
	return len(req.DiffFields(&csm)) == 0
}
