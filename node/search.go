package node

import (
	"encoding/json"
	"fmt"

	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/schema"
)

// 跨节点查询路由（scatter-gather）：
// 协调节点对每个分片优先查本地持有（primary 或 replica 均可），
// 否则经 gRPC ShardSearch 查远端（优先存活 primary，降级到存活副本）；
// 归并复用 index.ScatterPlan。
//
// 降级语义（文档化决策）：某分片不可用时返回部分结果并标记
// {"partial": true, "failed_shards": [...]}，而非整体报错。

// schemaOfIndex 从集群元数据解析索引 schema（协调节点无需本地分片）
func (n *Node) schemaOfIndex(indexName string) (*schema.Schema, error) {
	meta, ok := n.state().Index(indexName)
	if !ok {
		return nil, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	if len(meta.Mapping) == 0 {
		return schema.New(nil)
	}
	var sch schema.Schema
	if err := json.Unmarshal(meta.Mapping, &sch); err != nil {
		return nil, fmt.Errorf("node: 索引 %q mapping 解析失败: %w", indexName, err)
	}
	return &sch, nil
}

// Search 集群级查询：scatter-gather 全部分片
func (n *Node) Search(indexName string, dsl []byte) ([]byte, error) {
	sch, err := n.schemaOfIndex(indexName)
	if err != nil {
		return nil, err
	}
	routes, ok := n.state().Route(indexName)
	if !ok {
		return nil, fmt.Errorf("node: 索引 %q 无路由表", indexName)
	}
	plan, err := index.NewScatterPlan(dsl, sch)
	if err != nil {
		return nil, err
	}

	var partials []*index.Result
	var failed []int
	for shardID, route := range routes {
		res, err := n.searchOneShard(indexName, shardID, route.Primary, route.Replicas, plan.ScatterBody)
		if err != nil {
			// 降级：部分结果 + 标记
			failed = append(failed, shardID)
			continue
		}
		partials = append(partials, res)
	}
	res, err := plan.Merge(partials)
	if err != nil {
		return nil, err
	}
	if len(failed) > 0 {
		res.Partial = true
		res.FailedShards = failed
	}
	return json.Marshal(res)
}

// searchOneShard 查询单个分片：本地优先，远端按 primary → replicas 顺序降级
func (n *Node) searchOneShard(indexName string, shardID int, primary uint64, replicas []uint64, body []byte) (*index.Result, error) {
	// 本地持有（primary 或 replica）直接查
	if ix, ok := n.Mgr.Get(indexName); ok {
		if r, err := ix.SearchShard(shardID, body, true); err == nil {
			return r, nil
		}
	}
	// 远端：按序尝试存活 holder
	for _, id := range append([]uint64{primary}, replicas...) {
		if !n.state().NodeAlive(id) {
			continue
		}
		meta, ok := n.smNode(id)
		if !ok {
			continue
		}
		raw, err := n.Client.ShardSearch(meta.GRPCAddr, indexName, int32(shardID), body, true)
		if err != nil {
			continue
		}
		var res index.Result
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, err
		}
		return &res, nil
	}
	return nil, fmt.Errorf("node: 分片 %s[%d] 无可用 holder", indexName, shardID)
}

// GetDoc 集群级取文档：路由到分片后按 primary → replicas 降级
func (n *Node) GetDoc(indexName, id string) ([]byte, bool, error) {
	meta, ok := n.state().Index(indexName)
	if !ok {
		return nil, false, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	// 本地优先
	if ix, ok := n.Mgr.Get(indexName); ok {
		if raw, found, err := ix.Get(id); err == nil {
			return raw, found, nil
		}
	}
	shardID := shardIDOf(id, meta.NumShards)
	route, err := n.routeOf(indexName, shardID)
	if err != nil {
		return nil, false, err
	}
	for _, nid := range append([]uint64{route.Primary}, route.Replicas...) {
		if nid == n.Meta.ID || !n.state().NodeAlive(nid) {
			continue
		}
		nd, ok := n.smNode(nid)
		if !ok {
			continue
		}
		src, found, err := n.Client.ShardDoc(nd.GRPCAddr, indexName, id)
		if err != nil {
			continue
		}
		return src, found, nil
	}
	return nil, false, nil
}
