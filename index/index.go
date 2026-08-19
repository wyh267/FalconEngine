package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spaolacci/murmur3"

	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/translog"
)

// Index 一个逻辑索引 = N 个分片，每个分片是一个独立的 Engine（shard-<i>/ 目录）。
// 路由：shardID = murmur3(id) % numShards；自动 ID 写入由上层先生成 ID 再路由。
// shards[i] 为 nil 表示本分片不在本节点（5b 将转发到远端持有节点）。
type Index struct {
	name        string
	dir         string
	numShards   int
	numReplicas int

	shards []*Engine // 下标即 shardID
}

// OpenIndex 打开（或创建）一个索引的全部本地分片。
// localShards 为 nil 表示全部分片都在本地（单机模式）；
// 否则只打开 localShards 列出的分片（分布式模式由集群分配决定）。
func OpenIndex(dir, name string, numShards, numReplicas int, sch *schema.Schema, localShards []int) (*Index, error) {
	if numShards <= 0 {
		numShards = 1
	}
	ix := &Index{
		name:        name,
		dir:         dir,
		numShards:   numShards,
		numReplicas: numReplicas,
		shards:      make([]*Engine, numShards),
	}
	if localShards == nil {
		localShards = make([]int, numShards)
		for i := range localShards {
			localShards[i] = i
		}
	}
	for _, id := range localShards {
		if err := ix.OpenShard(id, sch); err != nil {
			return nil, err
		}
	}
	return ix, nil
}

// shardDir 分片数据目录
func (ix *Index) shardDir(shardID int) string {
	return filepath.Join(ix.dir, fmt.Sprintf("shard-%d", shardID))
}

// OpenShard 打开（或创建）指定分片；幂等
func (ix *Index) OpenShard(shardID int, sch *schema.Schema) error {
	if shardID < 0 || shardID >= ix.numShards {
		return fmt.Errorf("index: 分片号 %d 越界（共 %d 片）", shardID, ix.numShards)
	}
	if ix.shards[shardID] != nil {
		return nil
	}
	e, err := Open(ix.shardDir(shardID), sch)
	if err != nil {
		return fmt.Errorf("index: 打开索引 %s 分片 %d 失败: %w", ix.name, shardID, err)
	}
	ix.shards[shardID] = e
	return nil
}

// NumShards 分片数
func (ix *Index) NumShards() int { return ix.numShards }

// NumReplicas 副本数
func (ix *Index) NumReplicas() int { return ix.numReplicas }

// Schema 返回首个本地分片的 schema（各分片一致）
func (ix *Index) Schema() *schema.Schema {
	for _, e := range ix.shards {
		if e != nil {
			return e.Schema()
		}
	}
	return nil
}

// LocalShards 返回本地持有的分片号（升序）
func (ix *Index) LocalShards() []int {
	var out []int
	for i, e := range ix.shards {
		if e != nil {
			out = append(out, i)
		}
	}
	return out
}

// ShardOf 文档 ID 路由到的分片号
func (ix *Index) ShardOf(id string) int {
	return int(murmur3.Sum32([]byte(id)) % uint32(ix.numShards))
}

// shard 取本地分片引擎，不存在（路由到远端）时返回错误
func (ix *Index) shard(shardID int) (*Engine, error) {
	e := ix.shards[shardID]
	if e == nil {
		return nil, fmt.Errorf("index: 索引 %s 分片 %d 不在本节点", ix.name, shardID)
	}
	return e, nil
}

// Index 写入/更新文档：按 ID 路由到分片，返回该操作的 LSN
func (ix *Index) Index(id string, raw json.RawMessage) (int64, error) {
	e, err := ix.shard(ix.ShardOf(id))
	if err != nil {
		return -1, err
	}
	return e.Index(id, raw)
}

// Delete 删除文档：按 ID 路由，返回删除操作的 LSN（未删除时 lsn=-1）
func (ix *Index) Delete(id string) (bool, int64, error) {
	e, err := ix.shard(ix.ShardOf(id))
	if err != nil {
		return false, -1, err
	}
	return e.Delete(id)
}

// Get 取原文：按 ID 路由
func (ix *Index) Get(id string) (json.RawMessage, bool, error) {
	e, err := ix.shard(ix.ShardOf(id))
	if err != nil {
		return nil, false, err
	}
	return e.Get(id)
}

// Flush 刷新全部本地分片
func (ix *Index) Flush() error {
	for _, e := range ix.shards {
		if e == nil {
			continue
		}
		if err := e.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// DocCount 本地分片存活文档总数
func (ix *Index) DocCount() int {
	n := 0
	for _, e := range ix.shards {
		if e != nil {
			n += e.DocCount()
		}
	}
	return n
}

// SegCount 本地分片段总数
func (ix *Index) SegCount() int {
	n := 0
	for _, e := range ix.shards {
		if e != nil {
			n += e.segCount()
		}
	}
	return n
}

// Close 关闭全部本地分片
func (ix *Index) Close() error {
	var firstErr error
	for i, e := range ix.shards {
		if e == nil {
			continue
		}
		if err := e.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("index: 关闭索引 %s 分片 %d 失败: %w", ix.name, i, err)
		}
	}
	return firstErr
}

// ApplyOp 将一条 translog 操作应用到指定分片（复制协议接入口）。
// 幂等按 LSN 对齐：opLSN < 下一条可用 LSN 时跳过（已应用过），
// 返回该分片当前的最后一条 LSN。
// opLSN > 下一条可用 LSN 时返回错误（有缺口，调用方应先 pull 追赶）。
func (ix *Index) ApplyOp(shardID int, opLSN int64, op translog.Op) (int64, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return -1, err
	}
	next := e.LastLSN() + 1
	if opLSN < next {
		return e.LastLSN(), nil // 已应用，幂等跳过
	}
	if opLSN > next {
		return -1, fmt.Errorf("index: 分片 %d LSN 缺口: 期望 %d, 收到 %d", shardID, next, opLSN)
	}
	switch op.Type {
	case translog.OpIndex:
		if _, err := e.Index(op.ID, op.Doc); err != nil {
			return -1, err
		}
	case translog.OpDelete:
		if _, _, err := e.Delete(op.ID); err != nil {
			return -1, err
		}
	default:
		return -1, fmt.Errorf("index: 未知操作类型 %d", op.Type)
	}
	return e.LastLSN(), nil
}

// ShardLSN 返回本地分片最后一条 LSN
func (ix *Index) ShardLSN(shardID int) (int64, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return -1, err
	}
	return e.LastLSN(), nil
}

// ReadShardTranslog 从指定 LSN 起批量读取本地分片的 translog 操作
func (ix *Index) ReadShardTranslog(shardID int, fromLSN int64, limit int) ([]translog.Op, int64, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return nil, 0, err
	}
	return e.ReadTranslog(fromLSN, limit)
}

// SearchShard 查询单个本地分片（partial=true 返回聚合中间结果），transport 层用
func (ix *Index) SearchShard(shardID int, body []byte, partial bool) (*Result, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return nil, err
	}
	return e.SearchShardDSL(body, partial)
}

// SearchDSL 索引级查询：scatter-gather 全部本地分片（跨节点的远端分片
// 由 node 包用 ScatterPlan + transport 实现，见 node.Search）。
func (ix *Index) SearchDSL(body []byte) (*Result, error) {
	// 找一个本地分片取 schema 解析 DSL（各分片 schema 一致或超集）
	var parser *Engine
	for _, e := range ix.shards {
		if e != nil {
			parser = e
			break
		}
	}
	if parser == nil {
		return nil, fmt.Errorf("index: 索引 %s 在本节点没有分片", ix.name)
	}
	plan, err := NewScatterPlan(body, parser.Schema())
	if err != nil {
		return nil, err
	}

	// scatter：本地各分片顺序执行（共享引擎锁）
	partials := make([]*Result, 0, ix.numShards)
	for i, e := range ix.shards {
		if e == nil {
			// 远端分片：由 node 层经 transport 转发
			continue
		}
		r, err := e.SearchShardDSL(plan.ScatterBody, true)
		if err != nil {
			return nil, fmt.Errorf("index: 分片 %d 查询失败: %w", i, err)
		}
		partials = append(partials, r)
	}
	return plan.Merge(partials)
}

// RemoveIndexData 删除索引全部数据目录（Manager.Delete 用）
func RemoveIndexData(dir string) error {
	return os.RemoveAll(dir)
}
