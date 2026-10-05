// Package cluster 实现集群控制面：单 raft group 管理集群元数据（CSM）。
//
// 管理的元数据：
//   - 节点表：nodeID、grpc/http 地址、角色（master/data）
//   - 索引表：索引名 -> {分片数、副本数、mapping}
//   - 路由表：索引 -> 分片 -> {主分片节点、副本分片节点}
//
// 持久化（P0-1）：raft 日志/快照/成员 ID 落盘到 <data>/.falcon/raft/
// （自研精简 WAL，见 store.go），节点重启后从磁盘恢复 CSM；
// data-only 节点不进 raft group，通过 JoinNode gRPC 注册进元数据。
package cluster

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/transport"
)

// NodeMeta 集群中的节点（别名传输层定义，保持线上格式一致）
type NodeMeta = transport.NodeMeta

// IndexMeta 索引元数据
type IndexMeta struct {
	Name        string          `json:"name"`
	NumShards   int             `json:"num_shards"`
	NumReplicas int             `json:"num_replicas"`
	Mapping     json.RawMessage `json:"mapping,omitempty"` // schema JSON
	// WaitAll 对应 write.wait_for_active_shards=all：
	// 写请求等全部存活副本 ack 后才返回
	WaitAll bool `json:"wait_all,omitempty"`
	// DateDetection 动态 mapping 日期检测开关（nil 默认 true）：
	// 随 CSM 分发，各节点落地新分片时透传给索引设置
	DateDetection *bool `json:"date_detection,omitempty"`
}

// ShardRoute 一个分片的路由：主分片所在节点与副本节点列表
type ShardRoute struct {
	Primary  uint64   `json:"primary"`
	Replicas []uint64 `json:"replicas,omitempty"`
}

// CommandType raft 日志命令类型
type CommandType string

const (
	// CmdAddNode 注册节点（master 节点经 ConfChange 加入 raft group，
	// 本命令只负责把节点信息写入元数据；data-only 节点只走本命令）
	CmdAddNode CommandType = "add_node"
	// CmdAddIndex 创建索引（含分配好的路由表）
	CmdAddIndex CommandType = "add_index"
	// CmdDeleteIndex 删除索引
	CmdDeleteIndex CommandType = "delete_index"
	// CmdShardStatus 节点分片状态上报（心跳载荷：角色/LSN/文档数）
	CmdShardStatus CommandType = "shard_status"
	// CmdNodeDead 标记节点下线（master leader 故障检测后提案）
	CmdNodeDead CommandType = "node_dead"
	// CmdNodeAlive 标记节点重新上线
	CmdNodeAlive CommandType = "node_alive"
	// CmdUpdateRoute 更新单个分片的路由（故障转移/副本补建）
	CmdUpdateRoute CommandType = "update_route"
	// CmdRemoveNode 彻底摘除陈旧节点：节点表/Dead 表/分片状态清理，
	// 路由表中其分片摘除并确定性补建（remove_node，配合 raft 成员移除使用）
	CmdRemoveNode CommandType = "remove_node"
	// CmdUpdateMapping 更新索引 mapping（载荷为增量字段，CSM 只增合并）
	CmdUpdateMapping CommandType = "update_mapping"
)

// Command raft 日志承载的控制面命令
type Command struct {
	Type      CommandType  `json:"type"`
	Node      *NodeMeta    `json:"node,omitempty"`
	Index     *IndexMeta   `json:"index,omitempty"`
	Routes    []ShardRoute `json:"routes,omitempty"`     // CmdAddIndex 时携带
	IndexName string       `json:"index_name,omitempty"` // CmdDeleteIndex / CmdUpdateRoute / CmdUpdateMapping 时携带

	// CmdShardStatus / CmdNodeDead / CmdNodeAlive 载荷
	NodeID uint64        `json:"node_id,omitempty"`
	Shards []ShardStatus `json:"shards,omitempty"`

	// CmdUpdateRoute 载荷
	ShardID int        `json:"shard_id,omitempty"`
	Route   ShardRoute `json:"route,omitempty"`

	// CmdUpdateMapping 载荷：{"fields":[...]} 增量字段（CSM 只增合并）
	Mapping json.RawMessage `json:"mapping,omitempty"`
}

// ShardStatus 单分片状态（心跳上报与 CSM 存储共用）
type ShardStatus struct {
	Index   string `json:"index"`
	Shard   int    `json:"shard"`
	Primary bool   `json:"primary"`
	LSN     int64  `json:"lsn"`
	Docs    int64  `json:"docs"`
}

// StateMachine 集群状态机（CSM）：raft 已提交命令的应用结果
type StateMachine struct {
	mu      sync.RWMutex
	Nodes   map[uint64]NodeMeta     `json:"nodes"`
	Indices map[string]IndexMeta    `json:"indices"`
	Routes  map[string][]ShardRoute `json:"routes"` // 索引名 -> 分片路由（下标即分片号）
	// Dead 被判下线的节点（leader 故障检测提案写入，所有节点可见）
	Dead map[uint64]bool `json:"dead,omitempty"`
	// ShardStats 各分片各节点最近上报的状态：索引 -> 分片 -> 节点 -> 状态
	ShardStats map[string]map[int]map[uint64]ShardStatus `json:"shard_stats,omitempty"`
}

// NewStateMachine 创建空状态机
func NewStateMachine() *StateMachine {
	return &StateMachine{
		Nodes:      map[uint64]NodeMeta{},
		Indices:    map[string]IndexMeta{},
		Routes:     map[string][]ShardRoute{},
		Dead:       map[uint64]bool{},
		ShardStats: map[string]map[int]map[uint64]ShardStatus{},
	}
}

// Apply 应用一条已提交的命令（幂等）
func (sm *StateMachine) Apply(cmd Command) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch cmd.Type {
	case CmdAddNode:
		if cmd.Node != nil {
			sm.Nodes[cmd.Node.ID] = *cmd.Node
		}
	case CmdAddIndex:
		if cmd.Index != nil {
			sm.Indices[cmd.Index.Name] = *cmd.Index
			sm.Routes[cmd.Index.Name] = cmd.Routes
		}
	case CmdDeleteIndex:
		delete(sm.Indices, cmd.IndexName)
		delete(sm.Routes, cmd.IndexName)
		delete(sm.ShardStats, cmd.IndexName)
	case CmdShardStatus:
		for _, st := range cmd.Shards {
			// 索引已删除时忽略其状态上报：节点心跳可能先于本地删除发出，
			// 否则已删索引的分片状态会被在途心跳重新写回 CSM
			if _, ok := sm.Indices[st.Index]; !ok {
				continue
			}
			if sm.ShardStats[st.Index] == nil {
				sm.ShardStats[st.Index] = map[int]map[uint64]ShardStatus{}
			}
			if sm.ShardStats[st.Index][st.Shard] == nil {
				sm.ShardStats[st.Index][st.Shard] = map[uint64]ShardStatus{}
			}
			sm.ShardStats[st.Index][st.Shard][cmd.NodeID] = st
		}
	case CmdNodeDead:
		sm.Dead[cmd.NodeID] = true
	case CmdNodeAlive:
		delete(sm.Dead, cmd.NodeID)
	case CmdUpdateRoute:
		if routes, ok := sm.Routes[cmd.IndexName]; ok && cmd.ShardID >= 0 && cmd.ShardID < len(routes) {
			routes[cmd.ShardID] = cmd.Route
		}
	case CmdRemoveNode:
		sm.removeNodeLocked(cmd.NodeID)
	case CmdUpdateMapping:
		// mapping 只增不改：新字段并入 CSM（合并安全——并发提案天然做字段并集），
		// 已有字段定义以 CSM 现有为准。纯结构合并不依赖插件注册表，
		// 保证 FSM apply 的确定性（字段合法性由提案边界校验）。
		if meta, ok := sm.Indices[cmd.IndexName]; ok && len(cmd.Mapping) > 0 {
			if merged, err := schema.MergeMappingJSON(meta.Mapping, cmd.Mapping); err == nil {
				meta.Mapping = merged
				sm.Indices[cmd.IndexName] = meta
			}
		}
	}
}

// removeNodeLocked 彻底摘除节点：节点表/Dead 表摘除、分片状态清理、
// 路由表中其分片摘除并确定性补建副本。
// 确定性保证：全部节点按同一日志序列应用，CSM 状态一致，分配结果一致。
// 典型场景：节点磁盘清空后以同地址（同 NodeID）新 RaftID 重新加入，
// 其旧分片数据已不存在，必须摘除重分配。
func (sm *StateMachine) removeNodeLocked(id uint64) {
	delete(sm.Nodes, id)
	delete(sm.Dead, id)
	for _, byShard := range sm.ShardStats {
		for _, byNode := range byShard {
			delete(byNode, id)
		}
	}
	for name, routes := range sm.Routes {
		meta := sm.Indices[name]
		for i, r := range routes {
			// primary 摘除：优先提升首个副本；无副本时数据已随旧磁盘丢失，
			// 确定性选一个存活 data 节点作空 primary 恢复可写（语义同 ES 空分配）
			if r.Primary == id {
				r.Primary = 0
				if len(r.Replicas) > 0 {
					r.Primary = r.Replicas[0]
					r.Replicas = append([]uint64(nil), r.Replicas[1:]...)
				}
			}
			// 副本摘除
			if len(r.Replicas) > 0 {
				reps := make([]uint64, 0, len(r.Replicas))
				for _, rid := range r.Replicas {
					if rid != id {
						reps = append(reps, rid)
					}
				}
				r.Replicas = reps
			}
			if r.Primary == 0 {
				r.Primary = sm.pickDataNodeLocked(r)
			}
			// 副本补建：补足副本数（无更多存活节点则保持降级）
			for r.Primary != 0 && len(r.Replicas) < meta.NumReplicas {
				pick := sm.pickDataNodeLocked(r)
				if pick == 0 {
					break
				}
				r.Replicas = append(r.Replicas, pick)
			}
			routes[i] = r
		}
	}
}

// pickDataNodeLocked 为分片确定性挑选一个不持有该分片的存活 data 节点
// （负载最小、ID 最小优先）；无可用节点返回 0。调用方需持写锁。
func (sm *StateMachine) pickDataNodeLocked(r ShardRoute) uint64 {
	load := map[uint64]int{}
	for _, routes := range sm.Routes {
		for _, rt := range routes {
			load[rt.Primary]++
			for _, rid := range rt.Replicas {
				load[rid]++
			}
		}
	}
	var best uint64
	found := false
	for nid, n := range sm.Nodes {
		if !n.Data || sm.Dead[nid] || nid == r.Primary || contains(r.Replicas, nid) {
			continue
		}
		if !found || load[nid] < load[best] || (load[nid] == load[best] && nid < best) {
			best, found = nid, true
		}
	}
	return best
}

// csmSnapshot CSM 快照结构（Snapshot/Restore 共用）
type csmSnapshot struct {
	Nodes      map[uint64]NodeMeta                       `json:"nodes"`
	Indices    map[string]IndexMeta                      `json:"indices"`
	Routes     map[string][]ShardRoute                   `json:"routes"`
	Dead       map[uint64]bool                           `json:"dead,omitempty"`
	ShardStats map[string]map[int]map[uint64]ShardStatus `json:"shard_stats,omitempty"`
}

// Snapshot 返回状态机的 JSON 快照（ClusterState gRPC 与 raft 快照落盘共用）
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return json.Marshal(csmSnapshot{Nodes: sm.Nodes, Indices: sm.Indices, Routes: sm.Routes, Dead: sm.Dead, ShardStats: sm.ShardStats})
}

// Restore 从快照数据整体恢复状态机（raft 重启加载/收到 leader 快照时调用）
func (sm *StateMachine) Restore(data []byte) error {
	var snap csmSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("cluster: CSM 快照解码失败: %w", err)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Nodes = snap.Nodes
	sm.Indices = snap.Indices
	sm.Routes = snap.Routes
	sm.Dead = snap.Dead
	sm.ShardStats = snap.ShardStats
	// JSON 缺省字段恢复为 nil，补为空 map 保证 Apply 各 case 不用判空
	if sm.Nodes == nil {
		sm.Nodes = map[uint64]NodeMeta{}
	}
	if sm.Indices == nil {
		sm.Indices = map[string]IndexMeta{}
	}
	if sm.Routes == nil {
		sm.Routes = map[string][]ShardRoute{}
	}
	if sm.Dead == nil {
		sm.Dead = map[uint64]bool{}
	}
	if sm.ShardStats == nil {
		sm.ShardStats = map[string]map[int]map[uint64]ShardStatus{}
	}
	return nil
}

// AllNodes 返回全部节点
func (sm *StateMachine) AllNodes() []NodeMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]NodeMeta, 0, len(sm.Nodes))
	for _, n := range sm.Nodes {
		out = append(out, n)
	}
	return out
}

// Node 按 ID 查节点
func (sm *StateMachine) Node(id uint64) (NodeMeta, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	n, ok := sm.Nodes[id]
	return n, ok
}

// NodeAlive 节点是否存活（在节点表且未被标记下线）
func (sm *StateMachine) NodeAlive(id uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.Nodes[id]
	return ok && !sm.Dead[id]
}

// ShardStatusOf 取索引的分片状态表（分片 -> 节点 -> 状态），返回拷贝
func (sm *StateMachine) ShardStatusOf(indexName string) map[int]map[uint64]ShardStatus {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := map[int]map[uint64]ShardStatus{}
	for shard, m := range sm.ShardStats[indexName] {
		cp := map[uint64]ShardStatus{}
		for nid, st := range m {
			cp[nid] = st
		}
		out[shard] = cp
	}
	return out
}

// DataNodes 返回全部 data 节点（按 ID 升序，保证分配结果确定性）
func (sm *StateMachine) DataNodes() []NodeMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var out []NodeMeta
	for _, n := range sm.Nodes {
		if n.Data {
			out = append(out, n)
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].ID < out[i].ID {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// Index 取索引元数据
func (sm *StateMachine) Index(name string) (IndexMeta, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	m, ok := sm.Indices[name]
	return m, ok
}

// Route 取索引的分片路由
func (sm *StateMachine) Route(name string) ([]ShardRoute, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	r, ok := sm.Routes[name]
	return r, ok
}

// ErrNotLeader 非 leader 节点收到的 propose 请求
var ErrNotLeader = fmt.Errorf("cluster: 本节点不是 raft leader")
