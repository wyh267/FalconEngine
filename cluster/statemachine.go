// Package cluster 实现集群控制面：单 raft group 管理集群元数据（CSM）。
//
// 管理的元数据：
//   - 节点表：nodeID、grpc/http 地址、角色（master/data）
//   - 索引表：索引名 -> {分片数、副本数、mapping}
//   - 路由表：索引 -> 分片 -> {主分片节点、副本分片节点}
//
// 简化决策（注释见各函数）：
//   - 使用 raft.MemoryStorage，不做 raft WAL/快照持久化；
//     节点重启后以全新成员身份重新 join 重建状态
//   - data-only 节点不进 raft group，通过 JoinNode gRPC 注册进元数据
package cluster

import (
	"encoding/json"
	"fmt"
	"sync"

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
)

// Command raft 日志承载的控制面命令
type Command struct {
	Type      CommandType  `json:"type"`
	Node      *NodeMeta    `json:"node,omitempty"`
	Index     *IndexMeta   `json:"index,omitempty"`
	Routes    []ShardRoute `json:"routes,omitempty"`     // CmdAddIndex 时携带
	IndexName string       `json:"index_name,omitempty"` // CmdDeleteIndex / CmdUpdateRoute 时携带

	// CmdShardStatus / CmdNodeDead / CmdNodeAlive 载荷
	NodeID uint64        `json:"node_id,omitempty"`
	Shards []ShardStatus `json:"shards,omitempty"`

	// CmdUpdateRoute 载荷
	ShardID int        `json:"shard_id,omitempty"`
	Route   ShardRoute `json:"route,omitempty"`
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
	}
}

// Snapshot 返回状态机的 JSON 快照（ClusterState gRPC 用）
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return json.Marshal(struct {
		Nodes      map[uint64]NodeMeta                       `json:"nodes"`
		Indices    map[string]IndexMeta                      `json:"indices"`
		Routes     map[string][]ShardRoute                   `json:"routes"`
		Dead       map[uint64]bool                           `json:"dead,omitempty"`
		ShardStats map[string]map[int]map[uint64]ShardStatus `json:"shard_stats,omitempty"`
	}{Nodes: sm.Nodes, Indices: sm.Indices, Routes: sm.Routes, Dead: sm.Dead, ShardStats: sm.ShardStats})
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
