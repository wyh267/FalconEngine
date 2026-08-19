// Package transport 实现节点间 gRPC 通信。
//
// 协议 IDL 见 proto/transport.proto。本机无 protoc 工具链，
// 消息与 ServiceDesc 采用手写等价实现（types.go / server.go），
// 消息编码使用注册的 JSON codec（见 codec.go）；
// 安装 protoc 后可执行 `make proto` 切换为代码生成链路。
package transport

// 与 proto/transport.proto 保持一致的消息定义
// （JSON codec 序列化，字段名即线上格式）

// NodeMeta 节点元信息
type NodeMeta struct {
	ID       uint64 `json:"id"`                // 节点身份（由 grpc 地址派生，重启不变；路由表/心跳用）
	RaftID   uint64 `json:"raft_id,omitempty"` // raft 成员 ID（每次启动唯一，重启即新成员重新加入）
	Name     string `json:"name"`
	GRPCAddr string `json:"grpc_addr"`
	HTTPAddr string `json:"http_addr"`
	Master   bool   `json:"master"`
	Data     bool   `json:"data"`
}

type PingRequest struct {
	From NodeMeta `json:"from"`
}
type PingResponse struct {
	Node  NodeMeta `json:"node"`
	Error string   `json:"error,omitempty"`
}

type NodeInfoRequest struct{}
type NodeInfoResponse struct {
	Node NodeMeta `json:"node"`
}

type ShardSearchRequest struct {
	Index   string `json:"index"`
	Shard   int32  `json:"shard"`
	Dsl     []byte `json:"dsl"`
	Partial bool   `json:"partial"`
}
type ShardSearchResponse struct {
	Result []byte `json:"result"`
	Error  string `json:"error,omitempty"`
}

type ShardDocRequest struct {
	Index string `json:"index"`
	ID    string `json:"id"`
}
type ShardDocResponse struct {
	Source []byte `json:"source"`
	Found  bool   `json:"found"`
	Error  string `json:"error,omitempty"`
}

type ApplyOpRequest struct {
	Index string `json:"index"`
	Shard int32  `json:"shard"`
	Lsn   int64  `json:"lsn"` // primary 侧的操作 LSN，replica 按它对齐/判缺口
	Op    []byte `json:"op"`
}

// ForwardWriteRequest 协调节点转发给 primary 的写请求
type ForwardWriteRequest struct {
	Index   string `json:"index"`
	Shard   int32  `json:"shard"`
	Op      []byte `json:"op"`       // translog Op 的 JSON 编码
	WaitAll bool   `json:"wait_all"` // write.wait_for_active_shards=all：等全部副本 ack
}
type ForwardWriteResponse struct {
	Lsn   int64  `json:"lsn"`
	Error string `json:"error,omitempty"`
}

// FetchTranslogRequest 副本按 LSN 批量拉取 primary 的 translog
type FetchTranslogRequest struct {
	Index   string `json:"index"`
	Shard   int32  `json:"shard"`
	FromLsn int64  `json:"from_lsn"`
	Limit   int32  `json:"limit"`
}
type FetchTranslogResponse struct {
	Ops     [][]byte `json:"ops"`      // 每个元素为 translog Op 的 JSON 编码
	FromLsn int64    `json:"from_lsn"` // Ops[0] 对应的 LSN
	NextLsn int64    `json:"next_lsn"` // 下一条可用 LSN
	Error   string   `json:"error,omitempty"`
}

// ShardStatusItem 节点上报的单分片状态
type ShardStatusItem struct {
	Index   string `json:"index"`
	Shard   int32  `json:"shard"`
	Primary bool   `json:"primary"`
	Lsn     int64  `json:"lsn"`
	Docs    int64  `json:"docs"`
}

// ShardStatusRequest 心跳 + 分片状态上报
type ShardStatusRequest struct {
	Node   NodeMeta          `json:"node"`
	Shards []ShardStatusItem `json:"shards"`
}
type ShardStatusResponse struct {
	Error string `json:"error,omitempty"`
}
type ApplyOpResponse struct {
	Lsn   int64  `json:"lsn"`
	Error string `json:"error,omitempty"`
}

type RaftMessageRequest struct {
	Data []byte `json:"data"`
}
type RaftMessageResponse struct {
	Error string `json:"error,omitempty"`
}

type JoinNodeRequest struct {
	Node NodeMeta `json:"node"`
}
type JoinNodeResponse struct {
	Error  string   `json:"error,omitempty"`
	Leader NodeMeta `json:"leader"` // 处理本请求的 leader 元信息（加入方注册到本地视图用）
}

type GetClusterStateRequest struct{}
type GetClusterStateResponse struct {
	State []byte `json:"state"`
	Error string `json:"error,omitempty"`
}
