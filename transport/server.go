package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"
)

// Handler 传输服务的业务实现（由节点装配层提供）
type Handler interface {
	// Ping 探活/握手
	Ping(from NodeMeta) (NodeMeta, error)
	// ShardSearch 在本地分片执行 DSL 查询，返回序列化的 index.Result
	ShardSearch(indexName string, shard int32, dsl []byte, partial bool) ([]byte, error)
	// ShardDoc 取文档原文
	ShardDoc(indexName, id string) (source []byte, found bool, err error)
	// ApplyOp 把 primary 的 translog 操作按 LSN 应用到本地副本分片
	ApplyOp(indexName string, shard int32, lsn int64, op []byte) (int64, error)
	// ForwardWrite 协调节点转发来的写请求（本节点应为该分片 primary）
	ForwardWrite(indexName string, shard int32, op []byte, waitAll bool) (int64, error)
	// FetchTranslog 副本按 LSN 批量拉取本地 primary 分片的 translog
	// （nodeID 为拉取方节点：primary 按 fromLSN-1 刷新其副本 ack 并推进保留窗口）
	FetchTranslog(indexName string, shard int32, fromLSN int64, limit int32, nodeID uint64) (ops [][]byte, nextLSN int64, oldestLSN int64, err error)
	// PrepareShardRecovery 开始分片恢复源会话：冻结 Flush 并返回一致视图
	PrepareShardRecovery(indexName string, shard int32) (lsnBase int64, schemaJSON []byte, files []RecoveryFileInfo, err error)
	// FetchShardFile 按恢复点分块读取段文件
	FetchShardFile(indexName string, shard int32, segDir, name string, off, limit int64) ([]byte, error)
	// FinishShardRecovery 结束分片恢复源会话（解冻）
	FinishShardRecovery(indexName string, shard int32) error
	// ShardStatusReport 心跳与分片状态上报（master leader 处理）
	ShardStatusReport(from NodeMeta, shards []ShardStatusItem) error
	// ProposeMapping mapping 更新提案（master leader 处理；mapping 为增量 schema JSON）
	ProposeMapping(indexName string, mapping []byte) error
	// RaftMessage 投递一条 raftpb.Message（Marshal 后的字节）
	RaftMessage(data []byte) error
	// JoinNode 节点注册（master leader 处理），返回 leader 元信息
	JoinNode(node NodeMeta) (NodeMeta, error)
	// ClusterState 返回序列化的集群状态
	ClusterState() ([]byte, error)
}

// transportServer gRPC 方法级接口（等价于 protoc 生成的 TransportServer，
// RegisterService 的 HandlerType 类型检查用）
type transportServer interface {
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	NodeInfo(context.Context, *NodeInfoRequest) (*NodeInfoResponse, error)
	ShardSearch(context.Context, *ShardSearchRequest) (*ShardSearchResponse, error)
	ShardDoc(context.Context, *ShardDocRequest) (*ShardDocResponse, error)
	ApplyOp(context.Context, *ApplyOpRequest) (*ApplyOpResponse, error)
	RaftMessage(context.Context, *RaftMessageRequest) (*RaftMessageResponse, error)
	ForwardWrite(context.Context, *ForwardWriteRequest) (*ForwardWriteResponse, error)
	FetchTranslog(context.Context, *FetchTranslogRequest) (*FetchTranslogResponse, error)
	PrepareShardRecovery(context.Context, *PrepareShardRecoveryRequest) (*PrepareShardRecoveryResponse, error)
	FetchShardFile(context.Context, *FetchShardFileRequest) (*FetchShardFileResponse, error)
	FinishShardRecovery(context.Context, *FinishShardRecoveryRequest) (*FinishShardRecoveryResponse, error)
	ShardStatusReport(context.Context, *ShardStatusRequest) (*ShardStatusResponse, error)
	UpdateMapping(context.Context, *UpdateMappingRequest) (*UpdateMappingResponse, error)
	JoinNode(context.Context, *JoinNodeRequest) (*JoinNodeResponse, error)
	GetClusterState(context.Context, *GetClusterStateRequest) (*GetClusterStateResponse, error)
}

// Server gRPC 服务端
type Server struct {
	handler Handler
	grpc    *grpc.Server
	lis     net.Listener
}

// NewServer 创建传输服务端
func NewServer(h Handler) *Server {
	s := &Server{handler: h}
	s.grpc = grpc.NewServer()
	s.grpc.RegisterService(&transportServiceDesc, s)
	return s
}

// Start 监听地址（如 ":9991"）并开始服务（阻塞）
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.lis = lis
	return s.grpc.Serve(lis)
}

// Addr 实际监听地址
func (s *Server) Addr() string {
	if s.lis == nil {
		return ""
	}
	return s.lis.Addr().String()
}

// GracefulStop 优雅停止
func (s *Server) GracefulStop() { s.grpc.GracefulStop() }

// ---------- 各 RPC 方法实现（委托给 Handler，错误经 error 字段回传） ----------

func (s *Server) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	n, err := s.handler.Ping(req.From)
	return &PingResponse{Node: n, Error: errStr(err)}, nil
}

func (s *Server) NodeInfo(ctx context.Context, req *NodeInfoRequest) (*NodeInfoResponse, error) {
	n, err := s.handler.Ping(NodeMeta{})
	return &NodeInfoResponse{Node: n}, err
}

func (s *Server) ShardSearch(ctx context.Context, req *ShardSearchRequest) (*ShardSearchResponse, error) {
	res, err := s.handler.ShardSearch(req.Index, req.Shard, req.Dsl, req.Partial)
	return &ShardSearchResponse{Result: res, Error: errStr(err)}, nil
}

func (s *Server) ShardDoc(ctx context.Context, req *ShardDocRequest) (*ShardDocResponse, error) {
	src, found, err := s.handler.ShardDoc(req.Index, req.ID)
	return &ShardDocResponse{Source: src, Found: found, Error: errStr(err)}, nil
}

func (s *Server) ApplyOp(ctx context.Context, req *ApplyOpRequest) (*ApplyOpResponse, error) {
	lsn, err := s.handler.ApplyOp(req.Index, req.Shard, req.Lsn, req.Op)
	return &ApplyOpResponse{Lsn: lsn, Error: errStr(err)}, nil
}

func (s *Server) ForwardWrite(ctx context.Context, req *ForwardWriteRequest) (*ForwardWriteResponse, error) {
	lsn, err := s.handler.ForwardWrite(req.Index, req.Shard, req.Op, req.WaitAll)
	return &ForwardWriteResponse{Lsn: lsn, Error: errStr(err)}, nil
}

func (s *Server) FetchTranslog(ctx context.Context, req *FetchTranslogRequest) (*FetchTranslogResponse, error) {
	ops, next, oldest, err := s.handler.FetchTranslog(req.Index, req.Shard, req.FromLsn, req.Limit, req.NodeId)
	return &FetchTranslogResponse{Ops: ops, FromLsn: req.FromLsn, NextLsn: next, OldestLsn: oldest, Error: errStr(err)}, nil
}

func (s *Server) PrepareShardRecovery(ctx context.Context, req *PrepareShardRecoveryRequest) (*PrepareShardRecoveryResponse, error) {
	lsnBase, schemaJSON, files, err := s.handler.PrepareShardRecovery(req.Index, req.Shard)
	return &PrepareShardRecoveryResponse{LsnBase: lsnBase, SchemaJson: schemaJSON, Files: files, Error: errStr(err)}, nil
}

func (s *Server) FetchShardFile(ctx context.Context, req *FetchShardFileRequest) (*FetchShardFileResponse, error) {
	data, err := s.handler.FetchShardFile(req.Index, req.Shard, req.SegDir, req.Name, req.Off, req.Limit)
	return &FetchShardFileResponse{Data: data, Error: errStr(err)}, nil
}

func (s *Server) FinishShardRecovery(ctx context.Context, req *FinishShardRecoveryRequest) (*FinishShardRecoveryResponse, error) {
	return &FinishShardRecoveryResponse{Error: errStr(s.handler.FinishShardRecovery(req.Index, req.Shard))}, nil
}

func (s *Server) ShardStatusReport(ctx context.Context, req *ShardStatusRequest) (*ShardStatusResponse, error) {
	return &ShardStatusResponse{Error: errStr(s.handler.ShardStatusReport(req.Node, req.Shards))}, nil
}

func (s *Server) UpdateMapping(ctx context.Context, req *UpdateMappingRequest) (*UpdateMappingResponse, error) {
	return &UpdateMappingResponse{Error: errStr(s.handler.ProposeMapping(req.Index, req.Mapping))}, nil
}

func (s *Server) RaftMessage(ctx context.Context, req *RaftMessageRequest) (*RaftMessageResponse, error) {
	return &RaftMessageResponse{Error: errStr(s.handler.RaftMessage(req.Data))}, nil
}

func (s *Server) JoinNode(ctx context.Context, req *JoinNodeRequest) (*JoinNodeResponse, error) {
	leader, err := s.handler.JoinNode(req.Node)
	return &JoinNodeResponse{Error: errStr(err), Leader: leader}, nil
}

func (s *Server) GetClusterState(ctx context.Context, req *GetClusterStateRequest) (*GetClusterStateResponse, error) {
	state, err := s.handler.ClusterState()
	return &GetClusterStateResponse{State: state, Error: errStr(err)}, nil
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ---------- 手写 ServiceDesc（等价于 protoc 生成代码） ----------

// unaryHandler 构造一个标准的一元方法描述（与生成代码同构，支持拦截器）
func unaryHandler[Req, Resp any](
	name string,
	method func(s *Server, ctx context.Context, req *Req) (*Resp, error),
) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(Req)
			if err := dec(req); err != nil {
				return nil, err
			}
			if interceptor != nil {
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/falcon.transport.Transport/" + name}
				return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
					return method(srv.(*Server), ctx, req.(*Req))
				})
			}
			return method(srv.(*Server), ctx, req)
		},
	}
}

// transportServiceDesc 与 proto/transport.proto 的 service 定义一一对应
var transportServiceDesc = grpc.ServiceDesc{
	ServiceName: "falcon.transport.Transport",
	HandlerType: (*transportServer)(nil),
	Methods: []grpc.MethodDesc{
		unaryHandler("Ping", (*Server).Ping),
		unaryHandler("NodeInfo", (*Server).NodeInfo),
		unaryHandler("ShardSearch", (*Server).ShardSearch),
		unaryHandler("ShardDoc", (*Server).ShardDoc),
		unaryHandler("ApplyOp", (*Server).ApplyOp),
		unaryHandler("RaftMessage", (*Server).RaftMessage),
		unaryHandler("JoinNode", (*Server).JoinNode),
		unaryHandler("ForwardWrite", (*Server).ForwardWrite),
		unaryHandler("FetchTranslog", (*Server).FetchTranslog),
		unaryHandler("PrepareShardRecovery", (*Server).PrepareShardRecovery),
		unaryHandler("FetchShardFile", (*Server).FetchShardFile),
		unaryHandler("FinishShardRecovery", (*Server).FinishShardRecovery),
		unaryHandler("ShardStatusReport", (*Server).ShardStatusReport),
		unaryHandler("UpdateMapping", (*Server).UpdateMapping),
		unaryHandler("GetClusterState", (*Server).GetClusterState),
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "transport.proto",
}
