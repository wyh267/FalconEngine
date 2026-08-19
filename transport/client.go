package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client 节点间通信客户端（带连接池：同一地址复用一条连接）
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewClient 创建客户端
func NewClient() *Client {
	return &Client{conns: map[string]*grpc.ClientConn{}}
}

// conn 取到指定地址的连接（不存在则建立）
func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(CodecName)),
	)
	if err != nil {
		return nil, fmt.Errorf("transport: 连接 %s 失败: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// Close 关闭全部连接
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}

// callCtx 默认 10s 超时的调用上下文
func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// invoke 通用一元调用
func invoke[Req, Resp any](c *Client, addr, method string, req *Req) (*Resp, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := callCtx()
	defer cancel()
	resp := new(Resp)
	err = conn.Invoke(ctx, "/falcon.transport.Transport/"+method, req, resp)
	return resp, err
}

// Ping 探活并交换节点元信息
func (c *Client) Ping(addr string, from NodeMeta) (NodeMeta, error) {
	resp, err := invoke[PingRequest, PingResponse](c, addr, "Ping", &PingRequest{From: from})
	if err != nil {
		return NodeMeta{}, err
	}
	if resp.Error != "" {
		return NodeMeta{}, fmt.Errorf("%s", resp.Error)
	}
	return resp.Node, nil
}

// NodeInfo 取远端节点元信息
func (c *Client) NodeInfo(addr string) (NodeMeta, error) {
	resp, err := invoke[NodeInfoRequest, NodeInfoResponse](c, addr, "NodeInfo", &NodeInfoRequest{})
	if err != nil {
		return NodeMeta{}, err
	}
	return resp.Node, nil
}

// ShardSearch 远端分片查询，返回序列化的 index.Result
func (c *Client) ShardSearch(addr, indexName string, shard int32, dsl []byte, partial bool) ([]byte, error) {
	resp, err := invoke[ShardSearchRequest, ShardSearchResponse](c, addr, "ShardSearch",
		&ShardSearchRequest{Index: indexName, Shard: shard, Dsl: dsl, Partial: partial})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.Result, nil
}

// ShardDoc 远端取文档
func (c *Client) ShardDoc(addr, indexName, id string) ([]byte, bool, error) {
	resp, err := invoke[ShardDocRequest, ShardDocResponse](c, addr, "ShardDoc",
		&ShardDocRequest{Index: indexName, ID: id})
	if err != nil {
		return nil, false, err
	}
	if resp.Error != "" {
		return nil, false, fmt.Errorf("%s", resp.Error)
	}
	return resp.Source, resp.Found, nil
}

// ApplyOp 向远端副本推送一条复制操作（带 primary 侧 LSN），返回副本侧 LSN
func (c *Client) ApplyOp(addr, indexName string, shard int32, lsn int64, op []byte) (int64, error) {
	resp, err := invoke[ApplyOpRequest, ApplyOpResponse](c, addr, "ApplyOp",
		&ApplyOpRequest{Index: indexName, Shard: shard, Lsn: lsn, Op: op})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}
	return resp.Lsn, nil
}

// ForwardWrite 转发写请求到远端 primary，返回 LSN
func (c *Client) ForwardWrite(addr, indexName string, shard int32, op []byte, waitAll bool) (int64, error) {
	resp, err := invoke[ForwardWriteRequest, ForwardWriteResponse](c, addr, "ForwardWrite",
		&ForwardWriteRequest{Index: indexName, Shard: shard, Op: op, WaitAll: waitAll})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}
	return resp.Lsn, nil
}

// FetchTranslog 从远端 primary 批量拉取 translog
func (c *Client) FetchTranslog(addr, indexName string, shard int32, fromLSN int64, limit int32) ([][]byte, int64, error) {
	resp, err := invoke[FetchTranslogRequest, FetchTranslogResponse](c, addr, "FetchTranslog",
		&FetchTranslogRequest{Index: indexName, Shard: shard, FromLsn: fromLSN, Limit: limit})
	if err != nil {
		return nil, 0, err
	}
	if resp.Error != "" {
		return nil, 0, fmt.Errorf("%s", resp.Error)
	}
	return resp.Ops, resp.NextLsn, nil
}

// ShardStatusReport 上报心跳与分片状态
func (c *Client) ShardStatusReport(addr string, node NodeMeta, shards []ShardStatusItem) error {
	resp, err := invoke[ShardStatusRequest, ShardStatusResponse](c, addr, "ShardStatusReport",
		&ShardStatusRequest{Node: node, Shards: shards})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// SendRaftMessage 向远端节点投递 raft 消息
func (c *Client) SendRaftMessage(addr string, data []byte) error {
	resp, err := invoke[RaftMessageRequest, RaftMessageResponse](c, addr, "RaftMessage",
		&RaftMessageRequest{Data: data})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// JoinNode 向 master leader 注册本节点，返回 leader 元信息
func (c *Client) JoinNode(addr string, node NodeMeta) (NodeMeta, error) {
	resp, err := invoke[JoinNodeRequest, JoinNodeResponse](c, addr, "JoinNode",
		&JoinNodeRequest{Node: node})
	if err != nil {
		return NodeMeta{}, err
	}
	if resp.Error != "" {
		return NodeMeta{}, fmt.Errorf("%s", resp.Error)
	}
	return resp.Leader, nil
}

// ClusterState 拉取远端集群状态
func (c *Client) ClusterState(addr string) ([]byte, error) {
	resp, err := invoke[GetClusterStateRequest, GetClusterStateResponse](c, addr, "GetClusterState",
		&GetClusterStateRequest{})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.State, nil
}
