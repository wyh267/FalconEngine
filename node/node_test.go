package node

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/FalconEngine/falcon/config"
	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/transport"
)

// testNode 测试用节点：本地数据 + gRPC 服务（不加入集群）
type testNode struct {
	*Node
	grpc *transport.Server
	addr string
}

func startTestNode(t *testing.T, name string) *testNode {
	t.Helper()
	cfg := config.Default()
	cfg.Node.Name = name
	cfg.Data.Path = filepath.Join(t.TempDir(), "data")
	cfg.HTTP.Port = 0
	cfg.GRPC.Port = 0

	mgr, err := index.OpenManager(cfg.Data.Path)
	if err != nil {
		t.Fatal(err)
	}
	nd := New(cfg, mgr, transport.NewClient())
	// 单机元信息（地址在 gRPC 监听后回填）
	nd.Meta.Name = name

	gs := transport.NewServer(nd)
	go func() { gs.Start("127.0.0.1:0") }()
	// 等待监听就绪
	deadline := time.Now().Add(5 * time.Second)
	for gs.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	nd.Meta.GRPCAddr = gs.Addr()

	tn := &testNode{Node: nd, grpc: gs, addr: gs.Addr()}
	t.Cleanup(func() {
		gs.GracefulStop()
		nd.Client.Close()
		mgr.Close()
	})
	return tn
}

func (tn *testNode) createIndex(t *testing.T, name string, numShards int) {
	t.Helper()
	sch, err := schema.New([]schema.Field{
		{Name: "content", Type: "text"},
		{Name: "level", Type: "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tn.Mgr.Create(name, index.IndexSettings{NumShards: numShards}, sch); err != nil {
		t.Fatal(err)
	}
}

// TestShardSearchLoopback 两个节点互查：Ping/ShardSearch/ShardDoc/ApplyOp
func TestShardSearchLoopback(t *testing.T) {
	a := startTestNode(t, "A")
	b := startTestNode(t, "B")

	// 双方各建一个索引并写入不同数据
	a.createIndex(t, "logs", 1)
	b.createIndex(t, "logs", 1)
	ixA, _ := a.Mgr.Get("logs")
	ixB, _ := b.Mgr.Get("logs")
	if _, err := ixA.Index("a1", []byte(`{"content":"苹果 手机","level":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := ixB.Index("b1", []byte(`{"content":"苹果 香蕉","level":2}`)); err != nil {
		t.Fatal(err)
	}

	// Ping 互认
	meta, err := a.Client.Ping(b.addr, a.Meta)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Name != "B" {
		t.Fatalf("Ping(B).Name = %q, want B", meta.Name)
	}

	// A 查 B 的分片：只命中 B 上的文档
	dsl := []byte(`{"query":{"match":{"content":"香蕉"}},"size":10}`)
	raw, err := a.Client.ShardSearch(b.addr, "logs", 0, dsl, false)
	if err != nil {
		t.Fatal(err)
	}
	var res index.Result
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 || res.Hits[0].ID != "b1" {
		t.Fatalf("ShardSearch(B) = %+v, want 仅命中 b1", res)
	}

	// B 查 A 的分片：只命中 A 上的文档
	raw, err = b.Client.ShardSearch(a.addr, "logs", 0, []byte(`{"query":{"match":{"content":"手机"}}}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 || res.Hits[0].ID != "a1" {
		t.Fatalf("ShardSearch(A) = %+v, want 仅命中 a1", res)
	}

	// ShardDoc 远端取原文
	src, found, err := a.Client.ShardDoc(b.addr, "logs", "b1")
	if err != nil || !found {
		t.Fatalf("ShardDoc = %v %v %v", src, found, err)
	}

	// ApplyOp 远端写入（5b 复制接入口，当前直接写本地分片）
	op, _ := json.Marshal(map[string]any{"type": 1, "id": "b2", "doc": json.RawMessage(`{"content":"火龙果","level":3}`)})
	lsn, err := a.Client.ApplyOp(b.addr, "logs", 0, 1, op)
	if err != nil {
		t.Fatal(err)
	}
	if lsn < 0 {
		t.Fatalf("ApplyOp lsn = %d", lsn)
	}
	raw, _ = a.Client.ShardSearch(b.addr, "logs", 0, []byte(`{"query":{"match":{"content":{"query":"火龙果","operator":"and"}}}}`), false)
	json.Unmarshal(raw, &res)
	if res.Total != 1 || res.Hits[0].ID != "b2" {
		t.Fatalf("ApplyOp 后查询 = %+v, want 命中 b2", res)
	}

	// 查询不存在的索引应返回错误
	if _, err := a.Client.ShardSearch(b.addr, "nope", 0, dsl, false); err == nil {
		t.Fatal("查询不存在索引应报错")
	}
}
