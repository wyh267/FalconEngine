package index

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/schema"
)

// TestShardingRouting 验证 murmur3 路由与多分片写入/检索归并
func TestShardingRouting(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
	})
	mgr, err := OpenManager(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ix, err := mgr.Create("sharded", IndexSettings{NumShards: 3}, sch)
	if err != nil {
		t.Fatal(err)
	}

	// 路由确定性：同一 ID 恒落同一分片
	for _, id := range []string{"a", "b", "doc-42"} {
		if ix.ShardOf(id) != ix.ShardOf(id) {
			t.Fatal("路由不确定")
		}
		if ix.ShardOf(id) < 0 || ix.ShardOf(id) >= 3 {
			t.Fatalf("分片号越界: %d", ix.ShardOf(id))
		}
	}

	// 写 30 篇文档，确认分布到多个分片
	used := map[int]int{}
	for i := 0; i < 30; i++ {
		id := fmt.Sprintf("doc-%d", i)
		used[ix.ShardOf(id)]++
		mustIndexIndex(t, ix, id, fmt.Sprintf(`{"title":"go 教程 %d","tag":"tech","level":%d}`, i, i%7))
	}
	if len(used) < 2 {
		t.Fatalf("30 篇文档只路由到 %d 个分片", len(used))
	}
	if ix.DocCount() != 30 {
		t.Fatalf("DocCount = %d, want 30", ix.DocCount())
	}

	// 落盘一半分片，制造 缓冲+段 混合态
	ix.Flush()

	// 索引级 scatter-gather：total 为全部命中
	res := mustSearchIndex(t, ix, `{"query":{"match":{"title":"go"}},"size":100}`)
	if res.Total != 30 {
		t.Fatalf("scatter-gather Total = %d, want 30", res.Total)
	}

	// 分页正确性：全局排序后截取
	res = mustSearchIndex(t, ix, `{"query":{"match":{"title":"go"}},"sort":[{"level":"asc"}],"from":5,"size":5}`)
	if len(res.Hits) != 5 {
		t.Fatalf("分页 hits = %d, want 5", len(res.Hits))
	}
	// level asc：level=0 占位置 0-4（5 篇），level=1 占位置 5-9（5 篇），
	// from=5,size=5 应全部落在 level=1
	for i, h := range res.Hits {
		var doc struct {
			Level int64 `json:"level"`
		}
		if err := jsonUnmarshal(h.Source, &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Level != 1 {
			t.Fatalf("第 %d 条 level = %d, want 1", i, doc.Level)
		}
	}

	// 聚合跨分片两段式合并
	res = mustSearchIndex(t, ix, `{"query":{"match_all":{}},"size":1,"aggs":{
		"by_tag":{"terms":{"field":"tag"}},
		"sum_level":{"sum":{"field":"level"}},
		"cards":{"cardinality":{"field":"tag"}}}}`)
	if got := string(res.Aggs["by_tag"]); got != `{"buckets":[{"key":"tech","count":30}]}` {
		t.Fatalf("terms = %s", got)
	}
	// sum(0..6 循环 30 次) = 4*21 + 0+1 = 85
	if got := string(res.Aggs["sum_level"]); got != `{"value":85}` {
		t.Fatalf("sum = %s, want 85", got)
	}
	if got := string(res.Aggs["cards"]); got != `{"value":1}` {
		t.Fatalf("cardinality = %s, want 1", got)
	}

	// Get/Delete 按路由工作
	if _, found, err := ix.Get("doc-7"); err != nil || !found {
		t.Fatalf("Get 失败: %v %v", found, err)
	}
	if ok, _, err := ix.Delete("doc-7"); err != nil || !ok {
		t.Fatalf("Delete 失败: %v %v", ok, err)
	}
	if _, found, _ := ix.Get("doc-7"); found {
		t.Fatal("删除后仍可读到")
	}
	if ix.DocCount() != 29 {
		t.Fatalf("删除后 DocCount = %d, want 29", ix.DocCount())
	}
}

// TestShardReopen 重启后多分片索引完整恢复
func TestShardReopen(t *testing.T) {
	sch, _ := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	dir := filepath.Join(t.TempDir(), "data")

	mgr, _ := OpenManager(dir)
	ix, err := mgr.Create("multi", IndexSettings{NumShards: 4}, sch)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustIndexIndex(t, ix, fmt.Sprintf("d%d", i), `{"title":"恢复 测试"}`)
	}
	ix.Flush()
	mgr.Close()

	// 重开：meta + 分片目录扫描恢复
	mgr2, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()
	ix2, ok := mgr2.Get("multi")
	if !ok {
		t.Fatal("重启后索引丢失")
	}
	if ix2.NumShards() != 4 {
		t.Fatalf("重启后分片数 = %d, want 4", ix2.NumShards())
	}
	res := mustSearchIndex(t, ix2, `{"query":{"match":{"title":"恢复"}},"size":100}`)
	if res.Total != 10 {
		t.Fatalf("重启后 Total = %d, want 10", res.Total)
	}
}

func mustIndexIndex(t *testing.T, ix *Index, id, doc string) {
	t.Helper()
	if _, err := ix.Index(id, []byte(doc)); err != nil {
		t.Fatalf("Index(%s) error: %v", id, err)
	}
}

func mustSearchIndex(t *testing.T, ix *Index, dsl string) *Result {
	t.Helper()
	res, err := ix.SearchDSL([]byte(dsl))
	if err != nil {
		t.Fatalf("SearchDSL(%s) error: %v", dsl, err)
	}
	return res
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
