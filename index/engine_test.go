package index

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/schema"
)

func testSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
		{Name: "extra", Type: "stored"},
	})
	if err != nil {
		t.Fatalf("schema.New error: %v", err)
	}
	return s
}

func openTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "weibo")
	e, err := Open(dir, testSchema(t))
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return e, dir
}

func mustIndex(t *testing.T, e *Engine, id, doc string) {
	t.Helper()
	if _, err := e.Index(id, json.RawMessage(doc)); err != nil {
		t.Fatalf("Index(%s) error: %v", id, err)
	}
}

// searchDSL 执行 DSL 查询并断言无错误
func searchDSL(t *testing.T, e *Engine, dsl string) *Result {
	t.Helper()
	res, err := e.SearchDSL([]byte(dsl))
	if err != nil {
		t.Fatalf("SearchDSL(%s) error: %v", dsl, err)
	}
	return res
}

func hitIDs(res *Result) []string {
	ids := make([]string, 0, len(res.Hits))
	for _, h := range res.Hits {
		ids = append(ids, h.ID)
	}
	return ids
}

func TestIndexAndSearch(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"go 语言 入门 教程","tag":"tech","level":3,"extra":"x"}`)
	mustIndex(t, e, "2", `{"title":"go 进阶 教程","tag":"tech","level":5}`)
	mustIndex(t, e, "3", `{"title":"烹饪 教程","tag":"life","level":1}`)

	// 全文检索：教程 AND go
	res := searchDSL(t, e, `{"query":{"match":{"title":{"query":"go 教程","operator":"and"}}}}`)
	if res.Total != 2 {
		t.Fatalf("Total = %d, want 2, hits=%v", res.Total, hitIDs(res))
	}

	// keyword 精确匹配
	res = searchDSL(t, e, `{"query":{"term":{"tag":"tech"}}}`)
	if res.Total != 2 {
		t.Fatalf("keyword Total = %d, want 2", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"term":{"tag":"te"}}}`)
	if res.Total != 0 {
		t.Fatalf("keyword 前缀不应命中, Total = %d", res.Total)
	}

	// 数字过滤
	res = searchDSL(t, e, `{"query":{"bool":{
		"must":[{"match":{"title":"教程"}}],
		"filter":[{"range":{"level":{"gte":4,"lte":10}}}]}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("过滤结果 = %v, want [2]", hitIDs(res))
	}

	// match all + 分页
	res = searchDSL(t, e, `{"size":2}`)
	if res.Total != 3 || len(res.Hits) != 2 {
		t.Fatalf("match all: Total=%d len=%d, want 3/2", res.Total, len(res.Hits))
	}
	res = searchDSL(t, e, `{"from":2,"size":2}`)
	if len(res.Hits) != 1 {
		t.Fatalf("分页第二页 len=%d, want 1", len(res.Hits))
	}

	// 原文带 unknown 字段也应原样返回
	mustIndex(t, e, "4", `{"title":"unknown","unknown_field":123}`)
	res = searchDSL(t, e, `{"query":{"match":{"title":"unknown"}}}`)
	if res.Total != 1 || string(res.Hits[0].Source) != `{"title":"unknown","unknown_field":123}` {
		t.Fatalf("source = %s", res.Hits[0].Source)
	}
}

func TestUpdateAndDelete(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"旧 标题","tag":"a","level":1}`)
	mustIndex(t, e, "1", `{"title":"新 标题","tag":"b","level":2}`)

	if n := e.DocCount(); n != 1 {
		t.Fatalf("DocCount = %d, want 1", n)
	}
	res := searchDSL(t, e, `{"query":{"term":{"tag":"a"}}}`)
	if res.Total != 0 {
		t.Fatalf("旧版本仍可搜到, Total = %d", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"term":{"tag":"b"}}}`)
	if res.Total != 1 {
		t.Fatalf("新版本搜不到")
	}

	ok, _, err := e.Delete("1")
	if err != nil || !ok {
		t.Fatalf("Delete = %v, %v", ok, err)
	}
	if n := e.DocCount(); n != 0 {
		t.Fatalf("删除后 DocCount = %d, want 0", n)
	}
	ok, _, _ = e.Delete("1")
	if ok {
		t.Fatal("重复删除应返回 false")
	}
	ok, _, _ = e.Delete("不存在")
	if ok {
		t.Fatal("删除不存在文档应返回 false")
	}
}

func TestFlushAndReopen(t *testing.T) {
	e, dir := openTestEngine(t)

	mustIndex(t, e, "1", `{"title":"go 语言","tag":"tech","level":3}`)
	mustIndex(t, e, "2", `{"title":"go 进阶","tag":"tech","level":5}`)
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	// flush 后继续写
	mustIndex(t, e, "3", `{"title":"go 实战","tag":"tech","level":4}`)
	// 删除段内文档 + 更新（段内 + 缓冲混合）
	if ok, _, _ := e.Delete("1"); !ok {
		t.Fatal("Delete(1) failed")
	}
	mustIndex(t, e, "2", `{"title":"go 进阶 第二版","tag":"tech","level":6}`)

	// flush 后检索：段 + 缓冲合并
	res := searchDSL(t, e, `{"query":{"match":{"title":"go"}}}`)
	if res.Total != 2 {
		t.Fatalf("flush 后 Total = %d, want 2, hits=%v", res.Total, hitIDs(res))
	}

	// 不 flush 直接关闭，模拟重启后 translog 回放
	if err := e.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer e2.Close()

	res = searchDSL(t, e2, `{"query":{"match":{"title":"go"}}}`)
	if res.Total != 2 {
		t.Fatalf("reopen 后 Total = %d, want 2, hits=%v", res.Total, hitIDs(res))
	}
	// doc1 的删除、doc2 的更新都应在回放后生效
	res = searchDSL(t, e2, `{"query":{"match":{"title":"语言"}}}`)
	if res.Total != 0 {
		t.Fatalf("doc1 应已删除, Total = %d", res.Total)
	}
	res = searchDSL(t, e2, `{"query":{"match":{"title":{"query":"第二版","operator":"and"}}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("doc2 更新未生效, hits=%v", hitIDs(res))
	}
	// 更新后的 level 过滤
	res = searchDSL(t, e2, `{"query":{"bool":{
		"must":[{"match":{"title":"go"}}],
		"filter":[{"term":{"level":6}}]}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("level=6 过滤结果 = %v, want [2]", hitIDs(res))
	}
}

func TestFlushSkipsDeletedBufferDocs(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	// 缓冲内先写再更新再删除，旧版本不应被 flush 进段
	mustIndex(t, e, "1", `{"title":"旧 版本","tag":"a","level":1}`)
	mustIndex(t, e, "1", `{"title":"新 版本","tag":"a","level":1}`)
	mustIndex(t, e, "2", `{"title":"将被 删除","tag":"a","level":1}`)
	if ok, _, _ := e.Delete("2"); !ok {
		t.Fatal("Delete(2) failed")
	}
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}

	if n := e.DocCount(); n != 1 {
		t.Fatalf("DocCount = %d, want 1", n)
	}
	res := searchDSL(t, e, `{"query":{"term":{"tag":"a"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("Total=%d hits=%v, want [1]", res.Total, hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"match":{"title":"旧"}}}`)
	if res.Total != 0 {
		t.Fatalf("旧版本不应存在于段中, Total = %d", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"match":{"title":"删除"}}}`)
	if res.Total != 0 {
		t.Fatalf("已删除文档不应存在于段中, Total = %d", res.Total)
	}
}

func TestReopenAfterSecondFlush(t *testing.T) {
	e, dir := openTestEngine(t)
	mustIndex(t, e, "1", `{"title":"甲","tag":"a","level":1}`)
	e.Flush()
	mustIndex(t, e, "2", `{"title":"乙","tag":"b","level":2}`)
	e.Flush()
	e.Close()

	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer e2.Close()
	res := searchDSL(t, e2, `{}`)
	if res.Total != 2 {
		t.Fatalf("Total = %d, want 2", res.Total)
	}
}
