package index

import (
	"encoding/json"
	"fmt"
	"os"
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

// TestBufferRealtimeVisibility 契约测试：内存缓冲实时可搜——
// 写入后不 Flush 即可被 match/term/range/sort/agg 命中；
// 删除后不 Flush 立即不可见。该契约是 _refresh 轻量化的前提（见 api handleRefresh）。
func TestBufferRealtimeVisibility(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"go 语言 教程","tag":"tech","level":3}`)
	mustIndex(t, e, "2", `{"title":"go 进阶 教程","tag":"tech","level":5}`)
	mustIndex(t, e, "3", `{"title":"烹饪 教程","tag":"life","level":1}`)
	if n := e.segCount(); n != 0 {
		t.Fatalf("未 Flush 不应产生段, segs=%d", n)
	}

	// match 命中缓冲
	res := searchDSL(t, e, `{"query":{"match":{"title":"go"}}}`)
	if res.Total != 2 {
		t.Fatalf("match Total = %d, want 2", res.Total)
	}
	// term 命中缓冲
	res = searchDSL(t, e, `{"query":{"term":{"tag":"tech"}}}`)
	if res.Total != 2 {
		t.Fatalf("term Total = %d, want 2", res.Total)
	}
	// range 命中缓冲
	res = searchDSL(t, e, `{"query":{"range":{"level":{"gte":3}}}}`)
	if res.Total != 2 {
		t.Fatalf("range Total = %d, want 2", res.Total)
	}
	// sort 使用缓冲内排序键（level 降序应为 2,1,3）
	res = searchDSL(t, e, `{"sort":[{"level":"desc"}],"size":3}`)
	if ids := hitIDs(res); len(ids) != 3 || ids[0] != "2" || ids[1] != "1" || ids[2] != "3" {
		t.Fatalf("sort 顺序 = %v, want [2 1 3]", ids)
	}
	// agg 命中缓冲（terms：tech=2, life=1）
	res = searchDSL(t, e, `{"size":0,"aggs":{"by_tag":{"terms":{"field":"tag"}}}}`)
	var aggOut struct {
		Buckets []struct {
			Key   string `json:"key"`
			Count int64  `json:"count"`
		} `json:"buckets"`
	}
	if err := json.Unmarshal(res.Aggs["by_tag"], &aggOut); err != nil {
		t.Fatalf("解析聚合结果失败: %v", err)
	}
	if len(aggOut.Buckets) != 2 || aggOut.Buckets[0].Key != "tech" || aggOut.Buckets[0].Count != 2 ||
		aggOut.Buckets[1].Key != "life" || aggOut.Buckets[1].Count != 1 {
		t.Fatalf("聚合 buckets = %s, want tech:2,life:1", res.Aggs["by_tag"])
	}

	// 删除后不 Flush 立即不可见（各类查询路径一致）
	if ok, _, _ := e.Delete("2"); !ok {
		t.Fatal("Delete(2) failed")
	}
	if n := e.DocCount(); n != 2 {
		t.Fatalf("删除后 DocCount = %d, want 2", n)
	}
	res = searchDSL(t, e, `{"query":{"term":{"tag":"tech"}}}`)
	if res.Total != 1 {
		t.Fatalf("删除后 term Total = %d, want 1", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"match":{"title":"go"}}}`)
	if res.Total != 1 {
		t.Fatalf("删除后 match Total = %d, want 1", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"range":{"level":{"gte":3}}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("删除后 range 结果 = %v, want [1]", hitIDs(res))
	}
}

// TestBufferAutoFlush 缓冲达到 maxBufferDocs 阈值时写入侧自动落盘：
// 产段数、DocCount 正确；重启后（段 + translog 回放）数据完整
func TestBufferAutoFlush(t *testing.T) {
	e, dir := openTestEngine(t)
	e.maxBufferDocs = 3 // 调小阈值便于测试

	// 写 7 篇：第 3、6 篇触发自动落盘 → 2 段 + 缓冲 1 篇
	for i := 1; i <= 7; i++ {
		mustIndex(t, e, fmt.Sprintf("%d", i),
			fmt.Sprintf(`{"title":"自动 落盘 %d","tag":"auto","level":%d}`, i, i))
	}
	if n := e.segCount(); n != 2 {
		t.Fatalf("自动落盘段数 = %d, want 2", n)
	}
	if n := e.DocCount(); n != 7 {
		t.Fatalf("DocCount = %d, want 7", n)
	}
	// 阈值触发后查询立即可见（段 + 缓冲合并）
	res := searchDSL(t, e, `{"query":{"term":{"tag":"auto"}}}`)
	if res.Total != 7 {
		t.Fatalf("Total = %d, want 7", res.Total)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// 重启：2 段加载 + 缓冲内 1 篇经 translog 回放
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer e2.Close()
	if n := e2.DocCount(); n != 7 {
		t.Fatalf("reopen 后 DocCount = %d, want 7", n)
	}
	res = searchDSL(t, e2, `{"query":{"match":{"title":"落盘"}}}`)
	if res.Total != 7 {
		t.Fatalf("reopen 后 Total = %d, want 7", res.Total)
	}
}

// TestTranslogRetentionWindow translog 保留窗口：
// 默认只留当前代际；SetRetentionLSN 设置地板后历史代际保留、可跨代际连读；
// 地板推进后 Flush 清除出窗代际；manifest 重启后仍在
func TestTranslogRetentionWindow(t *testing.T) {
	e, dir := openTestEngine(t)
	indexN := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			mustIndex(t, e, fmt.Sprintf("%s-%d", prefix, i),
				fmt.Sprintf(`{"title":"%s 文档 %d","level":%d}`, prefix, i, i))
		}
	}

	// 默认（未设地板）：Flush 后只留当前代际
	indexN("a", 3) // LSN 0-2
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := e.OldestLSN(); got != 3 {
		t.Fatalf("默认保留 OldestLSN = %d, want 3（只留当前代际）", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "translog-1.log")); !os.IsNotExist(err) {
		t.Fatal("默认保留下 gen1 应被清除")
	}

	// 地板设为 0：历史代际全保留
	e.SetRetentionLSN(0)
	indexN("b", 2) // LSN 3-4
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := e.OldestLSN(); got != 3 {
		t.Fatalf("地板=0 OldestLSN = %d, want 3", got)
	}
	// 地板设为 -1：全量保留，跨代际连读
	e.SetRetentionLSN(-1)
	indexN("c", 3) // LSN 5-7
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	ops, next, err := e.ReadTranslog(3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 5 || next != 8 {
		t.Fatalf("跨代际连读 ops=%d next=%d, want 5/8", len(ops), next)
	}

	// 地板推进到 5：endLSN <= 5 的代际（gen2: LSN 3-4）被清除
	e.SetRetentionLSN(5)
	indexN("d", 1) // LSN 8
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := e.OldestLSN(); got != 5 {
		t.Fatalf("地板=5 OldestLSN = %d, want 5", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "translog-2.log")); !os.IsNotExist(err) {
		t.Fatal("gen2 应被清除出窗")
	}
	// 出窗读取：空序列 + 当前末尾 LSN（副本据此判定落后出窗）
	ops, next, err = e.ReadTranslog(3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 || next != 9 {
		t.Fatalf("出窗读取 ops=%d next=%d, want 0/9", len(ops), next)
	}
	ops, next, err = e.ReadTranslog(5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 4 || next != 9 {
		t.Fatalf("窗口内连读 ops=%d next=%d, want 4/9", len(ops), next)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：manifest 保留，窗口与跨代际连读不变
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if got := e2.OldestLSN(); got != 5 {
		t.Fatalf("重启后 OldestLSN = %d, want 5", got)
	}
	ops, _, err = e2.ReadTranslog(5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 4 {
		t.Fatalf("重启后跨代际连读 ops=%d, want 4", len(ops))
	}
}

// TestRecoveryRoundTrip 段拷贝恢复跨目录 round-trip：
// 含多次 Flush 后的新旧段混合 + 缓冲内未落盘数据（经 translog 差量补齐）+
// 已删除文档（del.bin 前缀拷贝）；恢复期间 Flush 报错、Finish 后恢复
func TestRecoveryRoundTrip(t *testing.T) {
	sch := testSchema(t)
	srcDir := filepath.Join(t.TempDir(), "src")
	src, err := OpenIndex(srcDir, "weibo", 1, 0, true, sch, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 第一批落盘；第二批 + 删除落盘；第三批留缓冲
	for i := 0; i < 10; i++ {
		if _, err := src.Index(fmt.Sprintf("a-%d", i), json.RawMessage(fmt.Sprintf(`{"title":"批次一 文档 %d","tag":"x","level":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := src.Index(fmt.Sprintf("b-%d", i), json.RawMessage(fmt.Sprintf(`{"title":"批次二 文档 %d","tag":"y","level":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := src.Delete("a-0"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := src.Delete("a-1"); err != nil {
		t.Fatal(err)
	}
	if err := src.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := src.Index(fmt.Sprintf("c-%d", i), json.RawMessage(fmt.Sprintf(`{"title":"批次三 文档 %d","tag":"z","level":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}

	rp, err := src.PrepareShardRecovery(0)
	if err != nil {
		t.Fatal(err)
	}
	if rp.LSNBase != 22 { // 10 + 10 + 2 删除
		t.Fatalf("LSNBase = %d, want 22", rp.LSNBase)
	}
	// 恢复窗口内：Flush/Merge 被冻结报错；窗口外文件读取被拒
	if err := src.Flush(); err == nil {
		t.Fatal("恢复期间 Flush 应报错")
	}
	if err := src.shards[0].Merge(); err == nil {
		t.Fatal("恢复期间 Merge 应报错")
	}
	if _, err := src.ReadShardFile(0, "..", "schema.json", 0, 64); err == nil {
		t.Fatal("快照外文件读取应报错")
	}
	// 恢复窗口内写入不阻塞（差量经 translog 补齐）
	if _, err := src.Index("d-0", json.RawMessage(`{"title":"窗口期 文档","tag":"w","level":99}`)); err != nil {
		t.Fatal(err)
	}

	// 目标索引：带一篇陈旧文档（恢复后应被抹掉）
	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := OpenIndex(dstDir, "weibo", 1, 0, true, sch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Index("stale-1", json.RawMessage(`{"title":"陈旧 文档","level":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := dst.Flush(); err != nil {
		t.Fatal(err)
	}

	fetch := func(fi FileInfo, off, limit int64) ([]byte, error) {
		return src.ReadShardFile(0, fi.SegDir, fi.Name, off, limit)
	}
	if err := dst.RecoverShard(0, rp, fetch); err != nil {
		t.Fatalf("RecoverShard error: %v", err)
	}
	// 重建后：LSN 基线 = LSNBase-1；陈旧数据被抹掉；del.bin 前缀拷贝生效
	if lsn, _ := dst.ShardLSN(0); lsn != rp.LSNBase-1 {
		t.Fatalf("重建后 ShardLSN = %d, want %d", lsn, rp.LSNBase-1)
	}
	if _, found, _ := dst.Get("stale-1"); found {
		t.Fatal("陈旧文档应被恢复抹掉")
	}
	if _, found, _ := dst.Get("a-0"); found {
		t.Fatal("a-0 已删除（del.bin 前缀拷贝），不应可见")
	}
	// 缓冲内文档尚未补齐（等 translog 差量）
	if _, found, _ := dst.Get("c-0"); found {
		t.Fatal("c-0 在缓冲内（未落盘），段拷贝后不应可见")
	}

	// 模拟拉取循环补差量：[LSNBase, 最新]
	ops, _, err := src.ReadShardTranslog(0, rp.LSNBase, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i, op := range ops {
		if _, err := dst.ApplyOp(0, rp.LSNBase+int64(i), op); err != nil {
			t.Fatalf("差量重放失败: %v", err)
		}
	}

	// 逐条一致（含窗口期写入的 d-0）
	ids := []string{"a-2", "a-5", "a-9", "b-0", "b-9", "c-0", "c-4", "d-0"}
	for _, id := range ids {
		sraw, sfound, err := src.Get(id)
		if err != nil || !sfound {
			t.Fatalf("源端 %s 应存在: found=%v err=%v", id, sfound, err)
		}
		draw, dfound, err := dst.Get(id)
		if err != nil || !dfound {
			t.Fatalf("目标端 %s 应存在: found=%v err=%v", id, dfound, err)
		}
		if string(sraw) != string(draw) {
			t.Fatalf("%s 内容不一致: src=%s dst=%s", id, sraw, draw)
		}
	}
	if n, m := src.DocCount(), dst.DocCount(); n != m {
		t.Fatalf("DocCount 不一致: src=%d dst=%d", n, m)
	}

	// Finish 后 Flush 恢复可用
	src.FinishShardRecovery(0)
	if err := src.Flush(); err != nil {
		t.Fatalf("FinishRecovery 后 Flush 应可用: %v", err)
	}
}

// TestRecoverShardRollback 段拷贝拉取中途失败：回滚旧目录，本地数据不丢
func TestRecoverShardRollback(t *testing.T) {
	sch := testSchema(t)
	srcDir := filepath.Join(t.TempDir(), "src")
	src, err := OpenIndex(srcDir, "weibo", 1, 0, true, sch, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := src.Index(fmt.Sprintf("s-%d", i), json.RawMessage(fmt.Sprintf(`{"title":"源 文档 %d","level":%d}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Flush(); err != nil {
		t.Fatal(err)
	}
	rp, err := src.PrepareShardRecovery(0)
	if err != nil {
		t.Fatal(err)
	}
	defer src.FinishShardRecovery(0)

	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := OpenIndex(dstDir, "weibo", 1, 0, true, sch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Index("keep-1", json.RawMessage(`{"title":"本地 文档","level":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := dst.Flush(); err != nil {
		t.Fatal(err)
	}

	// 第 2 个文件起拉取失败
	calls := 0
	fetch := func(fi FileInfo, off, limit int64) ([]byte, error) {
		calls++
		if calls > 1 {
			return nil, fmt.Errorf("模拟网络中断")
		}
		return src.ReadShardFile(0, fi.SegDir, fi.Name, off, limit)
	}
	if err := dst.RecoverShard(0, rp, fetch); err == nil {
		t.Fatal("拉取失败时 RecoverShard 应报错")
	}
	// 回滚：旧数据仍在
	if _, found, _ := dst.Get("keep-1"); !found {
		t.Fatal("回滚后本地文档应保留")
	}
	if lsn, _ := dst.ShardLSN(0); lsn != 0 {
		t.Fatalf("回滚后 ShardLSN = %d, want 0", lsn)
	}
	// 引擎仍可用
	if _, err := dst.Index("keep-2", json.RawMessage(`{"title":"回滚后 写入","level":2}`)); err != nil {
		t.Fatalf("回滚后写入应可用: %v", err)
	}
}

// ---------- 阶段 6：schema/mapping 体系 ----------

// openDynamicEngine 打开纯动态 mapping 引擎（空 schema）
func openDynamicEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "dyn")
	s, err := schema.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(dir, s)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return e, dir
}

// TestNestedObjectMapping 嵌套对象展开为点路径：写入 {"user":{"name":...}} 后
// match 查询 user.name 命中；multi-fields 子字段 user.name.keyword 精确命中整串
func TestNestedObjectMapping(t *testing.T) {
	e, dir := openDynamicEngine(t)
	defer e.Close()

	mustIndex(t, e, "1", `{"user":{"name":"张三","age":30}}`)
	mustIndex(t, e, "2", `{"user":{"name":"李四","age":20},"org":{"name":"雅礼"}}`)

	// 点路径字段已动态推断
	if _, ok := e.Schema().Field("user.name"); !ok {
		t.Fatal("user.name 未推断进 schema")
	}
	if _, ok := e.Schema().Field("user.name.keyword"); !ok {
		t.Fatal("user.name.keyword 子字段未推断进 schema")
	}

	// match 查询嵌套字段（缓冲内实时可见）
	res := searchDSL(t, e, `{"query":{"match":{"user.name":"张三"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("match user.name = %v, want [1]", hitIDs(res))
	}
	// 推断的 text 带 keyword 子字段：term 精确匹配整串
	res = searchDSL(t, e, `{"query":{"term":{"user.name.keyword":"李四"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("term user.name.keyword = %v, want [2]", hitIDs(res))
	}
	// 嵌套数字字段 range
	res = searchDSL(t, e, `{"query":{"range":{"user.age":{"gte":25}}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("range user.age = %v, want [1]", hitIDs(res))
	}

	// 落盘后（段路径）结果一致
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	res = searchDSL(t, e, `{"query":{"match":{"user.name":"张三"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("flush 后 match user.name = %v, want [1]", hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"term":{"user.name.keyword":"李四"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("flush 后 term user.name.keyword = %v, want [2]", hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"term":{"org.name.keyword":"雅礼"}}}`)
	if res.Total != 1 {
		t.Fatalf("二层嵌套 org.name = %v, want 1", hitIDs(res))
	}

	// 重启后点路径字段（含子字段）从 schema.json 回读
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if _, ok := e2.Schema().Field("user.name"); !ok {
		t.Fatal("重启后 user.name 字段丢失")
	}
	if _, ok := e2.Schema().Field("user.name.keyword"); !ok {
		t.Fatal("重启后 user.name.keyword 子字段丢失")
	}
	res = searchDSL(t, e2, `{"query":{"term":{"user.name.keyword":"李四"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "2" {
		t.Fatalf("重启后 term user.name.keyword = %v, want [2]", hitIDs(res))
	}
}

// TestMultiFieldsSearch 显式 multi-fields：title(text) + title.keyword(keyword)
func TestMultiFieldsSearch(t *testing.T) {
	s, err := schema.New([]schema.Field{
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "mf"), s)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"Hello World 你好"}`)
	mustIndex(t, e, "2", `{"title":"hello 世界"}`)

	// text 分词匹配
	res := searchDSL(t, e, `{"query":{"match":{"title":"hello"}}}`)
	if res.Total != 2 {
		t.Fatalf("match title = %v, want 2", hitIDs(res))
	}
	// keyword 子字段整串精确匹配（含空格与大小写）
	res = searchDSL(t, e, `{"query":{"term":{"title.keyword":"Hello World 你好"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("term title.keyword = %v, want [1]", hitIDs(res))
	}
	// 整串不匹配（大小写不同）
	res = searchDSL(t, e, `{"query":{"term":{"title.keyword":"hello world 你好"}}}`)
	if res.Total != 0 {
		t.Fatalf("整串不同不应命中, got %v", hitIDs(res))
	}

	// 落盘后一致
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	res = searchDSL(t, e, `{"query":{"term":{"title.keyword":"Hello World 你好"}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("flush 后 term title.keyword = %v, want [1]", hitIDs(res))
	}
}

// TestDateDetectionInfer 动态推断的日期检测开关：
// 开启（默认）时日期串推断为 date；关闭时推断为 text
func TestDateDetectionInfer(t *testing.T) {
	e, _ := openDynamicEngine(t)
	defer e.Close()

	mustIndex(t, e, "1", `{"d":"2024-01-01","s":"abc"}`)
	f, ok := e.Schema().Field("d")
	if !ok || f.Type != "date" {
		t.Fatalf("字段 d = %+v, want date", f)
	}
	f, ok = e.Schema().Field("s")
	if !ok || f.Type != "text" {
		t.Fatalf("字段 s = %+v, want text", f)
	}
	if _, ok := e.Schema().Field("s.keyword"); !ok {
		t.Fatal("text 推断应带 keyword 子字段")
	}
	// date 字段 range 可查
	res := searchDSL(t, e, `{"query":{"range":{"d":{"gte":"2023-06-01","lte":"2024-06-01"}}}}`)
	if res.Total != 1 {
		t.Fatalf("range d = %v, want 1", hitIDs(res))
	}

	// date_detection=false：日期串推断为 text
	dir := filepath.Join(t.TempDir(), "nodd")
	s, err := schema.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := open(dir, s, false)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	mustIndex(t, e2, "1", `{"d":"2024-01-01"}`)
	if f, ok := e2.Schema().Field("d"); !ok || f.Type != "text" {
		t.Fatalf("date_detection=false 时字段 d = %+v, want text", f)
	}
	res = searchDSL(t, e2, `{"query":{"match":{"d":"2024"}}}`)
	if res.Total != 1 {
		t.Fatalf("text 字段 match 应命中, got %v", hitIDs(res))
	}
}

// TestFieldAnalyzerOverride 字段级 analyzer 覆盖类型默认分词器（写入与查询两侧一致）
func TestFieldAnalyzerOverride(t *testing.T) {
	s, err := schema.New([]schema.Field{
		{Name: "t", Type: "text", Analyzer: "whitespace"},
	})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "ana"), s)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"t":"foo-bar baz"}`)
	// whitespace 分词器：foo-bar 是整词，match foo 不命中
	res := searchDSL(t, e, `{"query":{"match":{"t":"foo"}}}`)
	if res.Total != 0 {
		t.Fatalf("whitespace 下 foo 不应命中, got %v", hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"match":{"t":"foo-bar"}}}`)
	if res.Total != 1 {
		t.Fatalf("whitespace 下 foo-bar 应命中, got %v", hitIDs(res))
	}
}

// TestEngineUpdateMapping 引擎侧显式 mapping 更新：
// 新字段可查、冲突报错、幂等、子字段追加、重启后仍在
func TestEngineUpdateMapping(t *testing.T) {
	e, dir := openTestEngine(t)
	defer e.Close()

	// 新字段 + 已有字段的新子字段
	err := e.UpdateMapping([]schema.Field{
		{Name: "city", Type: "keyword"},
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
	})
	if err != nil {
		t.Fatalf("UpdateMapping error: %v", err)
	}
	// 幂等
	if err := e.UpdateMapping([]schema.Field{{Name: "city", Type: "keyword"}}); err != nil {
		t.Fatalf("幂等更新应通过: %v", err)
	}
	// 冲突
	if err := e.UpdateMapping([]schema.Field{{Name: "level", Type: "keyword"}}); err == nil {
		t.Fatal("修改已有字段类型应报错")
	}

	mustIndex(t, e, "1", `{"title":"北京 欢迎你","city":"北京","level":3}`)
	res := searchDSL(t, e, `{"query":{"term":{"city":"北京"}}}`)
	if res.Total != 1 {
		t.Fatalf("term city = %v, want 1", hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"term":{"title.keyword":"北京 欢迎你"}}}`)
	if res.Total != 1 {
		t.Fatalf("term title.keyword = %v, want 1", hitIDs(res))
	}

	// 重启后 mapping 仍在（schema.json 已持久化）
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if _, ok := e2.Schema().Field("city"); !ok {
		t.Fatal("重启后 city 字段丢失")
	}
	if _, ok := e2.Schema().Field("title.keyword"); !ok {
		t.Fatal("重启后 title.keyword 子字段丢失")
	}
}
