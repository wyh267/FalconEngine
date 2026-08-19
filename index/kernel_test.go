package index

import (
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/FalconEngine/falcon/plugins/fieldtype"
	"github.com/FalconEngine/falcon/schema"
)

// ---------- BM25（打分器插件） ----------

// TestBM25HandCalc 用手工计算的 BM25 分数对比引擎输出。
// 语料（全部在内存缓冲中）：
//
//	doc1: "a b b"  dl=3, tf(a)=1, tf(b)=2
//	doc2: "a c"    dl=2, tf(a)=1
//	doc3: "b"      dl=1, tf(b)=1
//
// N=3, avgdl=2, df(a)=df(b)=2
func TestBM25HandCalc(t *testing.T) {
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "bm25"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "doc1", `{"title":"a b b"}`)
	mustIndex(t, e, "doc2", `{"title":"a c"}`)
	mustIndex(t, e, "doc3", `{"title":"b"}`)

	// 手工计算（独立于引擎实现的公式展开）
	const k1, b = 1.2, 0.75
	idf := math.Log(1 + (3.0-2.0+0.5)/(2.0+0.5)) // ln(1.6)
	tfPart := func(tf, dl, avgdl float64) float64 {
		return tf * (k1 + 1) / (tf + k1*(1-b+b*dl/avgdl))
	}
	want := map[string]float64{
		"doc1": idf * (tfPart(1, 3, 2) + tfPart(2, 3, 2)), // a + b
		"doc2": idf * tfPart(1, 2, 2),                     // a
		"doc3": idf * tfPart(1, 1, 2),                     // b
	}

	dsl := `{"query":{"match":{"title":"a b"}},"size":10}`
	check := func(stage string) {
		res := searchDSL(t, e, dsl)
		if res.Total != 3 {
			t.Fatalf("%s Total = %d, want 3", stage, res.Total)
		}
		for _, h := range res.Hits {
			if math.Abs(h.Score-want[h.ID]) > 1e-9 {
				t.Errorf("%s %s score = %v, want %v", stage, h.ID, h.Score, want[h.ID])
			}
		}
		// 排名：doc1 > doc3 > doc2
		if got := hitIDs(res); !reflect.DeepEqual(got, []string{"doc1", "doc3", "doc2"}) {
			t.Errorf("%s 排名 = %v, want [doc1 doc3 doc2]", stage, got)
		}
	}

	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段") // 段落盘后 BM25 分数应一致
}

// ---------- operator OR / AND ----------

func TestMatchOperator(t *testing.T) {
	sch, _ := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	e, err := Open(filepath.Join(t.TempDir(), "op"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"苹果 香蕉"}`)
	mustIndex(t, e, "2", `{"title":"苹果"}`)
	mustIndex(t, e, "3", `{"title":"香蕉"}`)
	mustIndex(t, e, "4", `{"title":"西瓜"}`)

	// OR（默认）：命中任一 term 即可
	res := searchDSL(t, e, `{"query":{"match":{"title":"苹果 香蕉"}}}`)
	if res.Total != 3 {
		t.Fatalf("OR 默认 Total = %d, want 3, hits=%v", res.Total, hitIDs(res))
	}
	res = searchDSL(t, e, `{"query":{"match":{"title":{"query":"苹果 香蕉","operator":"or"}}}}`)
	if res.Total != 3 {
		t.Fatalf("OR 显式 Total = %d, want 3", res.Total)
	}

	// AND：必须同时命中
	res = searchDSL(t, e, `{"query":{"match":{"title":{"query":"苹果 香蕉","operator":"and"}}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("AND hits = %v, want [1]", hitIDs(res))
	}

	// 非法 operator
	if _, err := e.SearchDSL([]byte(`{"query":{"match":{"title":{"query":"苹果","operator":"xor"}}}}`)); err == nil {
		t.Fatal("非法 operator 应报错")
	}
}

// ---------- bool / must_not / ids ----------

func TestBoolAndIDs(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
	})
	e, err := Open(filepath.Join(t.TempDir(), "bool"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"go 语言","tag":"tech","level":3}`)
	mustIndex(t, e, "2", `{"title":"go 进阶","tag":"tech","level":5}`)
	mustIndex(t, e, "3", `{"title":"烹饪 大全","tag":"life","level":1}`)
	mustIndex(t, e, "4", `{"title":"go 烹饪","tag":"life","level":4}`)

	// must + must_not：含 go 但排除 tag=life
	res := searchDSL(t, e, `{"query":{"bool":{
		"must":[{"match":{"title":"go"}}],
		"must_not":[{"term":{"tag":"life"}}]}}}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"1", "2"}) {
		t.Fatalf("must+must_not = %v, want [1 2]", got)
	}

	// should 至少命中一个
	res = searchDSL(t, e, `{"query":{"bool":{
		"should":[{"term":{"tag":"tech"}},{"range":{"level":{"gte":1,"lte":2}}}]}}}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("should = %v, want [1 2 3]", got)
	}

	// filter 不影响打分但收窄结果（must 打分 + filter 过滤）
	res = searchDSL(t, e, `{"query":{"bool":{
		"must":[{"match":{"title":"go"}}],
		"filter":[{"range":{"level":{"gte":4}}}]}}}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"2", "4"}) {
		t.Fatalf("must+filter = %v, want [2 4]", got)
	}

	// terms：多值精确匹配
	res = searchDSL(t, e, `{"query":{"terms":{"tag":["tech","life"]}}}`)
	if res.Total != 4 {
		t.Fatalf("terms Total = %d, want 4", res.Total)
	}

	// ids
	res = searchDSL(t, e, `{"query":{"ids":{"values":["2","4","不存在"]}}}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"2", "4"}) {
		t.Fatalf("ids = %v, want [2 4]", got)
	}
}

// ---------- 过滤语义（字段缺失不匹配） ----------

func TestFilterMissingFieldNotMatched(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "level", Type: "number"},
	})
	e, err := Open(filepath.Join(t.TempDir(), "filter"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"有 等级","level":5}`)
	mustIndex(t, e, "2", `{"title":"无 等级"}`) // 缺 level 字段
	e.Flush()
	mustIndex(t, e, "3", `{"title":"缓冲 无 等级"}`) // 缓冲内缺 level

	// 带过滤：缺字段的文档不应命中（缓冲与段内都不应命中）
	res := searchDSL(t, e, `{"query":{"range":{"level":{"gte":0,"lte":100}}}}`)
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("过滤后 hits = %v, want [1]", hitIDs(res))
	}
	// 不带过滤：全部命中
	res = searchDSL(t, e, `{}`)
	if res.Total != 3 {
		t.Fatalf("无过滤 Total = %d, want 3", res.Total)
	}
}

// ---------- 排序 ----------

func TestSortByNumberField(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "level", Type: "number"},
	})
	e, err := Open(filepath.Join(t.TempDir(), "sort"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "a", `{"title":"x","level":3}`)
	mustIndex(t, e, "b", `{"title":"x","level":1}`)
	e.Flush()
	mustIndex(t, e, "c", `{"title":"x","level":2}`)
	mustIndex(t, e, "d", `{"title":"x"}`) // 无 level，恒排最后

	q := `{"query":{"match":{"title":"x"}},"size":10,"sort":[%s]}`
	// 升序
	res := searchDSL(t, e, fmt.Sprintf(q, `{"level":"asc"}`))
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"b", "c", "a", "d"}) {
		t.Fatalf("升序 = %v, want [b c a d]", got)
	}
	// 降序，缺失值仍排最后
	res = searchDSL(t, e, fmt.Sprintf(q, `{"level":"desc"}`))
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"a", "c", "b", "d"}) {
		t.Fatalf("降序 = %v, want [a c b d]", got)
	}
	// 排序后分页
	res = searchDSL(t, e, `{"query":{"match":{"title":"x"}},"sort":[{"level":"asc"}],"from":1,"size":2}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"c", "a"}) || res.Total != 4 {
		t.Fatalf("排序分页 = %v total=%d, want [c a] total=4", got, res.Total)
	}
	// 非法排序字段
	if _, err := e.SearchDSL([]byte(`{"sort":[{"title":"asc"}]}`)); err == nil {
		t.Fatal("text 字段排序应报错")
	}
	if _, err := e.SearchDSL([]byte(`{"sort":[{"nope":"asc"}]}`)); err == nil {
		t.Fatal("不存在字段排序应报错")
	}
	if _, err := e.SearchDSL([]byte(`{"sort":[{"level":"up"}]}`)); err == nil {
		t.Fatal("非法排序方向应报错")
	}
	// _score 显式排序
	res = searchDSL(t, e, `{"sort":[{"_score":"desc"}]}`)
	if res.Total != 4 {
		t.Fatalf("_score 排序 Total = %d, want 4", res.Total)
	}
}

// ---------- date / bool 字段 ----------

func TestDateAndBoolFields(t *testing.T) {
	sch, err := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "ts", Type: "date"},
		{Name: "vip", Type: "bool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "dt"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"甲","ts":"2024-01-02 03:04:05","vip":true}`)
	mustIndex(t, e, "2", `{"title":"乙","ts":"2024-06-01","vip":false}`)
	mustIndex(t, e, "3", `{"title":"丙","ts":"2025-01-01 00:00:00","vip":true}`)
	e.Flush()
	mustIndex(t, e, "4", `{"title":"丁","ts":"2025-06-01 12:00:00","vip":false}`)

	// 日期范围过滤（字符串边界由 date 插件解析）
	res := searchDSL(t, e, `{"query":{"range":{"ts":{"gte":"2024-06-01","lte":"2025-01-02 00:00:00"}}}}`)
	got := hitIDs(res)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"2", "3"}) {
		t.Fatalf("日期过滤 = %v, want [2 3]", got)
	}

	// bool 过滤：vip=true
	res = searchDSL(t, e, `{"query":{"term":{"vip":true}}}`)
	got = hitIDs(res)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"1", "3"}) {
		t.Fatalf("bool 过滤 = %v, want [1 3]", got)
	}

	// 日期排序
	res = searchDSL(t, e, `{"sort":[{"ts":"asc"}],"size":10}`)
	if got := hitIDs(res); !reflect.DeepEqual(got, []string{"1", "2", "3", "4"}) {
		t.Fatalf("日期升序 = %v, want [1 2 3 4]", got)
	}

	// 非法日期应报错
	if _, err := e.Index("bad", json.RawMessage(`{"title":"x","ts":"2024/01/01"}`)); err == nil {
		t.Fatal("非法日期格式应报错")
	}
	// ParseDate 单测
	if _, err := fieldtype.ParseDate("not-a-date"); err == nil {
		t.Fatal("ParseDate 非法输入应报错")
	}
}

// ---------- 聚合 ----------

func TestAggregations(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
	})
	e, err := Open(filepath.Join(t.TempDir(), "agg"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"go","tag":"tech","level":3}`)
	mustIndex(t, e, "2", `{"title":"go","tag":"tech","level":5}`)
	e.Flush() // 一半在段里
	mustIndex(t, e, "3", `{"title":"go","tag":"life","level":1}`)
	mustIndex(t, e, "4", `{"title":"go","tag":"tech","level":9}`)

	res := searchDSL(t, e, `{
		"query":{"match":{"title":"go"}},
		"size":10,
		"aggs":{
			"by_tag":{"terms":{"field":"tag","size":2}},
			"avg_level":{"avg":{"field":"level"}},
			"max_level":{"max":{"field":"level"}},
			"min_level":{"min":{"field":"level"}},
			"sum_level":{"sum":{"field":"level"}},
			"tags":{"cardinality":{"field":"tag"}}
		}}`)
	if res.Total != 4 {
		t.Fatalf("Total = %d, want 4", res.Total)
	}
	if got := string(res.Aggs["by_tag"]); got != `{"buckets":[{"key":"tech","count":3},{"key":"life","count":1}]}` {
		t.Fatalf("terms = %s", got)
	}
	if got := string(res.Aggs["avg_level"]); got != `{"value":4.5}` {
		t.Fatalf("avg = %s", got)
	}
	if got := string(res.Aggs["max_level"]); got != `{"value":9}` {
		t.Fatalf("max = %s", got)
	}
	if got := string(res.Aggs["min_level"]); got != `{"value":1}` {
		t.Fatalf("min = %s", got)
	}
	if got := string(res.Aggs["sum_level"]); got != `{"value":18}` {
		t.Fatalf("sum = %s", got)
	}
	if got := string(res.Aggs["tags"]); got != `{"value":2}` {
		t.Fatalf("cardinality = %s", got)
	}

	// terms size 截断
	res = searchDSL(t, e, `{"aggs":{"by_tag":{"terms":{"field":"tag","size":1}}}}`)
	if got := string(res.Aggs["by_tag"]); got != `{"buckets":[{"key":"tech","count":3}]}` {
		t.Fatalf("terms size=1 = %s", got)
	}

	// 聚合只作用于命中文档
	res = searchDSL(t, e, `{"query":{"term":{"tag":"life"}},"aggs":{"s":{"sum":{"field":"level"}}}}`)
	if got := string(res.Aggs["s"]); got != `{"value":1}` {
		t.Fatalf("过滤后 sum = %s, want 1", got)
	}

	// 非法聚合：非正排字段做 avg
	if _, err := e.SearchDSL([]byte(`{"aggs":{"x":{"avg":{"field":"tag"}}}}`)); err == nil {
		t.Fatal("非正排字段 avg 应报错")
	}
	// 未注册聚合类型
	if _, err := e.SearchDSL([]byte(`{"aggs":{"x":{"nope":{"field":"level"}}}}`)); err == nil {
		t.Fatal("未注册聚合应报错")
	}
}

// ---------- 动态 mapping ----------

func TestDynamicMapping(t *testing.T) {
	sch, _ := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	dir := filepath.Join(t.TempDir(), "dyn")
	e, err := Open(dir, sch)
	if err != nil {
		t.Fatal(err)
	}

	// 文档带未声明字段：string→text、数字→number、bool→bool
	mustIndex(t, e, "1", `{"title":"甲","nick":"小明","age":18,"vip":true,"meta":{"忽略":"对象"}}`)

	// 推断出的字段立即可检索
	res := searchDSL(t, e, `{"query":{"match":{"nick":"小明"}}}`)
	if res.Total != 1 {
		t.Fatalf("动态 text 字段不可检索, Total=%d", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"range":{"age":{"gte":10,"lte":20}}}}`)
	if res.Total != 1 {
		t.Fatalf("动态 number 字段不可过滤, Total=%d", res.Total)
	}
	res = searchDSL(t, e, `{"query":{"term":{"vip":true}}}`)
	if res.Total != 1 {
		t.Fatalf("动态 bool 字段不可过滤, Total=%d", res.Total)
	}
	// 对象字段不推断
	if _, ok := e.Schema().Field("meta"); ok {
		t.Fatal("对象字段不应被推断")
	}

	// schema 已持久化：重启后字段定义仍在
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer e2.Close()
	if _, ok := e2.Schema().Field("nick"); !ok {
		t.Fatal("重启后动态字段丢失")
	}
	res = searchDSL(t, e2, `{"query":{"match":{"nick":"小明"}}}`)
	if res.Total != 1 {
		t.Fatalf("重启后动态 text 字段不可检索, Total=%d", res.Total)
	}
	res = searchDSL(t, e2, `{"query":{"range":{"age":{"gte":10,"lte":20}}}}`)
	if res.Total != 1 {
		t.Fatalf("重启后动态 number 字段不可过滤（老段应视为有该字段）, Total=%d", res.Total)
	}

	// 动态推断后写入的新文档缺该字段：视为缺失，过滤不命中
	mustIndex(t, e2, "2", `{"title":"乙"}`)
	res = searchDSL(t, e2, `{"query":{"range":{"age":{"gte":0}}}}`)
	if res.Total != 1 {
		t.Fatalf("缺动态字段的文档不应命中过滤, Total=%d", res.Total)
	}
}

// ---------- 段合并 ----------

// snapshot 对一组 DSL 查询抓取（Total, 有序 ID 列表, 原文），用于合并前后对拍
func snapshot(t *testing.T, e *Engine, queries []string) [][]string {
	t.Helper()
	out := make([][]string, 0, len(queries))
	for _, q := range queries {
		res := searchDSL(t, e, q)
		row := []string{string(rune('0' + res.Total))}
		for _, h := range res.Hits {
			row = append(row, h.ID+"|"+string(h.Source))
		}
		out = append(out, row)
	}
	return out
}

func TestMergeConsistency(t *testing.T) {
	sch, _ := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
	})
	e, err := Open(filepath.Join(t.TempDir(), "merge"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// 调小合并参数以便测试触发
	e.smallSegDocs = 100
	e.mergeThreshold = 1000 // 禁止自动触发，手动控制

	// 分 4 批写入并 Flush，形成 4 个小段，期间夹杂更新与删除
	mustIndex(t, e, "1", `{"title":"go go go 语言","tag":"tech","level":3}`)
	mustIndex(t, e, "2", `{"title":"go 进阶","tag":"tech","level":5}`)
	e.Flush()
	mustIndex(t, e, "3", `{"title":"烹饪 大全","tag":"life","level":1}`)
	mustIndex(t, e, "2", `{"title":"go 进阶 第二版","tag":"tech","level":6}`) // 跨段更新
	e.Flush()
	mustIndex(t, e, "4", `{"title":"go 实战 项目","tag":"tech","level":4}`)
	if ok, _, _ := e.Delete("3"); !ok { // 删除段内文档
		t.Fatal("Delete(3) failed")
	}
	e.Flush()
	mustIndex(t, e, "5", `{"title":"生活 小贴士","tag":"life","level":2}`)
	e.Flush()

	if len(e.segs) != 4 {
		t.Fatalf("合并前段数 = %d, want 4", len(e.segs))
	}

	queries := []string{
		`{"query":{"match":{"title":"go"}},"size":100}`,                               // BM25 文本
		`{"query":{"term":{"tag":"life"}},"size":100}`,                                // keyword
		`{"query":{"range":{"level":{"gte":2,"lte":5}}},"size":100}`,                  // 过滤
		`{"sort":[{"level":"asc"}],"size":100}`,                                       // 排序 + match all
		`{"query":{"match":{"title":{"query":"go 语言","operator":"and"}}},"size":100}`, // AND
	}
	before := snapshot(t, e, queries)

	// 手动合并
	if err := e.Merge(); err != nil {
		t.Fatalf("Merge error: %v", err)
	}
	if len(e.segs) != 1 {
		t.Fatalf("合并后段数 = %d, want 1", len(e.segs))
	}

	after := snapshot(t, e, queries)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("合并前后结果不一致:\n前: %v\n后: %v", before, after)
	}

	// 重启（重新加载 seg-merge 段）后再对拍一次
	e.Close()
	e2, err := Open(e.dir, nil)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer e2.Close()
	if len(e2.segs) != 1 {
		t.Fatalf("重启后段数 = %d, want 1", len(e2.segs))
	}
	reopened := snapshot(t, e2, queries)
	if !reflect.DeepEqual(before, reopened) {
		t.Fatalf("重启后结果不一致:\n前: %v\n后: %v", before, reopened)
	}
}

func TestAutoMergeAfterFlush(t *testing.T) {
	sch, _ := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	e, err := Open(filepath.Join(t.TempDir(), "automerge"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.smallSegDocs = 100
	e.mergeThreshold = 2 // 小段数 > 2 时自动合并

	for i := 0; i < 3; i++ {
		mustIndex(t, e, string(rune('a'+i)), `{"title":"文档"}`)
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// 第 3 次 Flush 后小段数 3 > 2，应已自动合并为 1 个段
	if len(e.segs) != 1 {
		t.Fatalf("自动合并后段数 = %d, want 1", len(e.segs))
	}
	res := searchDSL(t, e, `{}`)
	if res.Total != 3 {
		t.Fatalf("自动合并后 Total = %d, want 3", res.Total)
	}
}
