package index

// 阶段 8 新查询插件的引擎级语义测试：match_phrase(DSL) / multi_match /
// prefix / wildcard / fuzzy / exists。解析器契约见 plugins/query 测试。

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/segment"
	"github.com/FalconEngine/falcon/segment/segmenttest"
)

// TestMatchPhraseDSL match_phrase 经 DSL 端到端：简写/完整形态命中，slop 校验报错
func TestMatchPhraseDSL(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "phrase-dsl"), phraseSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	indexPhraseDocs(t, e)

	check := func(stage string) {
		t.Helper()
		// 简写形态
		res := searchDSL(t, e, `{"query":{"match_phrase":{"title":"quick brown"}},"size":100}`)
		if res.Total != 3 {
			t.Fatalf("%s 简写 match_phrase Total = %d, want 3(d1/d3/d4), hits=%v", stage, res.Total, hitIDs(res))
		}
		// 完整形态（slop:0）
		res = searchDSL(t, e, `{"query":{"match_phrase":{"title":{"query":"the quick brown","slop":0}}},"size":100}`)
		if res.Total != 1 || res.Hits[0].ID != "d1" {
			t.Fatalf("%s 完整形态 hits = %v, want [d1]", stage, hitIDs(res))
		}
	}
	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")

	// slop>0 报"暂未支持"
	if _, err := e.SearchDSL([]byte(`{"query":{"match_phrase":{"title":{"query":"a b","slop":1}}}}`)); err == nil ||
		!strings.Contains(err.Error(), "暂未支持") {
		t.Fatalf("slop>0 应报\"暂未支持\", got %v", err)
	}
	// 非倒排字段报错
	if _, err := e.SearchDSL([]byte(`{"query":{"match_phrase":{"level":{"query":"1"}}}}`)); err == nil {
		t.Fatal("非倒排字段应报错")
	}
}

// multiMatchSchema title/content 双 text 字段（multi_match 用）
func multiMatchSchema(t *testing.T) *schema.Schema {
	t.Helper()
	sch, err := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "content", Type: "text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// TestMultiMatchBoost multi_match：字段加权影响排序；并集命中
func TestMultiMatchBoost(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "mm"), multiMatchSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// 对称语料：apple 在 d1 的 title、d2 的 content 各出现一次，基础分相同
	mustIndex(t, e, "d1", `{"title":"apple","content":"x"}`)
	mustIndex(t, e, "d2", `{"title":"y","content":"apple"}`)

	check := func(stage string) {
		t.Helper()
		// title^3：d1 排前，得分为 d2 的 3 倍
		res := searchDSL(t, e, `{"query":{"multi_match":{"query":"apple","fields":["title^3","content"]}}}`)
		if res.Total != 2 || res.Hits[0].ID != "d1" {
			t.Fatalf("%s title^3 排序 hits = %v, want d1 在前", stage, hitIDs(res))
		}
		if res.Hits[0].Score != 3*res.Hits[1].Score {
			t.Fatalf("%s boost 得分比 = %v/%v, want 3", stage, res.Hits[0].Score, res.Hits[1].Score)
		}
		// content^3：d2 排前
		res = searchDSL(t, e, `{"query":{"multi_match":{"query":"apple","fields":["title","content^3"]}}}`)
		if res.Total != 2 || res.Hits[0].ID != "d2" {
			t.Fatalf("%s content^3 排序 hits = %v, want d2 在前", stage, hitIDs(res))
		}
	}
	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
}

// TestPrefixQuery prefix：keyword/text 字段命中，横跨缓冲与段
func TestPrefixQuery(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "d1", `{"title":"hello world","tag":"ERR-01","level":1}`)
	mustIndex(t, e, "d2", `{"title":"hi there","tag":"WARN-01","level":2}`)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	// 段内 d1/d2 + 缓冲内 d3/d4，命中应横跨两侧
	mustIndex(t, e, "d3", `{"title":"help me","tag":"ERR-02","level":3}`)
	mustIndex(t, e, "d4", `{"title":"hero","tag":"WARN-02","level":4}`)

	// keyword 前缀：段内 ERR-01 + 缓冲 ERR-02
	res := searchDSL(t, e, `{"query":{"prefix":{"tag":"ERR"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("prefix(tag:ERR) Total = %d, want 2(d1/d3), hits=%v", res.Total, hitIDs(res))
	}
	// text 前缀（对分词后的 term 生效）：hel 命中 hello/help
	res = searchDSL(t, e, `{"query":{"prefix":{"title":"hel"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("prefix(title:hel) Total = %d, want 2(d1/d3), hits=%v", res.Total, hitIDs(res))
	}
	// 无匹配前缀
	res = searchDSL(t, e, `{"query":{"prefix":{"tag":"NOPE"}},"size":100}`)
	if res.Total != 0 {
		t.Fatalf("prefix 无匹配 Total = %d, want 0", res.Total)
	}
	// 恒定 1 分（filter 语义）
	res = searchDSL(t, e, `{"query":{"prefix":{"tag":"ERR"}},"size":100}`)
	for _, h := range res.Hits {
		if h.Score != 1 {
			t.Fatalf("prefix 命中得分应恒为 1, got %v", h.Score)
		}
	}
	// 非倒排字段报错
	if _, err := e.SearchDSL([]byte(`{"query":{"prefix":{"level":"1"}}}`)); err == nil {
		t.Fatal("prefix 作用于 number 字段应报错")
	}
}

// TestWildcardQuery wildcard：字面前缀缩范围与全词典扫描两种形态，横跨缓冲与段
func TestWildcardQuery(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "d1", `{"title":"a","tag":"ERR-01","level":1}`)
	mustIndex(t, e, "d2", `{"title":"b","tag":"WARN-01","level":2}`)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	mustIndex(t, e, "d3", `{"title":"c","tag":"ERR-02","level":3}`)

	// 首字符非通配：ERR-0? 命中段内 d1 + 缓冲 d3
	res := searchDSL(t, e, `{"query":{"wildcard":{"tag":"ERR-0?"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("wildcard(ERR-0?) Total = %d, want 2(d1/d3), hits=%v", res.Total, hitIDs(res))
	}
	// 首字符通配（全词典扫描）：*-01 命中 d1/d2
	res = searchDSL(t, e, `{"query":{"wildcard":{"tag":"*-01"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("wildcard(*-01) Total = %d, want 2(d1/d2), hits=%v", res.Total, hitIDs(res))
	}
	// 星号中间形态
	res = searchDSL(t, e, `{"query":{"wildcard":{"tag":"E*2"}},"size":100}`)
	if res.Total != 1 || res.Hits[0].ID != "d3" {
		t.Fatalf("wildcard(E*2) hits = %v, want [d3]", hitIDs(res))
	}
	// CJK：keyword 整串按 rune 通配
	mustIndex(t, e, "d4", `{"title":"d","tag":"你好世界","level":4}`)
	res = searchDSL(t, e, `{"query":{"wildcard":{"tag":"你?世*"}},"size":100}`)
	if res.Total != 1 || res.Hits[0].ID != "d4" {
		t.Fatalf("wildcard(你?世*) hits = %v, want [d4]", hitIDs(res))
	}
}

// TestExpandTermsTruncation 词典展开上限：引擎侧截断字典序最小者，fuzzy max_expansions 生效
func TestExpandTermsTruncation(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	// 缓冲侧 3 个 term + flush 后段侧 2 个 term，合并展开
	mustIndex(t, e, "d1", `{"title":"x","tag":"t1","level":1}`)
	mustIndex(t, e, "d2", `{"title":"x","tag":"t2","level":2}`)
	mustIndex(t, e, "d3", `{"title":"x","tag":"t3","level":3}`)
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	mustIndex(t, e, "d4", `{"title":"x","tag":"t4","level":4}`)
	mustIndex(t, e, "d5", `{"title":"x","tag":"t5","level":5}`)

	// 直接验证展开函数：上限 3 → 字典序最小 3 个（横跨缓冲与段去重合并）
	got := e.expandTerms("tag", "t", nil, 3)
	if !reflect.DeepEqual(got, []string{"t1", "t2", "t3"}) {
		t.Fatalf("expandTerms 截断 = %v, want [t1 t2 t3]", got)
	}
	if got := e.expandTerms("tag", "t", nil, 100); len(got) != 5 {
		t.Fatalf("expandTerms 全量 = %v, want 5 条", got)
	}

	// DSL 级：fuzzy max_expansions=2 截断（"t0" 与 t1~t5 距离均 ≤2）
	res := searchDSL(t, e, `{"query":{"fuzzy":{"tag":{"value":"t0","fuzziness":2,"max_expansions":2}}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("fuzzy max_expansions=2 Total = %d, want 2, hits=%v", res.Total, hitIDs(res))
	}
	// 默认上限足够时不截断
	res = searchDSL(t, e, `{"query":{"fuzzy":{"tag":{"value":"t0","fuzziness":2}}},"size":100}`)
	if res.Total != 5 {
		t.Fatalf("fuzzy 默认上限 Total = %d, want 5, hits=%v", res.Total, hitIDs(res))
	}
}

// TestPrefixMaxExpansions prefix/wildcard 的 1024 展开上限：单文档 1100 个 term
// 构造大词典（避免逐篇写入的开销），超出部分按字典序截断
func TestPrefixMaxExpansions(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	var sb strings.Builder
	for i := 0; i < 1100; i++ {
		fmt.Fprintf(&sb, "w%04d ", i)
	}
	mustIndex(t, e, "big", `{"title":"`+strings.TrimSpace(sb.String())+`","tag":"x","level":1}`)

	// 展开上限 1024：字典序最小者优先（w0000 ~ w1023）
	got := e.expandTerms("title", "w", nil, 1024)
	if len(got) != 1024 || got[0] != "w0000" || got[1023] != "w1023" {
		t.Fatalf("expandTerms 1024 截断: len=%d, head=%v, tail=%v", len(got), got[:2], got[len(got)-2:])
	}
	// DSL 级：prefix 命中（展开被截断但查询正常执行）
	res := searchDSL(t, e, `{"query":{"prefix":{"title":"w"}},"size":100}`)
	if res.Total != 1 || res.Hits[0].ID != "big" {
		t.Fatalf("大词典 prefix hits = %v, want [big]", hitIDs(res))
	}
}

// TestFuzzyQuery fuzzy 编辑距离边界：距离 1/2 命中、距离 3 不命中
func TestFuzzyQuery(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()
	mustIndex(t, e, "d1", `{"title":"a","tag":"hello","level":1}`)

	cases := []struct {
		dsl   string
		total int
	}{
		{`{"query":{"fuzzy":{"tag":"hello"}}}`, 1},                         // 距离 0
		{`{"query":{"fuzzy":{"tag":"helo"}}}`, 1},                          // 距离 1（删一个 l）
		{`{"query":{"fuzzy":{"tag":{"value":"heo","fuzziness":1}}}}`, 0},   // 距离 2 > 1
		{`{"query":{"fuzzy":{"tag":{"value":"heo","fuzziness":2}}}}`, 1},   // 距离 2 ≤ 2
		{`{"query":{"fuzzy":{"tag":{"value":"hxjxo","fuzziness":2}}}}`, 0}, // 距离 3 > 2
	}
	for _, c := range cases {
		res := searchDSL(t, e, c.dsl)
		if res.Total != c.total {
			t.Errorf("fuzzy %s Total = %d, want %d", c.dsl, res.Total, c.total)
		}
	}
	// fuzziness>2 报错
	if _, err := e.SearchDSL([]byte(`{"query":{"fuzzy":{"tag":{"value":"hello","fuzziness":3}}}}`)); err == nil {
		t.Fatal("fuzziness>2 应报错")
	}
}

// TestExistsQuery exists：字段缺失/null/空串/不可分词语义的引擎级验证（缓冲与段一致）
func TestExistsQuery(t *testing.T) {
	e, _ := openTestEngine(t)
	defer e.Close()

	mustIndex(t, e, "d1", `{"title":"hello","tag":"a","level":1,"extra":"x"}`)
	mustIndex(t, e, "d2", `{"title":"","tag":"b"}`)                 // title 空串 → 0 token；level/extra 缺失
	mustIndex(t, e, "d3", `{"title":"world","tag":null,"level":3}`) // tag null → 不存在
	mustIndex(t, e, "d4", `{"title":"！！！"}`)                        // 仅标点 → 0 token → 不存在

	want := map[string][]string{
		"title": {"d1", "d3"},
		"tag":   {"d1", "d2"},
		"level": {"d1", "d3"},
		"extra": {"d1"}, // stored 字段：原文可见即存在
	}
	check := func(stage string) {
		t.Helper()
		for field, ids := range want {
			res := searchDSL(t, e, `{"query":{"exists":{"field":"`+field+`"}},"size":100}`)
			if res.Total != len(ids) {
				t.Fatalf("%s exists(%s) Total = %d, want %d(%v), hits=%v", stage, field, res.Total, len(ids), ids, hitIDs(res))
			}
			got := map[string]bool{}
			for _, h := range res.Hits {
				got[h.ID] = true
			}
			for _, id := range ids {
				if !got[id] {
					t.Fatalf("%s exists(%s) 应命中 %s, hits=%v", stage, field, id, hitIDs(res))
				}
			}
		}
	}
	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
	// 混合：flush 后再写入 stored 字段文档
	mustIndex(t, e, "d5", `{"extra":"y"}`)
	res := searchDSL(t, e, `{"query":{"exists":{"field":"extra"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("混合 exists(extra) Total = %d, want 2(d1/d5), hits=%v", res.Total, hitIDs(res))
	}
	// 字段不存在报错
	if _, err := e.SearchDSL([]byte(`{"query":{"exists":{"field":"nope"}}}`)); err == nil {
		t.Fatal("exists 作用于不存在字段应报错")
	}
}

// TestExistsV1Segment v1 段 exists：number 字段走 has 位，text 字段回落原文检查
func TestExistsV1Segment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "existsv1")
	v1docs := []segment.Doc{
		{
			ID:    "d1",
			Raw:   []byte(`{"title":"quick brown","level":1}`),
			Terms: map[string][]plugin.Token{"title": toks("quick", "brown")},
			Nums:  map[string]int64{"level": 1},
		},
		{
			ID:    "d2",
			Raw:   []byte(`{"title":"","level":2}`), // title 空串：原文检查应判不存在
			Terms: map[string][]plugin.Token{"title": toks()},
			Nums:  map[string]int64{"level": 2},
		},
	}
	if err := segmenttest.WriteV1(filepath.Join(dir, "seg-1"), 1,
		[]string{"title"}, []string{"title"}, []string{"level"}, v1docs); err != nil {
		t.Fatal(err)
	}
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}, {Name: "level", Type: "number"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(dir, sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// v1 无 title has 位：回落原文 JSON 检查（d1 有值，d2 空串分词后 0 token）
	res := searchDSL(t, e, `{"query":{"exists":{"field":"title"}},"size":100}`)
	if res.Total != 1 || res.Hits[0].ID != "d1" {
		t.Fatalf("v1 段 exists(title) hits = %v, want [d1]", hitIDs(res))
	}
	// v1 number 字段有 has 位：d1/d2 均存在
	res = searchDSL(t, e, `{"query":{"exists":{"field":"level"}},"size":100}`)
	if res.Total != 2 {
		t.Fatalf("v1 段 exists(level) Total = %d, want 2, hits=%v", res.Total, hitIDs(res))
	}
}

// TestScatterPlanMultiTerm 协调层纯 schema 解析（无本地引擎、ExpandTerms 为空）：
// multi-term 类查询不展开也能通过解析——真正展开发生在分片侧重新解析时
func TestScatterPlanMultiTerm(t *testing.T) {
	sch := testSchema(t)
	for _, q := range []string{
		`{"query":{"prefix":{"tag":"E"}}}`,
		`{"query":{"wildcard":{"tag":"E*"}}}`,
		`{"query":{"fuzzy":{"tag":"hello"}}}`,
		`{"query":{"exists":{"field":"tag"}}}`,
		`{"query":{"multi_match":{"query":"x","fields":["title^2"]}}}`,
	} {
		if _, err := NewScatterPlan([]byte(q), sch); err != nil {
			t.Fatalf("协调层解析 %s 失败: %v", q, err)
		}
	}
}

// TestMultiTermScatterGather 多分片 scatter-gather：prefix/wildcard/fuzzy/exists
// 经协调层重写分页后由各分片本地展开执行，归并结果正确
func TestMultiTermScatterGather(t *testing.T) {
	sch := testSchema(t)
	mgr, err := OpenManager(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	ix, err := mgr.Create("mt", IndexSettings{NumShards: 3}, sch)
	if err != nil {
		t.Fatal(err)
	}

	// 跨分片写入（ID 哈希路由，必然散布多个分片）
	docs := map[string]string{
		"doc-1": `{"title":"hello","tag":"ERR-01","level":1,"extra":"x"}`,
		"doc-2": `{"title":"world","tag":"ERR-02","level":2}`,
		"doc-3": `{"title":"hi","tag":"WARN-01","level":3}`,
		"doc-4": `{"title":"hey","tag":"WARN-02","level":4}`,
		"doc-5": `{"title":"helo","tag":"ERR-03"}`,
		"doc-6": `{"title":"halo","tag":"INFO-01","level":6}`,
	}
	for id, doc := range docs {
		mustIndexIndex(t, ix, id, doc)
	}
	used := map[int]int{}
	for id := range docs {
		used[ix.ShardOf(id)]++
	}
	if len(used) < 2 {
		t.Skipf("文档未散布到多个分片: %v", used)
	}
	// 一半分片落盘，制造 缓冲+段 混合态
	_ = ix.Flush()
	mustIndexIndex(t, ix, "doc-7", `{"title":"hello","tag":"ERR-04","level":7}`)

	cases := []struct {
		dsl   string
		total int
	}{
		{`{"query":{"prefix":{"tag":"ERR"}},"size":100}`, 4},                           // doc-1/2/5/7
		{`{"query":{"wildcard":{"tag":"*-0?"}},"size":100}`, 7},                        // 全部 tag 均以 -0X 结尾
		{`{"query":{"wildcard":{"tag":"ERR-0?"}},"size":100}`, 4},                      // doc-1/2/5/7
		{`{"query":{"fuzzy":{"tag":{"value":"ERR-01","fuzziness":1}}},"size":100}`, 4}, // ERR-01~04 距离均 ≤1
		{`{"query":{"exists":{"field":"level"}},"size":100}`, 6},                       // 除 doc-5 外
		{`{"query":{"exists":{"field":"extra"}},"size":100}`, 1},                       // 仅 doc-1
		{`{"query":{"match_phrase":{"title":"helo"}},"size":100}`, 1},                  // 仅 doc-5
	}
	for _, c := range cases {
		res := mustSearchIndex(t, ix, c.dsl)
		if res.Total != c.total {
			t.Errorf("scatter-gather %s Total = %d, want %d", c.dsl, res.Total, c.total)
		}
	}
}
