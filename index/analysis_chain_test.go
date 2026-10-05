package index

// 分析链三级化（char filter → tokenizer → token filter）的引擎级测试：
// stop 词元过滤器造成 position 跳号时 match/match_phrase 仍正确（核心用例），
// mapping 字符过滤器经字段级 analyzer 覆盖生效。

import (
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
)

// 测试用链式分词器：mapping 字符过滤器 + standard 切词器 + lowercase，
// 验证 char filter 经注册表组合后在引擎写入/查询两侧同时生效
func init() {
	cf, err := plugin.GetCharFilter("mapping")
	if err != nil {
		panic(err)
	}
	tk, err := plugin.GetTokenizer("standard")
	if err != nil {
		panic(err)
	}
	tf, err := plugin.GetTokenFilter("lowercase")
	if err != nil {
		panic(err)
	}
	plugin.RegisterAnalyzer("test_mapping_chain", plugin.NewChainAnalyzer(
		[]plugin.CharFilter{cf}, tk, []plugin.TokenFilter{tf}))
}

// stopSchema title 字段使用内置 "stop" 分词器（standard 切词 + lowercase + 去停用词）
func stopSchema(t *testing.T) *schema.Schema {
	t.Helper()
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text", Analyzer: "stop"}})
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// TestStopFilterPhrasePositionGap 核心用例：stop 词元过滤器删除停用词造成 position 跳号时，
// match_phrase 仍正确（索引侧与查询侧走同一分析链，相对偏移同构）：
//
//	d1: the quick fox   → quick@1 fox@2（the 被删，位置跳号）
//	d2: quick fox       → quick@0 fox@1
//	d3: quick the fox   → quick@0 fox@2
func TestStopFilterPhrasePositionGap(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "stopphrase"), stopSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIndex(t, e, "d1", `{"title":"the quick fox"}`)
	mustIndex(t, e, "d2", `{"title":"quick fox"}`)
	mustIndex(t, e, "d3", `{"title":"quick the fox"}`)

	check := func(stage string) {
		t.Helper()

		// "quick fox"（偏移 [0,1]）：命中 d1/d2；d3 中间隔一个停用词位置，不命中
		res := phraseSearch(t, e, "title", "quick fox")
		if res.Total != 2 {
			t.Fatalf("%s phrase \"quick fox\" hits = %v, want [d1 d2]", stage, hitIDs(res))
		}
		got := map[string]bool{}
		for _, h := range res.Hits {
			got[h.ID] = true
		}
		if !got["d1"] || !got["d2"] {
			t.Fatalf("%s phrase \"quick fox\" hits = %v, want [d1 d2]", stage, hitIDs(res))
		}

		// "quick the fox"（查询侧 the 也被删 → 偏移 [0,2]）：只命中 d3
		res = phraseSearch(t, e, "title", "quick the fox")
		if res.Total != 1 || res.Hits[0].ID != "d3" {
			t.Fatalf("%s phrase \"quick the fox\" hits = %v, want [d3]", stage, hitIDs(res))
		}

		// "the quick fox"（首词被删 → 偏移归一化为 [0,1]）：等价于 "quick fox"
		res = phraseSearch(t, e, "title", "the quick fox")
		if res.Total != 2 {
			t.Fatalf("%s phrase \"the quick fox\" hits = %v, want [d1 d2]", stage, hitIDs(res))
		}

		// match 回归："quick" 命中全部三篇；停用词 "the" 分析后无 token → 无命中
		res = searchDSL(t, e, `{"query":{"match":{"title":"quick"}},"size":100}`)
		if res.Total != 3 {
			t.Fatalf("%s match \"quick\" Total = %d, want 3", stage, res.Total)
		}
		res = searchDSL(t, e, `{"query":{"match":{"title":"the"}},"size":100}`)
		if res.Total != 0 {
			t.Fatalf("%s match \"the\" 应无命中（停用词被过滤）, got %d", stage, res.Total)
		}
	}

	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
}

// TestCharFilterEngine char filter 生效用例：mapping（"ph"→"f"）在写入与查询两侧同时生效
func TestCharFilterEngine(t *testing.T) {
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text", Analyzer: "test_mapping_chain"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "charfilter"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIndex(t, e, "1", `{"title":"phone x"}`) // "ph"→"f"：索引侧实际词元为 "fone"
	mustIndex(t, e, "2", `{"title":"value"}`)

	check := func(stage string) {
		t.Helper()
		// 查询词 "phone" 同样经 mapping 变为 "fone" → 命中文档 1（分析链对称性）
		res := searchDSL(t, e, `{"query":{"match":{"title":"phone"}},"size":10}`)
		if res.Total != 1 || res.Hits[0].ID != "1" {
			t.Fatalf("%s match \"phone\" hits = %v, want [1]", stage, hitIDs(res))
		}
		// 未含映射子串的词不受影响
		res = searchDSL(t, e, `{"query":{"match":{"title":"value"}},"size":10}`)
		if res.Total != 1 || res.Hits[0].ID != "2" {
			t.Fatalf("%s match \"value\" hits = %v, want [2]", stage, hitIDs(res))
		}
		// 短语查询同样过 char filter
		res = phraseSearch(t, e, "title", "phone x")
		if res.Total != 1 || res.Hits[0].ID != "1" {
			t.Fatalf("%s phrase \"phone x\" hits = %v, want [1]", stage, hitIDs(res))
		}
	}

	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
}

// TestStandardChainEngineRegression 引擎级回归：lowercase 拆为独立 token filter 后，
// 默认 text 字段（standard 链）行为不变——大写查询词命中、CJK 单字命中
func TestStandardChainEngineRegression(t *testing.T) {
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "stdreg"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIndex(t, e, "1", `{"title":"Hello, World 世界"}`)

	check := func(stage string) {
		t.Helper()
		for _, q := range []string{"hello", "HELLO", "世界"} {
			res := searchDSL(t, e, `{"query":{"match":{"title":"`+q+`"}},"size":10}`)
			if res.Total != 1 || res.Hits[0].ID != "1" {
				t.Fatalf("%s match %q hits = %v, want [1]", stage, q, hitIDs(res))
			}
		}
		// 大写短语也命中（查询侧同样转小写）
		res := phraseSearch(t, e, "title", "Hello World")
		if res.Total != 1 {
			t.Fatalf("%s phrase \"Hello World\" hits = %v, want [1]", stage, hitIDs(res))
		}
	}

	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
}
