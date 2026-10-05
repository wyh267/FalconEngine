package index

// match_phrase 执行器与 v1/v2 段共存的引擎级测试。
// match_phrase 的 DSL 解析器在阶段 8 注册，本阶段直接构造 plugin.PhraseNode 测试执行器。

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/segment"
	"github.com/FalconEngine/falcon/segment/segmenttest"
)

// phraseSearch 直接以 PhraseNode 执行查询
func phraseSearch(t *testing.T, e *Engine, field, text string) *Result {
	t.Helper()
	res, err := e.Search(plugin.PhraseNode{Field: field, Text: text}, &SearchOptions{Size: 100})
	if err != nil {
		t.Fatalf("phrase(%q, %q) 查询失败: %v", field, text, err)
	}
	return res
}

// toks 便捷构造 token 序列（Position 取下标）
func toks(terms ...string) []plugin.Token {
	out := make([]plugin.Token, 0, len(terms))
	for i, term := range terms {
		out = append(out, plugin.Token{Term: term, Position: i})
	}
	return out
}

func phraseSchema(t *testing.T) *schema.Schema {
	t.Helper()
	sch, err := schema.New([]schema.Field{
		{Name: "title", Type: "text"},
		{Name: "tag", Type: "keyword"},
		{Name: "level", Type: "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// phraseDocs 固定语料（standard 分词器按空白切分 ASCII 词，下标即位置）：
//
//	d1: the quick brown fox        quick@1 brown@2 → 命中
//	d2: the brown quick fox        brown@1 quick@2（顺序颠倒，不构成短语）
//	d3: quick brown                quick@0 brown@1 → 命中
//	d4: brown quick brown          quick@1 brown@2 → 命中
func indexPhraseDocs(t *testing.T, e *Engine) {
	t.Helper()
	mustIndex(t, e, "d1", `{"title":"the quick brown fox","tag":"a","level":1}`)
	mustIndex(t, e, "d2", `{"title":"the brown quick fox","tag":"b","level":2}`)
	mustIndex(t, e, "d3", `{"title":"quick brown","tag":"a","level":3}`)
	mustIndex(t, e, "d4", `{"title":"brown quick brown","tag":"c","level":4}`)
}

// TestMatchPhrase 短语查询：缓冲与段路径结果一致，位置约束生效
func TestMatchPhrase(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "phrase"), phraseSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	indexPhraseDocs(t, e)

	// "quick brown" 命中 d1/d3/d4；d2 顺序颠倒不命中
	want := []string{"d1", "d3", "d4"}
	check := func(stage string) {
		t.Helper()
		res := phraseSearch(t, e, "title", "quick brown")
		if res.Total != len(want) {
			t.Fatalf("%s phrase Total = %d, want %d, hits=%v", stage, res.Total, len(want), hitIDs(res))
		}
		got := map[string]bool{}
		for _, h := range res.Hits {
			got[h.ID] = true
			if h.Score <= 0 {
				t.Fatalf("%s %s 得分应 > 0, got %v", stage, h.ID, h.Score)
			}
		}
		for _, id := range want {
			if !got[id] {
				t.Fatalf("%s phrase 应命中 %s, hits=%v", stage, id, hitIDs(res))
			}
		}
	}

	check("缓冲")
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	check("段")
	// 混合：flush 后再写入，命中横跨缓冲与段
	mustIndex(t, e, "d5", `{"title":"a quick brown b","tag":"d","level":5}`)
	res := phraseSearch(t, e, "title", "quick brown")
	if res.Total != 4 {
		t.Fatalf("混合 hits = %v, want 4", hitIDs(res))
	}
	// 三词短语
	res = phraseSearch(t, e, "title", "the quick brown")
	if res.Total != 1 || res.Hits[0].ID != "d1" {
		t.Fatalf("三词短语 hits = %v, want [d1]", hitIDs(res))
	}
	// 单 term 短语等价于 term 查询
	res = phraseSearch(t, e, "title", "fox")
	if res.Total != 2 {
		t.Fatalf("单 term 短语 Total = %d, want 2(d1/d2)", res.Total)
	}
	// 无命中：term 都存在但位置不连续
	res = phraseSearch(t, e, "title", "brown fox the")
	if res.Total != 0 {
		t.Fatalf("不连续短语应无命中, got %v", hitIDs(res))
	}
	// 查询分词为空
	res = phraseSearch(t, e, "title", "！！！")
	if res.Total != 0 {
		t.Fatalf("空 token 短语应无命中, got %d", res.Total)
	}
}

// TestMatchPhraseRepeatedTerm 重复 term 短语："go go" 要求同一 doc 内两个相邻位置都是 go
func TestMatchPhraseRepeatedTerm(t *testing.T) {
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(filepath.Join(t.TempDir(), "phrase2"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	mustIndex(t, e, "1", `{"title":"go go go"}`)  // 0,1,2 相邻 → 命中
	mustIndex(t, e, "2", `{"title":"go x go"}`)   // 0,2 不相邻 → 不命中
	mustIndex(t, e, "3", `{"title":"go"}`)        // 单次出现 → 不命中
	mustIndex(t, e, "4", `{"title":"x go go y"}`) // 1,2 相邻 → 命中

	for _, stage := range []string{"缓冲", "段"} {
		res := phraseSearch(t, e, "title", "go go")
		got := map[string]bool{}
		for _, h := range res.Hits {
			got[h.ID] = true
		}
		if !got["1"] || !got["4"] || got["2"] || got["3"] || len(got) != 2 {
			t.Fatalf("%s: \"go go\" hits = %v, want {1,4}", stage, hitIDs(res))
		}
		if stage == "缓冲" {
			if err := e.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestMatchPhraseErrors 错误路径：字段不存在/非倒排字段/slop 未支持
func TestMatchPhraseErrors(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "phrase3"), phraseSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIndex(t, e, "1", `{"title":"a b","tag":"x","level":1}`)

	if _, err := e.Search(plugin.PhraseNode{Field: "nope", Text: "a"}, nil); err == nil {
		t.Fatal("字段不存在应报错")
	}
	if _, err := e.Search(plugin.PhraseNode{Field: "level", Text: "1"}, nil); err == nil {
		t.Fatal("非倒排字段应报错")
	}
	if _, err := e.Search(plugin.PhraseNode{Field: "title", Text: "a b", Slop: 1}, nil); err == nil ||
		!strings.Contains(err.Error(), "暂未支持") {
		t.Fatalf("slop>0 应报\"暂未支持\", got %v", err)
	}
	// keyword 字段（无 norms）短语可用且恒定 1 分
	res := phraseSearch(t, e, "tag", "x")
	if res.Total != 1 || res.Hits[0].Score != 1 {
		t.Fatalf("keyword 短语 hits=%v score 应恒 1", hitIDs(res))
	}
}

// TestMatchPhraseV1SegmentError v1 段不含 positions，短语查询应报带恢复指引的错误
func TestMatchPhraseV1SegmentError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "phrasev1")
	// 先用 v1 writer 固化一个旧格式段
	v1docs := []segment.Doc{{
		ID:    "old1",
		Raw:   []byte(`{"title":"quick brown"}`),
		Terms: map[string][]plugin.Token{"title": toks("quick", "brown")},
	}}
	if err := segmenttest.WriteV1(filepath.Join(dir, "seg-1"), 1, []string{"title"}, []string{"title"}, nil, v1docs); err != nil {
		t.Fatal(err)
	}
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(dir, sch)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	_, err = e.Search(plugin.PhraseNode{Field: "title", Text: "quick brown"}, nil)
	if err == nil || !strings.Contains(err.Error(), "v1") || !strings.Contains(err.Error(), "_flush") {
		t.Fatalf("v1 段短语查询应报含 v1/_flush 指引的错误, got %v", err)
	}
	// 普通 match 在 v1 段上不受影响
	res := searchDSL(t, e, `{"query":{"match":{"title":"quick"}}}`)
	if res.Total != 1 {
		t.Fatalf("v1 段 match 应正常命中, got %d", res.Total)
	}
}

// TestV1V2SegmentCoexistence v1 段与 v2 段共存：同批文档在两种格式混合下的
// match/range/sort/agg 结果与全 v2 引擎一致
func TestV1V2SegmentCoexistence(t *testing.T) {
	sch := phraseSchema(t)

	// 语料：前两篇进 v1 段，后两篇经引擎写入（缓冲/flush 为 v2）
	v1docs := []segment.Doc{
		{
			ID:    "d1",
			Raw:   []byte(`{"title":"the quick brown fox","tag":"a","level":1}`),
			Terms: map[string][]plugin.Token{"title": toks("the", "quick", "brown", "fox"), "tag": toks("a")},
			Nums:  map[string]int64{"level": 1},
		},
		{
			ID:    "d2",
			Raw:   []byte(`{"title":"the brown quick fox","tag":"b","level":2}`),
			Terms: map[string][]plugin.Token{"title": toks("the", "brown", "quick", "fox"), "tag": toks("b")},
			Nums:  map[string]int64{"level": 2},
		},
	}
	newDocs := []struct{ id, raw string }{
		{"d3", `{"title":"quick brown","tag":"a","level":3}`},
		{"d4", `{"title":"brown quick brown","tag":"c","level":4}`},
	}

	// 混合引擎：预置 v1 段 + 引擎写入新文档后 flush（v2 段）
	mixDir := filepath.Join(t.TempDir(), "mix")
	if err := segmenttest.WriteV1(filepath.Join(mixDir, "seg-1"), 1,
		[]string{"title", "tag"}, []string{"title"}, []string{"level"}, v1docs); err != nil {
		t.Fatal(err)
	}
	mix, err := Open(mixDir, sch)
	if err != nil {
		t.Fatal(err)
	}
	defer mix.Close()
	for _, d := range newDocs {
		mustIndex(t, mix, d.id, d.raw)
	}
	if err := mix.Flush(); err != nil {
		t.Fatal(err)
	}

	// 参照引擎：全部文档经引擎写入（纯 v2）
	ref, err := Open(filepath.Join(t.TempDir(), "ref"), sch)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	for _, d := range v1docs {
		mustIndex(t, ref, d.ID, string(d.Raw))
	}
	for _, d := range newDocs {
		mustIndex(t, ref, d.id, d.raw)
	}
	if err := ref.Flush(); err != nil {
		t.Fatal(err)
	}

	queries := []string{
		`{"query":{"match":{"title":"quick brown"}},"size":100}`,
		`{"query":{"match":{"title":{"query":"quick brown","operator":"and"}}},"size":100}`,
		`{"query":{"term":{"tag":"a"}},"size":100}`,
		`{"query":{"range":{"level":{"gte":2,"lte":3}}},"size":100}`,
		`{"query":{"match_all":{}},"sort":[{"level":"desc"}],"size":100}`,
		`{"query":{"match_all":{}},"aggs":{"by_tag":{"terms":{"field":"tag","size":10}},"avg_level":{"avg":{"field":"level"}}},"size":100}`,
	}
	for _, q := range queries {
		got := searchDSL(t, mix, q)
		want := searchDSL(t, ref, q)
		if got.Total != want.Total {
			t.Fatalf("v1/v2 混合 vs 纯 v2 Total 不一致: %d vs %d, query=%s", got.Total, want.Total, q)
		}
		if !reflect.DeepEqual(hitIDs(got), hitIDs(want)) {
			t.Fatalf("命中顺序不一致: %v vs %v, query=%s", hitIDs(got), hitIDs(want), q)
		}
		for i := range got.Hits {
			if got.Hits[i].Score != want.Hits[i].Score {
				t.Fatalf("得分不一致: %v vs %v, query=%s hit=%s",
					got.Hits[i].Score, want.Hits[i].Score, q, got.Hits[i].ID)
			}
		}
		if !reflect.DeepEqual(got.Aggs, want.Aggs) {
			t.Fatalf("聚合结果不一致: %v vs %v, query=%s", got.Aggs, want.Aggs, q)
		}
	}

	// 混合引擎的短语查询：d1/d2 在 v1 段（无 positions），查询应报 v1 错误
	if _, err := mix.Search(plugin.PhraseNode{Field: "title", Text: "quick brown"}, nil); err == nil {
		t.Fatal("含 v1 段时短语查询应报错")
	}
	// v1 段无 kw 列：按 keyword 字段排序报带 _flush 指引的错误
	// （排序器无法像聚合那样回落原文解析）
	if _, err := mix.SearchDSL([]byte(`{"sort":[{"tag":"asc"}]}`)); err == nil ||
		!strings.Contains(err.Error(), "v1") || !strings.Contains(err.Error(), "_flush") {
		t.Fatalf("v1 段 keyword 排序应报含 v1/_flush 指引的错误, got %v", err)
	}
	// merge 后老段重建为 v2，短语查询恢复可用（d1/d3/d4 命中，d2 顺序颠倒不命中）
	if err := mix.Merge(); err != nil {
		t.Fatal(err)
	}
	// merge 升级 v2 后 keyword 排序恢复可用，结果与参照引擎一致
	mixSorted := searchDSL(t, mix, `{"sort":[{"tag":"asc"}],"size":100}`)
	refSorted := searchDSL(t, ref, `{"sort":[{"tag":"asc"}],"size":100}`)
	if !reflect.DeepEqual(hitIDs(mixSorted), hitIDs(refSorted)) {
		t.Fatalf("merge 后 keyword 排序 = %v, want %v", hitIDs(mixSorted), hitIDs(refSorted))
	}
	res := phraseSearch(t, mix, "title", "quick brown")
	got := map[string]bool{}
	for _, h := range res.Hits {
		got[h.ID] = true
	}
	if !got["d1"] || !got["d3"] || !got["d4"] || len(got) != 3 {
		t.Fatalf("merge 升级后短语 hits = %v, want {d1,d3,d4}", hitIDs(res))
	}
}

// TestKeywordColumnEngine 引擎接线：keyword 字段写入缓冲与段后 kw 列可读
func TestKeywordColumnEngine(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "kw"), phraseSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIndex(t, e, "1", `{"title":"a","tag":"tech","level":1}`)
	mustIndex(t, e, "2", `{"title":"b","tag":"life","level":2}`)
	mustIndex(t, e, "3", `{"title":"c","level":3}`) // 无 tag

	// 缓冲侧 kw 访问器
	if v, ok := e.buf.kw("tag", 0); !ok || v != "tech" {
		t.Fatalf("buf.kw(tag,0) = %q,%v; want tech,true", v, ok)
	}
	if _, ok := e.buf.kw("tag", 2); ok {
		t.Fatal("buf.kw(tag,2) 应为 false（无 tag）")
	}

	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	// 段侧 kw 列
	seg := e.segs[0]
	kr, ok := seg.Kw("tag")
	if !ok {
		t.Fatal("段应有 kw.tag 列")
	}
	for docID, want := range []string{"tech", "life"} {
		ord, ok := kr.Ord(uint32(docID))
		if !ok || kr.Term(ord) != want {
			t.Fatalf("kw doc %d = %q,%v; want %q", docID, kr.Term(ord), ok, want)
		}
	}
	if _, ok := kr.Ord(2); ok {
		t.Fatal("kw doc 2 应无值")
	}
	// v2 段倒排 positions 可读
	it, ok := seg.Postings("title", "a")
	if !ok {
		t.Fatal("Postings(title,a) 应存在")
	}
	if !it.Next() || it.Positions() == nil {
		t.Fatal("v2 段 Positions 应非 nil")
	}
	// 全字段 has 位：倒排字段也有标记
	if !seg.Has("title", 0) || !seg.Has("tag", 1) || seg.Has("tag", 2) {
		t.Fatal("v2 段全字段 has 位错误")
	}
}

// TestPhraseReopenReplay 重启回放路径：未 flush 的文档经 translog 回放后
// 短语查询可用（缓冲）；flush 后重启短语查询仍可用（v2 段）
func TestPhraseReopenReplay(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "replay")
	sch, err := schema.New([]schema.Field{{Name: "title", Type: "text"}})
	if err != nil {
		t.Fatal(err)
	}

	e, err := Open(dir, sch)
	if err != nil {
		t.Fatal(err)
	}
	mustIndex(t, e, "1", `{"title":"the quick brown fox"}`)
	mustIndex(t, e, "2", `{"title":"quick the brown"}`)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启（translog 回放进缓冲）：短语命中
	e2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := phraseSearch(t, e2, "title", "quick brown")
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("回放后短语 hits = %v, want [1]", hitIDs(res))
	}
	// flush 落段 → 再重启：短语从 v2 段命中
	if err := e2.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	e3, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e3.Close()
	res = phraseSearch(t, e3, "title", "quick brown")
	if res.Total != 1 || res.Hits[0].ID != "1" {
		t.Fatalf("段落盘后重启短语 hits = %v, want [1]", hitIDs(res))
	}
}
