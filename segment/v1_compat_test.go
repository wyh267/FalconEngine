// v1 历史格式段的读兼容测试（v1 段由 segmenttest.WriteV1 固化生成）
package segment_test

import (
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/segment"
	"github.com/FalconEngine/falcon/segment/segmenttest"
)

// toks 便捷构造 token 序列（Position 取下标）
func toks(terms ...string) []plugin.Token {
	out := make([]plugin.Token, 0, len(terms))
	for i, term := range terms {
		out = append(out, plugin.Token{Term: term, Position: i})
	}
	return out
}

// TestV1SegmentCompat v1 段应可读：doc/freq 正确、Positions 为 nil、
// 无 kw 列、has 标记仅覆盖 number 类字段
func TestV1SegmentCompat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "seg-1")
	docs := []segment.Doc{
		{
			ID:    "doc1",
			Raw:   []byte(`{"title":"go 语言","level":3}`),
			Terms: map[string][]plugin.Token{"title": toks("go", "语", "言")},
			Nums:  map[string]int64{"level": 3},
		},
		{
			ID:    "doc2",
			Raw:   []byte(`{"title":"go 进阶"}`),
			Terms: map[string][]plugin.Token{"title": toks("go", "进", "阶")},
			// 无 level 值
		},
	}
	if err := segmenttest.WriteV1(dir, 1, []string{"title"}, []string{"title"}, []string{"level"}, docs); err != nil {
		t.Fatalf("WriteV1 error: %v", err)
	}

	r, err := segment.Open(dir)
	if err != nil {
		t.Fatalf("Open v1 段失败: %v", err)
	}
	defer r.Close()

	// 基本倒排可读
	it, ok := r.Postings("title", "go")
	if !ok {
		t.Fatal("v1 段 Postings(title, go) 应存在")
	}
	var gotDocs, gotFreqs []uint32
	for it.Next() {
		if it.Positions() != nil {
			t.Fatalf("v1 段 Positions 应恒为 nil, got %v", it.Positions())
		}
		gotDocs = append(gotDocs, it.DocID())
		gotFreqs = append(gotFreqs, it.Freq())
	}
	if len(gotDocs) != 2 || gotDocs[0] != 0 || gotDocs[1] != 1 {
		t.Fatalf("v1 段倒排 docs = %v, want [0 1]", gotDocs)
	}
	if gotFreqs[0] != 1 || gotFreqs[1] != 1 {
		t.Fatalf("v1 段倒排 freqs = %v, want [1 1]", gotFreqs)
	}
	if df := r.DocFreq("title", "go"); df != 2 {
		t.Fatalf("v1 段 DocFreq = %d, want 2", df)
	}

	// number 正排与存在性标记
	if v, ok := r.Num("level", 0); !ok || v != 3 {
		t.Fatalf("v1 段 Num(level,0) = %d,%v; want 3,true", v, ok)
	}
	if _, ok := r.Num("level", 1); ok {
		t.Fatal("v1 段 Num(level,1) 应为 false")
	}
	if !r.Has("level", 0) || r.Has("level", 1) {
		t.Fatal("v1 段 has(level) 应与 number 存在性一致")
	}
	// v1 段倒排字段无 has 标记
	if r.Has("title", 0) {
		t.Fatal("v1 段倒排字段应无 has 标记")
	}
	// v1 段无 kw 列
	if _, ok := r.Kw("title"); ok {
		t.Fatal("v1 段应无 kw 列")
	}
	// 原文与 norms
	if raw, err := r.Stored(1); err != nil || string(raw) != `{"title":"go 进阶"}` {
		t.Fatalf("v1 段 Stored(1) = %s, %v", raw, err)
	}
	if r.Norm("title", 0) != 3 || r.AvgDL("title") != 3.0 {
		t.Fatalf("v1 段 norm/avgdl 错误: %d %v", r.Norm("title", 0), r.AvgDL("title"))
	}
}
