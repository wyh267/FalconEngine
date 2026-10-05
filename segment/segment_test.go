package segment

import (
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
)

// toks 便捷构造 token 序列（Position 取下标）
func toks(terms ...string) []plugin.Token {
	out := make([]plugin.Token, 0, len(terms))
	for i, term := range terms {
		out = append(out, plugin.Token{Term: term, Position: i})
	}
	return out
}

func buildTestSegment(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "seg-0")
	docs := []Doc{
		{
			ID:  "doc1",
			Raw: []byte(`{"title":"go 语言 入门","level":3}`),
			Terms: map[string][]plugin.Token{
				"title": toks("go", "语", "言", "入", "门"),
				"tag":   toks("tech"),
			},
			Nums: map[string]int64{"level": 3},
			Kws:  map[string]string{"tag": "tech"},
		},
		{
			ID:  "doc2",
			Raw: []byte(`{"title":"go 进阶","level":5}`),
			Terms: map[string][]plugin.Token{
				"title": toks("go", "进", "阶"),
				"tag":   toks("tech"),
			},
			Nums: map[string]int64{"level": 5},
			Kws:  map[string]string{"tag": "tech"},
		},
		{
			ID:  "doc3",
			Raw: []byte(`{"title":"烹饪 大全"}`),
			Terms: map[string][]plugin.Token{
				"title": toks("烹", "饪", "大", "全"),
				"tag":   toks("life"),
			},
			Nums: map[string]int64{"level": 0},
			Kws:  map[string]string{"tag": "life"},
		},
	}
	if err := Write(dir, 1, []string{"title", "tag"}, []string{"title"}, []string{"level"}, []string{"tag"}, docs); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	return dir
}

// collect 迭代完整条倒排，返回 (docID, freq) 对
func collect(t *testing.T, r *Reader, field, term string) ([]uint32, []uint32) {
	t.Helper()
	it, ok := r.Postings(field, term)
	if !ok {
		return nil, nil
	}
	var docs, freqs []uint32
	for it.Next() {
		docs = append(docs, it.DocID())
		freqs = append(freqs, it.Freq())
	}
	return docs, freqs
}

// collectPos 迭代完整条倒排，返回各 doc 的 positions
func collectPos(t *testing.T, r *Reader, field, term string) [][]uint32 {
	t.Helper()
	it, ok := r.Postings(field, term)
	if !ok {
		return nil
	}
	var out [][]uint32
	for it.Next() {
		out = append(out, append([]uint32(nil), it.Positions()...))
	}
	return out
}

func TestWriteAndOpen(t *testing.T) {
	dir := buildTestSegment(t)
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer r.Close()

	if r.DocCount() != 3 || r.LiveCount() != 3 {
		t.Fatalf("DocCount=%d LiveCount=%d, want 3/3", r.DocCount(), r.LiveCount())
	}

	// 外部 ID -> docID
	id, ok := r.LocalID("doc2")
	if !ok || id != 1 {
		t.Fatalf("LocalID(doc2) = %d, %v; want 1, true", id, ok)
	}
	if _, ok := r.LocalID("nope"); ok {
		t.Fatal("LocalID(nope) should not exist")
	}
	if r.ID(0) != "doc1" {
		t.Fatalf("ID(0) = %q, want doc1", r.ID(0))
	}

	// 倒排查询
	docs, _ := collect(t, r, "title", "go")
	if len(docs) != 2 || docs[0] != 0 || docs[1] != 1 {
		t.Fatalf("Postings(title, go) docs = %v, want [0 1]", docs)
	}
	if df := r.DocFreq("title", "go"); df != 2 {
		t.Fatalf("DocFreq(title, go) = %d, want 2", df)
	}
	if _, ok := r.Postings("title", "不存在"); ok {
		t.Fatal("Postings(title, 不存在) should not exist")
	}

	// 词频
	_, freqs := collect(t, r, "title", "语")
	if len(freqs) != 1 || freqs[0] != 1 {
		t.Fatalf("tf = %v, want [1]", freqs)
	}

	// 原文
	raw, err := r.Stored(2)
	if err != nil || string(raw) != `{"title":"烹饪 大全"}` {
		t.Fatalf("Stored(2) = %s, %v", raw, err)
	}

	// number 正排
	v, ok := r.Num("level", 1)
	if !ok || v != 5 {
		t.Fatalf("Num(level,1) = %d, %v; want 5, true", v, ok)
	}
	if _, ok := r.Num("nope", 0); ok {
		t.Fatal("Num(nope) should not exist")
	}

	// 段序号
	if r.Seq() != 1 {
		t.Fatalf("Seq() = %d, want 1", r.Seq())
	}

	// norms：三篇文档 title 的 term 数分别为 5/3/4
	wantNorms := []int64{5, 3, 4}
	for i, want := range wantNorms {
		if got := r.Norm("title", uint32(i)); got != want {
			t.Fatalf("Norm(title,%d) = %d, want %d", i, got, want)
		}
	}
	if got := r.Norm("tag", 0); got != 0 {
		t.Fatalf("keyword 字段不应有 norm, got %d", got)
	}
	// avgdl = (5+3+4)/3 = 4
	if got := r.AvgDL("title"); got != 4.0 {
		t.Fatalf("AvgDL(title) = %v, want 4", got)
	}
}

// TestNumPresence 验证 number 字段的存在性标记：无该字段值的文档 Num 返回 false
func TestNumPresence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "seg-0")
	docs := []Doc{
		{ID: "a", Raw: []byte(`{}`), Nums: map[string]int64{"level": 7}},
		{ID: "b", Raw: []byte(`{}`)}, // 无 level 值
	}
	if err := Write(dir, 1, nil, nil, []string{"level"}, nil, docs); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer r.Close()

	if v, ok := r.Num("level", 0); !ok || v != 7 {
		t.Fatalf("Num(level,0) = %d, %v; want 7, true", v, ok)
	}
	if _, ok := r.Num("level", 1); ok {
		t.Fatal("Num(level,1) 文档无该字段值，应返回 false")
	}
}

func TestDeletePersistence(t *testing.T) {
	dir := buildTestSegment(t)

	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	if err := r.Delete(1); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	// 重复删除应幂等
	if err := r.Delete(1); err != nil {
		t.Fatalf("re-Delete error: %v", err)
	}
	r.Close()

	// 重新打开后删除标记仍在
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open error: %v", err)
	}
	defer r2.Close()
	if !r2.Deleted(1) || r2.Deleted(0) {
		t.Fatalf("Deleted state wrong: %v %v", r2.Deleted(1), r2.Deleted(0))
	}
	if r2.LiveCount() != 2 {
		t.Fatalf("LiveCount = %d, want 2", r2.LiveCount())
	}
}

// TestFormatV2Markers v2 段：meta 版本号、倒排 positions、全字段 has 位、keyword 列
func TestFormatV2Markers(t *testing.T) {
	dir := buildTestSegment(t)
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer r.Close()

	// 版本号
	if r.m.Version != segmentFormatV2 {
		t.Fatalf("meta.Version = %d, want %d", r.m.Version, segmentFormatV2)
	}

	// positions：title "go" 在 doc0/doc1 的位置都是 0；"语" 在 doc0 的位置是 1
	pos := collectPos(t, r, "title", "go")
	if len(pos) != 2 || pos[0][0] != 0 || pos[1][0] != 0 {
		t.Fatalf("positions(title, go) = %v, want [[0] [0]]", pos)
	}
	pos = collectPos(t, r, "title", "语")
	if len(pos) != 1 || len(pos[0]) != 1 || pos[0][0] != 1 {
		t.Fatalf("positions(title, 语) = %v, want [[1]]", pos)
	}

	// 全字段 has 位（v2 起覆盖倒排字段）
	if !r.Has("title", 0) || !r.Has("tag", 2) || !r.Has("level", 0) {
		t.Fatal("Has 应对有值字段返回 true")
	}
	// doc3 的 level 显式置 0（有值）；字段完全缺失的情形见 TestNumPresence/TestKeywordEmptyMeansAbsent
	if !r.Has("level", 2) {
		t.Fatal("Has(level, 2) 应为 true（doc3 显式置 0）")
	}
	if r.Has("不存在字段", 0) {
		t.Fatal("Has(不存在字段) 应为 false")
	}

	// keyword 列
	kr, ok := r.Kw("tag")
	if !ok {
		t.Fatal("Kw(tag) 应存在")
	}
	if kr.Len() != 3 || kr.NumOrds() != 2 {
		t.Fatalf("kw Len=%d NumOrds=%d, want 3/2", kr.Len(), kr.NumOrds())
	}
	for docID, want := range []string{"tech", "tech", "life"} {
		ord, ok := kr.Ord(uint32(docID))
		if !ok {
			t.Fatalf("kw doc %d 应有值", docID)
		}
		if got := kr.Term(ord); got != want {
			t.Fatalf("kw doc %d = %q, want %q", docID, got, want)
		}
	}
	if ord, ok := kr.Lookup("life"); !ok || kr.Term(ord) != "life" {
		t.Fatal("kw Lookup(life) 失败")
	}
	if _, ok := r.Kw("title"); ok {
		t.Fatal("Kw(title) 不应存在（text 字段无 kw 列）")
	}
}

// TestKeywordEmptyMeansAbsent keyword 空串视为无值
func TestKeywordEmptyMeansAbsent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "seg-0")
	docs := []Doc{
		{ID: "a", Raw: []byte(`{}`), Kws: map[string]string{"tag": "x"}},
		{ID: "b", Raw: []byte(`{}`), Kws: map[string]string{"tag": ""}}, // 空串 = 无值
		{ID: "c", Raw: []byte(`{}`)},                                    // 无该字段
	}
	if err := Write(dir, 1, nil, nil, nil, []string{"tag"}, docs); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer r.Close()

	kr, ok := r.Kw("tag")
	if !ok {
		t.Fatal("Kw(tag) 应存在")
	}
	if _, ok := kr.Ord(0); !ok {
		t.Fatal("doc0 应有值")
	}
	if _, ok := kr.Ord(1); ok {
		t.Fatal("doc1 空串应视为无值")
	}
	if _, ok := kr.Ord(2); ok {
		t.Fatal("doc2 无字段应视为无值")
	}
	// has 位与 kw 列一致
	if !r.Has("tag", 0) || r.Has("tag", 1) || r.Has("tag", 2) {
		t.Fatal("has(tag) 应与 kw 列一致")
	}
}
