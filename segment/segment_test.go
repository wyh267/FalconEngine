package segment

import (
	"path/filepath"
	"testing"
)

func buildTestSegment(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "seg-0")
	docs := []Doc{
		{
			ID:  "doc1",
			Raw: []byte(`{"title":"go 语言 入门","level":3}`),
			Terms: map[string][]string{
				"title": {"go", "语", "言", "入", "门"},
				"tag":   {"tech"},
			},
			Nums: map[string]int64{"level": 3},
		},
		{
			ID:  "doc2",
			Raw: []byte(`{"title":"go 进阶","level":5}`),
			Terms: map[string][]string{
				"title": {"go", "进", "阶"},
				"tag":   {"tech"},
			},
			Nums: map[string]int64{"level": 5},
		},
		{
			ID:  "doc3",
			Raw: []byte(`{"title":"烹饪 大全"}`),
			Terms: map[string][]string{
				"title": {"烹", "饪", "大", "全"},
				"tag":   {"life"},
			},
			Nums: map[string]int64{"level": 0},
		},
	}
	if err := Write(dir, 1, []string{"title", "tag"}, []string{"title"}, []string{"level"}, docs); err != nil {
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
	if err := Write(dir, 1, nil, nil, []string{"level"}, docs); err != nil {
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
