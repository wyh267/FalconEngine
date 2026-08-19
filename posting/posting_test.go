package posting

import (
	"math/rand"
	"testing"

	"github.com/FalconEngine/falcon/storage"
)

// docFreq 测试用倒排记录
type wantPosting struct {
	docID uint32
	freq  uint32
}

// writeReadRoundTrip 把 data 写入内存存储并重新打开为 FieldReader
func writeReadRoundTrip(t *testing.T, data map[string][]wantPosting) *FieldReader {
	t.Helper()
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	for term, list := range data {
		for _, p := range list {
			if err := w.Add(term, p.docID, p.freq); err != nil {
				t.Fatalf("Add(%q, %d, %d) 失败: %v", term, p.docID, p.freq, err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish 失败: %v", err)
	}
	r, err := OpenFieldReader(dicS, pstS)
	if err != nil {
		t.Fatalf("OpenFieldReader 失败: %v", err)
	}
	return r
}

// iterateAll 用 Next 遍历整个迭代器，收集结果
func iterateAll(it Iterator) []wantPosting {
	var got []wantPosting
	for it.Next() {
		got = append(got, wantPosting{docID: it.DocID(), freq: it.Freq()})
	}
	return got
}

func equalPostings(a, b []wantPosting) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRandomRoundTrip 随机数据对拍：写入后逐 term 用 Next 遍历，与原始数据对比
func TestRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := make(map[string][]wantPosting)
	// 覆盖多种链长：单 doc、块内、跨块、块边界
	for _, n := range []int{1, 2, 127, 128, 129, 255, 256, 257, 1000} {
		term := string(rune('a'+len(data))) + "term"
		list := make([]wantPosting, 0, n)
		docID := uint32(rng.Intn(3)) // 首 doc 可以为 0
		for i := 0; i < n; i++ {
			docID += uint32(rng.Intn(5)) + 1
			list = append(list, wantPosting{docID: docID, freq: uint32(rng.Intn(100) + 1)})
		}
		data[term] = list
	}
	// 再加一批随机 term
	for i := 0; i < 50; i++ {
		term := string([]byte{byte(rng.Intn(26) + 'a'), byte(rng.Intn(26) + 'a'), byte(i)})
		n := rng.Intn(600)
		list := make([]wantPosting, 0, n)
		docID := uint32(0)
		for j := 0; j < n; j++ {
			docID += uint32(rng.Intn(10)) + 1
			list = append(list, wantPosting{docID: docID, freq: uint32(rng.Intn(1000))})
		}
		data[term] = list
	}

	r := writeReadRoundTrip(t, data)
	for term, want := range data {
		if got := r.DocFreq(term); got != len(want) {
			t.Fatalf("term %q DocFreq = %d, 期望 %d", term, got, len(want))
		}
		it, ok := r.Iterator(term)
		if !ok {
			t.Fatalf("term %q 应存在", term)
		}
		got := iterateAll(it)
		if !equalPostings(got, want) {
			t.Fatalf("term %q 遍历结果不一致: got %d 条, want %d 条", term, len(got), len(want))
		}
		// 耗尽后继续 Next 仍应返回 false
		if it.Next() {
			t.Fatalf("term %q 耗尽后 Next 应返回 false", term)
		}
	}
}

// TestAdvanceRandom 随机 Advance 对拍：每次 Advance 后与"从当前位置线性扫描
// 找第一个 docID >= target"的结果对比，交替混入 Next
func TestAdvanceRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	list := make([]wantPosting, 0, 2000)
	docID := uint32(0)
	for i := 0; i < 2000; i++ {
		docID += uint32(rng.Intn(4)) + 1
		list = append(list, wantPosting{docID: docID, freq: uint32(i)})
	}
	r := writeReadRoundTrip(t, map[string][]wantPosting{"t": list})

	it, ok := r.Iterator("t")
	if !ok {
		t.Fatal("term t 应存在")
	}
	pos := 0 // 线性扫描的当前位置（list 下标）
	for step := 0; step < 500; step++ {
		if rng.Intn(3) == 0 {
			// 混入 Next
			wantOK := pos < len(list)
			gotOK := it.Next()
			if gotOK != wantOK {
				t.Fatalf("step %d: Next() = %v, 期望 %v", step, gotOK, wantOK)
			}
			if wantOK {
				if it.DocID() != list[pos].docID || it.Freq() != list[pos].freq {
					t.Fatalf("step %d: Next 得到 (%d,%d), 期望 %v", step, it.DocID(), it.Freq(), list[pos])
				}
				pos++
			}
			continue
		}
		target := uint32(rng.Intn(int(docID) + 10))
		// 线性扫描：从 pos 起找第一个 docID >= target
		wantIdx := -1
		for i := pos; i < len(list); i++ {
			if list[i].docID >= target {
				wantIdx = i
				break
			}
		}
		gotOK := it.Advance(target)
		if wantIdx < 0 {
			if gotOK {
				t.Fatalf("step %d: Advance(%d) 应返回 false，却得到 docID=%d", step, target, it.DocID())
			}
			pos = len(list)
			continue
		}
		if !gotOK {
			t.Fatalf("step %d: Advance(%d) 应命中 %v", step, target, list[wantIdx])
		}
		if it.DocID() != list[wantIdx].docID || it.Freq() != list[wantIdx].freq {
			t.Fatalf("step %d: Advance(%d) 得到 (%d,%d), 期望 %v",
				step, target, it.DocID(), it.Freq(), list[wantIdx])
		}
		pos = wantIdx + 1
	}
}

// TestAdvanceSkipBlocks Advance 应能整块跳过：target 落在后面的块时结果与线性扫描一致
func TestAdvanceSkipBlocks(t *testing.T) {
	// 1000 个 doc，docID = i*2，跨 8 块
	list := make([]wantPosting, 0, 1000)
	for i := 0; i < 1000; i++ {
		list = append(list, wantPosting{docID: uint32(i * 2), freq: uint32(i + 1)})
	}
	r := writeReadRoundTrip(t, map[string][]wantPosting{"t": list})

	it, ok := r.Iterator("t")
	if !ok {
		t.Fatal("term t 应存在")
	}
	// 直接跳到第 6 块附近（docID = 800*2? 第 6 块起始 doc 下标 768，docID=1536）
	if !it.Advance(1537) {
		t.Fatal("Advance(1537) 应命中")
	}
	if it.DocID() != 1538 { // 第一个 >= 1537 的 docID 是 1538（下标 769）
		t.Fatalf("Advance(1537) docID = %d, 期望 1538", it.DocID())
	}
	if it.Freq() != 770 {
		t.Fatalf("Advance(1537) freq = %d, 期望 770", it.Freq())
	}
	// 继续 Next 应紧接其后
	if !it.Next() || it.DocID() != 1540 {
		t.Fatalf("Advance 后 Next 应为 1540，得到 %d", it.DocID())
	}
	// 超过最大值应返回 false
	if it.Advance(1999) {
		t.Fatal("Advance(1999) 应返回 false")
	}
	if it.Next() {
		t.Fatal("耗尽后 Next 应返回 false")
	}
}

// TestTermNotFound 不存在的 term
func TestTermNotFound(t *testing.T) {
	r := writeReadRoundTrip(t, map[string][]wantPosting{
		"apple": {{docID: 1, freq: 2}},
	})
	if _, ok := r.Iterator("banana"); ok {
		t.Fatal("不存在的 term 应返回 false")
	}
	if got := r.DocFreq("banana"); got != 0 {
		t.Fatalf("不存在的 term DocFreq = %d, 期望 0", got)
	}
}

// TestEdgeCases 边界：空 term、单 doc、docID=0、恰好块边界（128 的倍数）
func TestEdgeCases(t *testing.T) {
	data := map[string][]wantPosting{
		"":        {{docID: 0, freq: 1}},                                  // 空 term + docID=0
		"single":  {{docID: 5, freq: 9}},                                  // 单 doc
		"b128":    makeSeq(128, 3),                                        // 恰好一块
		"b256":    makeSeq(256, 1),                                        // 恰好两块
		"b128x3":  makeSeq(384, 7),                                        // 恰好三块
		"big":     makeSeq(128*10+1, 1),                                   // 10 整块 + 1
		"maxfreq": {{docID: 100, freq: 1 << 31}},                          // 大 freq
		"bigdoc":  {{docID: 1 << 30, freq: 1}, {docID: 1 << 31, freq: 2}}, // 大 docID 跨度
	}
	r := writeReadRoundTrip(t, data)
	for term, want := range data {
		if got := r.DocFreq(term); got != len(want) {
			t.Fatalf("term %q DocFreq = %d, 期望 %d", term, got, len(want))
		}
		it, ok := r.Iterator(term)
		if !ok {
			t.Fatalf("term %q 应存在", term)
		}
		if got := iterateAll(it); !equalPostings(got, want) {
			t.Fatalf("term %q 遍历结果不一致", term)
		}
		// 块边界 term 也验证 Advance
		it2, _ := r.Iterator(term)
		last := want[len(want)-1].docID
		if !it2.Advance(last) || it2.DocID() != last {
			t.Fatalf("term %q Advance(%d) 应命中最后一个 doc", term, last)
		}
		if it2.Advance(last + 1) {
			t.Fatalf("term %q Advance 越过末尾应返回 false", term)
		}
	}
}

// makeSeq 生成 n 个 doc，docID 从 0 开始步长为 step
func makeSeq(n int, step uint32) []wantPosting {
	list := make([]wantPosting, 0, n)
	for i := 0; i < n; i++ {
		list = append(list, wantPosting{docID: uint32(i) * step, freq: uint32(i%13 + 1)})
	}
	return list
}

// TestAddNotIncreasing 同一 term 的 docID 非递增应报错
func TestAddNotIncreasing(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	if err := w.Add("t", 5, 1); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}
	if err := w.Add("t", 5, 1); err == nil {
		t.Fatal("相同 docID 应报错")
	}
	if err := w.Add("t", 3, 1); err == nil {
		t.Fatal("递减 docID 应报错")
	}
	if err := w.Add("t", 6, 1); err != nil {
		t.Fatalf("递增 docID 不应报错: %v", err)
	}
	// 不同 term 互不影响
	if err := w.Add("u", 1, 1); err != nil {
		t.Fatalf("不同 term 的 docID 应互不影响: %v", err)
	}
}

// TestFinishTwice 重复 Finish 应报错
func TestFinishTwice(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	if err := w.Finish(); err != nil {
		t.Fatalf("首次 Finish 失败: %v", err)
	}
	if err := w.Finish(); err == nil {
		t.Fatal("重复 Finish 应报错")
	}
	if err := w.Add("t", 1, 1); err == nil {
		t.Fatal("Finish 后 Add 应报错")
	}
}

// TestFileRoundTrip 用真实文件后端验证回环（独立打开读取）
func TestFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dicPath := dir + "/test.dic"
	pstPath := dir + "/test.pst"

	dicW, err := storage.NewFileWriter(dicPath)
	if err != nil {
		t.Fatalf("NewFileWriter dic 失败: %v", err)
	}
	pstW, err := storage.NewFileWriter(pstPath)
	if err != nil {
		t.Fatalf("NewFileWriter pst 失败: %v", err)
	}
	data := map[string][]wantPosting{
		"hello": makeSeq(300, 2),
		"world": {{docID: 0, freq: 3}, {docID: 128, freq: 4}},
	}
	w := NewFieldWriter(dicW, pstW)
	for term, list := range data {
		for _, p := range list {
			if err := w.Add(term, p.docID, p.freq); err != nil {
				t.Fatalf("Add 失败: %v", err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish 失败: %v", err)
	}
	dicW.Close()
	pstW.Close()

	// 用 mmap 独立重新打开
	dicR, err := storage.NewMmapReader(dicPath)
	if err != nil {
		t.Fatalf("NewMmapReader dic 失败: %v", err)
	}
	defer dicR.Close()
	pstR, err := storage.NewMmapReader(pstPath)
	if err != nil {
		t.Fatalf("NewMmapReader pst 失败: %v", err)
	}
	defer pstR.Close()

	r, err := OpenFieldReader(dicR, pstR)
	if err != nil {
		t.Fatalf("OpenFieldReader 失败: %v", err)
	}
	for term, want := range data {
		it, ok := r.Iterator(term)
		if !ok {
			t.Fatalf("term %q 应存在", term)
		}
		if got := iterateAll(it); !equalPostings(got, want) {
			t.Fatalf("term %q 文件回环结果不一致", term)
		}
	}
}
