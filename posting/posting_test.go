package posting

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/dict"
	"github.com/FalconEngine/falcon/storage"
)

// docFreq 测试用倒排记录
type wantPosting struct {
	docID     uint32
	positions []uint32 // freq = len(positions)
}

// freq 由 positions 推导
func (p wantPosting) freq() uint32 { return uint32(len(p.positions)) }

// makePositions 生成 n 个严格递增的位置
func makePositions(rng *rand.Rand, n int) []uint32 {
	pos := make([]uint32, 0, n)
	cur := uint32(0)
	for i := 0; i < n; i++ {
		cur += uint32(rng.Intn(3)) + 1
		pos = append(pos, cur)
	}
	return pos
}

// writeReadRoundTrip 把 data 以 v2 格式写入内存存储并重新打开为 FieldReader
func writeReadRoundTrip(t *testing.T, data map[string][]wantPosting) *FieldReader {
	t.Helper()
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	for term, list := range data {
		for _, p := range list {
			if err := w.Add(term, p.docID, p.positions); err != nil {
				t.Fatalf("Add(%q, %d, %v) 失败: %v", term, p.docID, p.positions, err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish 失败: %v", err)
	}
	r, err := OpenFieldReader(dicS, pstS, FormatV2)
	if err != nil {
		t.Fatalf("OpenFieldReader 失败: %v", err)
	}
	return r
}

// iterateAll 用 Next 遍历整个迭代器，收集结果（positions 拷贝持有，避免复用缓冲失效）
func iterateAll(it Iterator) []wantPosting {
	var got []wantPosting
	for it.Next() {
		got = append(got, wantPosting{
			docID:     it.DocID(),
			positions: append([]uint32(nil), it.Positions()...),
		})
	}
	return got
}

func equalPostings(a, b []wantPosting) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].docID != b[i].docID || a[i].freq() != b[i].freq() {
			return false
		}
		if len(a[i].positions) != len(b[i].positions) {
			return false
		}
		for j := range a[i].positions {
			if a[i].positions[j] != b[i].positions[j] {
				return false
			}
		}
	}
	return true
}

// TestRandomRoundTrip 随机数据对拍：写入后逐 term 用 Next 遍历，与原始数据对比
// （docID、freq、positions 全字段对拍）
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
			list = append(list, wantPosting{docID: docID, positions: makePositions(rng, rng.Intn(100)+1)})
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
			list = append(list, wantPosting{docID: docID, positions: makePositions(rng, rng.Intn(1000)+1)})
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
		if it.Positions() != nil {
			t.Fatalf("term %q 耗尽后 Positions 应返回 nil", term)
		}
	}
}

// TestAdvanceRandom 随机 Advance 对拍：每次 Advance 后与"从当前位置线性扫描
// 找第一个 docID >= target"的结果对比，交替混入 Next；同时校验 positions
func TestAdvanceRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	list := make([]wantPosting, 0, 2000)
	docID := uint32(0)
	for i := 0; i < 2000; i++ {
		docID += uint32(rng.Intn(4)) + 1
		list = append(list, wantPosting{docID: docID, positions: makePositions(rng, i%50+1)})
	}
	r := writeReadRoundTrip(t, map[string][]wantPosting{"t": list})

	it, ok := r.Iterator("t")
	if !ok {
		t.Fatal("term t 应存在")
	}
	checkCur := func(step int, want wantPosting) {
		t.Helper()
		if it.DocID() != want.docID || it.Freq() != want.freq() {
			t.Fatalf("step %d: 得到 (doc=%d,freq=%d), 期望 (doc=%d,freq=%d)",
				step, it.DocID(), it.Freq(), want.docID, want.freq())
		}
		gotPos := it.Positions()
		if len(gotPos) != len(want.positions) {
			t.Fatalf("step %d: positions 长度 = %d, 期望 %d", step, len(gotPos), len(want.positions))
		}
		for j := range gotPos {
			if gotPos[j] != want.positions[j] {
				t.Fatalf("step %d: positions[%d] = %d, 期望 %d", step, j, gotPos[j], want.positions[j])
			}
		}
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
				checkCur(step, list[pos])
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
		checkCur(step, list[wantIdx])
		pos = wantIdx + 1
	}
}

// TestAdvanceSkipBlocks Advance 应能整块跳过：target 落在后面的块时结果与线性扫描一致
func TestAdvanceSkipBlocks(t *testing.T) {
	// 1000 个 doc，docID = i*2，跨 8 块
	list := make([]wantPosting, 0, 1000)
	for i := 0; i < 1000; i++ {
		list = append(list, wantPosting{docID: uint32(i * 2), positions: []uint32{1, uint32(i + 2)}})
	}
	r := writeReadRoundTrip(t, map[string][]wantPosting{"t": list})

	it, ok := r.Iterator("t")
	if !ok {
		t.Fatal("term t 应存在")
	}
	// 直接跳到第 6 块附近（第 6 块起始 doc 下标 768，docID=1536）
	if !it.Advance(1537) {
		t.Fatal("Advance(1537) 应命中")
	}
	if it.DocID() != 1538 { // 第一个 >= 1537 的 docID 是 1538（下标 769）
		t.Fatalf("Advance(1537) docID = %d, 期望 1538", it.DocID())
	}
	if it.Freq() != 2 {
		t.Fatalf("Advance(1537) freq = %d, 期望 2", it.Freq())
	}
	if pos := it.Positions(); len(pos) != 2 || pos[0] != 1 || pos[1] != 771 {
		t.Fatalf("Advance(1537) positions = %v, 期望 [1 771]", pos)
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
		"apple": {{docID: 1, positions: []uint32{0, 3}}},
	})
	if _, ok := r.Iterator("banana"); ok {
		t.Fatal("不存在的 term 应返回 false")
	}
	if got := r.DocFreq("banana"); got != 0 {
		t.Fatalf("不存在的 term DocFreq = %d, 期望 0", got)
	}
}

// TestEdgeCases 边界：空 term、单 doc、docID=0、恰好块边界（128 的倍数）、大 freq
func TestEdgeCases(t *testing.T) {
	data := map[string][]wantPosting{
		"":        {{docID: 0, positions: []uint32{0}}},                                                    // 空 term + docID=0
		"single":  {{docID: 5, positions: []uint32{2, 5, 9}}},                                              // 单 doc 多位置
		"b128":    makeSeq(128, 3),                                                                         // 恰好一块
		"b256":    makeSeq(256, 1),                                                                         // 恰好两块
		"b128x3":  makeSeq(384, 7),                                                                         // 恰好三块
		"big":     makeSeq(128*10+1, 1),                                                                    // 10 整块 + 1
		"maxfreq": {{docID: 100, positions: makePositions(rand.New(rand.NewSource(1)), 1<<16)}},            // 大 freq
		"bigdoc":  {{docID: 1 << 30, positions: []uint32{1}}, {docID: 1 << 31, positions: []uint32{0, 1}}}, // 大 docID 跨度
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

// makeSeq 生成 n 个 doc，docID 从 0 开始步长为 step；positions 为 [step, step+1, ...] 式递增序列
func makeSeq(n int, step uint32) []wantPosting {
	list := make([]wantPosting, 0, n)
	for i := 0; i < n; i++ {
		npos := i%13 + 1
		pos := make([]uint32, npos)
		for k := range pos {
			pos[k] = uint32(k)
		}
		list = append(list, wantPosting{docID: uint32(i) * step, positions: pos})
	}
	return list
}

// TestAddNotIncreasing 同一 term 的 docID 非递增应报错
func TestAddNotIncreasing(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	if err := w.Add("t", 5, []uint32{0}); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}
	if err := w.Add("t", 5, []uint32{0}); err == nil {
		t.Fatal("相同 docID 应报错")
	}
	if err := w.Add("t", 3, []uint32{0}); err == nil {
		t.Fatal("递减 docID 应报错")
	}
	if err := w.Add("t", 6, []uint32{0}); err != nil {
		t.Fatalf("递增 docID 不应报错: %v", err)
	}
	// 不同 term 互不影响
	if err := w.Add("u", 1, []uint32{0}); err != nil {
		t.Fatalf("不同 term 的 docID 应互不影响: %v", err)
	}
}

// TestAddEmptyPositions positions 为空应报错（freq 由 positions 推导，空位置无意义）
func TestAddEmptyPositions(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	if err := w.Add("t", 1, nil); err == nil {
		t.Fatal("nil positions 应报错")
	}
	if err := w.Add("t", 1, []uint32{}); err == nil {
		t.Fatal("空 positions 应报错")
	}
}

// TestAddPositionsNotIncreasing positions 非严格递增应报错
func TestAddPositionsNotIncreasing(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	w := NewFieldWriter(dicS, pstS)
	if err := w.Add("t", 1, []uint32{3, 3}); err == nil {
		t.Fatal("重复位置应报错")
	}
	if err := w.Add("t", 1, []uint32{5, 2}); err == nil {
		t.Fatal("递减位置应报错")
	}
	if err := w.Add("t", 1, []uint32{2, 5}); err != nil {
		t.Fatalf("递增位置不应报错: %v", err)
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
	if err := w.Add("t", 1, []uint32{0}); err == nil {
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
		"world": {{docID: 0, positions: []uint32{0, 1, 2}}, {docID: 128, positions: []uint32{4}}},
	}
	w := NewFieldWriter(dicW, pstW)
	for term, list := range data {
		for _, p := range list {
			if err := w.Add(term, p.docID, p.positions); err != nil {
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

	r, err := OpenFieldReader(dicR, pstR, FormatV2)
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

// writeV1ForTest 以 v1 历史格式（无 flags、无 positions）写入倒排，供读兼容测试使用。
// 编码逻辑与 v1 writer 一致：[uvarint docFreq][8B 块索引偏移][块数据][块索引]，
// 块内记录为 [uvarint docDelta][uvarint freq]。
func writeV1ForTest(t *testing.T, pstW interface {
	WriteUvarint(uint64) error
	WriteUint64(uint64) error
	WriteBytes([]byte) (int64, error)
	Offset() int64
}, list []wantPosting) int64 {
	t.Helper()
	var data, index bytes.Buffer
	prev := uint64(0)
	for start := 0; start < len(list); start += BlockSize {
		end := min(start+BlockSize, len(list))
		appendUvarint(&index, uint64(list[end-1].docID))
		appendUvarint(&index, uint64(data.Len()))
		for _, p := range list[start:end] {
			appendUvarint(&data, uint64(p.docID)-prev)
			prev = uint64(p.docID)
			appendUvarint(&data, uint64(len(p.positions)))
		}
	}
	segOff := pstW.Offset()
	headerLen := int64(uvarintLen(uint64(len(list)))) + 8
	indexOff := segOff + headerLen + int64(data.Len())
	if err := pstW.WriteUvarint(uint64(len(list))); err != nil {
		t.Fatal(err)
	}
	if err := pstW.WriteUint64(uint64(indexOff)); err != nil {
		t.Fatal(err)
	}
	if _, err := pstW.WriteBytes(data.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := pstW.WriteBytes(index.Bytes()); err != nil {
		t.Fatal(err)
	}
	return segOff
}

// TestV1ReadCompat v1 格式段应能正常读 doc/freq，Positions 恒为 nil
func TestV1ReadCompat(t *testing.T) {
	dicS := storage.NewMemoryStore()
	pstS := storage.NewMemoryStore()
	data := map[string][]wantPosting{
		"go":  makeSeq(300, 2),
		"rpc": {{docID: 1, positions: []uint32{0, 2}}, {docID: 9, positions: []uint32{1}}},
	}
	db := dict.NewBuilder(dicS)
	for _, term := range []string{"go", "rpc"} { // 字典须按序写入
		segOff := writeV1ForTest(t, pstS, data[term])
		if err := db.Add(term, uint64(segOff)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFieldReader(dicS, pstS, FormatV1)
	if err != nil {
		t.Fatalf("OpenFieldReader(v1) 失败: %v", err)
	}
	for term, want := range data {
		if got := r.DocFreq(term); got != len(want) {
			t.Fatalf("v1 term %q DocFreq = %d, 期望 %d", term, got, len(want))
		}
		it, ok := r.Iterator(term)
		if !ok {
			t.Fatalf("v1 term %q 应存在", term)
		}
		var got []wantPosting
		for it.Next() {
			if it.Positions() != nil {
				t.Fatalf("v1 迭代 Positions 应恒为 nil, got %v", it.Positions())
			}
			got = append(got, wantPosting{docID: it.DocID(), positions: make([]uint32, it.Freq())})
		}
		// v1 只对拍 docID 与 freq（freq = len(positions)，具体位置值不存在）
		if len(got) != len(want) {
			t.Fatalf("v1 term %q 条数 = %d, 期望 %d", term, len(got), len(want))
		}
		for i := range got {
			if got[i].docID != want[i].docID || got[i].freq() != want[i].freq() {
				t.Fatalf("v1 term %q 第 %d 条 = (doc=%d,freq=%d), 期望 (doc=%d,freq=%d)",
					term, i, got[i].docID, got[i].freq(), want[i].docID, want[i].freq())
			}
		}
		// Advance 在 v1 上同样可用
		it2, _ := r.Iterator(term)
		last := want[len(want)-1].docID
		if !it2.Advance(last) || it2.DocID() != last {
			t.Fatalf("v1 term %q Advance(%d) 应命中最后一个 doc", term, last)
		}
	}
}

// TestCorruptBlockData 损坏的块数据应使迭代失败（返回 false）而非 panic 或产出错误数据
func TestCorruptBlockData(t *testing.T) {
	write := func() (dicS, pstS *storage.MemoryStore) {
		dicS = storage.NewMemoryStore()
		pstS = storage.NewMemoryStore()
		w := NewFieldWriter(dicS, pstS)
		for i := 0; i < 200; i++ {
			if err := w.Add("t", uint32(i), []uint32{0, 2, 5}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		return dicS, pstS
	}

	corruptAndRead := func(mutate func(b []byte)) {
		t.Helper()
		dicS, pstS := write()
		raw, err := pstS.ReadBytes(0, pstS.Len())
		if err != nil {
			t.Fatal(err)
		}
		mutated := append([]byte(nil), raw...)
		mutate(mutated)
		badS := storage.NewMemoryStore()
		if _, err := badS.Write(mutated); err != nil {
			t.Fatal(err)
		}
		r, err := OpenFieldReader(dicS, badS, FormatV2)
		if err != nil {
			t.Fatalf("OpenFieldReader 失败: %v", err)
		}
		it, ok := r.Iterator("t")
		if !ok {
			return // 头部损坏导致 term 不可读，可接受
		}
		// 迭代应最终失败或完整读出正确数据，不得产出错误 doc 序列
		var docs []uint32
		for it.Next() {
			docs = append(docs, it.DocID())
		}
		if len(docs) != 200 {
			return // 中途失败，可接受
		}
		for i, d := range docs {
			if d != uint32(i) {
				t.Fatalf("损坏数据产出了错误结果: docs[%d]=%d", i, d)
			}
		}
	}

	// 篡改块数据区中间字节（freq 被放大，触发"freq 超过剩余数据"检测或解码失败）
	dicS, pstS := write()
	it, ok := func() (Iterator, bool) {
		r, err := OpenFieldReader(dicS, pstS, FormatV2)
		if err != nil {
			t.Fatal(err)
		}
		return r.Iterator("t")
	}()
	if !ok {
		t.Fatal("term t 应存在")
	}
	pit := it.(*postingIterator)
	mid := (pit.dataStart + pit.indexOff) / 2
	corruptAndRead(func(b []byte) {
		for i := int64(0); i < 8; i++ {
			b[mid+i] = 0xff
		}
	})
	// 篡改头部块索引偏移为越界值
	corruptAndRead(func(b []byte) {
		for i := 0; i < 8; i++ {
			b[2+i] = 0xff // docFreq=200 占 2 字节，其后是 flags(1B)+indexOff(8B)
		}
	})
}

// TestCorruptFileTruncate 文件整体截断后打开/迭代应报错而非 panic
func TestCorruptFileTruncate(t *testing.T) {
	dir := t.TempDir()
	dicPath := filepath.Join(dir, "t.dic")
	pstPath := filepath.Join(dir, "t.pst")
	dicW, _ := storage.NewFileWriter(dicPath)
	pstW, _ := storage.NewFileWriter(pstPath)
	w := NewFieldWriter(dicW, pstW)
	for i := 0; i < 500; i++ {
		if err := w.Add("t", uint32(i), []uint32{uint32(i % 7)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	dicW.Close()
	pstW.Close()

	fi, err := os.Stat(pstPath)
	if err != nil {
		t.Fatal(err)
	}
	// 截断到一半：打开可能成功（字典自足），迭代应在某处失败而非 panic
	if err := os.Truncate(pstPath, fi.Size()/2); err != nil {
		t.Fatal(err)
	}
	dicR, err := storage.NewMmapReader(dicPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dicR.Close()
	pstR, err := storage.NewMmapReader(pstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer pstR.Close()
	r, err := OpenFieldReader(dicR, pstR, FormatV2)
	if err != nil {
		return // 打开即失败，可接受
	}
	it, ok := r.Iterator("t")
	if !ok {
		return
	}
	n := 0
	for it.Next() {
		n++
	}
	if n != 500 {
		t.Logf("截断后读出 %d 条（<500，迭代中途失败，符合预期）", n)
	}
}

// TestFieldReaderTerms 词典枚举：全量 / 前缀缩范围 / match 过滤 / maxExpansions 截断
func TestFieldReaderTerms(t *testing.T) {
	data := map[string][]wantPosting{}
	add := func(term string) {
		data[term] = []wantPosting{{docID: 0, positions: []uint32{0}}}
	}
	// 40 条 err- 前缀（跨 dict 块）+ 邻接 key
	add("abc")
	for i := 0; i < 40; i++ {
		add(fmt.Sprintf("err-%04d", i))
	}
	add("warn")
	r := writeReadRoundTrip(t, data)

	// 全量枚举（字典序）
	all := r.Terms("", nil, 100)
	if len(all) != 42 || all[0] != "abc" || all[1] != "err-0000" || all[41] != "warn" {
		t.Fatalf("Terms 全量枚举错误: %d 条, head=%v", len(all), all[:3])
	}
	// 前缀缩范围
	got := r.Terms("err-", nil, 100)
	if len(got) != 40 || got[0] != "err-0000" || got[39] != "err-0039" {
		t.Fatalf("Terms(err-) = %d 条，期望 40", len(got))
	}
	// match 过滤（配合前缀：wildcard 的字面前缀 + 模式匹配形态）
	got = r.Terms("err-", func(k string) bool { return k[len(k)-1] == '9' }, 100)
	if len(got) != 4 { // err-0009/0019/0029/0039
		t.Fatalf("Terms match 过滤 = %v，期望 4 条尾号 9", got)
	}
	// maxExpansions 截断：字典序最小者优先
	got = r.Terms("", nil, 5)
	if len(got) != 5 || got[4] != "err-0003" {
		t.Fatalf("Terms 截断 = %v，期望前 5 条字典序", got)
	}
	// 上限非正数 / 无匹配前缀
	if got := r.Terms("", nil, 0); len(got) != 0 {
		t.Fatalf("Terms(maxExpansions=0) = %v，期望空", got)
	}
	if got := r.Terms("nope", nil, 10); len(got) != 0 {
		t.Fatalf("Terms(不存在前缀) = %v，期望空", got)
	}
}
