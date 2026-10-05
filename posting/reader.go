package posting

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/FalconEngine/falcon/dict"
	"github.com/FalconEngine/falcon/types"
)

// Iterator 倒排链迭代器。Next / Advance 返回 true 后 DocID / Freq / Positions 有效。
type Iterator interface {
	// Next 定位到下一个 doc；首次调用定位到第一个 doc；返回 false 表示耗尽
	Next() bool
	// Advance 定位到当前位置起第一个 docID >= target 的 doc
	Advance(target uint32) bool
	DocID() uint32
	Freq() uint32
	// Positions 返回当前 doc 内 term 的出现位置（升序）。
	// v1 段无 positions，恒返回 nil（调用方据此识别旧格式）。
	// 返回的切片复用内部缓冲，仅在下一次 Next/Advance 前有效。
	Positions() []uint32
}

// FieldReader 单字段倒排索引只读 reader。
type FieldReader struct {
	dic     *dict.Reader
	pstR    types.RandomReader
	version int // 段格式版本（FormatV1/FormatV2），决定头部解析方式
}

// OpenFieldReader 打开倒排索引：字典索引区常驻内存，倒排数据按需读取。
// version 为段格式版本（FormatV1/FormatV2），由段 meta 给出。
func OpenFieldReader(dicR, pstR types.RandomReader, version int) (*FieldReader, error) {
	d, err := dict.Open(dicR)
	if err != nil {
		return nil, err
	}
	return &FieldReader{dic: d, pstR: pstR, version: version}, nil
}

// Terms 枚举字段词典中满足 match 的 term：字典序，最多 maxExpansions 个（超出截断）。
// prefix 非空时借稀疏索引只扫描该前缀区间，否则全词典扫描；match 为 nil 表示全收。
// 词典扫描为 O(词典)，大词典不承诺性能，调用方以 maxExpansions 截断兜底。
func (r *FieldReader) Terms(prefix string, match func(string) bool, maxExpansions int) []string {
	if maxExpansions <= 0 {
		return nil
	}
	var out []string
	visit := func(k string, _ uint64) bool {
		if match != nil && !match(k) {
			return true
		}
		out = append(out, k)
		return len(out) < maxExpansions
	}
	if prefix != "" {
		r.dic.ScanPrefix(prefix, visit)
	} else {
		r.dic.Scan(visit)
	}
	return out
}

// DocFreq 返回 term 的文档数；term 不存在时返回 0。
func (r *FieldReader) DocFreq(term string) int {
	segOff, ok := r.dic.Get(term)
	if !ok {
		return 0
	}
	// docFreq 是两版头部共同的第一个字段
	df, _, err := r.pstR.ReadUvarint(int64(segOff))
	if err != nil {
		return 0
	}
	return int(df)
}

// Iterator 返回 term 的倒排迭代器；bool=false 表示 term 不存在。
func (r *FieldReader) Iterator(term string) (Iterator, bool) {
	segOff, ok := r.dic.Get(term)
	if !ok {
		return nil, false
	}
	// 段头部 v1: [uvarint docFreq][定长 8B 块索引绝对偏移]
	//       v2: [uvarint docFreq][uvarint flags][定长 8B 块索引绝对偏移]
	df, off, err := r.pstR.ReadUvarint(int64(segOff))
	if err != nil {
		return nil, false
	}
	hasPos := false
	if r.version >= FormatV2 {
		var flags uint64
		flags, off, err = r.pstR.ReadUvarint(off)
		if err != nil {
			return nil, false
		}
		hasPos = flags&flagHasPositions != 0
	}
	indexOff, err := r.pstR.ReadUint64(off)
	if err != nil {
		return nil, false
	}
	if df == 0 {
		return nil, false
	}
	it := &postingIterator{
		r:            r.pstR,
		dataStart:    off + 8, // 块数据区起点 = 头部结束位置
		indexOff:     int64(indexOff),
		docFreq:      df,
		hasPositions: hasPos,
		blockIdx:     -1,
	}
	if err := it.loadIndex(); err != nil {
		return nil, false
	}
	return it, true
}

// postingIterator 惰性按块解码的倒排迭代器：
// 打开时只读块索引，块数据按需解码，单块最多 BlockSize 条，内存可控。
type postingIterator struct {
	r            types.RandomReader
	dataStart    int64 // 块数据区起点（绝对偏移）
	indexOff     int64 // 块索引区起点（绝对偏移，兼作块数据区终点）
	docFreq      uint64
	hasPositions bool // v2 且 flags bit0 置位

	blockMax []uint32 // 每块最大 docID
	blockOff []int64  // 每块数据绝对偏移

	blockIdx int      // 当前已解码块序号，-1 表示未开始
	docIDs   []uint32 // 当前块的 docID
	freqs    []uint32 // 当前块的 freq
	// positions 解码为扁平数组：第 j 个 doc 的位置序列为
	// posFlat[posStart[j] : posStart[j]+freqs[j]]，避免每 doc 一次分配
	posFlat  []uint32
	posStart []uint32
	pos      int // 当前块内已消费位置

	seen    uint64 // 已消费 doc 总数
	curDoc  uint32
	curFreq uint32
	curPos  []uint32
	valid   bool
}

// loadIndex 读取块索引区
func (it *postingIterator) loadIndex() error {
	numBlocks := (it.docFreq + BlockSize - 1) / BlockSize
	it.blockMax = make([]uint32, 0, numBlocks)
	it.blockOff = make([]int64, 0, numBlocks)
	off := it.indexOff
	for i := uint64(0); i < numBlocks; i++ {
		maxDoc, off2, err := it.r.ReadUvarint(off)
		if err != nil {
			return err
		}
		rel, off3, err := it.r.ReadUvarint(off2)
		if err != nil {
			return err
		}
		it.blockMax = append(it.blockMax, uint32(maxDoc))
		it.blockOff = append(it.blockOff, it.dataStart+int64(rel))
		off = off3
	}
	return nil
}

// decodeBlock 解码第 i 块到内存
func (it *postingIterator) decodeBlock(i int) error {
	start := it.blockOff[i]
	end := it.indexOff
	if i+1 < len(it.blockOff) {
		end = it.blockOff[i+1]
	}
	buf, err := it.r.ReadBytes(start, end-start)
	if err != nil {
		return err
	}

	base := uint64(0)
	if i > 0 {
		base = uint64(it.blockMax[i-1]) // 首条 delta 的基准是前一块的 maxDocID
	}
	n := int(min(uint64(BlockSize), it.docFreq-uint64(i)*BlockSize))
	if cap(it.docIDs) < n {
		it.docIDs = make([]uint32, n)
		it.freqs = make([]uint32, n)
		it.posStart = make([]uint32, n)
	} else {
		it.docIDs = it.docIDs[:n]
		it.freqs = it.freqs[:n]
		it.posStart = it.posStart[:n]
	}

	cur := base
	totalPos := 0
	for j := 0; j < n; j++ {
		delta, m := binary.Uvarint(buf)
		if m <= 0 {
			return errors.New("posting: 块数据损坏（docDelta）")
		}
		buf = buf[m:]
		cur += delta
		it.docIDs[j] = uint32(cur)

		f, m := binary.Uvarint(buf)
		if m <= 0 {
			return errors.New("posting: 块数据损坏（freq）")
		}
		buf = buf[m:]
		it.freqs[j] = uint32(f)
		it.posStart[j] = uint32(totalPos)

		if !it.hasPositions {
			continue
		}
		// 完整性兜底：每个 posDelta 至少占 1 字节，freq 超过剩余字节数必为损坏
		if f > uint64(len(buf)) {
			return errors.New("posting: 块数据损坏（freq 超过剩余数据）")
		}
		if cap(it.posFlat) < totalPos+int(f) {
			// 扩容时保留本块已解码的位置
			grown := make([]uint32, totalPos+int(f))
			copy(grown, it.posFlat[:totalPos])
			it.posFlat = grown
		} else {
			it.posFlat = it.posFlat[:totalPos+int(f)]
		}
		prevPos := uint64(0)
		for k := int64(0); k < int64(f); k++ {
			pd, m := binary.Uvarint(buf)
			if m <= 0 {
				return errors.New("posting: 块数据损坏（posDelta）")
			}
			buf = buf[m:]
			prevPos += pd
			it.posFlat[totalPos+int(k)] = uint32(prevPos)
		}
		totalPos += int(f)
	}
	it.blockIdx = i
	it.pos = 0
	return nil
}

// setCur 记录当前消费位置的 doc/freq/positions（调用时 pos 指向待消费项）
func (it *postingIterator) setCur() {
	j := it.pos - 1
	it.curDoc = it.docIDs[j]
	it.curFreq = it.freqs[j]
	if it.hasPositions {
		it.curPos = it.posFlat[it.posStart[j] : it.posStart[j]+it.curFreq]
	} else {
		it.curPos = nil
	}
	it.valid = true
}

// Next 定位到下一个 doc
func (it *postingIterator) Next() bool {
	if it.seen >= it.docFreq {
		it.valid = false
		return false
	}
	if it.blockIdx < 0 || it.pos >= len(it.docIDs) {
		next := it.blockIdx + 1
		if err := it.decodeBlock(next); err != nil {
			it.valid = false
			return false
		}
		it.seen = uint64(next) * BlockSize
	}
	it.pos++
	it.seen++
	it.setCur()
	return true
}

// Advance 定位到当前之后第一个 docID >= target 的 doc（Lucene 语义：
// 即使当前 doc 已满足条件，也要继续向后移动）。
// 用块索引的 maxDocID 二分定位目标块，整块跳过而不解码。
func (it *postingIterator) Advance(target uint32) bool {
	if it.seen >= it.docFreq {
		it.valid = false
		return false
	}
	// 从当前块起，找第一个 maxDocID >= target 的块
	lo := max(it.blockIdx, 0)
	i := lo + sort.Search(len(it.blockMax)-lo, func(k int) bool {
		return it.blockMax[lo+k] >= target
	})
	if i >= len(it.blockMax) {
		it.seen = it.docFreq
		it.valid = false
		return false
	}
	if i != it.blockIdx {
		if err := it.decodeBlock(i); err != nil {
			it.valid = false
			return false
		}
		it.seen = uint64(i) * BlockSize
	}
	// 块内线性推进，跨块时逐块继续
	for {
		for it.pos < len(it.docIDs) && it.docIDs[it.pos] < target {
			it.pos++
			it.seen++
		}
		if it.pos < len(it.docIDs) {
			it.pos++
			it.seen++
			it.setCur()
			return true
		}
		// 当前块耗尽且未命中；块 i 存在的充要条件是 i*BlockSize < docFreq
		if uint64(it.blockIdx+1)*BlockSize >= it.docFreq {
			it.valid = false
			return false
		}
		if err := it.decodeBlock(it.blockIdx + 1); err != nil {
			it.valid = false
			return false
		}
		it.seen = uint64(it.blockIdx) * BlockSize
	}
}

// DocID 返回当前 docID，仅在 Next / Advance 返回 true 后有效
func (it *postingIterator) DocID() uint32 { return it.curDoc }

// Freq 返回当前 doc 的 freq，仅在 Next / Advance 返回 true 后有效
func (it *postingIterator) Freq() uint32 { return it.curFreq }

// Positions 返回当前 doc 内 term 的出现位置（升序）；v1 段恒返回 nil。
// 仅在 Next / Advance 返回 true 后有效；切片复用内部缓冲，下次迭代后失效。
func (it *postingIterator) Positions() []uint32 {
	if !it.valid {
		return nil
	}
	return it.curPos
}
