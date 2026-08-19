package posting

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/FalconEngine/falcon/dict"
	"github.com/FalconEngine/falcon/types"
)

// Iterator 倒排链迭代器。Next / Advance 返回 true 后 DocID / Freq 有效。
type Iterator interface {
	// Next 定位到下一个 doc；首次调用定位到第一个 doc；返回 false 表示耗尽
	Next() bool
	// Advance 定位到当前位置起第一个 docID >= target 的 doc
	Advance(target uint32) bool
	DocID() uint32
	Freq() uint32
}

// FieldReader 单字段倒排索引只读 reader。
type FieldReader struct {
	dic  *dict.Reader
	pstR types.RandomReader
}

// OpenFieldReader 打开倒排索引：字典索引区常驻内存，倒排数据按需读取。
func OpenFieldReader(dicR, pstR types.RandomReader) (*FieldReader, error) {
	d, err := dict.Open(dicR)
	if err != nil {
		return nil, err
	}
	return &FieldReader{dic: d, pstR: pstR}, nil
}

// DocFreq 返回 term 的文档数；term 不存在时返回 0。
func (r *FieldReader) DocFreq(term string) int {
	segOff, ok := r.dic.Get(term)
	if !ok {
		return 0
	}
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
	// 段头部：[uvarint docFreq][定长 8B 块索引绝对偏移]
	df, off, err := r.pstR.ReadUvarint(int64(segOff))
	if err != nil {
		return nil, false
	}
	indexOff, err := r.pstR.ReadUint64(off)
	if err != nil {
		return nil, false
	}
	if df == 0 {
		return nil, false
	}
	it := &postingIterator{
		r:         r.pstR,
		dataStart: off + 8, // 块数据区起点 = 头部结束位置
		indexOff:  int64(indexOff),
		docFreq:   df,
		blockIdx:  -1,
	}
	if err := it.loadIndex(); err != nil {
		return nil, false
	}
	return it, true
}

// postingIterator 惰性按块解码的倒排迭代器：
// 打开时只读块索引，块数据按需解码，单块最多 BlockSize 条，内存可控。
type postingIterator struct {
	r         types.RandomReader
	dataStart int64 // 块数据区起点（绝对偏移）
	indexOff  int64 // 块索引区起点（绝对偏移，兼作块数据区终点）
	docFreq   uint64

	blockMax []uint32 // 每块最大 docID
	blockOff []int64  // 每块数据绝对偏移

	blockIdx int      // 当前已解码块序号，-1 表示未开始
	docIDs   []uint32 // 当前块的 docID
	freqs    []uint32 // 当前块的 freq
	pos      int      // 当前块内已消费位置

	seen    uint64 // 已消费 doc 总数
	curDoc  uint32
	curFreq uint32
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
	} else {
		it.docIDs = it.docIDs[:n]
		it.freqs = it.freqs[:n]
	}

	cur := base
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
	}
	it.blockIdx = i
	it.pos = 0
	return nil
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
	it.curDoc = it.docIDs[it.pos]
	it.curFreq = it.freqs[it.pos]
	it.pos++
	it.seen++
	it.valid = true
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
			it.curDoc = it.docIDs[it.pos]
			it.curFreq = it.freqs[it.pos]
			it.pos++
			it.seen++
			it.valid = true
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
