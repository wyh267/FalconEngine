// Package posting 实现单字段倒排索引：term -> (docID, freq, positions) 倒排链的持久化与迭代读取。
//
// 涉及两个文件（文件句柄均由调用方提供）：
//
//   - 字典文件：复用 dict 包的有序字典，value 为 term 段在倒排文件中的起始偏移。
//   - 倒排文件（.pst）：每个 term 一段，段布局：
//
// v1（历史格式，只读兼容）：
//
//	[uvarint docFreq][定长 8B 块索引绝对偏移][块数据区][块索引区]
//	块内记录：[uvarint docDelta][uvarint freq]
//
// v2（当前写入格式）：
//
//	[uvarint docFreq][uvarint flags][定长 8B 块索引绝对偏移][块数据区][块索引区]
//	flags bit0 = hasPositions（v2 恒为 1）
//	块内记录：[uvarint docDelta][uvarint freq][freq × uvarint posDelta]
//
// docDelta 相对前一个 doc（首块基准为 0，后续块基准为前一块的 maxDocID），
// 因此任意一块配合块索引中的前块 maxDocID 即可独立解码。
// posDelta 为 doc 内相邻位置的差值（首个位置基准为 0），positions 严格递增。
//
// 块索引区（两版相同）：每块一条 [uvarint maxDocID][uvarint 块数据相对偏移（相对块数据区起点）]，
// 块数 = ceil(docFreq / BlockSize)，由 docFreq 推算，不再显式存储。
// 块索引不变意味着 Advance 整块跳读的能力两版一致。
package posting

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/FalconEngine/falcon/dict"
	"github.com/FalconEngine/falcon/types"
)

// BlockSize 每个倒排块包含的 doc 个数
const BlockSize = 128

// 段格式版本号（reader 按版本分派头部解析；writer 恒写 FormatV2）
const (
	FormatV1 = 1 // 无 flags、无 positions
	FormatV2 = 2 // 带 flags 与 positions
)

// flagHasPositions v2 头部 flags bit0：块内记录携带 positions
const flagHasPositions uint64 = 1

// posting 单条倒排记录
type posting struct {
	docID     uint32
	positions []uint32 // term 在 doc 内的出现位置（严格递增），freq = len(positions)
}

// FieldWriter 单字段倒排索引写入器（恒写 FormatV2）。
// 构建期在内存中按 term 收集倒排记录，Finish 时 term 排序后统一落盘。
type FieldWriter struct {
	dicW types.Writer
	pstW types.Writer

	terms    map[string][]posting // term -> docID 严格递增的倒排记录
	finished bool
}

// NewFieldWriter 创建写入器，dicW / pstW 分别为字典文件与倒排文件的写入句柄。
func NewFieldWriter(dicW, pstW types.Writer) *FieldWriter {
	return &FieldWriter{
		dicW:  dicW,
		pstW:  pstW,
		terms: make(map[string][]posting),
	}
}

// Add 追加一条倒排记录。同一 term 的 docID 必须严格递增；term 之间顺序任意。
// positions 为 term 在该 doc 内的出现位置，必须非空且严格递增（freq = len(positions)）。
func (w *FieldWriter) Add(term string, docID uint32, positions []uint32) error {
	if w.finished {
		return errors.New("posting: Finish 已调用，不能再 Add")
	}
	if len(positions) == 0 {
		return fmt.Errorf("posting: term %q 在 doc %d 的 positions 不能为空（freq 由 positions 推导）", term, docID)
	}
	for i := 1; i < len(positions); i++ {
		if positions[i] <= positions[i-1] {
			return fmt.Errorf("posting: term %q 在 doc %d 的 positions 必须严格递增: %v", term, docID, positions)
		}
	}
	list := w.terms[term]
	if n := len(list); n > 0 && docID <= list[n-1].docID {
		return fmt.Errorf("posting: term %q 的 docID 必须严格递增，当前 %d，上一个 %d",
			term, docID, list[n-1].docID)
	}
	w.terms[term] = append(list, posting{docID: docID, positions: positions})
	return nil
}

// Finish 将内存中的倒排数据落盘：先逐个 term 写倒排文件段，
// 再按 term 字典序写字典文件。只 Sync 不 Close，句柄由调用方管理。
func (w *FieldWriter) Finish() error {
	if w.finished {
		return errors.New("posting: Finish 不能重复调用")
	}
	w.finished = true

	terms := make([]string, 0, len(w.terms))
	for t := range w.terms {
		terms = append(terms, t)
	}
	sort.Strings(terms)

	db := dict.NewBuilder(w.dicW)
	for _, t := range terms {
		segOff, err := w.writeSegment(w.terms[t])
		if err != nil {
			return err
		}
		if err := db.Add(t, uint64(segOff)); err != nil {
			return err
		}
	}
	if err := db.Finish(); err != nil {
		return err
	}
	return w.pstW.Sync()
}

// writeSegment 把一个 term 的倒排链按 v2 格式写入倒排文件，返回段起始偏移。
// 块数据先在内存中编码（构建期数据本就在内存），以便头部长度与索引偏移一次算清。
func (w *FieldWriter) writeSegment(list []posting) (int64, error) {
	var data, index bytes.Buffer

	prev := uint64(0) // 上一个 docID，首块基准为 0
	for start := 0; start < len(list); start += BlockSize {
		end := min(start+BlockSize, len(list))
		// 块索引项：maxDocID + 块数据相对偏移（相对块数据区起点）
		appendUvarint(&index, uint64(list[end-1].docID))
		appendUvarint(&index, uint64(data.Len()))
		for _, p := range list[start:end] {
			appendUvarint(&data, uint64(p.docID)-prev)
			prev = uint64(p.docID)
			appendUvarint(&data, uint64(len(p.positions)))
			prevPos := uint64(0) // doc 内位置 delta 基准
			for _, pos := range p.positions {
				appendUvarint(&data, uint64(pos)-prevPos)
				prevPos = uint64(pos)
			}
		}
	}

	segOff := w.pstW.Offset()
	// 头部：[uvarint docFreq][uvarint flags][定长 8B 块索引绝对偏移]
	headerLen := int64(uvarintLen(uint64(len(list)))) + int64(uvarintLen(flagHasPositions)) + 8
	indexOff := segOff + headerLen + int64(data.Len())

	if err := w.pstW.WriteUvarint(uint64(len(list))); err != nil {
		return 0, err
	}
	if err := w.pstW.WriteUvarint(flagHasPositions); err != nil {
		return 0, err
	}
	if err := w.pstW.WriteUint64(uint64(indexOff)); err != nil {
		return 0, err
	}
	if _, err := w.pstW.WriteBytes(data.Bytes()); err != nil {
		return 0, err
	}
	if _, err := w.pstW.WriteBytes(index.Bytes()); err != nil {
		return 0, err
	}
	return segOff, nil
}

// appendUvarint 向 buffer 追加一个 uvarint
func appendUvarint(b *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	b.Write(tmp[:n])
}

// uvarintLen 返回 v 的 uvarint 编码长度
func uvarintLen(v uint64) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutUvarint(tmp[:], v)
}
