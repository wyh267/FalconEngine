package docvalues

import (
	"fmt"
	"sort"

	"github.com/FalconEngine/falcon/types"
)

// keywordMagic 文件尾魔数，用于 Reader 打开时校验文件类型
const keywordMagic uint64 = 0xF41C0D4B45595731 // "FALCONKW1" 的混淆常量

// keywordFooterSize 文件尾元信息定长：docCount(8) + numTerms(8) + magic(8)
const keywordFooterSize = 24

// KeywordWriter keyword 列写入器（ord 编码）。
// 单文件布局（自足，Reader 仅凭文件尾即可定位全部结构）：
//
//	[term 表区]  uvarint numTerms + numTerms × (uvarint termLen + termBytes)，term 按字典序，序号为 ord
//	[ord 区]     docCount × uvarint(ord+1)，0 表示该 doc 无值
//	[文件尾]     uint64 docCount + uint64 numTerms + uint64 magic（定长 24B）
type KeywordWriter struct {
	w    types.Writer
	next uint32   // 下一个期望的 docID
	docs []string // 每个 doc 的原始值，空串表示无值
}

// NewKeywordWriter 基于底层顺序写入器创建 keyword 列写入器
func NewKeywordWriter(dataW types.Writer) *KeywordWriter {
	return &KeywordWriter{w: dataW}
}

// Add 追加一条 keyword 值，docID 必须严格递增（从 0 开始连续），空字符串表示该 doc 无值
func (w *KeywordWriter) Add(docID uint32, value string) error {
	if docID != w.next {
		return fmt.Errorf("docvalues: keyword docID 必须连续递增, 期望 %d, 收到 %d", w.next, docID)
	}
	w.docs = append(w.docs, value)
	w.next++
	return nil
}

// Finish 收集 term 字典（排序分配 ord）并落盘全部内容，最后刷盘
func (w *KeywordWriter) Finish() error {
	// 收集去重 term 并排序，字典序序号即 ord
	set := make(map[string]struct{})
	for _, v := range w.docs {
		if v != "" {
			set[v] = struct{}{}
		}
	}
	terms := make([]string, 0, len(set))
	for term := range set {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	ordOf := make(map[string]uint32, len(terms))
	for i, term := range terms {
		ordOf[term] = uint32(i)
	}

	// term 表区
	if err := w.w.WriteUvarint(uint64(len(terms))); err != nil {
		return err
	}
	for _, term := range terms {
		if err := w.w.WriteUvarint(uint64(len(term))); err != nil {
			return err
		}
		if _, err := w.w.WriteBytes([]byte(term)); err != nil {
			return err
		}
	}
	// ord 区：ord+1 编码，0 表示无值
	for _, v := range w.docs {
		code := uint64(0)
		if v != "" {
			code = uint64(ordOf[v]) + 1
		}
		if err := w.w.WriteUvarint(code); err != nil {
			return err
		}
	}
	// 文件尾元信息
	if err := w.w.WriteUint64(uint64(len(w.docs))); err != nil {
		return err
	}
	if err := w.w.WriteUint64(uint64(len(terms))); err != nil {
		return err
	}
	if err := w.w.WriteUint64(keywordMagic); err != nil {
		return err
	}
	return w.w.Sync()
}

// KeywordReader keyword 列读取器，打开时把 term 表与 ord 数组载入内存
type KeywordReader struct {
	terms []string // ord → term，已按字典序
	ords  []uint32 // docID → ord+1，0 表示无值
}

// OpenKeywordReader 打开 keyword 列并校验文件尾魔数
func OpenKeywordReader(r types.RandomReader) (*KeywordReader, error) {
	if r.Len() < keywordFooterSize {
		return nil, fmt.Errorf("docvalues: 非法 keyword 文件, 长度 %d 过小", r.Len())
	}
	footerOff := r.Len() - keywordFooterSize
	docCount, err := r.ReadUint64(footerOff)
	if err != nil {
		return nil, err
	}
	numTerms, err := r.ReadUint64(footerOff + 8)
	if err != nil {
		return nil, err
	}
	magic, err := r.ReadUint64(footerOff + 16)
	if err != nil {
		return nil, err
	}
	if magic != keywordMagic {
		return nil, fmt.Errorf("docvalues: keyword 文件魔数不匹配: %#x", magic)
	}

	// 读 term 表（顺序解析后即到达 ord 区起始位置）
	off := int64(0)
	headerTerms, off, err := r.ReadUvarint(off)
	if err != nil {
		return nil, err
	}
	if headerTerms != numTerms {
		return nil, fmt.Errorf("docvalues: term 数量不一致, 表头 %d, 文件尾 %d", headerTerms, numTerms)
	}
	terms := make([]string, 0, numTerms)
	for i := uint64(0); i < numTerms; i++ {
		var l uint64
		l, off, err = r.ReadUvarint(off)
		if err != nil {
			return nil, err
		}
		var b []byte
		b, err = r.ReadBytes(off, int64(l))
		if err != nil {
			return nil, err
		}
		// ReadBytes 可能返回零拷贝切片，复制后再转成 string 持有
		terms = append(terms, string(append([]byte(nil), b...)))
		off += int64(l)
	}

	// 读 ord 区
	ords := make([]uint32, 0, docCount)
	for i := uint64(0); i < docCount; i++ {
		var code uint64
		code, off, err = r.ReadUvarint(off)
		if err != nil {
			return nil, err
		}
		if code > uint64(numTerms) {
			return nil, fmt.Errorf("docvalues: doc %d 的 ord 编码 %d 越界", i, code)
		}
		ords = append(ords, uint32(code))
	}
	if off != footerOff {
		return nil, fmt.Errorf("docvalues: keyword 数据区结束于 %d, 与文件尾位置 %d 不符", off, footerOff)
	}
	return &KeywordReader{terms: terms, ords: ords}, nil
}

// Ord 返回 docID 对应的 ord；bool=false 表示该 doc 无值
func (r *KeywordReader) Ord(docID uint32) (uint32, bool) {
	if int(docID) >= len(r.ords) {
		return 0, false
	}
	code := r.ords[docID]
	if code == 0 {
		return 0, false
	}
	return code - 1, true
}

// Term 返回 ord 对应的 term；ord 越界时返回空串
func (r *KeywordReader) Term(ord uint32) string {
	if int(ord) >= len(r.terms) {
		return ""
	}
	return r.terms[ord]
}

// Lookup 二分查找 term 对应的 ord
func (r *KeywordReader) Lookup(term string) (uint32, bool) {
	i := sort.SearchStrings(r.terms, term)
	if i < len(r.terms) && r.terms[i] == term {
		return uint32(i), true
	}
	return 0, false
}

// NumOrds 返回字典中 term 的数量
func (r *KeywordReader) NumOrds() int { return len(r.terms) }

// Len 返回文档数
func (r *KeywordReader) Len() int { return len(r.ords) }
