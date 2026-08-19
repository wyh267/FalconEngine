package index

import (
	"github.com/FalconEngine/falcon/segment"
)

// bufPosting 内存缓冲中的一条倒排记录
type bufPosting struct {
	docID uint32
	tf    uint32
}

// memBuf 内存索引缓冲：新写入的文档先进入缓冲，Flush 时整体落盘为一个段。
// 结构与 segment 对应，便于直接转换。
type memBuf struct {
	docs    []segment.Doc
	id2loc  map[string]uint32
	inv     map[string]map[string][]bufPosting // field -> term -> 倒排（docID 递增）
	deleted map[uint32]bool
}

func newMemBuf() *memBuf {
	return &memBuf{
		id2loc:  make(map[string]uint32),
		inv:     make(map[string]map[string][]bufPosting),
		deleted: make(map[uint32]bool),
	}
}

// add 追加一篇文档（字段解析与分词已完成），返回分配的缓冲内 docID
func (b *memBuf) add(d segment.Doc) uint32 {
	docID := uint32(len(b.docs))
	b.docs = append(b.docs, d)
	b.id2loc[d.ID] = docID

	for field, tokens := range d.Terms {
		tf := make(map[string]uint32, len(tokens))
		for _, tok := range tokens {
			tf[tok]++
		}
		terms := b.inv[field]
		if terms == nil {
			terms = make(map[string][]bufPosting)
			b.inv[field] = terms
		}
		for term, cnt := range tf {
			terms[term] = append(terms[term], bufPosting{docID: docID, tf: cnt})
		}
	}
	return docID
}

// postings 查缓冲内 field 上 term 的倒排
func (b *memBuf) postings(field, term string) ([]bufPosting, bool) {
	terms, ok := b.inv[field]
	if !ok {
		return nil, false
	}
	p, ok := terms[term]
	return p, ok
}

// num 读缓冲内 number 类字段值；字段或该文档的值不存在返回 false
func (b *memBuf) num(field string, docID uint32) (int64, bool) {
	v, ok := b.docs[docID].Nums[field]
	return v, ok
}

// docLen 返回 text 字段在缓冲内 docID 处的文档长度（term 数），BM25 用
func (b *memBuf) docLen(field string, docID uint32) int64 {
	return int64(len(b.docs[docID].Terms[field]))
}

// liveDF 返回缓冲内 field 上 term 的存活文档数（跳过已删除），BM25 的 df 用
func (b *memBuf) liveDF(field, term string) int {
	plist, ok := b.postings(field, term)
	if !ok {
		return 0
	}
	n := 0
	for _, p := range plist {
		if !b.deleted[p.docID] {
			n++
		}
	}
	return n
}

// avgDL 返回缓冲内 text 字段的平均文档长度（term 总数 / 文档总数），BM25 用
func (b *memBuf) avgDL(field string) float64 {
	if len(b.docs) == 0 {
		return 0
	}
	total := 0
	for _, d := range b.docs {
		total += len(d.Terms[field])
	}
	return float64(total) / float64(len(b.docs))
}

// empty 判断缓冲是否为空
func (b *memBuf) empty() bool { return len(b.docs) == 0 }
