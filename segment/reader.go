package segment

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/FalconEngine/falcon/docvalues"
	"github.com/FalconEngine/falcon/posting"
	"github.com/FalconEngine/falcon/storage"
	"github.com/FalconEngine/falcon/stored"
)

// Reader 段的只读访问句柄（删除标记除外，删除是段上唯一允许的写操作）
type Reader struct {
	dir string
	m   meta

	id2loc map[string]uint32                   // 外部 ID -> 段内 docID（由 meta.IDs 构建）
	inv    map[string]*posting.FieldReader     // 倒排字段
	stored *stored.Reader                      // 原文存储
	nums   map[string]*docvalues.NumberReader  // number 类正排列
	kws    map[string]*docvalues.KeywordReader // keyword ord 列（v2 起）
	has    map[string][]byte                   // 字段存在性标记（常驻内存，1B/文档；v1 仅 number 类，v2 覆盖全部字段）
	norms  map[string]*docvalues.NumberReader  // text 字段文档长度（BM25 用）

	deleted map[uint32]bool // 删除标记（del.bin + 运行时新增）

	closers []io.Closer // 全部底层 mmap 句柄，Close 时统一释放
}

// Open 打开一个已构建完成的段。meta.json 不存在视为坏段。
func Open(dir string) (*Reader, error) {
	mb, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("segment: 读取 meta.json 失败: %w", err)
	}
	r := &Reader{dir: dir, deleted: make(map[uint32]bool)}
	if err := json.Unmarshal(mb, &r.m); err != nil {
		return nil, fmt.Errorf("segment: 解析 meta.json 失败: %w", err)
	}
	if len(r.m.IDs) != r.m.Docs {
		return nil, fmt.Errorf("segment: meta 中 IDs 数量 %d 与文档数 %d 不匹配", len(r.m.IDs), r.m.Docs)
	}
	r.id2loc = make(map[string]uint32, r.m.Docs)
	for i, id := range r.m.IDs {
		r.id2loc[id] = uint32(i)
	}

	mmap := func(name string) (*storage.MmapReader, error) {
		mr, err := storage.NewMmapReader(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		r.closers = append(r.closers, mr)
		return mr, nil
	}

	// 倒排索引（按段格式版本分派头部解析：v1 无 positions，v2 携带 positions）
	postingVersion := posting.FormatV1
	if r.m.Version >= segmentFormatV2 {
		postingVersion = posting.FormatV2
	}
	r.inv = make(map[string]*posting.FieldReader, len(r.m.InvFields))
	for _, field := range r.m.InvFields {
		dicR, err := mmap("inv." + field + ".dic")
		if err != nil {
			return nil, fmt.Errorf("segment: 打开 inv.%s.dic 失败: %w", field, err)
		}
		pstR, err := mmap("inv." + field + ".pst")
		if err != nil {
			return nil, fmt.Errorf("segment: 打开 inv.%s.pst 失败: %w", field, err)
		}
		fr, err := posting.OpenFieldReader(dicR, pstR, postingVersion)
		if err != nil {
			return nil, fmt.Errorf("segment: 打开字段 %q 倒排失败: %w", field, err)
		}
		r.inv[field] = fr
	}

	// 原文存储
	dataR, err := mmap("stored.dat")
	if err != nil {
		return nil, fmt.Errorf("segment: 打开 stored.dat 失败: %w", err)
	}
	idxR, err := mmap("stored.idx")
	if err != nil {
		return nil, fmt.Errorf("segment: 打开 stored.idx 失败: %w", err)
	}
	if r.stored, err = stored.OpenReader(dataR, idxR); err != nil {
		return nil, fmt.Errorf("segment: 打开原文存储失败: %w", err)
	}
	if r.stored.Len() != r.m.Docs {
		return nil, fmt.Errorf("segment: 原文数 %d 与 meta 文档数 %d 不匹配", r.stored.Len(), r.m.Docs)
	}

	// number 正排列
	r.nums = make(map[string]*docvalues.NumberReader, len(r.m.NumFields))
	for _, field := range r.m.NumFields {
		mr, err := mmap("num." + field)
		if err != nil {
			return nil, fmt.Errorf("segment: 打开 num.%s 失败: %w", field, err)
		}
		nr, err := docvalues.OpenNumberReader(mr)
		if err != nil {
			return nil, err
		}
		r.nums[field] = nr
	}

	// keyword ord 列（v1 段 meta 无 KwFields，天然为空）
	r.kws = make(map[string]*docvalues.KeywordReader, len(r.m.KwFields))
	for _, field := range r.m.KwFields {
		mr, err := mmap("kw." + field)
		if err != nil {
			return nil, fmt.Errorf("segment: 打开 kw.%s 失败: %w", field, err)
		}
		kr, err := docvalues.OpenKeywordReader(mr)
		if err != nil {
			return nil, fmt.Errorf("segment: 解析 kw.%s 失败: %w", field, err)
		}
		if kr.Len() != r.m.Docs {
			return nil, fmt.Errorf("segment: kw.%s 文档数 %d 与 meta 文档数 %d 不匹配", field, kr.Len(), r.m.Docs)
		}
		r.kws[field] = kr
	}

	// 存在性标记：v1 仅 number 类字段；v2 覆盖全部字段（倒排/正排/keyword 并集）
	hasFields := r.m.NumFields
	if r.m.Version >= segmentFormatV2 {
		hasFields = unionFields(r.m.InvFields, r.m.NumFields, r.m.KwFields)
	}
	r.has = make(map[string][]byte, len(hasFields))
	for _, field := range hasFields {
		hb, err := os.ReadFile(filepath.Join(dir, "has."+field))
		if err != nil {
			return nil, fmt.Errorf("segment: 读取 has.%s 失败: %w", field, err)
		}
		if len(hb) != r.m.Docs {
			return nil, fmt.Errorf("segment: has.%s 长度 %d 与文档数 %d 不匹配", field, len(hb), r.m.Docs)
		}
		r.has[field] = hb
	}

	// text 字段 norms（BM25 用）
	r.norms = make(map[string]*docvalues.NumberReader, len(r.m.TextFields))
	for _, field := range r.m.TextFields {
		mr, err := mmap("norm." + field)
		if err != nil {
			return nil, fmt.Errorf("segment: 打开 norm.%s 失败: %w", field, err)
		}
		nr, err := docvalues.OpenNumberReader(mr)
		if err != nil {
			return nil, err
		}
		r.norms[field] = nr
	}

	// 回放删除标记（可不存在）
	if err := r.loadDeleted(); err != nil {
		return nil, err
	}
	return r, nil
}

// loadDeleted 读取 del.bin 中的全部删除标记；文件不存在视为无删除
func (r *Reader) loadDeleted() error {
	path := filepath.Join(r.dir, "del.bin")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	mr, err := storage.NewMmapReader(path)
	if err != nil {
		return err
	}
	defer mr.Close()
	var off int64
	for off < mr.Len() {
		id, noff, err := mr.ReadUvarint(off)
		if err != nil {
			return fmt.Errorf("segment: 解析 del.bin 失败: %w", err)
		}
		off = noff
		r.deleted[uint32(id)] = true
	}
	return nil
}

// DocCount 返回段内文档总数（含已删除）
func (r *Reader) DocCount() int { return r.m.Docs }

// Seq 返回段序号（全局单调递增，越大越新）
func (r *Reader) Seq() int { return r.m.Seq }

// Dir 返回段目录路径
func (r *Reader) Dir() string { return r.dir }

// Version 返回段格式版本（0/1 为历史 v1 段：无 positions、无 kw 列；2 为当前）
func (r *Reader) Version() int { return r.m.Version }

// LiveCount 返回未删除的文档数
func (r *Reader) LiveCount() int { return r.m.Docs - len(r.deleted) }

// NumFields 返回段内的 number 字段名
func (r *Reader) NumFields() []string { return r.m.NumFields }

// AvgDL 返回 text 字段在本段的平均文档长度；字段不存在返回 0
func (r *Reader) AvgDL(field string) float64 { return r.m.AvgDL[field] }

// Norm 返回 text 字段在 docID 处的文档长度（term 数）；字段不存在返回 0
func (r *Reader) Norm(field string, docID uint32) int64 {
	nr, ok := r.norms[field]
	if !ok {
		return 0
	}
	return nr.Get(docID)
}

// LocalID 按外部 ID 查段内 docID
func (r *Reader) LocalID(id string) (uint32, bool) {
	v, ok := r.id2loc[id]
	return v, ok
}

// ID 按段内 docID 查外部 ID
func (r *Reader) ID(docID uint32) string {
	if int(docID) >= len(r.m.IDs) {
		return ""
	}
	return r.m.IDs[docID]
}

// Postings 返回 field 上 term 的倒排迭代器；字段或 term 不存在返回 false
func (r *Reader) Postings(field, term string) (posting.Iterator, bool) {
	fr, ok := r.inv[field]
	if !ok {
		return nil, false
	}
	return fr.Iterator(term)
}

// DocFreq 返回 field 上 term 的文档数
func (r *Reader) DocFreq(field, term string) int {
	fr, ok := r.inv[field]
	if !ok {
		return 0
	}
	return fr.DocFreq(term)
}

// Terms 枚举 field 词典中满足 match 的 term（见 posting.FieldReader.Terms）；
// 字段无倒排返回 nil。prefix/wildcard/fuzzy 查询的词典展开用。
func (r *Reader) Terms(field, prefix string, match func(string) bool, maxExpansions int) []string {
	fr, ok := r.inv[field]
	if !ok {
		return nil
	}
	return fr.Terms(prefix, match, maxExpansions)
}

// Stored 读取段内 docID 对应的原文
func (r *Reader) Stored(docID uint32) ([]byte, error) {
	return r.stored.Get(docID)
}

// Num 读取 number 类字段在 docID 处的值；字段不存在或该文档无此字段值时返回 false
func (r *Reader) Num(field string, docID uint32) (int64, bool) {
	nr, ok := r.nums[field]
	if !ok {
		return 0, false
	}
	if hb := r.has[field]; int(docID) >= len(hb) || hb[docID] == 0 {
		return 0, false
	}
	return nr.Get(docID), true
}

// Kw 返回 keyword 字段的 ord 列读取器；字段无 kw 列（非 keyword dv 字段或 v1 段）返回 false
func (r *Reader) Kw(field string) (*docvalues.KeywordReader, bool) {
	kr, ok := r.kws[field]
	return kr, ok
}

// Has 判断 docID 是否含有字段值（存在性标记）；字段无 has 标记返回 false。
// v1 段仅 number 类字段有标记，其余字段恒为 false。
func (r *Reader) Has(field string, docID uint32) bool {
	hb, ok := r.has[field]
	return ok && int(docID) < len(hb) && hb[docID] != 0
}

// HasField 返回段是否持有该字段的存在性标记（v2 覆盖全部字段，v1 仅 number 类）；
// false 时 Has 不可信，调用方需回落原文判断（exists 查询用）
func (r *Reader) HasField(field string) bool {
	_, ok := r.has[field]
	return ok
}

// Deleted 判断 docID 是否已被删除
func (r *Reader) Deleted(docID uint32) bool { return r.deleted[docID] }

// Delete 标记删除 docID，并把标记追加持久化到 del.bin
func (r *Reader) Delete(docID uint32) error {
	if r.deleted[docID] {
		return nil
	}
	r.deleted[docID] = true
	w, err := storage.OpenFileWriterForAppend(filepath.Join(r.dir, "del.bin"))
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.WriteUvarint(uint64(docID)); err != nil {
		return err
	}
	return w.Sync()
}

// Close 释放全部底层句柄
func (r *Reader) Close() error {
	for _, c := range r.closers {
		c.Close()
	}
	r.closers = nil
	return nil
}
