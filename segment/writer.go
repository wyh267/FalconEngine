// Package segment 实现不可变段（segment）的落盘构建与只读访问。
//
// 段目录文件布局：
//
//	meta.json     段元信息（格式版本、段序号、文档数、外部 ID 列表、字段清单、各 text 字段 avgdl）
//	inv.<field>.dic / inv.<field>.pst  倒排索引（见 posting 包；v2 起携带 positions）
//	stored.dat / stored.idx            原文存储（见 stored 包）
//	num.<field>   number 类字段（number/date/bool）正排列（见 docvalues 包）
//	kw.<field>    keyword 字段的 ord 列（见 docvalues 包，v2 起）
//	has.<field>   字段存在性标记：每文档 1 字节（0/1）。
//	              v1 仅覆盖 number 类字段；v2 起覆盖全部字段（倒排/正排/keyword）
//	norm.<field>  text 字段的文档长度（term 数），定长 8B 数组，BM25 用
//	del.bin       删除标记：追加写的 uvarint docID 序列（可不存在）
//
// 段格式版本：meta.Version 缺省/0/1 视为 v1（历史段，倒排无 positions、无 kw 列）；
// writer 恒写 v2。老段经引擎 merge（从原文重新解析再落盘）自动升级为 v2。
package segment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/FalconEngine/falcon/docvalues"
	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/posting"
	"github.com/FalconEngine/falcon/storage"
	"github.com/FalconEngine/falcon/stored"
)

// segmentFormatV2 当前段格式版本：posting 携带 positions、全字段 has 位、keyword ord 列
const segmentFormatV2 = 2

// Doc 一篇待写入段的文档（已完成字段解析与分词）
type Doc struct {
	ID    string                    // 外部文档 ID
	Raw   []byte                    // 存储原文（JSON）
	Terms map[string][]plugin.Token // text/keyword 字段 -> token 序列（Token.Position 即位置，token filter 可能跳号）
	Nums  map[string]int64          // number 类字段 -> 值
	Kws   map[string]string         // keyword docvalues 字段 -> 原始字符串（空串视为无值）
}

// meta 段元信息
type meta struct {
	Version    int                `json:"version"`     // 段格式版本（缺省/0/1 视为 v1）
	Seq        int                `json:"seq"`         // 段序号，全局单调递增，决定段的新旧顺序
	Docs       int                `json:"docs"`        // 文档总数
	IDs        []string           `json:"ids"`         // 外部文档 ID，下标即段内 docID
	InvFields  []string           `json:"inv_fields"`  // 倒排字段
	TextFields []string           `json:"text_fields"` // text 字段（有 norms）
	NumFields  []string           `json:"num_fields"`  // number 类字段
	KwFields   []string           `json:"kw_fields"`   // keyword docvalues 字段（v2 起）
	AvgDL      map[string]float64 `json:"avgdl"`       // 各 text 字段的平均文档长度（BM25 用）
}

// Write 将一批文档构建为一个不可变段（v2 格式），写入 dir（目录将被创建）。
// seq 为段序号（全局单调递增）；invFields/textFields/numFields/kwFields 为全部倒排、
// text、number 类、keyword docvalues 字段名（即使本批文档都没有值也要建空文件）。
// 文档在 docs 中的顺序即段内 docID（0,1,2...）。
func Write(dir string, seq int, invFields, textFields, numFields, kwFields []string, docs []Doc) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("segment: 创建目录失败: %w", err)
	}

	// 1. 倒排索引：逐字段构建 inv.<field>.dic / inv.<field>.pst（v2 携带 positions）
	for _, field := range invFields {
		if err := writeInverted(dir, field, docs); err != nil {
			return err
		}
	}

	// 2. 原文：stored.dat + stored.idx
	if err := writeStored(dir, docs); err != nil {
		return err
	}

	// 3. num.<field>：number 类正排列，缺失值补 0（存在性由统一的 has.<field> 承担）
	for _, field := range numFields {
		if err := writeNumber(dir, field, docs); err != nil {
			return err
		}
	}

	// 4. kw.<field>：keyword ord 列
	for _, field := range kwFields {
		if err := writeKeyword(dir, field, docs); err != nil {
			return err
		}
	}

	// 5. has.<field>：v2 起覆盖全部字段（倒排/正排/keyword 的并集），exists 查询用
	if err := writeHas(dir, unionFields(invFields, numFields, kwFields), docs); err != nil {
		return err
	}

	// 6. norm.<field>：text 字段文档长度（term 数），同时统计 avgdl
	avgdl := make(map[string]float64, len(textFields))
	for _, field := range textFields {
		total, err := writeNorm(dir, field, docs)
		if err != nil {
			return err
		}
		if len(docs) > 0 {
			avgdl[field] = float64(total) / float64(len(docs))
		}
	}

	// 7. meta.json 最后写入，作为段构建完成的标志
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	mb, err := json.Marshal(meta{
		Version: segmentFormatV2,
		Seq:     seq, Docs: len(docs), IDs: ids,
		InvFields: invFields, TextFields: textFields, NumFields: numFields, KwFields: kwFields,
		AvgDL: avgdl,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json.tmp"), mb, 0o644); err != nil {
		return err
	}
	return os.Rename(filepath.Join(dir, "meta.json.tmp"), filepath.Join(dir, "meta.json"))
}

// unionFields 求多组字段名的并集（保持输入顺序、去重）
func unionFields(groups ...[]string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, g := range groups {
		for _, f := range g {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	return out
}

// writeInverted 构建一个字段的倒排索引：position 取自 Token.Position
// （token filter 删除词元会造成跳号），term 频次由 positions 推导
func writeInverted(dir, field string, docs []Doc) error {
	dicW, err := storage.NewFileWriter(filepath.Join(dir, "inv."+field+".dic"))
	if err != nil {
		return err
	}
	pstW, err := storage.NewFileWriter(filepath.Join(dir, "inv."+field+".pst"))
	if err != nil {
		dicW.Close()
		return err
	}

	w := posting.NewFieldWriter(dicW, pstW)
	for docID, d := range docs {
		// 汇总本 doc 内各 term 的出现位置（Token.Position 即位置）
		pos := make(map[string][]uint32, len(d.Terms[field]))
		for _, tok := range d.Terms[field] {
			pos[tok.Term] = append(pos[tok.Term], uint32(tok.Position))
		}
		for term, ps := range pos {
			if err := w.Add(term, uint32(docID), ps); err != nil {
				dicW.Close()
				pstW.Close()
				return err
			}
		}
	}
	if err := w.Finish(); err != nil {
		dicW.Close()
		pstW.Close()
		return err
	}
	if err := dicW.Close(); err != nil {
		return err
	}
	return pstW.Close()
}

// writeStored 构建原文存储
func writeStored(dir string, docs []Doc) error {
	dataW, err := storage.NewFileWriter(filepath.Join(dir, "stored.dat"))
	if err != nil {
		return err
	}
	idxW, err := storage.NewFileWriter(filepath.Join(dir, "stored.idx"))
	if err != nil {
		dataW.Close()
		return err
	}
	w := stored.NewWriter(dataW, idxW)
	for docID, d := range docs {
		if err := w.Add(uint32(docID), d.Raw); err != nil {
			dataW.Close()
			idxW.Close()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		dataW.Close()
		idxW.Close()
		return err
	}
	if err := dataW.Close(); err != nil {
		return err
	}
	return idxW.Close()
}

// writeNumber 构建一个 number 类字段的正排列，缺失值补 0
func writeNumber(dir, field string, docs []Doc) error {
	fw, err := storage.NewFileWriter(filepath.Join(dir, "num."+field))
	if err != nil {
		return err
	}
	w := docvalues.NewNumberWriter(fw)
	for docID, d := range docs {
		if err := w.Add(uint32(docID), d.Nums[field]); err != nil {
			fw.Close()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		fw.Close()
		return err
	}
	return fw.Close()
}

// writeKeyword 构建一个 keyword 字段的 ord 列；空串表示该 doc 无值
func writeKeyword(dir, field string, docs []Doc) error {
	fw, err := storage.NewFileWriter(filepath.Join(dir, "kw."+field))
	if err != nil {
		return err
	}
	w := docvalues.NewKeywordWriter(fw)
	for docID, d := range docs {
		if err := w.Add(uint32(docID), d.Kws[field]); err != nil {
			fw.Close()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		fw.Close()
		return err
	}
	return fw.Close()
}

// writeHas 为全部字段写存在性标记：每文档 1 字节（0/1）
func writeHas(dir string, fields []string, docs []Doc) error {
	for _, field := range fields {
		has := make([]byte, len(docs))
		for docID, d := range docs {
			if d.HasField(field) {
				has[docID] = 1
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "has."+field), has, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// HasField 判断文档是否含有字段值：有分词结果、有数值或 keyword 值非空。
// 与段 has 位同一套语义（writeHas 与引擎缓冲侧的 exists 判断共用本方法）
func (d Doc) HasField(field string) bool {
	if len(d.Terms[field]) > 0 {
		return true
	}
	if _, ok := d.Nums[field]; ok {
		return true
	}
	return d.Kws[field] != ""
}

// writeNorm 构建一个 text 字段的 norm（每文档 term 数），返回全部文档的 term 总数
func writeNorm(dir, field string, docs []Doc) (int64, error) {
	fw, err := storage.NewFileWriter(filepath.Join(dir, "norm."+field))
	if err != nil {
		return 0, err
	}
	w := docvalues.NewNumberWriter(fw)
	var total int64
	for docID, d := range docs {
		dl := int64(len(d.Terms[field]))
		total += dl
		if err := w.Add(uint32(docID), dl); err != nil {
			fw.Close()
			return 0, err
		}
	}
	if err := w.Finish(); err != nil {
		fw.Close()
		return 0, err
	}
	return total, fw.Close()
}
