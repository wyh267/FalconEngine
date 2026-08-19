// Package segment 实现不可变段（segment）的落盘构建与只读访问。
//
// 段目录文件布局：
//
//	meta.json     段元信息（段序号、文档数、外部 ID 列表、字段清单、各 text 字段 avgdl）
//	inv.<field>.dic / inv.<field>.pst  倒排索引（见 posting 包）
//	stored.dat / stored.idx            原文存储（见 stored 包）
//	num.<field>   number 类字段（number/date/bool）正排列（见 docvalues 包）
//	has.<field>   number 类字段的存在性标记：每文档 1 字节（0/1）
//	norm.<field>  text 字段的文档长度（term 数），定长 8B 数组，BM25 用
//	del.bin       删除标记：追加写的 uvarint docID 序列（可不存在）
package segment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/FalconEngine/falcon/docvalues"
	"github.com/FalconEngine/falcon/posting"
	"github.com/FalconEngine/falcon/storage"
	"github.com/FalconEngine/falcon/stored"
)

// Doc 一篇待写入段的文档（已完成字段解析与分词）
type Doc struct {
	ID    string              // 外部文档 ID
	Raw   []byte              // 存储原文（JSON）
	Terms map[string][]string // text/keyword 字段 -> token 序列
	Nums  map[string]int64    // number 类字段 -> 值
}

// meta 段元信息
type meta struct {
	Seq        int                `json:"seq"`         // 段序号，全局单调递增，决定段的新旧顺序
	Docs       int                `json:"docs"`        // 文档总数
	IDs        []string           `json:"ids"`         // 外部文档 ID，下标即段内 docID
	InvFields  []string           `json:"inv_fields"`  // 倒排字段
	TextFields []string           `json:"text_fields"` // text 字段（有 norms）
	NumFields  []string           `json:"num_fields"`  // number 类字段
	AvgDL      map[string]float64 `json:"avgdl"`       // 各 text 字段的平均文档长度（BM25 用）
}

// Write 将一批文档构建为一个不可变段，写入 dir（目录将被创建）。
// seq 为段序号（全局单调递增）；invFields/textFields/numFields 为全部倒排、text、
// number 类字段名（即使本批文档都没有值也要建空文件）。
// 文档在 docs 中的顺序即段内 docID（0,1,2...）。
func Write(dir string, seq int, invFields, textFields, numFields []string, docs []Doc) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("segment: 创建目录失败: %w", err)
	}

	// 1. 倒排索引：逐字段构建 inv.<field>.dic / inv.<field>.pst
	for _, field := range invFields {
		if err := writeInverted(dir, field, docs); err != nil {
			return err
		}
	}

	// 2. 原文：stored.dat + stored.idx
	if err := writeStored(dir, docs); err != nil {
		return err
	}

	// 3. num.<field> + has.<field>：number 类正排列与存在性标记，缺失值补 0
	for _, field := range numFields {
		if err := writeNumber(dir, field, docs); err != nil {
			return err
		}
	}

	// 4. norm.<field>：text 字段文档长度（term 数），同时统计 avgdl
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

	// 5. meta.json 最后写入，作为段构建完成的标志
	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	mb, err := json.Marshal(meta{
		Seq: seq, Docs: len(docs), IDs: ids,
		InvFields: invFields, TextFields: textFields, NumFields: numFields,
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

// writeInverted 构建一个字段的倒排索引
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
		// 统计本 doc 内各 term 词频
		tf := make(map[string]uint32, len(d.Terms[field]))
		for _, tok := range d.Terms[field] {
			tf[tok]++
		}
		for term, cnt := range tf {
			if err := w.Add(term, uint32(docID), cnt); err != nil {
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

// writeNumber 构建一个 number 类字段的正排列与存在性标记
func writeNumber(dir, field string, docs []Doc) error {
	fw, err := storage.NewFileWriter(filepath.Join(dir, "num."+field))
	if err != nil {
		return err
	}
	w := docvalues.NewNumberWriter(fw)
	has := make([]byte, len(docs))
	for docID, d := range docs {
		v, ok := d.Nums[field]
		if ok {
			has[docID] = 1
		}
		if err := w.Add(uint32(docID), v); err != nil {
			fw.Close()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		fw.Close()
		return err
	}
	if err := fw.Close(); err != nil {
		return err
	}
	// 存在性标记：每文档 1 字节
	return os.WriteFile(filepath.Join(dir, "has."+field), has, 0o644)
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
