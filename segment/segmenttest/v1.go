// Package segmenttest 提供段格式的测试辅助能力。
//
// 当前唯一用途是生成 v1 历史格式段（现 writer 恒写 v2），
// 供 v1/v2 段共存对拍与旧格式读兼容测试使用。
// 本包代码即 v1 writer 的固化副本，不随段格式演进。
package segmenttest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/FalconEngine/falcon/dict"
	"github.com/FalconEngine/falcon/docvalues"
	"github.com/FalconEngine/falcon/segment"
	"github.com/FalconEngine/falcon/storage"
	"github.com/FalconEngine/falcon/stored"
)

// metaV1 v1 段元信息（无版本号、无 keyword 字段清单）
type metaV1 struct {
	Seq        int                `json:"seq"`
	Docs       int                `json:"docs"`
	IDs        []string           `json:"ids"`
	InvFields  []string           `json:"inv_fields"`
	TextFields []string           `json:"text_fields"`
	NumFields  []string           `json:"num_fields"`
	AvgDL      map[string]float64 `json:"avgdl"`
}

// WriteV1 以 v1 历史格式构建段：倒排不带 positions，has 标记仅覆盖 number 类字段，
// 无 kw 列，meta 无版本号。参数语义与 segment.Write 一致。
func WriteV1(dir string, seq int, invFields, textFields, numFields []string, docs []segment.Doc) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("segmenttest: 创建目录失败: %w", err)
	}
	for _, field := range invFields {
		if err := writeInvertedV1(dir, field, docs); err != nil {
			return err
		}
	}
	if err := writeStoredV1(dir, docs); err != nil {
		return err
	}
	for _, field := range numFields {
		if err := writeNumberV1(dir, field, docs); err != nil {
			return err
		}
	}
	avgdl := make(map[string]float64, len(textFields))
	for _, field := range textFields {
		total, err := writeNormV1(dir, field, docs)
		if err != nil {
			return err
		}
		if len(docs) > 0 {
			avgdl[field] = float64(total) / float64(len(docs))
		}
	}

	ids := make([]string, len(docs))
	for i, d := range docs {
		ids[i] = d.ID
	}
	mb, err := json.Marshal(metaV1{
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

// writeInvertedV1 构建 v1 倒排：term -> (docID, freq)，无 positions
func writeInvertedV1(dir, field string, docs []segment.Doc) error {
	dicW, err := storage.NewFileWriter(filepath.Join(dir, "inv."+field+".dic"))
	if err != nil {
		return err
	}
	pstW, err := storage.NewFileWriter(filepath.Join(dir, "inv."+field+".pst"))
	if err != nil {
		dicW.Close()
		return err
	}

	// 收集 term -> 倒排记录（docID 递增）
	type docFreq struct {
		docID uint32
		freq  uint32
	}
	terms := make(map[string][]docFreq)
	for docID, d := range docs {
		tf := make(map[string]uint32, len(d.Terms[field]))
		for _, tok := range d.Terms[field] {
			tf[tok.Term]++
		}
		for term, cnt := range tf {
			terms[term] = append(terms[term], docFreq{docID: uint32(docID), freq: cnt})
		}
	}
	sorted := make([]string, 0, len(terms))
	for t := range terms {
		sorted = append(sorted, t)
	}
	sort.Strings(sorted)

	// 逐个 term 写 v1 倒排段：[uvarint docFreq][8B 块索引偏移][块数据][块索引]，
	// 块内记录 [uvarint docDelta][uvarint freq]，每 128 doc 一块（与 posting.BlockSize 一致）
	const blockSize = 128
	db := dict.NewBuilder(dicW)
	for _, t := range sorted {
		list := terms[t]
		var data, index bytes.Buffer
		prev := uint64(0)
		for start := 0; start < len(list); start += blockSize {
			end := min(start+blockSize, len(list))
			appendUvarintV1(&index, uint64(list[end-1].docID))
			appendUvarintV1(&index, uint64(data.Len()))
			for _, p := range list[start:end] {
				appendUvarintV1(&data, uint64(p.docID)-prev)
				prev = uint64(p.docID)
				appendUvarintV1(&data, uint64(p.freq))
			}
		}
		segOff := pstW.Offset()
		headerLen := int64(uvarintLenV1(uint64(len(list)))) + 8
		if err := pstW.WriteUvarint(uint64(len(list))); err != nil {
			return err
		}
		if err := pstW.WriteUint64(uint64(segOff + headerLen + int64(data.Len()))); err != nil {
			return err
		}
		if _, err := pstW.WriteBytes(data.Bytes()); err != nil {
			return err
		}
		if _, err := pstW.WriteBytes(index.Bytes()); err != nil {
			return err
		}
		if err := db.Add(t, uint64(segOff)); err != nil {
			return err
		}
	}
	if err := db.Finish(); err != nil {
		return err
	}
	if err := pstW.Sync(); err != nil {
		return err
	}
	if err := dicW.Close(); err != nil {
		return err
	}
	return pstW.Close()
}

func writeStoredV1(dir string, docs []segment.Doc) error {
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

// writeNumberV1 构建 v1 number 正排列与存在性标记（v1 的 has 仅覆盖 number 类字段）
func writeNumberV1(dir, field string, docs []segment.Doc) error {
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
	return os.WriteFile(filepath.Join(dir, "has."+field), has, 0o644)
}

func writeNormV1(dir, field string, docs []segment.Doc) (int64, error) {
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

func appendUvarintV1(b *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	b.Write(tmp[:n])
}

func uvarintLenV1(v uint64) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutUvarint(tmp[:], v)
}
