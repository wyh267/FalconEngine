// Package stored 提供文档原文（stored fields）存储。
// 由两个文件组成（句柄均由调用方提供）：
//
//	数据文件：[uvarint len][doc bytes] 顺序追加
//	索引文件：docID → 数据文件偏移的定长 8B 数组（小端序，可直接 mmap 当数组读）
package stored

import (
	"fmt"

	"github.com/FalconEngine/falcon/types"
)

// Writer 原文写入器，同时向数据文件与索引文件追加
type Writer struct {
	dataW types.Writer
	idxW  types.Writer
	next  uint32 // 下一个期望的 docID
}

// NewWriter 创建原文写入器，dataW 为数据文件，idxW 为索引文件
func NewWriter(dataW, idxW types.Writer) *Writer {
	return &Writer{dataW: dataW, idxW: idxW}
}

// Add 追加一篇文档原文，docID 必须严格递增（从 0 开始连续）
func (w *Writer) Add(docID uint32, doc []byte) error {
	if docID != w.next {
		return fmt.Errorf("stored: docID 必须连续递增, 期望 %d, 收到 %d", w.next, docID)
	}
	off := w.dataW.Offset()
	if err := w.dataW.WriteUvarint(uint64(len(doc))); err != nil {
		return err
	}
	if _, err := w.dataW.WriteBytes(doc); err != nil {
		return err
	}
	if err := w.idxW.WriteUint64(uint64(off)); err != nil {
		return err
	}
	w.next++
	return nil
}

// Finish 刷盘收尾（不关闭底层句柄，句柄由调用方管理）
func (w *Writer) Finish() error {
	if err := w.dataW.Sync(); err != nil {
		return err
	}
	return w.idxW.Sync()
}

// Reader 原文读取器
type Reader struct {
	dataR types.RandomReader
	idxR  types.RandomReader
	n     int
}

// OpenReader 打开原文存储，索引文件长度必须是 8 的整数倍
func OpenReader(dataR, idxR types.RandomReader) (*Reader, error) {
	if idxR.Len()%8 != 0 {
		return nil, fmt.Errorf("stored: 非法索引文件, 长度 %d 不是 8 的整数倍", idxR.Len())
	}
	return &Reader{dataR: dataR, idxR: idxR, n: int(idxR.Len() / 8)}, nil
}

// Get 读取 docID 对应的文档原文（返回拷贝，调用方可安全修改）
func (r *Reader) Get(docID uint32) ([]byte, error) {
	if int(docID) >= r.n {
		return nil, fmt.Errorf("stored: docID %d 越界, 共 %d 篇", docID, r.n)
	}
	off, err := r.idxR.ReadUint64(int64(docID) * 8)
	if err != nil {
		return nil, err
	}
	l, dataOff, err := r.dataR.ReadUvarint(int64(off))
	if err != nil {
		return nil, err
	}
	b, err := r.dataR.ReadBytes(dataOff, int64(l))
	if err != nil {
		return nil, err
	}
	// ReadBytes 可能返回零拷贝切片，复制一份再交给调用方
	return append([]byte(nil), b...), nil
}

// Len 返回文档数
func (r *Reader) Len() int { return r.n }
