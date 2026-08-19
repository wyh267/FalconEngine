// Package docvalues 提供列式正排（doc values）存储：数字列与 keyword 列。
// 构建期按 docID=0,1,2... 顺序追加，写完后可由独立 Reader 重新打开读取。
package docvalues

import (
	"fmt"

	"github.com/FalconEngine/falcon/types"
)

// NumberWriter 数字列写入器：每个 docID 定长 8 字节，小端序紧密排列
type NumberWriter struct {
	w    types.Writer
	next uint32 // 下一个期望的 docID
}

// NewNumberWriter 基于底层顺序写入器创建数字列写入器
func NewNumberWriter(w types.Writer) *NumberWriter {
	return &NumberWriter{w: w}
}

// Add 追加一条数字值，docID 必须严格递增（从 0 开始连续）
func (w *NumberWriter) Add(docID uint32, value int64) error {
	if docID != w.next {
		return fmt.Errorf("docvalues: number docID 必须连续递增, 期望 %d, 收到 %d", w.next, docID)
	}
	if err := w.w.WriteUint64(uint64(value)); err != nil {
		return err
	}
	w.next++
	return nil
}

// Finish 刷盘收尾（不关闭底层句柄，句柄由调用方管理）
func (w *NumberWriter) Finish() error {
	return w.w.Sync()
}

// NumberReader 数字列读取器：直接按偏移当定长数组随机读
type NumberReader struct {
	r types.RandomReader
	n int
}

// OpenNumberReader 打开数字列，文件长度必须是 8 的整数倍
func OpenNumberReader(r types.RandomReader) (*NumberReader, error) {
	if r.Len()%8 != 0 {
		return nil, fmt.Errorf("docvalues: 非法 number 文件, 长度 %d 不是 8 的整数倍", r.Len())
	}
	return &NumberReader{r: r, n: int(r.Len() / 8)}, nil
}

// Get 读取 docID 对应的值；越界时返回 0
func (r *NumberReader) Get(docID uint32) int64 {
	v, err := r.r.ReadUint64(int64(docID) * 8)
	if err != nil {
		return 0
	}
	return int64(v)
}

// Len 返回文档数
func (r *NumberReader) Len() int { return r.n }
