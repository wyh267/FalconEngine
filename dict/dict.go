// Package dict 实现有序字符串字典：term -> uint64 value 的持久化映射。
//
// 文件格式（单个文件）：
//
//	[数据区][索引区][文件尾 8B = 索引区起始偏移]
//
// 数据区：按 key 字典序排列的记录序列，每 32 条为一个块。
//
//	记录格式：[uvarint keyLen][key bytes][uvarint value]
//
// 索引区：稀疏索引，每块第一条 key 的条目：
//
//	[uvarint 块数] 然后每块 [uvarint keyLen][key][uvarint 块在数据区的起始偏移]
package dict

import (
	"errors"
	"fmt"
	"sort"

	"github.com/FalconEngine/falcon/types"
)

// blockSize 每个数据块包含的记录条数
const blockSize = 32

// Builder 有序字典构建器，要求 key 按字典序严格递增写入。
type Builder struct {
	w types.Writer

	count int    // 已写入的记录总数
	last  string // 上一次写入的 key，用于校验严格递增

	// 稀疏索引（每块第一条 key 及其块起始偏移），Finish 时落盘
	indexKeys    []string
	indexOffsets []uint64
}

// NewBuilder 基于底层顺序写入器创建字典构建器。
func NewBuilder(w types.Writer) *Builder {
	return &Builder{w: w}
}

// Add 追加一条 key->value 记录。key 必须按字典序严格递增，否则返回错误。
func (b *Builder) Add(key string, value uint64) error {
	if b.count > 0 && key <= b.last {
		return fmt.Errorf("dict: key 必须严格递增，当前 %q，上一条 %q", key, b.last)
	}

	// 每块第一条记录建立稀疏索引项
	if b.count%blockSize == 0 {
		b.indexKeys = append(b.indexKeys, key)
		b.indexOffsets = append(b.indexOffsets, uint64(b.w.Offset()))
	}

	// 记录：[uvarint keyLen][key bytes][uvarint value]
	if err := b.w.WriteUvarint(uint64(len(key))); err != nil {
		return err
	}
	if _, err := b.w.WriteBytes([]byte(key)); err != nil {
		return err
	}
	if err := b.w.WriteUvarint(value); err != nil {
		return err
	}

	b.last = key
	b.count++
	return nil
}

// Finish 写完索引区和文件尾，并 Sync 底层写入器。
// 调用后不应再调用 Add。
func (b *Builder) Finish() error {
	indexOffset := b.w.Offset()

	// 索引区：[uvarint 块数] 每块 [uvarint keyLen][key][uvarint 块起始偏移]
	if err := b.w.WriteUvarint(uint64(len(b.indexKeys))); err != nil {
		return err
	}
	for i, key := range b.indexKeys {
		if err := b.w.WriteUvarint(uint64(len(key))); err != nil {
			return err
		}
		if _, err := b.w.WriteBytes([]byte(key)); err != nil {
			return err
		}
		if err := b.w.WriteUvarint(b.indexOffsets[i]); err != nil {
			return err
		}
	}

	// 文件尾 8B：索引区起始偏移
	if err := b.w.WriteUint64(uint64(indexOffset)); err != nil {
		return err
	}
	return b.w.Sync()
}

// Count 返回已写入的记录条数。
func (b *Builder) Count() int {
	return b.count
}

// Reader 只读字典。索引区常驻内存，数据区留在存储后端按需读取。
type Reader struct {
	r types.RandomReader

	keys    []string // 每块第一条 key
	offsets []uint64 // 每块在数据区的起始偏移

	indexOffset int64 // 索引区起始偏移（即数据区结束位置），用于界定最后一个块的边界
	count       int   // 记录总数
}

// Open 打开字典：只把索引区加载进内存，数据区留在存储后端。
func Open(r types.RandomReader) (*Reader, error) {
	n := r.Len()
	if n < 8 {
		return nil, errors.New("dict: 文件太小，缺少文件尾")
	}
	indexOffset, err := r.ReadUint64(n - 8)
	if err != nil {
		return nil, fmt.Errorf("dict: 读取文件尾失败: %w", err)
	}
	if indexOffset > uint64(n-8) {
		return nil, errors.New("dict: 索引区偏移越界")
	}

	// 读取索引区
	off := int64(indexOffset)
	blockNum, off, err := r.ReadUvarint(off)
	if err != nil {
		return nil, fmt.Errorf("dict: 读取块数失败: %w", err)
	}

	reader := &Reader{
		r:           r,
		keys:        make([]string, 0, blockNum),
		offsets:     make([]uint64, 0, blockNum),
		indexOffset: int64(indexOffset),
	}
	for i := uint64(0); i < blockNum; i++ {
		keyLen, noff, err := r.ReadUvarint(off)
		if err != nil {
			return nil, fmt.Errorf("dict: 读取索引 keyLen 失败: %w", err)
		}
		off = noff
		keyBytes, err := r.ReadBytes(off, int64(keyLen))
		if err != nil {
			return nil, fmt.Errorf("dict: 读取索引 key 失败: %w", err)
		}
		off += int64(keyLen)
		blkOff, noff, err := r.ReadUvarint(off)
		if err != nil {
			return nil, fmt.Errorf("dict: 读取索引偏移失败: %w", err)
		}
		off = noff
		// ReadBytes 可能返回零拷贝子切片，key 需要常驻内存，拷贝一份
		reader.keys = append(reader.keys, string(keyBytes))
		reader.offsets = append(reader.offsets, blkOff)
	}

	// 计算记录总数：前 blockNum-1 个块都是满的，扫描最后一个块数出条数
	if blockNum > 0 {
		last := int(blockNum) - 1
		cnt, err := reader.scanBlock(last, nil)
		if err != nil {
			return nil, fmt.Errorf("dict: 统计最后一块记录数失败: %w", err)
		}
		reader.count = last*blockSize + cnt
	}

	return reader, nil
}

// Get 查询 key 对应的 value。二分索引定位块，块内顺序扫描。
func (r *Reader) Get(key string) (uint64, bool) {
	if len(r.keys) == 0 {
		return 0, false
	}

	// 定位块：找到第一个 keys[i] > key 的块，目标块是其前一块
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] > key })
	if i == 0 {
		// key 比首块首 key 还小，不存在
		return 0, false
	}
	blk := i - 1

	var found uint64
	var hit bool
	_, err := r.scanBlock(blk, func(k string, v uint64) bool {
		if k == key {
			found, hit = v, true
			return false // 命中，停止扫描
		}
		// 块内 key 字典序递增，越过目标即可提前结束
		return k < key
	})
	if err != nil {
		return 0, false
	}
	return found, hit
}

// scanBlock 顺序扫描第 blk 个块内的记录。
// visit 为 nil 时只统计条数；否则对每条记录调用 visit，返回 false 时提前停止。
// 返回扫描过的记录条数。
func (r *Reader) scanBlock(blk int, visit func(k string, v uint64) bool) (int, error) {
	off := int64(r.offsets[blk])
	// 块结束位置：下一块的起始偏移；最后一块到索引区为止
	end := r.indexOffset
	if blk+1 < len(r.offsets) {
		end = int64(r.offsets[blk+1])
	}

	scanned := 0
	for off < end && scanned < blockSize {
		keyLen, noff, err := r.r.ReadUvarint(off)
		if err != nil {
			return 0, err
		}
		off = noff
		keyBytes, err := r.r.ReadBytes(off, int64(keyLen))
		if err != nil {
			return 0, err
		}
		off += int64(keyLen)
		value, noff, err := r.r.ReadUvarint(off)
		if err != nil {
			return 0, err
		}
		off = noff

		scanned++
		if visit != nil {
			// key 在块内字典序递增，一旦超过目标就可以提前结束（由调用方判断）
			if !visit(string(keyBytes), value) {
				break
			}
		}
	}
	return scanned, nil
}

// Count 返回字典中的记录总数。
func (r *Reader) Count() int {
	return r.count
}
