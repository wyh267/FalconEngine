package utils

import (
	"fmt"
)

// 暂时只支持 1 << 32 位（可以扩展到 1 << 64)
// The Max Size is 0x01 << 32 at present(can expand to 0x01 << 64)
const BitmapSize = 0x01 << 32

// Bitmap 数据结构定义
type Bitmap struct {
	// 保存实际的 bit 数据
	data []byte
	// 指示该 Bitmap 的 bit 容量
	bitsize uint64
	// 该 Bitmap 被设置为 1 的最大位置（方便遍历）
	maxpos uint64
}

// NewBitmap 使用默认容量实例化一个 Bitmap
func NewBitmap() *Bitmap {
	return NewBitmapSize(BitmapSize)
}

// NewBitmapSize 根据指定的 size 实例化一个 Bitmap
func NewBitmapSize(size int) *Bitmap {
	if size == 0 || size > BitmapSize {
		size = BitmapSize
	} else if remainder := size % 8; remainder != 0 {
		size += 8 - remainder
	}

	return &Bitmap{data: make([]byte, size>>3), bitsize: uint64(size - 1)}
}

// SetBit 将 offset 位置的 bit 置为 value (0/1)
func (this *Bitmap) SetBit(offset uint64, value uint8) bool {
	index, pos := offset/8, offset%8

	if this.bitsize < offset {
		return false
	}

	if value == 0 {
		// &^ 清位
		this.data[index] &^= 0x01 << pos
	} else {
		this.data[index] |= 0x01 << pos

		// 记录曾经设置为 1 的最大位置
		if this.maxpos < offset {
			this.maxpos = offset
		}
	}

	return true
}

// GetBit 获得 offset 位置处的 value
func (this *Bitmap) GetBit(offset uint64) uint8 {
	index, pos := offset/8, offset%8

	if this.bitsize < offset {
		return 0
	}

	return (this.data[index] >> pos) & 0x01
}

// Maxpos 获的置为 1 的最大位置
func (this *Bitmap) Maxpos() uint64 {
	return this.maxpos
}

// String 实现 Stringer 接口（只输出开始的100个元素）
func (this *Bitmap) String() string {
	var maxTotal, bitTotal uint64 = 100, this.maxpos + 1
	
	if this.maxpos > maxTotal {
		bitTotal = maxTotal
	}
	
	numSlice := make([]uint64, 0, bitTotal)

	var offset uint64
	for offset = 0; offset < bitTotal; offset++ {
		if this.GetBit(offset) == 1 {
			numSlice = append(numSlice, offset)
		}
	}

	return fmt.Sprintf("%v", numSlice)
}