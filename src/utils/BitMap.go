package utils

import (
	"fmt"
	"os"
	"syscall"
)

// 暂时只支持 1 << 32 位（可以扩展到 1 << 64)
// The Max Size is 0x01 << 32 at present(can expand to 0x01 << 64)
const BitmapSize = 0x01 << 32

// Bitmap 数据结构定义
type Bitmap struct {
	// 保存实际的 bit 数据
	Data []byte
	// 指示该 Bitmap 的 bit 容量
	BitSize uint64
	// 该 Bitmap 被设置为 1 的最大位置（方便遍历）
	Maxpostion uint64
	
	bitmapMmap	*Mmap
}

// NewBitmap 使用默认容量实例化一个 Bitmap
func NewBitmap() *Bitmap {
	return NewBitmapSize(BitmapSize)
}



func MakeBitmapFile() error {
	size := BitmapSize
	if size == 0 || size > BitmapSize {
		size = BitmapSize
	} else if remainder := size % 8; remainder != 0 {
		size += 8 - remainder
	}
	
	
	file_name := "./index/bitmap.bit"
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		return err
	}
	err=syscall.Ftruncate(int(fout.Fd()),int64(size>>3))
	if err != nil {
		fmt.Printf("ftruncate error : %v\n",err)
		return err
	}
	
	return nil
	
}


// NewBitmapSize 根据指定的 size 实例化一个 Bitmap
func NewBitmapSize(size int) *Bitmap {
	if size == 0 || size > BitmapSize {
		size = BitmapSize
	} else if remainder := size % 8; remainder != 0 {
		size += 8 - remainder
	}
	this:=&Bitmap{Data: make([]byte, size>>3), BitSize: uint64(size - 1)}
	
	this.ReadBitmapFile()
	return this
	//return &Bitmap{Data: make([]byte, size>>3), BitSize: uint64(size - 1)}
}

// SetBit 将 offset 位置的 bit 置为 value (0/1)
func (this *Bitmap) SetBit(offset uint64, value uint8) bool {
	index, pos := offset/8, offset%8

	if this.BitSize < offset {
		return false
	}

	if value == 0 {
		// &^ 清位
		this.Data[index] &^= 0x01 << pos
	} else {
		this.Data[index] |= 0x01 << pos

		// 记录曾经设置为 1 的最大位置
		if this.Maxpostion < offset {
			this.Maxpostion = offset
		}
	}

	return true
}

// GetBit 获得 offset 位置处的 value
func (this *Bitmap) GetBit(offset uint64) uint8 {
	index, pos := offset/8, offset%8

	if this.BitSize < offset {
		return 0
	}

	return (this.Data[index] >> pos) & 0x01
}

// Maxpos 获的置为 1 的最大位置
func (this *Bitmap) Maxpos() uint64 {
	return this.Maxpostion
}

// String 实现 Stringer 接口（只输出开始的100个元素）
func (this *Bitmap) String() string {
	var maxTotal, bitTotal uint64 = 100, this.Maxpostion + 1

	if this.Maxpostion > maxTotal {
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




func (this *Bitmap) ReadBitmapFile () error {
	
	f, err := os.OpenFile("./index/bitmap.bit",os.O_RDWR,0664)
	
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}
	
	this.Data, err = syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return err
	}
	
	return nil
	
}