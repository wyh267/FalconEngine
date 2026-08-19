package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"

	"github.com/FalconEngine/falcon/types"
)

// MmapReader 基于 mmap 的只读随机访问实现（零拷贝）
type MmapReader struct {
	data []byte
	path string
}

// NewMmapReader 将整个文件映射进内存（只读、共享映射）
func NewMmapReader(path string) (*MmapReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", path, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size == 0 {
		return &MmapReader{path: path}, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return &MmapReader{data: data, path: path}, nil
}

func (r *MmapReader) ReadBytes(offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset+length > int64(len(r.data)) {
		return nil, fmt.Errorf("read out of range: offset=%d length=%d file=%d", offset, length, len(r.data))
	}
	return r.data[offset : offset+length], nil
}

func (r *MmapReader) ReadUint64(offset int64) (uint64, error) {
	b, err := r.ReadBytes(offset, 8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *MmapReader) ReadUvarint(offset int64) (uint64, int64, error) {
	if offset < 0 || offset >= int64(len(r.data)) {
		return 0, offset, fmt.Errorf("read out of range: offset=%d file=%d", offset, len(r.data))
	}
	v, n := binary.Uvarint(r.data[offset:])
	if n <= 0 {
		return 0, offset, fmt.Errorf("bad uvarint at offset %d", offset)
	}
	return v, offset + int64(n), nil
}

func (r *MmapReader) Len() int64 { return int64(len(r.data)) }

func (r *MmapReader) Close() error {
	if r.data == nil {
		return nil
	}
	err := syscall.Munmap(r.data)
	r.data = nil
	return err
}

var _ types.RandomReader = (*MmapReader)(nil)
