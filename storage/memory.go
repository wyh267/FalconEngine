package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/FalconEngine/falcon/types"
)

// MemoryStore 纯内存的读写实现，用于测试与"全内存模式"
type MemoryStore struct {
	buf    []byte
	closed bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) Write(p []byte) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("memory store closed")
	}
	m.buf = append(m.buf, p...)
	return len(p), nil
}

func (m *MemoryStore) WriteBytes(b []byte) (int64, error) {
	off := int64(len(m.buf))
	_, err := m.Write(b)
	return off, err
}

func (m *MemoryStore) WriteUint64(v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, err := m.Write(buf[:])
	return err
}

func (m *MemoryStore) WriteUvarint(v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := m.Write(buf[:n])
	return err
}

func (m *MemoryStore) Offset() int64 { return int64(len(m.buf)) }

func (m *MemoryStore) Sync() error { return nil }

func (m *MemoryStore) Close() error {
	m.closed = true
	return nil
}

// Reopen 清空关闭标记（仅测试用）
func (m *MemoryStore) Reopen() { m.closed = false }

func (m *MemoryStore) ReadBytes(offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset+length > int64(len(m.buf)) {
		return nil, fmt.Errorf("read out of range: offset=%d length=%d size=%d", offset, length, len(m.buf))
	}
	return m.buf[offset : offset+length], nil
}

func (m *MemoryStore) ReadUint64(offset int64) (uint64, error) {
	b, err := m.ReadBytes(offset, 8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (m *MemoryStore) ReadUvarint(offset int64) (uint64, int64, error) {
	if offset < 0 || offset >= int64(len(m.buf)) {
		return 0, offset, fmt.Errorf("read out of range: offset=%d size=%d", offset, len(m.buf))
	}
	v, n := binary.Uvarint(m.buf[offset:])
	if n <= 0 {
		return 0, offset, fmt.Errorf("bad uvarint at offset %d", offset)
	}
	return v, offset + int64(n), nil
}

func (m *MemoryStore) Len() int64 { return int64(len(m.buf)) }

var (
	_ types.Writer       = (*MemoryStore)(nil)
	_ types.RandomReader = (*MemoryStore)(nil)
)
