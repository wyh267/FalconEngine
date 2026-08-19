package storage

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/FalconEngine/falcon/types"
)

// FileWriter 基于文件的顺序追加写实现
type FileWriter struct {
	file   *os.File
	offset int64
}

// NewFileWriter 创建（或截断）一个文件并返回顺序写入器
func NewFileWriter(path string) (*FileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", path, err)
	}
	return &FileWriter{file: f}, nil
}

// OpenFileWriterForAppend 以追加模式打开已有文件
func OpenFileWriterForAppend(path string) (*FileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &FileWriter{file: f, offset: st.Size()}, nil
}

func (w *FileWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.offset += int64(n)
	return n, err
}

func (w *FileWriter) WriteBytes(b []byte) (int64, error) {
	off := w.offset
	_, err := w.Write(b)
	return off, err
}

func (w *FileWriter) WriteUint64(v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func (w *FileWriter) WriteUvarint(v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

func (w *FileWriter) Offset() int64 { return w.offset }

func (w *FileWriter) Sync() error { return w.file.Sync() }

func (w *FileWriter) Close() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

var _ types.Writer = (*FileWriter)(nil)
