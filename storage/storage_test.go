package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestFileMmapRoundTrip 文件写入 → mmap 读取回环
func TestFileMmapRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	w, err := NewFileWriter(path)
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	off1, err := w.WriteBytes([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteBytes: %v", err)
	}
	if off1 != 0 {
		t.Fatalf("first offset should be 0, got %d", off1)
	}
	if err := w.WriteUint64(0xdeadbeef); err != nil {
		t.Fatalf("WriteUint64: %v", err)
	}
	if err := w.WriteUvarint(300); err != nil {
		t.Fatalf("WriteUvarint: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewMmapReader(path)
	if err != nil {
		t.Fatalf("NewMmapReader: %v", err)
	}
	defer r.Close()

	b, err := r.ReadBytes(0, 5)
	if err != nil || !bytes.Equal(b, []byte("hello")) {
		t.Fatalf("ReadBytes: %v %q", err, b)
	}
	v, err := r.ReadUint64(5)
	if err != nil || v != 0xdeadbeef {
		t.Fatalf("ReadUint64: %v %x", err, v)
	}
	uv, next, err := r.ReadUvarint(13)
	if err != nil || uv != 300 {
		t.Fatalf("ReadUvarint: %v %d", err, uv)
	}
	if next != r.Len() {
		t.Fatalf("uvarint next=%d len=%d", next, r.Len())
	}
}

// TestMmapOutOfRange 越界读取必须报错
func TestMmapOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.dat")
	w, _ := NewFileWriter(path)
	w.WriteBytes([]byte("abc"))
	w.Close()

	r, err := NewMmapReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadBytes(2, 5); err == nil {
		t.Fatal("expect out of range error")
	}
}

// TestMemoryStore 内存后端读写回环
func TestMemoryStore(t *testing.T) {
	ms := NewMemoryStore()
	ms.WriteBytes([]byte("falcon"))
	ms.WriteUint64(42)
	ms.WriteUvarint(7)

	b, _ := ms.ReadBytes(0, 6)
	if !bytes.Equal(b, []byte("falcon")) {
		t.Fatalf("bytes: %q", b)
	}
	v, _ := ms.ReadUint64(6)
	if v != 42 {
		t.Fatalf("uint64: %d", v)
	}
	uv, next, _ := ms.ReadUvarint(14)
	if uv != 7 || next != 15 {
		t.Fatalf("uvarint: %d next=%d", uv, next)
	}
}

// TestFileAppend 追加模式续写偏移正确
func TestFileAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.dat")

	w1, _ := NewFileWriter(path)
	w1.WriteBytes([]byte("12345"))
	w1.Close()

	w2, err := OpenFileWriterForAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	off, _ := w2.WriteBytes([]byte("678"))
	if off != 5 {
		t.Fatalf("append offset: %d", off)
	}
	w2.Close()

	st, _ := os.Stat(path)
	if st.Size() != 8 {
		t.Fatalf("size: %d", st.Size())
	}
}
