package stored

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/storage"
)

// TestStoredRoundTripRandom 随机数据写入 → 独立 Reader 回环对拍
func TestStoredRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	const n = 20000
	docs := make([][]byte, n)
	for i := range docs {
		// 模拟 json 原文，长度随机（含少量超长文档）
		l := rng.Intn(200)
		if rng.Intn(100) == 0 {
			l = 5000 + rng.Intn(5000)
		}
		b := make([]byte, l)
		rng.Read(b)
		// 包一层 json 外壳，贴近真实场景
		docs[i] = append(append([]byte(`{"id":`), []byte(fmt.Sprintf("%d", i))...), append([]byte(`,"body":"`), b...)...)
		docs[i] = append(docs[i], '"', '}')
	}

	dataMS := storage.NewMemoryStore()
	idxMS := storage.NewMemoryStore()
	w := NewWriter(dataMS, idxMS)
	for i, d := range docs {
		if err := w.Add(uint32(i), d); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenReader(dataMS, idxMS)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	if r.Len() != n {
		t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
	}
	for i, want := range docs {
		got, err := r.Get(uint32(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%d) 长度 %d, 期望 %d", i, len(got), len(want))
		}
	}
}

// TestStoredFileRoundTrip 文件写入 → mmap 重新打开回环
func TestStoredFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "docs.dat")
	idxPath := filepath.Join(dir, "docs.idx")

	dataFW, err := storage.NewFileWriter(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	idxFW, err := storage.NewFileWriter(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	docs := [][]byte{
		[]byte(`{"title":"hello"}`),
		[]byte(`{}`),
		[]byte(`{"title":"世界","tags":["a","b"]}`),
	}
	w := NewWriter(dataFW, idxFW)
	for i, d := range docs {
		if err := w.Add(uint32(i), d); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := dataFW.Close(); err != nil {
		t.Fatal(err)
	}
	if err := idxFW.Close(); err != nil {
		t.Fatal(err)
	}

	dataR, err := storage.NewMmapReader(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dataR.Close()
	idxR, err := storage.NewMmapReader(idxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idxR.Close()

	r, err := OpenReader(dataR, idxR)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != len(docs) {
		t.Fatalf("Len = %d", r.Len())
	}
	for i, want := range docs {
		got, err := r.Get(uint32(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(%d) = %q, %v", i, got, err)
		}
	}
}

// TestStoredEdge 边界：0 文档、1 文档、10 万文档
func TestStoredEdge(t *testing.T) {
	for _, n := range []int{0, 1, 100000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			dataMS := storage.NewMemoryStore()
			idxMS := storage.NewMemoryStore()
			w := NewWriter(dataMS, idxMS)
			for i := 0; i < n; i++ {
				if err := w.Add(uint32(i), []byte(fmt.Sprintf(`{"id":%d}`, i))); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(dataMS, idxMS)
			if err != nil {
				t.Fatal(err)
			}
			if r.Len() != n {
				t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
			}
			for i := 0; i < n; i++ {
				want := []byte(fmt.Sprintf(`{"id":%d}`, i))
				got, err := r.Get(uint32(i))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("Get(%d) = %q, %v", i, got, err)
				}
			}
		})
	}
}

// TestStoredNonStrictDocID docID 不连续递增必须报错
func TestStoredNonStrictDocID(t *testing.T) {
	dataMS := storage.NewMemoryStore()
	idxMS := storage.NewMemoryStore()
	w := NewWriter(dataMS, idxMS)
	if err := w.Add(1, []byte("x")); err == nil {
		t.Fatal("起始 docID 非 0 应报错")
	}
	if err := w.Add(0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(2, []byte("y")); err == nil {
		t.Fatal("跳号 docID 应报错")
	}
}

// TestStoredOutOfRange 越界 Get 必须报错
func TestStoredOutOfRange(t *testing.T) {
	dataMS := storage.NewMemoryStore()
	idxMS := storage.NewMemoryStore()
	w := NewWriter(dataMS, idxMS)
	w.Add(0, []byte("only"))
	w.Finish()
	r, err := OpenReader(dataMS, idxMS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(1); err == nil {
		t.Fatal("越界 Get 应报错")
	}
}
