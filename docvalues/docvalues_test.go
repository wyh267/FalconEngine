package docvalues

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/storage"
)

// TestNumberRoundTripRandom 随机数据写入 → 独立 Reader 回环对拍
func TestNumberRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const n = 10000
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = rng.Int63() - (1 << 62) // 覆盖正负
	}

	ms := storage.NewMemoryStore()
	w := NewNumberWriter(ms)
	for i, v := range vals {
		if err := w.Add(uint32(i), v); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenNumberReader(ms)
	if err != nil {
		t.Fatalf("OpenNumberReader: %v", err)
	}
	if r.Len() != n {
		t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
	}
	for i, v := range vals {
		if got := r.Get(uint32(i)); got != v {
			t.Fatalf("Get(%d) = %d, 期望 %d", i, got, v)
		}
	}
}

// TestNumberFileRoundTrip 文件写入 → mmap 重新打开回环
func TestNumberFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "num.dv")
	fw, err := storage.NewFileWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewNumberWriter(fw)
	for i := 0; i < 100; i++ {
		if err := w.Add(uint32(i), int64(i*i-50)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	mr, err := storage.NewMmapReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	r, err := OpenNumberReader(mr)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 100 {
		t.Fatalf("Len = %d", r.Len())
	}
	for i := 0; i < 100; i++ {
		if got := r.Get(uint32(i)); got != int64(i*i-50) {
			t.Fatalf("Get(%d) = %d", i, got)
		}
	}
}

// TestNumberEdge 边界：0 文档、1 文档、10 万文档
func TestNumberEdge(t *testing.T) {
	for _, n := range []int{0, 1, 100000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ms := storage.NewMemoryStore()
			w := NewNumberWriter(ms)
			for i := 0; i < n; i++ {
				if err := w.Add(uint32(i), int64(i)*7-3); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenNumberReader(ms)
			if err != nil {
				t.Fatal(err)
			}
			if r.Len() != n {
				t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
			}
			for i := 0; i < n; i++ {
				if got := r.Get(uint32(i)); got != int64(i)*7-3 {
					t.Fatalf("Get(%d) = %d", i, got)
				}
			}
		})
	}
}

// TestNumberNonStrictDocID docID 不连续递增必须报错
func TestNumberNonStrictDocID(t *testing.T) {
	ms := storage.NewMemoryStore()
	w := NewNumberWriter(ms)
	if err := w.Add(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(0, 2); err == nil {
		t.Fatal("重复 docID 应报错")
	}
	if err := w.Add(5, 3); err == nil {
		t.Fatal("跳号 docID 应报错")
	}
}

// TestKeywordRoundTripRandom 随机数据写入 → 独立 Reader 回环对拍（含无值 doc）
func TestKeywordRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const n = 5000
	// 小字典，保证大量重复 term
	dict := make([]string, 50)
	for i := range dict {
		dict[i] = fmt.Sprintf("term-%02d", rng.Intn(40)) // 故意制造重复
	}
	vals := make([]string, n)
	for i := range vals {
		if rng.Intn(4) == 0 {
			vals[i] = "" // 约 1/4 的 doc 无值
		} else {
			vals[i] = dict[rng.Intn(len(dict))]
		}
	}

	ms := storage.NewMemoryStore()
	w := NewKeywordWriter(ms)
	for i, v := range vals {
		if err := w.Add(uint32(i), v); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := OpenKeywordReader(ms)
	if err != nil {
		t.Fatalf("OpenKeywordReader: %v", err)
	}
	if r.Len() != n {
		t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
	}
	// 逐 doc 对拍：无值则 Ord=false，有值则 Term(Ord)==原值 且 Lookup 自洽
	for i, v := range vals {
		ord, ok := r.Ord(uint32(i))
		if v == "" {
			if ok {
				t.Fatalf("doc %d 应无值, 却返回 ord=%d", i, ord)
			}
			continue
		}
		if !ok {
			t.Fatalf("doc %d 应有值 %q", i, v)
		}
		if term := r.Term(ord); term != v {
			t.Fatalf("doc %d Term(%d) = %q, 期望 %q", i, ord, term, v)
		}
		ord2, found := r.Lookup(v)
		if !found || ord2 != ord {
			t.Fatalf("Lookup(%q) = (%d, %v), 期望 (%d, true)", v, ord2, found, ord)
		}
	}
	// NumOrds 应等于去重后的 term 数
	uniq := make(map[string]struct{})
	for _, v := range vals {
		if v != "" {
			uniq[v] = struct{}{}
		}
	}
	if r.NumOrds() != len(uniq) {
		t.Fatalf("NumOrds = %d, 期望 %d", r.NumOrds(), len(uniq))
	}
	// 不存在的 term
	if _, found := r.Lookup("no-such-term"); found {
		t.Fatal("不存在的 term 不应查到")
	}
}

// TestKeywordFileRoundTrip 文件写入 → mmap 重新打开回环
func TestKeywordFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kw.dv")
	fw, err := storage.NewFileWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	vals := []string{"apple", "", "banana", "apple", "", "cherry"}
	w := NewKeywordWriter(fw)
	for i, v := range vals {
		if err := w.Add(uint32(i), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	mr, err := storage.NewMmapReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	r, err := OpenKeywordReader(mr)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != len(vals) || r.NumOrds() != 3 {
		t.Fatalf("Len=%d NumOrds=%d", r.Len(), r.NumOrds())
	}
	for i, v := range vals {
		ord, ok := r.Ord(uint32(i))
		if v == "" {
			if ok {
				t.Fatalf("doc %d 应无值", i)
			}
			continue
		}
		if !ok || r.Term(ord) != v {
			t.Fatalf("doc %d: ok=%v term=%q", i, ok, r.Term(ord))
		}
	}
}

// TestKeywordEdge 边界：0 文档、1 文档、10 万文档
func TestKeywordEdge(t *testing.T) {
	for _, n := range []int{0, 1, 100000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ms := storage.NewMemoryStore()
			w := NewKeywordWriter(ms)
			for i := 0; i < n; i++ {
				v := ""
				if i%2 == 0 {
					v = fmt.Sprintf("t%04d", i%100)
				}
				if err := w.Add(uint32(i), v); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenKeywordReader(ms)
			if err != nil {
				t.Fatal(err)
			}
			if r.Len() != n {
				t.Fatalf("Len = %d, 期望 %d", r.Len(), n)
			}
			for i := 0; i < n; i++ {
				ord, ok := r.Ord(uint32(i))
				if i%2 != 0 {
					if ok {
						t.Fatalf("doc %d 应无值", i)
					}
					continue
				}
				want := fmt.Sprintf("t%04d", i%100)
				if !ok || r.Term(ord) != want {
					t.Fatalf("doc %d: ok=%v term=%q 期望 %q", i, ok, r.Term(ord), want)
				}
			}
		})
	}
}

// TestKeywordNonStrictDocID docID 不连续递增必须报错
func TestKeywordNonStrictDocID(t *testing.T) {
	ms := storage.NewMemoryStore()
	w := NewKeywordWriter(ms)
	if err := w.Add(1, "x"); err == nil {
		t.Fatal("起始 docID 非 0 应报错")
	}
	if err := w.Add(0, "x"); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(0, "y"); err == nil {
		t.Fatal("重复 docID 应报错")
	}
}

// TestKeywordBadMagic 非 keyword 文件打开应报错
func TestKeywordBadMagic(t *testing.T) {
	ms := storage.NewMemoryStore()
	w := NewNumberWriter(ms)
	for i := 0; i < 10; i++ {
		w.Add(uint32(i), int64(i))
	}
	w.Finish()
	if _, err := OpenKeywordReader(ms); err == nil {
		t.Fatal("魔数不匹配应报错")
	}
}
