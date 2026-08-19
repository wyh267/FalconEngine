package dict

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/FalconEngine/falcon/storage"
)

// 构建并重新打开一个字典，返回 Reader 和写入的 kv（用于断言）
func buildAndOpen(t *testing.T, kvs map[string]uint64, keys []string) *Reader {
	t.Helper()
	store := storage.NewMemoryStore()
	b := NewBuilder(store)
	for _, k := range keys {
		if err := b.Add(k, kvs[k]); err != nil {
			t.Fatalf("Add(%q) 失败: %v", k, err)
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("Finish 失败: %v", err)
	}
	if b.Count() != len(keys) {
		t.Fatalf("Builder.Count() = %d，期望 %d", b.Count(), len(keys))
	}

	r, err := Open(store)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	return r
}

// TestEmptyRoundTrip 空字典回环
func TestEmptyRoundTrip(t *testing.T) {
	r := buildAndOpen(t, map[string]uint64{}, nil)
	if r.Count() != 0 {
		t.Fatalf("Count() = %d，期望 0", r.Count())
	}
	if _, ok := r.Get("anything"); ok {
		t.Fatal("空字典 Get 应返回 false")
	}
}

// TestSingleEntry 1 条记录回环
func TestSingleEntry(t *testing.T) {
	kvs := map[string]uint64{"hello": 42}
	r := buildAndOpen(t, kvs, []string{"hello"})

	if r.Count() != 1 {
		t.Fatalf("Count() = %d，期望 1", r.Count())
	}
	v, ok := r.Get("hello")
	if !ok || v != 42 {
		t.Fatalf("Get(hello) = %d, %v，期望 42, true", v, ok)
	}
	// 比它小的、比它大的、同前缀的 key 都不应命中
	for _, miss := range []string{"", "hell", "hellp", "z"} {
		if _, ok := r.Get(miss); ok {
			t.Fatalf("Get(%q) 应返回 false", miss)
		}
	}
}

// TestLargeRoundTrip 10 万条随机 key 回环，全部 Get 命中
func TestLargeRoundTrip(t *testing.T) {
	const n = 100000
	rng := rand.New(rand.NewSource(20240819))

	kvs := make(map[string]uint64, n)
	for len(kvs) < n {
		// 变长随机 key，保证足够分散、跨大量块
		key := fmt.Sprintf("%016x-%d", rng.Uint64(), rng.Uint64()%8)
		kvs[key] = rng.Uint64()
	}
	keys := make([]string, 0, n)
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	r := buildAndOpen(t, kvs, keys)
	if r.Count() != n {
		t.Fatalf("Count() = %d，期望 %d", r.Count(), n)
	}
	for _, k := range keys {
		v, ok := r.Get(k)
		if !ok {
			t.Fatalf("Get(%q) 未命中", k)
		}
		if v != kvs[k] {
			t.Fatalf("Get(%q) = %d，期望 %d", k, v, kvs[k])
		}
	}
}

// TestMissingKey 不存在的 key 返回 false
func TestMissingKey(t *testing.T) {
	kvs := map[string]uint64{"apple": 1, "banana": 2, "cherry": 3}
	r := buildAndOpen(t, kvs, []string{"apple", "banana", "cherry"})

	for _, miss := range []string{"", "aardvark", "appl", "applf", "blueberry", "cherrz", "zebra"} {
		if _, ok := r.Get(miss); ok {
			t.Fatalf("Get(%q) 应返回 false", miss)
		}
	}
	// 确认已有 key 不受影响
	for k, want := range kvs {
		if v, ok := r.Get(k); !ok || v != want {
			t.Fatalf("Get(%q) = %d, %v，期望 %d, true", k, v, ok, want)
		}
	}
}

// TestOutOfOrderAdd 乱序 Add 报错
func TestOutOfOrderAdd(t *testing.T) {
	store := storage.NewMemoryStore()
	b := NewBuilder(store)

	if err := b.Add("banana", 1); err != nil {
		t.Fatalf("首条 Add 失败: %v", err)
	}
	// 更小的 key：报错
	if err := b.Add("apple", 2); err == nil {
		t.Fatal("乱序 Add 应返回错误")
	}
	// 相等的 key：报错（严格递增）
	if err := b.Add("banana", 3); err == nil {
		t.Fatal("重复 key Add 应返回错误")
	}
	// 正确的递增 key：仍然可以写入
	if err := b.Add("cherry", 4); err != nil {
		t.Fatalf("递增 Add 失败: %v", err)
	}
	if b.Count() != 2 {
		t.Fatalf("Count() = %d，期望 2（失败的 Add 不计数）", b.Count())
	}
}

// TestBlockBoundary 跨越块边界（32 条/块）的回环
func TestBlockBoundary(t *testing.T) {
	const n = 32*3 + 1 // 覆盖：满块 + 半块 + 块首/块尾
	kvs := make(map[string]uint64, n)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		kvs[k] = uint64(i * 7)
		keys = append(keys, k)
	}

	r := buildAndOpen(t, kvs, keys)
	if r.Count() != n {
		t.Fatalf("Count() = %d，期望 %d", r.Count(), n)
	}
	// 重点断言每块首条、末条
	for _, i := range []int{0, 1, 31, 32, 33, 63, 64, 95, 96} {
		k := keys[i]
		if v, ok := r.Get(k); !ok || v != kvs[k] {
			t.Fatalf("Get(%q) = %d, %v，期望 %d, true", k, v, ok, kvs[k])
		}
	}
	// 块边界附近不存在的 key
	for _, miss := range []string{"key-000031\x00", "key-000032\x00", "key-000096\x00"} {
		if _, ok := r.Get(miss); ok {
			t.Fatalf("Get(%q) 应返回 false", miss)
		}
	}
}
