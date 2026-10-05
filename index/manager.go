package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/FalconEngine/falcon/schema"
)

// indexNamePattern 合法索引名：字母数字、下划线、连字符（防止路径穿越）
var indexNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// IndexSettings 索引设置
type IndexSettings struct {
	NumShards   int `json:"number_of_shards"`   // 分片数，默认 1
	NumReplicas int `json:"number_of_replicas"` // 副本数，默认 0
	// WaitAll 对应 write.wait_for_active_shards："all" 时写请求等全部
	// 存活副本 ack 后才返回；默认 "1"（仅 primary）
	WaitAll string `json:"write_wait_for_active_shards,omitempty"`
	// DateDetection 动态 mapping 日期检测开关：字符串值可解析为日期时
	// 推断为 date 字段（nil 表示默认 true，对齐 ES）
	DateDetection *bool `json:"date_detection,omitempty"`
}

// DateDetectionOn 动态 mapping 日期检测是否开启（未设置时默认 true）
func (s IndexSettings) DateDetectionOn() bool {
	return s.DateDetection == nil || *s.DateDetection
}

// indexMeta 索引元信息文件（<data>/<index>/index.json）：
// 记录分片设置，重启后按此恢复各分片
type indexMeta struct {
	NumShards   int    `json:"number_of_shards"`
	NumReplicas int    `json:"number_of_replicas"`
	WaitAll     string `json:"write_wait_for_active_shards,omitempty"`
	// DateDetection 动态 mapping 日期检测开关（nil 默认 true，见 IndexSettings）
	DateDetection *bool `json:"date_detection,omitempty"`
}

// dateDetectionOn 同 IndexSettings.DateDetectionOn（元信息视图）
func (m indexMeta) dateDetectionOn() bool {
	return m.DateDetection == nil || *m.DateDetection
}

// IndexInfo 索引摘要信息
type IndexInfo struct {
	Name   string `json:"name"`
	Docs   int    `json:"docs"`
	Segs   int    `json:"segs"`
	Shards int    `json:"shards"`
}

// Manager 多索引管理器：一个数据目录下的全部索引（每个索引 N 个分片）
type Manager struct {
	dir string

	mu      sync.RWMutex
	indices map[string]*Index
}

// OpenManager 打开数据目录，扫描并加载全部已有索引及其分片
func OpenManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("index: 创建数据目录失败: %w", err)
	}
	m := &Manager{dir: dir, indices: map[string]*Index{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		if !ent.IsDir() || !indexNamePattern.MatchString(ent.Name()) {
			continue
		}
		sub := filepath.Join(dir, ent.Name())
		meta, err := loadIndexMeta(sub)
		if err != nil {
			continue // 非索引目录
		}
		// 扫描本地持有的分片目录 shard-<i>
		var shardIDs []int
		subEntries, err := os.ReadDir(sub)
		if err != nil {
			return nil, err
		}
		for _, se := range subEntries {
			if se.IsDir() && strings.HasPrefix(se.Name(), "shard-") {
				if id, err := strconv.Atoi(strings.TrimPrefix(se.Name(), "shard-")); err == nil {
					shardIDs = append(shardIDs, id)
				}
			}
		}
		ix, err := OpenIndex(sub, ent.Name(), meta.NumShards, meta.NumReplicas, meta.dateDetectionOn(), nil, shardIDs)
		if err != nil {
			return nil, fmt.Errorf("index: 加载索引 %q 失败: %w", ent.Name(), err)
		}
		m.indices[ent.Name()] = ix
	}
	return m, nil
}

// loadIndexMeta 读取索引元信息；无 index.json 视为非索引目录
func loadIndexMeta(dir string) (indexMeta, error) {
	var meta indexMeta
	b, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return meta, err
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return meta, fmt.Errorf("index: 解析 index.json 失败: %w", err)
	}
	if meta.NumShards <= 0 {
		meta.NumShards = 1
	}
	return meta, nil
}

// Create 创建索引。sch 为 nil 时使用空 schema（纯动态 mapping）；索引已存在返回错误
func (m *Manager) Create(name string, settings IndexSettings, sch *schema.Schema) (*Index, error) {
	if !indexNamePattern.MatchString(name) {
		return nil, fmt.Errorf("index: 非法索引名 %q（仅允许字母数字、_、-）", name)
	}
	if settings.NumShards <= 0 {
		settings.NumShards = 1
	}
	if sch == nil {
		var err error
		if sch, err = schema.New(nil); err != nil {
			return nil, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.indices[name]; dup {
		return nil, fmt.Errorf("index: 索引 %q 已存在", name)
	}
	dir := filepath.Join(m.dir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// 先写元信息，再建分片（元信息存在即视为索引存在）
	mb, _ := json.Marshal(indexMeta{NumShards: settings.NumShards, NumReplicas: settings.NumReplicas, WaitAll: settings.WaitAll, DateDetection: settings.DateDetection})
	if err := os.WriteFile(filepath.Join(dir, "index.json"), mb, 0o644); err != nil {
		return nil, err
	}
	ix, err := OpenIndex(dir, name, settings.NumShards, settings.NumReplicas, settings.DateDetectionOn(), sch, nil)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	m.indices[name] = ix
	return ix, nil
}

// EnsureShard 确保索引存在且本地持有指定分片（集群分配落地用）。
// 索引不存在时以空分片集创建（只持有分配的片），幂等。
func (m *Manager) EnsureShard(name string, settings IndexSettings, sch *schema.Schema, shardID int) error {
	if !indexNamePattern.MatchString(name) {
		return fmt.Errorf("index: 非法索引名 %q（仅允许字母数字、_、-）", name)
	}
	if settings.NumShards <= 0 {
		settings.NumShards = 1
	}
	m.mu.Lock()
	ix, ok := m.indices[name]
	m.mu.Unlock()
	if !ok {
		// 创建只含空分片集的索引
		dir := filepath.Join(m.dir, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		mb, _ := json.Marshal(indexMeta{NumShards: settings.NumShards, NumReplicas: settings.NumReplicas, WaitAll: settings.WaitAll, DateDetection: settings.DateDetection})
		if err := os.WriteFile(filepath.Join(dir, "index.json"), mb, 0o644); err != nil {
			return err
		}
		var err error
		ix, err = OpenIndex(dir, name, settings.NumShards, settings.NumReplicas, settings.DateDetectionOn(), sch, []int{})
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.indices[name] = ix
		m.mu.Unlock()
	}
	return ix.OpenShard(shardID, sch)
}

// Get 取索引；不存在返回 false
func (m *Manager) Get(name string) (*Index, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ix, ok := m.indices[name]
	return ix, ok
}

// Delete 删除索引：关闭全部分片并移除数据目录；不存在返回 false
func (m *Manager) Delete(name string) (bool, error) {
	m.mu.Lock()
	ix, ok := m.indices[name]
	if ok {
		delete(m.indices, name)
	}
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	if err := ix.Close(); err != nil {
		return true, err
	}
	return true, os.RemoveAll(filepath.Join(m.dir, name))
}

// List 返回全部索引的摘要信息（按名字升序）
func (m *Manager) List() []IndexInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]IndexInfo, 0, len(m.indices))
	for name, ix := range m.indices {
		out = append(out, IndexInfo{Name: name, Docs: ix.DocCount(), Segs: ix.SegCount(), Shards: ix.NumShards()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names 返回全部索引名（升序）
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.indices))
	for name := range m.indices {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Close 关闭全部索引
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for name, ix := range m.indices {
		if err := ix.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("index: 关闭索引 %q 失败: %w", name, err)
		}
	}
	m.indices = map[string]*Index{}
	return firstErr
}
