// raft 元数据持久化层：自研精简 WAL + 快照文件，与 translog/translog.go 同构
// （复用 pkg/coding CRC 帧 + storage.FileWriter，零新增依赖）。
//
// 目录布局（<data>/.falcon/raft/）：
//
//	node.json         持久化 RaftID（每数据目录唯一，重启复用）
//	wal-<n>.log       预写日志：一串 CRC 帧，payload 为 JSON walRecord
//	                  （HardState/Entry 先 proto.Marshal 成字节再装帧，JSON 自动 base64）
//	snap-<index>.json 快照：index/term/conf_state（proto 字节）+ CSM data
//
// 崩溃窗口约定：快照文件先落盘再轮替 WAL（轮替后的新 WAL 先写完并 Sync
// 再删旧 WAL）；快照 index 与 WAL 条目重叠合法，重启按 index 对齐幂等重放。
package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/FalconEngine/falcon/pkg/coding"
	"github.com/FalconEngine/falcon/storage"
)

// walRecord 一条 WAL 记录：一批待追加日志或一次 HardState 更新
type walRecord struct {
	Kind      string   `json:"kind"`                 // walKindHardState / walKindEntries
	HardState []byte   `json:"hard_state,omitempty"` // proto.Marshal(raftpb.HardState)
	Entries   [][]byte `json:"entries,omitempty"`    // 每条 proto.Marshal(raftpb.Entry)
}

const (
	walKindHardState = "hardstate"
	walKindEntries   = "entries"
)

// snapFile 快照文件（snap-<index>.json）内容
type snapFile struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	ConfState []byte `json:"conf_state"` // proto.Marshal(raftpb.ConfState)
	Data      []byte `json:"data"`       // CSM JSON（StateMachine.Snapshot 产出）
}

// RaftDir 返回 raft 元数据目录（<data>/.falcon/raft/）。
// 点开头目录不会被 index.Manager 的索引扫描误认（indexNamePattern 不允许点开头）。
func RaftDir(dataDir string) string {
	return filepath.Join(dataDir, ".falcon", "raft")
}

// LoadOrCreateRaftID 读取 <dataDir>/.falcon/raft/node.json 中持久化的 RaftID；
// 不存在时用 gen 生成并先落盘（先落盘再启动 raft，保证同一数据目录重启后 RaftID 不变）。
func LoadOrCreateRaftID(dataDir string, gen func() uint64) (uint64, error) {
	dir := RaftDir(dataDir)
	path := filepath.Join(dir, "node.json")
	if b, err := os.ReadFile(path); err == nil {
		var v struct {
			RaftID uint64 `json:"raft_id"`
		}
		if err := json.Unmarshal(b, &v); err == nil && v.RaftID != 0 {
			return v.RaftID, nil
		}
	}
	id := gen()
	if id == 0 {
		return 0, fmt.Errorf("cluster: 生成的 RaftID 为 0")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	b, _ := json.Marshal(struct {
		RaftID uint64 `json:"raft_id"`
	}{RaftID: id})
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return 0, fmt.Errorf("cluster: 持久化 RaftID 失败: %w", err)
	}
	return id, nil
}

// raftStore 单 raft 成员的持久化存储（仅 master 节点持有）
type raftStore struct {
	mu     sync.Mutex
	dir    string
	walGen uint64 // 当前 WAL 代际（wal-<n>.log 的 n）
	wal    *storage.FileWriter
	// hasSnap 启动扫描时是否存在快照文件
	hasSnap  bool
	walCount int64 // 启动扫描到的有效 WAL 记录数
	closed   bool
}

func walPath(dir string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%d.log", gen))
}

// openRaftStore 打开（必要时创建）raft 元数据目录。
// 启动扫描：取最高代际 WAL（尾部半帧截断容忍并修复截断）、最高 index 快照；
// 崩溃在轮替窗口留下的旧 WAL/旧快照一并清理。
func openRaftStore(dir string) (*raftStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &raftStore{dir: dir}

	// 快照：取最高 index，清理其余
	var snapIndexes []uint64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasPrefix(name, "snap-") && strings.HasSuffix(name, ".json") {
			if idx, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "snap-"), ".json"), 10, 64); err == nil {
				snapIndexes = append(snapIndexes, idx)
			}
		}
	}
	if len(snapIndexes) > 0 {
		best := maxUint64(snapIndexes)
		s.hasSnap = true
		for _, idx := range snapIndexes {
			if idx != best {
				os.Remove(filepath.Join(dir, fmt.Sprintf("snap-%d.json", idx)))
			}
		}
	}

	// WAL：取最高代际，清理其余（轮替崩溃窗口可能留下两代）
	var gens []uint64
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			if g, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), 10, 64); err == nil {
				gens = append(gens, g)
			}
		}
	}
	if len(gens) > 0 {
		s.walGen = maxUint64(gens)
		for _, g := range gens {
			if g != s.walGen {
				os.Remove(walPath(dir, g))
			}
		}
		// 扫描有效记录数并修复尾部半帧（与 translog 相同的截断容忍模式）
		validEnd, count, err := scanWAL(walPath(dir, s.walGen), nil)
		if err != nil {
			return nil, err
		}
		s.walCount = count
		if st, err := os.Stat(walPath(dir, s.walGen)); err == nil && st.Size() != validEnd {
			if err := os.Truncate(walPath(dir, s.walGen), validEnd); err != nil {
				return nil, fmt.Errorf("cluster: 截断 WAL 尾部半帧失败: %w", err)
			}
		}
	}

	w, err := storage.OpenFileWriterForAppend(walPath(dir, s.walGen))
	if err != nil {
		return nil, err
	}
	s.wal = w
	return s, nil
}

func maxUint64(v []uint64) uint64 {
	m := v[0]
	for _, x := range v[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

// hasState 是否有可恢复的 raft 状态（有快照或 WAL 中有有效记录）
func (s *raftStore) hasState() bool { return s.hasSnap || s.walCount > 0 }

// load 读取最新快照与 WAL 全部有效记录。
// 无快照时 snap 为 nil；无 HardState 记录时 hs 为 nil。
func (s *raftStore) load() (snap *raftpb.Snapshot, hs *raftpb.HardState, entries []*raftpb.Entry, err error) {
	if s.hasSnap {
		snap, err = s.loadLatestSnapshot()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	_, _, err = scanWAL(walPath(s.dir, s.walGen), func(rec walRecord) error {
		switch rec.Kind {
		case walKindHardState:
			var h raftpb.HardState
			if err := proto.Unmarshal(rec.HardState, &h); err != nil {
				return fmt.Errorf("cluster: WAL hardstate 解码失败: %w", err)
			}
			hs = &h
		case walKindEntries:
			for _, raw := range rec.Entries {
				var e raftpb.Entry
				if err := proto.Unmarshal(raw, &e); err != nil {
					return fmt.Errorf("cluster: WAL entry 解码失败: %w", err)
				}
				entries = append(entries, &e)
			}
		}
		return nil
	})
	return snap, hs, entries, err
}

// loadLatestSnapshot 读取最高 index 的快照文件
func (s *raftStore) loadLatestSnapshot() (*raftpb.Snapshot, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	best := int64(-1)
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasPrefix(name, "snap-") && strings.HasSuffix(name, ".json") {
			if idx, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "snap-"), ".json"), 10, 64); err == nil && idx > best {
				best = idx
			}
		}
	}
	if best < 0 {
		return nil, fmt.Errorf("cluster: 快照文件不存在")
	}
	b, err := os.ReadFile(filepath.Join(s.dir, fmt.Sprintf("snap-%d.json", best)))
	if err != nil {
		return nil, err
	}
	var sf snapFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return nil, fmt.Errorf("cluster: 快照文件解码失败: %w", err)
	}
	var cs raftpb.ConfState
	if len(sf.ConfState) > 0 {
		if err := proto.Unmarshal(sf.ConfState, &cs); err != nil {
			return nil, fmt.Errorf("cluster: 快照 conf_state 解码失败: %w", err)
		}
	}
	return &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: ptrUint64(sf.Index), Term: ptrUint64(sf.Term), ConfState: &cs},
		Data:     sf.Data,
	}, nil
}

func ptrUint64(v uint64) *uint64 { return &v }

// append 把一批 raft 日志追加到 WAL 并刷盘
func (s *raftStore) append(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	rec := walRecord{Kind: walKindEntries, Entries: make([][]byte, 0, len(entries))}
	for _, e := range entries {
		raw, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		rec.Entries = append(rec.Entries, raw)
	}
	return s.writeRecord(rec)
}

// saveHardState 把 HardState 追加到 WAL 并刷盘
func (s *raftStore) saveHardState(hs *raftpb.HardState) error {
	raw, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	return s.writeRecord(walRecord{Kind: walKindHardState, HardState: raw})
}

// saveSnapshot 落盘快照并轮替 WAL：
//  1. 写 snap-<index>.json 并 Sync（崩溃窗口：先 snap 落盘再截断 WAL）
//  2. 新建 wal-<n+1>.log，写入 HardState 与快照之后的尾部未提交条目并 Sync
//  3. 删除旧 WAL 与旧快照
//
// HardState.Commit 小于快照 index 时提升到快照 index（快照已覆盖之前的日志；
// hs 克隆后修改，不污染 raft 侧的 Ready 状态）。
func (s *raftStore) saveSnapshot(snap *raftpb.Snapshot, hs *raftpb.HardState, tail []*raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("cluster: raft store 已关闭")
	}
	idx := snap.Metadata.GetIndex()
	hs = proto.Clone(hs).(*raftpb.HardState)
	if hs.GetCommit() < idx {
		hs.Commit = ptrUint64(idx)
	}

	// 1. 快照文件落盘
	csRaw, err := proto.Marshal(snap.Metadata.GetConfState())
	if err != nil {
		return err
	}
	sf := snapFile{Index: idx, Term: snap.Metadata.GetTerm(), ConfState: csRaw, Data: snap.Data}
	b, err := json.Marshal(sf)
	if err != nil {
		return err
	}
	snapPath := filepath.Join(s.dir, fmt.Sprintf("snap-%d.json", idx))
	if err := os.WriteFile(snapPath, b, 0o644); err != nil {
		return err
	}
	if f, err := os.Open(snapPath); err == nil {
		f.Sync()
		f.Close()
	}

	// 2. 轮替 WAL：新文件先写完并 Sync，再删旧文件
	oldPath := walPath(s.dir, s.walGen)
	if err := s.wal.Close(); err != nil {
		return err
	}
	s.walGen++
	w, err := storage.NewFileWriter(walPath(s.dir, s.walGen))
	if err != nil {
		return err
	}
	s.wal = w
	hsRaw, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	if err := s.writeRecordLocked(walRecord{Kind: walKindHardState, HardState: hsRaw}); err != nil {
		return err
	}
	if len(tail) > 0 {
		rec := walRecord{Kind: walKindEntries, Entries: make([][]byte, 0, len(tail))}
		for _, e := range tail {
			raw, err := proto.Marshal(e)
			if err != nil {
				return err
			}
			rec.Entries = append(rec.Entries, raw)
		}
		if err := s.writeRecordLocked(rec); err != nil {
			return err
		}
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	os.Remove(oldPath)

	// 3. 清理旧快照
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasPrefix(name, "snap-") && strings.HasSuffix(name, ".json") {
			if old, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "snap-"), ".json"), 10, 64); err == nil && old < idx {
				os.Remove(filepath.Join(s.dir, name))
			}
		}
	}
	return nil
}

func (s *raftStore) writeRecord(rec walRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeRecordLocked(rec); err != nil {
		return err
	}
	return s.wal.Sync()
}

func (s *raftStore) writeRecordLocked(rec walRecord) error {
	if s.closed {
		return fmt.Errorf("cluster: raft store 已关闭")
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = coding.WriteFrame(s.wal, payload)
	return err
}

// close 刷盘并关闭（幂等）
func (s *raftStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.Close()
}

// scanWAL 顺序扫描 WAL，对每条完整记录回调；尾部半帧/损坏帧截断容忍。
// 返回有效数据末尾偏移与有效记录数。文件不存在视为空。
func scanWAL(path string, fn func(rec walRecord) error) (validEnd int64, count int64, err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, 0, nil
	}
	r, err := storage.NewMmapReader(path)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()
	var off int64
	for off < r.Len() {
		payload, next, err := coding.ReadFrame(r, off)
		if err != nil {
			break // 尾部半帧 / CRC 损坏：截断容忍
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			break // payload 非合法记录，同样视为损坏终止
		}
		if fn != nil {
			if err := fn(rec); err != nil {
				return 0, 0, err
			}
		}
		off = next
		count++
	}
	return off, count, nil
}
