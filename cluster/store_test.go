package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// mkEntry 构造一条测试日志
func mkEntry(index, term uint64, data string) *raftpb.Entry {
	t := raftpb.EntryType(raftpb.EntryNormal)
	return &raftpb.Entry{Term: &term, Index: &index, Type: &t, Data: []byte(data)}
}

func mkHardState(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

// TestRaftStoreWALRoundTrip WAL 写读回环：append/saveHardState → close → reopen → load
func TestRaftStoreWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.hasState() {
		t.Fatal("新目录不应有状态")
	}
	if err := s.append([]*raftpb.Entry{mkEntry(1, 1, "cmd-a"), mkEntry(2, 1, "cmd-b")}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveHardState(mkHardState(1, 5, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.append([]*raftpb.Entry{mkEntry(3, 2, "cmd-c")}); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	s2, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	if !s2.hasState() {
		t.Fatal("重开后应有状态")
	}
	snap, hs, entries, err := s2.load()
	if err != nil {
		t.Fatal(err)
	}
	if snap != nil {
		t.Fatalf("未写快照, got %+v", snap)
	}
	if hs.GetTerm() != 1 || hs.GetVote() != 5 || hs.GetCommit() != 2 {
		t.Fatalf("HardState 不一致: %+v", hs)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.GetIndex() != uint64(i+1) || string(e.Data) != fmt.Sprintf("cmd-%c", 'a'+i) {
			t.Fatalf("entry %d 不一致: index=%d data=%s", i, e.GetIndex(), e.Data)
		}
	}
}

// TestRaftStoreWALTruncation 尾部半帧截断容忍：损坏字节被截掉，前缀完好可续写
func TestRaftStoreWALTruncation(t *testing.T) {
	dir := t.TempDir()
	s, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.append([]*raftpb.Entry{mkEntry(1, 1, "ok-1"), mkEntry(2, 1, "ok-2")}); err != nil {
		t.Fatal(err)
	}
	s.close()

	// 追加一段写了一半的垃圾帧（模拟崩溃在半帧写入中）
	f, err := os.OpenFile(walPath(dir, 0), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	_, _, entries, err := s2.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("截断后 entries = %d, want 2", len(entries))
	}
	// 截断修复后可继续追加且可读
	if err := s2.append([]*raftpb.Entry{mkEntry(3, 1, "ok-3")}); err != nil {
		t.Fatal(err)
	}
	s2.close()
	s3, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.close()
	_, _, entries, err = s3.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || string(entries[2].Data) != "ok-3" {
		t.Fatalf("续写后 entries = %v", entries)
	}
}

// TestRaftStoreSnapshotRotation 快照落盘 + WAL 轮替回环：
// 快照后旧 WAL 删除、新 WAL 只剩 HardState+尾部条目，重启 load 得到快照+尾部
func TestRaftStoreSnapshotRotation(t *testing.T) {
	dir := t.TempDir()
	s, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := s.append([]*raftpb.Entry{mkEntry(i, 1, fmt.Sprintf("cmd-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.saveHardState(mkHardState(1, 1, 8)); err != nil {
		t.Fatal(err)
	}

	// 在 index=8 处做快照，WAL 保留 (8,10] 尾部
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	snap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: ptrUint64(8), Term: ptrUint64(1), ConfState: cs},
		Data:     []byte(`{"nodes":{}}`),
	}
	tail := []*raftpb.Entry{mkEntry(9, 1, "cmd-9"), mkEntry(10, 1, "cmd-10")}
	if err := s.saveSnapshot(snap, mkHardState(1, 1, 8), tail); err != nil {
		t.Fatal(err)
	}
	s.close()

	// 旧 WAL 已删除，新 WAL 存在
	if _, err := os.Stat(walPath(dir, 0)); !os.IsNotExist(err) {
		t.Fatal("旧 WAL 应已删除")
	}
	if _, err := os.Stat(walPath(dir, 1)); err != nil {
		t.Fatal("新 WAL 应存在")
	}
	if _, err := os.Stat(filepath.Join(dir, "snap-8.json")); err != nil {
		t.Fatal("快照文件应存在")
	}

	s2, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	if !s2.hasState() {
		t.Fatal("快照+WAL 应有状态")
	}
	gotSnap, hs, entries, err := s2.load()
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap == nil || gotSnap.Metadata.GetIndex() != 8 || gotSnap.Metadata.GetTerm() != 1 {
		t.Fatalf("快照不一致: %+v", gotSnap)
	}
	if vs := gotSnap.Metadata.GetConfState().GetVoters(); len(vs) != 3 || vs[0] != 1 || vs[2] != 3 {
		t.Fatalf("conf_state 不一致: %v", vs)
	}
	if string(gotSnap.Data) != `{"nodes":{}}` {
		t.Fatalf("快照 data 不一致: %s", gotSnap.Data)
	}
	if hs.GetCommit() != 8 {
		t.Fatalf("HardState commit = %d, want 8", hs.GetCommit())
	}
	if len(entries) != 2 || entries[0].GetIndex() != 9 || entries[1].GetIndex() != 10 {
		t.Fatalf("尾部条目不一致: %v", entries)
	}

	// 二次快照：覆盖更新，旧快照文件清理
	snap2 := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: ptrUint64(10), Term: ptrUint64(1), ConfState: cs},
		Data:     []byte(`{"nodes":{"1":{}}}`),
	}
	if err := s2.saveSnapshot(snap2, mkHardState(1, 1, 10), nil); err != nil {
		t.Fatal(err)
	}
	s2.close()
	if _, err := os.Stat(filepath.Join(dir, "snap-8.json")); !os.IsNotExist(err) {
		t.Fatal("旧快照应已清理")
	}
	s3, err := openRaftStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.close()
	gotSnap, hs, entries, err = s3.load()
	if err != nil {
		t.Fatal(err)
	}
	if gotSnap.Metadata.GetIndex() != 10 || string(gotSnap.Data) != `{"nodes":{"1":{}}}` {
		t.Fatalf("二次快照不一致: %+v", gotSnap)
	}
	if hs.GetCommit() != 10 {
		t.Fatalf("二次快照 commit = %d, want 10", hs.GetCommit())
	}
	if len(entries) != 0 {
		t.Fatalf("二次快照后不应有尾部条目: %v", entries)
	}
}

// TestLoadOrCreateRaftID RaftID 持久化：同目录复用，生成前先落盘
func TestLoadOrCreateRaftID(t *testing.T) {
	dir := t.TempDir()
	gen := func() uint64 { return 42 }
	id1, err := LoadOrCreateRaftID(dir, gen)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != 42 {
		t.Fatalf("id1 = %d, want 42", id1)
	}
	// 同目录复用（即使 gen 给出别的值）
	id2, err := LoadOrCreateRaftID(dir, func() uint64 { return 99 })
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 42 {
		t.Fatalf("id2 = %d, want 42（应复用持久化值）", id2)
	}
	if _, err := os.Stat(filepath.Join(RaftDir(dir), "node.json")); err != nil {
		t.Fatal("node.json 应已落盘")
	}
	// 新目录生成新值
	id3, err := LoadOrCreateRaftID(t.TempDir(), func() uint64 { return 99 })
	if err != nil {
		t.Fatal(err)
	}
	if id3 != 99 {
		t.Fatalf("id3 = %d, want 99", id3)
	}
}
