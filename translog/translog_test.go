package translog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/FalconEngine/falcon/pkg/coding"
)

// 收集回放结果的辅助结构
type replayed struct {
	lsn int64
	op  Op
}

func collect(t *testing.T, dir string, generation uint64) ([]replayed, int64) {
	t.Helper()
	var got []replayed
	next, err := Replay(dir, generation, 0, func(lsn int64, op Op) error {
		got = append(got, replayed{lsn: lsn, op: op})
		return nil
	})
	if err != nil {
		t.Fatalf("Replay 失败: %v", err)
	}
	return got, next
}

// TestWriteReplayLoop 写入→关闭→Replay 回环
func TestWriteReplayLoop(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 1, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	ops := []Op{
		{Type: OpIndex, ID: "a", Doc: json.RawMessage(`{"x":1}`)},
		{Type: OpDelete, ID: "b"},
		{Type: OpIndex, ID: "c", Doc: json.RawMessage(`{"y":[2,3]}`)},
	}
	for i, op := range ops {
		lsn, err := l.Append(op)
		if err != nil {
			t.Fatalf("Append 失败: %v", err)
		}
		if lsn != int64(i) {
			t.Fatalf("LSN 不匹配: got %d want %d", lsn, i)
		}
	}
	if l.NextLSN() != 3 {
		t.Fatalf("NextLSN got %d want 3", l.NextLSN())
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	got, next := collect(t, dir, 1)
	if next != 3 {
		t.Fatalf("Replay nextLSN got %d want 3", next)
	}
	if len(got) != len(ops) {
		t.Fatalf("回放条数 got %d want %d", len(got), len(ops))
	}
	for i, r := range got {
		if r.lsn != int64(i) {
			t.Fatalf("回放 LSN got %d want %d", r.lsn, i)
		}
		if r.op.Type != ops[i].Type || r.op.ID != ops[i].ID {
			t.Fatalf("第 %d 条 Op 不匹配: got %+v want %+v", i, r.op, ops[i])
		}
		if string(r.op.Doc) != string(ops[i].Doc) {
			t.Fatalf("第 %d 条 Doc 不匹配: got %s want %s", i, r.op.Doc, ops[i].Doc)
		}
	}
}

// TestReopenRestoresLSN 关闭后重新 Open（追加模式），LSN 计数应恢复
func TestReopenRestoresLSN(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 7, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := l.Append(Op{Type: OpIndex, ID: fmt.Sprintf("d%d", i)}); err != nil {
			t.Fatalf("Append 失败: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	l2, err := Open(dir, 7, 0)
	if err != nil {
		t.Fatalf("重新 Open 失败: %v", err)
	}
	defer l2.Close()
	if l2.NextLSN() != 5 {
		t.Fatalf("重开后 NextLSN got %d want 5", l2.NextLSN())
	}
	lsn, err := l2.Append(Op{Type: OpDelete, ID: "d0"})
	if err != nil {
		t.Fatalf("重开后 Append 失败: %v", err)
	}
	if lsn != 5 {
		t.Fatalf("重开后 Append LSN got %d want 5", lsn)
	}

	got, next := collect(t, dir, 7)
	if next != 6 || len(got) != 6 {
		t.Fatalf("回放结果不对: next=%d count=%d want 6/6", next, len(got))
	}
	if got[5].op.Type != OpDelete || got[5].op.ID != "d0" {
		t.Fatalf("最后一条 Op 不匹配: %+v", got[5].op)
	}
}

// TestLargeAppendLSN 追加 1 万条后 LSN 正确，且回放条数一致
func TestLargeAppendLSN(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 2, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	const n = 10000
	for i := 0; i < n; i++ {
		lsn, err := l.Append(Op{Type: OpIndex, ID: fmt.Sprintf("doc-%d", i), Doc: json.RawMessage(`{"v":1}`)})
		if err != nil {
			t.Fatalf("Append 第 %d 条失败: %v", i, err)
		}
		if lsn != int64(i) {
			t.Fatalf("第 %d 条 LSN got %d", i, lsn)
		}
	}
	if l.NextLSN() != n {
		t.Fatalf("NextLSN got %d want %d", l.NextLSN(), n)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	count := 0
	var lastLSN int64 = -1
	next, err := Replay(dir, 2, 0, func(lsn int64, op Op) error {
		count++
		lastLSN = lsn
		return nil
	})
	if err != nil {
		t.Fatalf("Replay 失败: %v", err)
	}
	if next != n || count != n || lastLSN != n-1 {
		t.Fatalf("回放统计不对: next=%d count=%d last=%d want %d/%d/%d", next, count, lastLSN, n, n, n-1)
	}
}

// TestReplayToleratesTornTail 模拟尾部半帧损坏：Replay 应容忍并返回已读部分
func TestReplayToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 3, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := l.Append(Op{Type: OpIndex, ID: fmt.Sprintf("doc-%d", i)}); err != nil {
			t.Fatalf("Append 失败: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	// 构造两种尾部损坏：1) 帧头被截断 2) 完整帧头 + payload 只写了一半
	cases := map[string][]byte{
		"partial_header":  make([]byte, 5), // 不足 12 字节帧头
		"partial_payload": nil,             // 运行时构造
	}
	// partial_payload：写一个声明长度为 100 的帧头，payload 只给 3 字节
	hdr := make([]byte, coding.FrameHeaderSize)
	hdr[4] = 100 // little-endian 长度字段低字节
	cases["partial_payload"] = append(hdr, 1, 2, 3)

	for name, garbage := range cases {
		t.Run(name, func(t *testing.T) {
			sub := t.TempDir()
			src := logPath(dir, 3)
			data, err := os.ReadFile(src)
			if err != nil {
				t.Fatalf("读取源日志失败: %v", err)
			}
			dst := logPath(sub, 3)
			if err := os.WriteFile(dst, append(data, garbage...), 0o644); err != nil {
				t.Fatalf("写入损坏日志失败: %v", err)
			}

			count := 0
			next, err := Replay(sub, 3, 0, func(lsn int64, op Op) error {
				count++
				return nil
			})
			if err != nil {
				t.Fatalf("Replay 应容忍尾部损坏, 却报错: %v", err)
			}
			if next != n || count != n {
				t.Fatalf("损坏尾部场景回放不对: next=%d count=%d want %d/%d", next, count, n, n)
			}

			// Open 应将文件截断到有效末尾，之后追加的数据仍可完整回放
			l2, err := Open(sub, 3, 0)
			if err != nil {
				t.Fatalf("损坏日志重新 Open 失败: %v", err)
			}
			if l2.NextLSN() != n {
				t.Fatalf("重开后 NextLSN got %d want %d", l2.NextLSN(), n)
			}
			if _, err := l2.Append(Op{Type: OpDelete, ID: "doc-0"}); err != nil {
				t.Fatalf("截断后 Append 失败: %v", err)
			}
			if err := l2.Close(); err != nil {
				t.Fatalf("Close 失败: %v", err)
			}
			got, next := collect(t, sub, 3)
			if next != n+1 || len(got) != n+1 {
				t.Fatalf("截断后续写回放不对: next=%d count=%d want %d/%d", next, len(got), n+1, n+1)
			}
			if got[n].op.Type != OpDelete {
				t.Fatalf("截断后追加的记录不匹配: %+v", got[n].op)
			}
		})
	}
}

// TestReadFrom 从中间位置读取
func TestReadFrom(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 4, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer l.Close()
	const n = 100
	for i := 0; i < n; i++ {
		if _, err := l.Append(Op{Type: OpIndex, ID: fmt.Sprintf("doc-%d", i)}); err != nil {
			t.Fatalf("Append 失败: %v", err)
		}
	}

	var got []replayed
	if err := l.ReadFrom(42, func(lsn int64, op Op) error {
		got = append(got, replayed{lsn: lsn, op: op})
		return nil
	}); err != nil {
		t.Fatalf("ReadFrom 失败: %v", err)
	}
	if len(got) != n-42 {
		t.Fatalf("ReadFrom 条数 got %d want %d", len(got), n-42)
	}
	for i, r := range got {
		wantLSN := int64(42 + i)
		if r.lsn != wantLSN {
			t.Fatalf("第 %d 条 LSN got %d want %d", i, r.lsn, wantLSN)
		}
		if r.op.ID != fmt.Sprintf("doc-%d", wantLSN) {
			t.Fatalf("第 %d 条 ID got %s", i, r.op.ID)
		}
	}

	// 从 0 读等价于全量
	count := 0
	if err := l.ReadFrom(0, func(lsn int64, op Op) error { count++; return nil }); err != nil {
		t.Fatalf("ReadFrom(0) 失败: %v", err)
	}
	if count != n {
		t.Fatalf("ReadFrom(0) 条数 got %d want %d", count, n)
	}

	// 非法入参应报错
	if err := l.ReadFrom(-1, func(lsn int64, op Op) error { return nil }); err == nil {
		t.Fatal("ReadFrom(-1) 应报错")
	}
}

// TestReplayMissingFile 日志文件不存在时 Replay 返回空结果且不报错
func TestReplayMissingFile(t *testing.T) {
	dir := t.TempDir()
	got, next := collect(t, dir, 99)
	if next != 0 || len(got) != 0 {
		t.Fatalf("缺失文件回放应返回空: next=%d count=%d", next, len(got))
	}
}

// TestFileName 验证日志文件命名约定
func TestFileName(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 123, 0)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if _, err := l.Append(Op{Type: OpIndex, ID: "x"}); err != nil {
		t.Fatalf("Append 失败: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "translog-123.log")); err != nil {
		t.Fatalf("日志文件命名不符约定: %v", err)
	}
}

// TestManifestRoundTrip 代际保留清单写读回环；不存在时返回 (nil, nil)
func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	gens, err := ReadManifest(dir)
	if err != nil || gens != nil {
		t.Fatalf("缺失清单应返回 (nil, nil), got %v, %v", gens, err)
	}
	want := []GenInfo{
		{Gen: 1, BaseLSN: 0, Count: 10},
		{Gen: 2, BaseLSN: 10, Count: 5},
		{Gen: 3, BaseLSN: 15, Count: 0},
	}
	if err := WriteManifest(dir, want); err != nil {
		t.Fatalf("WriteManifest 失败: %v", err)
	}
	got, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest 失败: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("清单条数 got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 条 got %+v want %+v", i, got[i], want[i])
		}
	}
	// 全量重写语义：再写一份更短的清单应整体覆盖
	if err := WriteManifest(dir, want[2:]); err != nil {
		t.Fatalf("重写 WriteManifest 失败: %v", err)
	}
	got, _ = ReadManifest(dir)
	if len(got) != 1 || got[0] != want[2] {
		t.Fatalf("重写后清单不对: %+v", got)
	}
}
