// Package translog 实现每个分片一个的预写日志（WAL），
// 用于崩溃恢复与主备复制的数据源。
//
// 文件格式：<dir>/translog-<generation>.log
// 文件由一串 CRC 帧顺序拼接而成（见 pkg/coding.WriteFrame）：
//
//	[4B CRC32(payload)][8B 长度][payload] * N
//
// 每帧 payload 为 JSON 编码的 Op。
// LSN 为 int64，从 0 开始，每追加一条记录递增 1，与帧顺序一一对应。
package translog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/FalconEngine/falcon/pkg/coding"
	"github.com/FalconEngine/falcon/storage"
)

// OpType 操作类型
type OpType uint8

const (
	OpIndex  OpType = 1 // 写入/更新文档
	OpDelete OpType = 2 // 删除文档
)

// Op 一条 translog 记录
type Op struct {
	Type OpType          `json:"type"`
	ID   string          `json:"id"`
	Doc  json.RawMessage `json:"doc,omitempty"`
}

// Log 绑定一个目录与代际号的预写日志。
// LSN 全局单调递增（跨代际）：本代际第 i 条记录的 LSN = baseLSN + i。
type Log struct {
	mu         sync.Mutex
	path       string
	writer     *storage.FileWriter
	baseLSN    int64 // 本代际第一条记录的 LSN（此前所有代际的记录总数）
	nextLSN    int64 // 下一条可用 LSN
	generation uint64
	closed     bool
}

// logPath 返回该代际日志文件路径
func logPath(dir string, generation uint64) string {
	return filepath.Join(dir, fmt.Sprintf("translog-%d.log", generation))
}

// GenInfo 一个代际在保留清单中的记录。
// 该代际覆盖的 LSN 区间为 [BaseLSN, BaseLSN+Count)；
// 当前活跃代际的 Count 只反映最近一次 Flush 落盘清单时的值（读取时以文件实际内容为准）。
type GenInfo struct {
	Gen     uint64 `json:"gen"`
	BaseLSN int64  `json:"base_lsn"`
	Count   int64  `json:"count"`
}

// manifestPath 代际保留清单路径（Flush 轮替后全量重写）
func manifestPath(dir string) string { return filepath.Join(dir, "translog-gens.json") }

// ReadManifest 读取代际保留清单；文件不存在返回 (nil, nil)（老数据目录无清单，
// 调用方按"只认最新代际"的兼容逻辑处理）
func ReadManifest(dir string) ([]GenInfo, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var gens []GenInfo
	if err := json.Unmarshal(b, &gens); err != nil {
		return nil, fmt.Errorf("translog: 解析代际清单失败: %w", err)
	}
	return gens, nil
}

// WriteManifest 全量重写代际保留清单（先写临时文件再原子 rename，
// 避免崩溃留下半个 JSON 导致 Open 失败）
func WriteManifest(dir string, gens []GenInfo) error {
	b, err := json.Marshal(gens)
	if err != nil {
		return err
	}
	p := manifestPath(dir)
	if err := os.WriteFile(p+".tmp", b, 0o644); err != nil {
		return fmt.Errorf("translog: 写入代际清单失败: %w", err)
	}
	return os.Rename(p+".tmp", p)
}

// Open 创建（或追加打开）该代际日志。baseLSN 为本代际第一条记录的 LSN
// （此前所有代际的记录总数；首个代际传 0），保证 LSN 跨代际单调递增，
// 复制协议（按 LSN 拉取/对齐）依赖此性质。
// 若文件已存在，先扫描恢复 LSN 计数；发现尾部半帧损坏时，
// 将文件截断到最后一条完整记录之后，保证后续追加的数据可被完整回放。
func Open(dir string, generation uint64, baseLSN int64) (*Log, error) {
	path := logPath(dir, generation)

	// 先扫描已有内容，恢复 LSN 计数与有效数据末尾偏移
	validEnd, count, err := scan(path, nil)
	if err != nil {
		return nil, err
	}

	// 存在尾部损坏时截断，避免新数据追加在垃圾字节之后导致不可回放
	if st, err := os.Stat(path); err == nil && st.Size() != validEnd {
		if err := os.Truncate(path, validEnd); err != nil {
			return nil, fmt.Errorf("truncate %s: %w", path, err)
		}
	}

	w, err := storage.OpenFileWriterForAppend(path)
	if err != nil {
		return nil, err
	}
	return &Log{path: path, writer: w, baseLSN: baseLSN, nextLSN: baseLSN + count, generation: generation}, nil
}

// BaseLSN 返回本代际第一条记录的 LSN
func (l *Log) BaseLSN() int64 { return l.baseLSN }

// Append 追加一条记录，返回分配的 LSN
func (l *Log) Append(op Op) (int64, error) {
	payload, err := json.Marshal(op)
	if err != nil {
		return 0, fmt.Errorf("marshal op: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, fmt.Errorf("translog %s already closed", l.path)
	}
	if _, err := coding.WriteFrame(l.writer, payload); err != nil {
		return 0, err
	}
	lsn := l.nextLSN
	l.nextLSN++
	return lsn, nil
}

// Sync 强制刷盘
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return fmt.Errorf("translog %s already closed", l.path)
	}
	return l.writer.Sync()
}

// NextLSN 返回下一条可用 LSN
func (l *Log) NextLSN() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextLSN
}

// Close 关闭日志（内部会 Sync）
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.writer.Close()
}

// scan 顺序扫描日志文件，返回有效数据末尾偏移与下一条可用 LSN。
// fn 非空时对每条完整记录回调；遇到尾部损坏帧（写了一半崩溃）时停止扫描，
// 容忍截断而不报错。文件不存在视为空日志。
func scan(path string, fn func(lsn int64, op Op) error) (validEnd int64, nextLSN int64, err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, 0, nil
	}
	r, err := storage.NewMmapReader(path)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()

	var off, lsn int64
	for off < r.Len() {
		payload, next, err := coding.ReadFrame(r, off)
		if err != nil {
			// 尾部半帧 / CRC 损坏：停止扫描，截断容忍
			break
		}
		var op Op
		if err := json.Unmarshal(payload, &op); err != nil {
			// payload 不是合法 Op JSON，同样视为损坏终止
			break
		}
		if fn != nil {
			if err := fn(lsn, op); err != nil {
				return 0, 0, err
			}
		}
		off = next
		lsn++
	}
	return off, lsn, nil
}

// Replay 顺序回放全部记录，返回下一条可用 LSN。
// baseLSN 语义同 Open：回放回调拿到的 lsn = baseLSN + 序号。
// 遇到尾部损坏帧（写了一半崩溃）时截断容忍，返回已读部分。
func Replay(dir string, generation uint64, baseLSN int64, fn func(lsn int64, op Op) error) (int64, error) {
	_, count, err := scan(logPath(dir, generation), func(lsn int64, op Op) error {
		return fn(baseLSN+lsn, op)
	})
	return baseLSN + count, err
}

// ReadFrom 从指定 LSN 起读取（复制用）。
// 每次调用重新打开文件映射，因此可以读到调用时刻已写入的最新数据；
// 尾部损坏帧同样截断容忍。
func (l *Log) ReadFrom(fromLSN int64, fn func(lsn int64, op Op) error) error {
	if fromLSN < 0 {
		return fmt.Errorf("fromLSN must be >= 0, got %d", fromLSN)
	}
	l.mu.Lock()
	base := l.baseLSN
	l.mu.Unlock()
	_, _, err := scan(l.path, func(lsn int64, op Op) error {
		global := base + lsn
		if global < fromLSN {
			return nil
		}
		return fn(global, op)
	})
	return err
}
