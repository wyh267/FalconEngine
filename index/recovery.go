package index

// 副本恢复（P0-2，peer recovery）引擎侧 API。
//
// primary 侧（恢复源）：PrepareRecovery 冻结 Flush/合并/代际轮替并捕获
// 一致的段文件清单，ReadShardFile 按清单分块供给，FinishRecovery 解冻。
// 恢复窗口内写入不阻塞：Prepare 之后的操作经 translog 差量拉取补齐。
//
// replica 侧（恢复目标）：RecoverShard 备份旧目录后按恢复点逐文件重建，
// 写入 LSN 基线再重开引擎；失败回滚旧目录。
//
// 已知限制（文档化）：恢复窗口内显式 _flush/_merge 报错；恢复期间该分片
// 查询走已有的 partial 降级（node/search.go）。

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FileInfo 恢复拷贝单元：分片目录内一个段目录下的一个文件
type FileInfo struct {
	SegDir string `json:"seg_dir"` // 段目录名（相对分片目录，如 "seg-3"）
	Name   string `json:"name"`
	// Size 为 Prepare 时刻的文件大小：del.bin 等追加写文件按此前缀拷贝，
	// 之后追加的差量由 translog 重放补齐（前缀幂等，多拷无害）
	Size int64 `json:"size"`
}

// RecoveryPoint 恢复源在 Prepare 时刻的一致视图
type RecoveryPoint struct {
	LSNBase    int64      `json:"lsn_base"` // 当前代际基线 LSN：副本重建后从此处继续拉取差量
	SchemaJSON []byte     `json:"schema_json"`
	Files      []FileInfo `json:"files"`
}

// shardFileChunk 恢复文件分块大小（1MB）
const shardFileChunk = 1 << 20

// PrepareRecovery 捕获一致视图并冻结 Flush/代际轮替，直到 FinishRecovery。
// 同一分片同时只允许一个恢复源会话（并发调用报错，调用方稍后重试）。
func (e *Engine) PrepareRecovery() (*RecoveryPoint, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fmt.Errorf("index: 引擎已关闭")
	}
	if e.recovering {
		return nil, fmt.Errorf("index: 分片已有恢复会话进行中")
	}
	schemaJSON, err := json.Marshal(e.schema)
	if err != nil {
		return nil, err
	}
	rp := &RecoveryPoint{LSNBase: e.lsnBase, SchemaJSON: schemaJSON}
	snap := make(map[string]int64)
	for _, seg := range e.segs {
		segDir := filepath.Base(seg.Dir())
		entries, err := os.ReadDir(seg.Dir())
		if err != nil {
			return nil, fmt.Errorf("index: 恢复快照读取段目录失败: %w", err)
		}
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			fi, err := ent.Info()
			if err != nil {
				return nil, err
			}
			rp.Files = append(rp.Files, FileInfo{SegDir: segDir, Name: ent.Name(), Size: fi.Size()})
			snap[segDir+"/"+ent.Name()] = fi.Size()
		}
	}
	e.recovering = true
	e.recoverSnap = snap
	return rp, nil
}

// FinishRecovery 解除恢复源冻结（幂等）
func (e *Engine) FinishRecovery() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recovering = false
	e.recoverSnap = nil
}

// ReadShardFile 按恢复快照读取段文件的一个分块（off 起最多 limit 字节）。
// 只允许读 PrepareRecovery 快照中的文件（白名单即路径校验，防穿越），
// 且按快照大小截断：del.bin 等追加写文件只拷 Prepare 时刻前缀。
func (e *Engine) ReadShardFile(segDir, name string, off, limit int64) ([]byte, error) {
	e.mu.RLock()
	size, ok := e.recoverSnap[segDir+"/"+name]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("index: 文件 %s/%s 不在恢复快照中", segDir, name)
	}
	if off < 0 || off >= size {
		return nil, nil
	}
	if limit <= 0 || limit > shardFileChunk {
		limit = shardFileChunk
	}
	if remain := size - off; remain < limit {
		limit = remain
	}
	// 每次调用独立打开，与段 reader 生命周期解耦（段文件在恢复窗口内不变）
	f, err := os.Open(filepath.Join(e.dir, segDir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, limit)
	n, _ := f.ReadAt(buf, off) // 按快照大小截断后读到末尾属正常（io.EOF）
	return buf[:n], nil
}

// PrepareShardRecovery primary 侧：开始分片恢复源会话（transport 层入口）
func (ix *Index) PrepareShardRecovery(shardID int) (*RecoveryPoint, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return nil, err
	}
	return e.PrepareRecovery()
}

// ReadShardFile primary 侧：读恢复文件分块（transport 层入口）
func (ix *Index) ReadShardFile(shardID int, segDir, name string, off, limit int64) ([]byte, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return nil, err
	}
	return e.ReadShardFile(segDir, name, off, limit)
}

// FinishShardRecovery primary 侧：结束恢复源会话（不持有分片时为空操作）
func (ix *Index) FinishShardRecovery(shardID int) {
	if shardID >= 0 && shardID < len(ix.shards) && ix.shards[shardID] != nil {
		ix.shards[shardID].FinishRecovery()
	}
}

// RecoverShard 副本侧：用恢复点重建本地分片。
// 流程：备份旧目录 → 逐文件拉取重建 → 写 LSN 基线 → 重开引擎；失败回滚旧目录。
// 恢复期间本分片摘牌不可用：查询走 partial 降级，复制 ApplyOp 报错被推送方
// 跳过（语义同 ES initializing 分片，不计入 wait_for_active_shards）。
// fetch 由调用方实现（通常经 transport.FetchShardFile 逐块拉取）。
func (ix *Index) RecoverShard(shardID int, rp *RecoveryPoint, fetch func(fi FileInfo, off, limit int64) ([]byte, error)) error {
	e, err := ix.shard(shardID)
	if err != nil {
		return err
	}
	dir := ix.shardDir(shardID)
	bak := dir + ".recover-bak"
	os.RemoveAll(bak) // 清理上次崩溃可能残留的备份目录

	// 摘牌并关闭旧引擎：摘牌后 ApplyOp/拉取/查询对该分片报错跳过
	ix.shards[shardID] = nil
	rollback := func(cause error) error {
		os.RemoveAll(dir)
		if rbErr := os.Rename(bak, dir); rbErr == nil {
			if old, oErr := open(dir, nil, ix.dateDetection); oErr == nil {
				ix.shards[shardID] = old
			}
		}
		return cause
	}
	if err := e.Close(); err != nil {
		ix.shards[shardID] = e // 关闭失败：放弃恢复，维持原引擎
		return fmt.Errorf("index: 恢复前关闭分片失败: %w", err)
	}
	if err := os.Rename(dir, bak); err != nil {
		ix.shards[shardID] = e
		return fmt.Errorf("index: 备份旧分片目录失败: %w", err)
	}

	if err := rebuildShardDir(dir, rp, fetch); err != nil {
		return rollback(err)
	}
	ne, err := open(dir, nil, ix.dateDetection)
	if err != nil {
		return rollback(fmt.Errorf("index: 重开恢复分片失败: %w", err))
	}
	ix.shards[shardID] = ne
	os.RemoveAll(bak)
	return nil
}

// rebuildShardDir 按恢复点内容重建分片目录（schema + LSN 基线 + 全部段文件）
func rebuildShardDir(dir string, rp *RecoveryPoint, fetch func(fi FileInfo, off, limit int64) ([]byte, error)) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "schema.json"), rp.SchemaJSON, 0o644); err != nil {
		return err
	}
	// LSN 基线：重建后从 rp.LSNBase 起继续向 primary 拉取差量
	if err := os.WriteFile(lsnBasePath(dir), []byte(strconv.FormatInt(rp.LSNBase, 10)), 0o644); err != nil {
		return err
	}
	for _, fi := range rp.Files {
		// 路径校验：恢复点来自远端，防止目录穿越
		if fi.Name != filepath.Base(fi.Name) || fi.SegDir != filepath.Base(fi.SegDir) ||
			fi.Name == ".." || fi.SegDir == ".." || strings.Contains(fi.SegDir, "\\") {
			return fmt.Errorf("index: 非法恢复文件路径 %q/%q", fi.SegDir, fi.Name)
		}
		segDir := filepath.Join(dir, fi.SegDir)
		if err := os.MkdirAll(segDir, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(segDir, fi.Name))
		if err != nil {
			return err
		}
		for off := int64(0); off < fi.Size; {
			chunk, err := fetch(fi, off, shardFileChunk)
			if err != nil {
				f.Close()
				return fmt.Errorf("index: 拉取恢复文件 %s/%s 失败: %w", fi.SegDir, fi.Name, err)
			}
			if len(chunk) == 0 {
				break
			}
			if _, err := f.Write(chunk); err != nil {
				f.Close()
				return err
			}
			off += int64(len(chunk))
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// SetRetentionLSN 设置分片 translog 保留地板（primary 按副本 ack 推进，见 node 包）
func (ix *Index) SetRetentionLSN(shardID int, min int64) {
	if shardID >= 0 && shardID < len(ix.shards) && ix.shards[shardID] != nil {
		ix.shards[shardID].SetRetentionLSN(min)
	}
}

// ShardOldestLSN 返回分片保留窗口内最老可用 LSN
func (ix *Index) ShardOldestLSN(shardID int) (int64, error) {
	e, err := ix.shard(shardID)
	if err != nil {
		return -1, err
	}
	return e.OldestLSN(), nil
}

// primary-role 标记文件：reconcile 发现本分片是 primary 时落盘，
// 副本重建（RecoverShard）/分片摘除（DropShard）时随目录消失。
// 用于进程重启后仍能识别"primary→replica"角色降级（内存态无法跨进程），
// 触发强制段拷贝恢复，规避故障转移后"同 LSN 不同内容"的静默分叉
// （旧 primary 降级时未复制的写入可丢弃，对齐 ES 语义）。
func primaryRolePath(dir string) string { return filepath.Join(dir, "primary-role") }

// MarkPrimaryRole 持久化 primary 角色标记（幂等，尽力而为：
// 写失败仅丢失跨重启的降级识别，不阻断主流程）
func (e *Engine) MarkPrimaryRole() {
	if _, err := os.Stat(primaryRolePath(e.dir)); err == nil {
		return
	}
	os.WriteFile(primaryRolePath(e.dir), []byte("1"), 0o644)
}

// WasPrimaryRole 本分片是否以 primary 角色运行过（标记文件存在）
func (e *Engine) WasPrimaryRole() bool {
	_, err := os.Stat(primaryRolePath(e.dir))
	return err == nil
}

// MarkShardPrimaryRole 落地分片 primary 角色标记（不持有分片时为空操作）
func (ix *Index) MarkShardPrimaryRole(shardID int) {
	if shardID >= 0 && shardID < len(ix.shards) && ix.shards[shardID] != nil {
		ix.shards[shardID].MarkPrimaryRole()
	}
}

// ShardWasPrimaryRole 分片是否以 primary 角色运行过
func (ix *Index) ShardWasPrimaryRole(shardID int) bool {
	return shardID >= 0 && shardID < len(ix.shards) &&
		ix.shards[shardID] != nil && ix.shards[shardID].WasPrimaryRole()
}
