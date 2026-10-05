package node

// 副本恢复（P0-2，peer recovery）：段拷贝 + translog 补差两阶段。
//
// 触发点（三处，见 replication.go）：
//  1. 落后出窗：FetchTranslog 返回 fromLSN < OldestLsn（primary 保留窗口
//     已清除副本需要的历史）；
//  2. 历史分叉：返回空 ops 且 nextLSN < fromLSN（副本超前于 primary）；
//  3. 角色降级：reconcile 发现 primary→replica 切换（含重启后，靠分片目录的
//     primary-role 标记识别）——旧 primary 未复制的写入可丢弃（对齐 ES），
//     强制段拷贝恢复，规避"同 LSN 不同内容"的静默分叉。
//
// 流程：向 primary Prepare（冻结其对端 Flush/轮替）→ 按恢复点逐文件分块
// 重建本地分片目录 → defer Finish 解冻 → 拉取循环从 LSNBase 继续补差量。
//
// 并发控制：每分片 recovering 标记防重入；恢复期间拉取器跳过、
// ApplyOp 报错——恢复中副本对 waitAll 表现为"不存在"（语义同 ES
// initializing 分片，不计入 write_wait_for_active_shards）。
//
// 已知限制：文件分块经 JSON codec 传输（[]byte 自动 base64），
// 大段拷贝慢——后续可换流式 RPC；分块不做 CRC（gRPC 层已有完整性保证）。

import (
	"fmt"

	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/transport"
)

// isRecovering 分片是否正在做段拷贝恢复
func (n *Node) isRecovering(key shardKey) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.recovering[key]
}

// startRecovery 触发一次分片恢复（异步执行，每分片同时只允许一个）
func (n *Node) startRecovery(key shardKey, primary transport.NodeMeta) {
	n.mu.Lock()
	if n.recovering[key] {
		n.mu.Unlock()
		return
	}
	n.recovering[key] = true
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			delete(n.recovering, key)
			n.mu.Unlock()
		}()
		if err := n.recoverShard(key, primary); err != nil {
			mlog.Warn("node %s 分片 %s[%d] 段拷贝恢复失败: %v（由后续触发点重试）",
				n.Meta.Name, key.index, key.shard, err)
		}
	}()
}

// recoverShard 段拷贝恢复本体：Prepare → 重建 → defer Finish → 差量由拉取循环补齐
func (n *Node) recoverShard(key shardKey, primary transport.NodeMeta) error {
	ix, ok := n.Mgr.Get(key.index)
	if !ok {
		return fmt.Errorf("node: 索引 %q 不在本节点", key.index)
	}
	mlog.Info("node %s 分片 %s[%d] 开始段拷贝恢复（源=%s）", n.Meta.Name, key.index, key.shard, primary.Name)
	lsnBase, schemaJSON, files, err := n.Client.PrepareShardRecovery(primary.GRPCAddr, key.index, int32(key.shard))
	if err != nil {
		return fmt.Errorf("node: Prepare 恢复失败: %w", err)
	}
	// 无论重建成败都结束恢复源会话（解冻 primary 的 Flush/轮替）
	defer n.Client.FinishShardRecovery(primary.GRPCAddr, key.index, int32(key.shard))

	rp := &index.RecoveryPoint{LSNBase: lsnBase, SchemaJSON: schemaJSON}
	for _, f := range files {
		rp.Files = append(rp.Files, index.FileInfo{SegDir: f.SegDir, Name: f.Name, Size: f.Size})
	}
	fetch := func(fi index.FileInfo, off, limit int64) ([]byte, error) {
		return n.Client.FetchShardFile(primary.GRPCAddr, key.index, int32(key.shard), fi.SegDir, fi.Name, off, limit)
	}
	if err := ix.RecoverShard(key.shard, rp, fetch); err != nil {
		return err
	}
	mlog.Info("node %s 分片 %s[%d] 段拷贝恢复完成（lsnBase=%d，继续拉取差量）",
		n.Meta.Name, key.index, key.shard, lsnBase)
	return nil
}
