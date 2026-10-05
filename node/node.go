// Package node 节点装配层：把 index.Manager、transport、gRPC 与 cluster raft
// 粘合成一个可运行的 falcon 节点。
//
// 集群组建规则：
//   - master 且无 seeds：自举为单 master 集群（单机模式，行为与阶段4一致）
//   - master 且有 seeds：空成员启动 raft，向 seed 发 JoinNode，由 leader 经
//     ConfChange 加入 raft group
//   - data-only：不进 raft group，向 seed 的 leader 发 JoinNode 注册进元数据，
//     随后周期轮询集群状态，按路由表创建本地分片
//
// 分片落地：master 节点经 raft apply 回调、data-only 节点经轮询，
// 发现自己持有的分片就调用 Manager.EnsureShard 创建。
// 数据面（5b）见 replication.go（复制）/ search.go（跨节点查询）/
// failover.go（心跳、故障检测与转移）。
package node

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/FalconEngine/falcon/cluster"
	"github.com/FalconEngine/falcon/config"
	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/translog"
	"github.com/FalconEngine/falcon/transport"
)

// Node 一个 falcon 节点
type Node struct {
	Meta   transport.NodeMeta
	Mgr    *index.Manager
	Client *transport.Client

	// 时间参数（测试可调小；需配合 raft 选举间隔，见 cluster.raftConfig）
	HeartbeatInterval time.Duration // 心跳/状态上报间隔，默认 1s
	DeadTimeout       time.Duration // 连续未心跳判下线时长，默认 6s
	PullInterval      time.Duration // 副本拉取间隔，默认 1s

	mu       sync.RWMutex
	raft     *cluster.RaftNode     // master 节点才有
	sm       *cluster.StateMachine // 集群元数据视图（data-only 节点为轮询快照，整体替换）
	seeds    []string
	dataDir  string               // 数据目录（raft 元数据在 <dataDir>/.falcon/raft/）
	lastSeen map[uint64]time.Time // leader 本地：各节点最近一次上报时间

	pullers map[shardKey]chan struct{} // 副本拉取器：key -> 停止信号
	pushQ   map[shardKey]chan replTask // primary 推送队列：key -> 顺序队列

	// 副本恢复状态（P0-2）：
	// replAcks 为 primary 侧各副本已确认到的 LSN（retention lease 最小形态，
	// 用于推进 translog 保留窗口，见 replication.go updateRetentionLSN）；
	// recovering 为正在段拷贝恢复的分片标记（见 recovery.go）
	replAcks   map[shardKey]map[uint64]int64
	recovering map[shardKey]bool

	// mappingBcast 记录各索引最近一次广播 mapping 时的本地字段并集数量，
	// 抑制动态推断新字段后的重复广播（见 mapping.go maybeBroadcastMapping）
	mappingBcast map[string]int

	stopc chan struct{}
}

// New 创建节点（gRPC 服务由外部以本节点为 Handler 创建）
func New(cfg *config.Config, mgr *index.Manager, client *transport.Client) *Node {
	n := &Node{
		Mgr:     mgr,
		Client:  client,
		sm:      cluster.NewStateMachine(),
		seeds:   cfg.Cluster.Seeds,
		dataDir: cfg.Data.Path,
		stopc:   make(chan struct{}),

		HeartbeatInterval: time.Second,
		DeadTimeout:       6 * time.Second,
		PullInterval:      time.Second,

		lastSeen:   map[uint64]time.Time{},
		pullers:    map[shardKey]chan struct{}{},
		pushQ:      map[shardKey]chan replTask{},
		replAcks:   map[shardKey]map[uint64]int64{},
		recovering: map[shardKey]bool{},

		mappingBcast: map[string]int{},
	}
	grpcAddr := fmt.Sprintf("%s:%d", advertiseHost(), cfg.GRPC.Port)
	httpAddr := fmt.Sprintf("%s:%d", advertiseHost(), cfg.HTTP.Port)
	n.Meta = transport.NodeMeta{
		ID:       NodeID(grpcAddr),
		Name:     cfg.Node.Name,
		GRPCAddr: grpcAddr,
		HTTPAddr: httpAddr,
		Master:   cfg.Node.Master,
		Data:     cfg.Node.Data,
	}
	// RaftID 持久化复用（P0-1）：语义为"每数据目录唯一"。
	// master 节点先读 <data>/.falcon/raft/node.json，有则复用，
	// 无则生成后先落盘再启动 raft（重启后 raft 从磁盘恢复，成员身份不变）。
	// data-only 节点不进 raft group，保持每次启动唯一即可。
	if cfg.Node.Master {
		raftID, err := cluster.LoadOrCreateRaftID(cfg.Data.Path, func() uint64 {
			return NodeID(fmt.Sprintf("%s#%d", grpcAddr, time.Now().UnixNano()))
		})
		if err != nil {
			mlog.Warn("node %s 持久化 RaftID 失败，退化为每次启动唯一: %v", cfg.Node.Name, err)
			n.Meta.RaftID = NodeID(fmt.Sprintf("%s#%d", grpcAddr, time.Now().UnixNano()))
		} else {
			n.Meta.RaftID = raftID
		}
	} else {
		n.Meta.RaftID = NodeID(fmt.Sprintf("%s#%d", grpcAddr, time.Now().UnixNano()))
	}
	return n
}

// state 取当前集群元数据视图（data-only 节点轮询时整体替换，需加锁）
func (n *Node) state() *cluster.StateMachine {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.sm
}

// advertiseHost 本节点对外地址（单机部署用回环地址）
func advertiseHost() string { return "127.0.0.1" }

// NodeID 由地址派生稳定节点 ID
func NodeID(grpcAddr string) uint64 {
	id := murmur3.Sum64([]byte(grpcAddr))
	if id == 0 {
		id = 1
	}
	return id
}

// Start 按角色启动集群控制面与数据面循环
func (n *Node) Start() error {
	raftDir := cluster.RaftDir(n.dataDir)
	switch {
	case n.Meta.Master && len(n.seeds) == 0:
		// 单机模式：自举单 master 集群（带持久化；已有状态时从磁盘恢复）
		rn, err := cluster.BootstrapMasterPersistent(n.Meta, n.sm, raftDir, n.sendRaft, n.onApply)
		if err != nil {
			return err
		}
		n.raft = rn
		rn.SetOnSnapshot(n.onRaftSnapshot)
		mlog.Info("node %s bootstrap 单 master 集群 (id=%d, 从磁盘恢复=%v)", n.Meta.Name, n.Meta.ID, rn.RestoredFromStore())
		// 兜底：CSM 恢复后本循环天然空转；raft 状态缺失（如目录被清）时
		// 把本地已有索引重新注册进 CSM（索引数据本身在磁盘上完好）
		go n.reRegisterLocalIndices()
	case n.Meta.Master:
		// master 且有 seeds：空成员启动（带持久化）
		rn, err := cluster.JoinRaftPersistent(n.Meta, n.sm, raftDir, n.sendRaft, n.onApply)
		if err != nil {
			return err
		}
		n.raft = rn
		rn.SetOnSnapshot(n.onRaftSnapshot)
		if !rn.RestoredFromStore() {
			// 无本地状态：向 seed 发 JoinNode，由 leader 经 ConfChange 加入
			if err := n.joinViaSeeds(); err != nil {
				return err
			}
		} else {
			// 从磁盘恢复：仍是集群成员，地址表在恢复的 CSM 里，跳过重新加入
			mlog.Info("node %s 从磁盘恢复 raft 状态，跳过 joinViaSeeds (id=%d)", n.Meta.Name, n.Meta.ID)
		}
	default:
		// data-only：向 master leader 注册
		if err := n.joinViaSeeds(); err != nil {
			return err
		}
		go n.pollLoop()
	}
	// 数据面循环：心跳上报（所有 data 节点）、角色对账（副本拉取启停）、
	// 故障检测与转移（仅 master leader 实际动作）
	if n.Meta.Data {
		go n.reportLoop()
		go n.reconcileLoop()
	}
	if n.Meta.Master {
		go n.failoverLoop()
	}
	return nil
}

// reRegisterLocalIndices 单机重启后把本地已有索引重新注册进 CSM（兜底路径：
// raft 元数据已持久化时 CSM 恢复自带索引表，本循环天然空转；
// 仅在 raft 状态缺失（如元数据目录被清）而索引数据完好时真正生效）
func (n *Node) reRegisterLocalIndices() {
	if !n.raft.WaitLeader(30 * time.Second) {
		return
	}
	// 等自身元信息应用
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := n.state().Node(n.Meta.ID); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, name := range n.Mgr.Names() {
		ix, ok := n.Mgr.Get(name)
		if !ok {
			continue
		}
		if _, ok := n.state().Index(name); ok {
			continue
		}
		var mapping json.RawMessage
		if sch := ix.Schema(); sch != nil {
			mapping, _ = json.Marshal(sch)
		}
		meta := cluster.IndexMeta{
			Name:        name,
			NumShards:   ix.NumShards(),
			NumReplicas: ix.NumReplicas(),
			Mapping:     mapping,
		}
		// 单机：全部分片 primary 都在本节点
		routes := make([]cluster.ShardRoute, meta.NumShards)
		for i := range routes {
			routes[i] = cluster.ShardRoute{Primary: n.Meta.ID}
		}
		if err := n.raft.Propose(cluster.Command{Type: cluster.CmdAddIndex, Index: &meta, Routes: routes}); err != nil {
			mlog.Warn("重注册索引 %s 失败: %v", name, err)
		}
	}
}

// joinViaSeeds 向种子节点逐个尝试 JoinNode，直到成功或 30s 超时
// （seed 可能尚未完成选主，需重试）。
// 成功后把 leader 元信息注册进本地视图：新节点的 raft 回包需要 leader 地址，
// 而地址表要等日志应用才知道——不经此预注册会陷入"提交等回包、回包等提交"的死锁。
func (n *Node) joinViaSeeds() error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, seed := range n.seeds {
			leader, err := n.Client.JoinNode(seed, n.Meta)
			if err != nil {
				lastErr = err
				mlog.Warn("join via %s 失败: %v", seed, err)
				continue
			}
			if leader.ID != 0 && leader.ID != n.Meta.ID {
				n.state().Apply(cluster.Command{Type: cluster.CmdAddNode, Node: &leader})
			}
			mlog.Info("node %s 经 %s 加入集群 (leader=%s)", n.Meta.Name, seed, leader.Name)
			return nil
		}
		select {
		case <-n.stopc:
			return fmt.Errorf("node: 已停止")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("node: 全部 seeds 加入失败: %w", lastErr)
}

// sendRaft 经 gRPC 把 raft 消息发给目标 raft 成员。
// raft 成员 ID ≠ 节点身份（重启即新成员），需经节点表的 RaftID 映射解析地址；
// 解析不到（旧成员已重启换 ID）时报错，raft 重发机制会容忍。
func (n *Node) sendRaft(raftID uint64, data []byte) error {
	sm := n.state()
	for _, meta := range sm.DataNodes() {
		if cluster.RaftMemberID(meta) == raftID {
			return n.Client.SendRaftMessage(meta.GRPCAddr, data)
		}
	}
	// master-only 节点也在节点表中
	raw, err := sm.Snapshot()
	if err != nil {
		return err
	}
	var snap struct {
		Nodes map[uint64]transport.NodeMeta `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		return err
	}
	for _, meta := range snap.Nodes {
		if cluster.RaftMemberID(meta) == raftID {
			return n.Client.SendRaftMessage(meta.GRPCAddr, data)
		}
	}
	return fmt.Errorf("node: 未知 raft 对端 %d", raftID)
}

// onApply raft 已提交命令回调：落地本节点持有的分片并对账角色
func (n *Node) onApply(cmd cluster.Command) {
	switch cmd.Type {
	case cluster.CmdAddNode:
		// 以加入时间作为 lastSeen 兜底：节点可能在第一次心跳前就被杀，
		// 仅凭上报记录会漏判下线
		if cmd.Node != nil {
			n.mu.Lock()
			if _, ok := n.lastSeen[cmd.Node.ID]; !ok {
				n.lastSeen[cmd.Node.ID] = time.Now()
			}
			n.mu.Unlock()
		}
	case cluster.CmdAddIndex:
		if cmd.Index != nil {
			n.ensureLocalShards(*cmd.Index, cmd.Routes)
		}
	case cluster.CmdUpdateRoute:
		// 故障转移/副本补建：本节点可能新持有了该分片（需先建分片再对账角色）
		if meta, ok := n.state().Index(cmd.IndexName); ok {
			if routes, ok := n.state().Route(cmd.IndexName); ok {
				n.ensureLocalShards(meta, routes)
			}
		}
		n.reconcile()
	case cluster.CmdDeleteIndex:
		// 删索引：清理本节点持有的该索引全部数据
		n.deleteLocalIndex(cmd.IndexName)
	case cluster.CmdRemoveNode:
		// 陈旧节点摘除：路由表已重分配，本节点可能被补建了分片；
		// 若本节点正是被摘除者（磁盘清空后重加入，历史日志重放期间曾按
		// 旧路由建过分片），不再路由到本节点的分片一并清除
		n.ensureAllLocalShards()
		n.pruneUnroutedShards()
	case cluster.CmdUpdateMapping:
		// mapping 变更（动态推断广播/显式 PUT _mapping）：合并进本节点全部本地分片
		n.applyLocalMapping(cmd.IndexName, cmd.Mapping)
	}
}

// pruneUnroutedShards 删除"本地持有但路由表已不分配给本节点"的分片。
// 仅在 CmdRemoveNode apply 后调用：存活节点的分片分配在 remove_node 中
// 只增不减，不受影响；被摘除节点（磁盘清空重加入）的残留分片必须清除，
// 否则本地优先的查询会读到空分片。
func (n *Node) pruneUnroutedShards() {
	if !n.Meta.Data {
		return
	}
	for _, name := range n.Mgr.Names() {
		ix, ok := n.Mgr.Get(name)
		if !ok {
			continue
		}
		routes, ok := n.state().Route(name)
		if !ok {
			continue // 索引已删由 deleteLocalIndex 路径处理
		}
		for _, shardID := range ix.LocalShards() {
			if shardID >= len(routes) {
				continue
			}
			r := routes[shardID]
			if r.Primary == n.Meta.ID || containsNode(r.Replicas, n.Meta.ID) {
				continue
			}
			// 先停拉取器/清推送队列，避免引擎关闭后后台 goroutine 访问
			key := shardKey{index: name, shard: shardID}
			n.stopPuller(key)
			n.mu.Lock()
			if ch, ok := n.pushQ[key]; ok {
				delete(n.pushQ, key)
				close(ch)
			}
			n.mu.Unlock()
			if err := ix.DropShard(shardID); err != nil {
				mlog.Error("node %s 摘除未路由分片 %s[%d] 失败: %v", n.Meta.Name, name, shardID, err)
				continue
			}
			mlog.Info("node %s 摘除未路由分片 %s[%d]", n.Meta.Name, name, shardID)
		}
	}
}

// onRaftSnapshot raft 应用了 leader 快照（lagging follower 追赶快照）后的回调：
// 快照可能跨过若干建索引/路由变更命令，遍历恢复的 CSM 幂等补齐本地分片
func (n *Node) onRaftSnapshot() {
	n.ensureAllLocalShards()
}

// ensureAllLocalShards 遍历 CSM 全部索引，落地本节点持有的分片（幂等）并对账角色
func (n *Node) ensureAllLocalShards() {
	sm := n.state()
	raw, err := sm.Snapshot()
	if err != nil {
		return
	}
	var snap struct {
		Indices map[string]cluster.IndexMeta    `json:"indices"`
		Routes  map[string][]cluster.ShardRoute `json:"routes"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		return
	}
	for name, meta := range snap.Indices {
		n.ensureLocalShards(meta, snap.Routes[name])
	}
}

// deleteLocalIndex 删除本节点持有的索引：停该索引全部副本拉取器、
// 清推送队列对应 entry（队列 goroutine 随 channel 关闭退出）、
// 关闭引擎并移除数据目录。raft apply 回调与 data-only 轮询对账共用；
// 索引不在本地时为空操作。
func (n *Node) deleteLocalIndex(name string) {
	var pullKeys []shardKey
	n.mu.RLock()
	for key := range n.pullers {
		if key.index == name {
			pullKeys = append(pullKeys, key)
		}
	}
	n.mu.RUnlock()
	for _, key := range pullKeys {
		n.stopPuller(key)
	}
	n.mu.Lock()
	for key, ch := range n.pushQ {
		if key.index == name {
			delete(n.pushQ, key)
			close(ch)
		}
	}
	// 副本 ack 记账一并清除（恢复中的分片由恢复 goroutine 自行清标记）
	for key := range n.replAcks {
		if key.index == name {
			delete(n.replAcks, key)
		}
	}
	n.mu.Unlock()
	if _, err := n.Mgr.Delete(name); err != nil {
		mlog.Error("node %s 删除本地索引 %s 失败: %v", n.Meta.Name, name, err)
		return
	}
	mlog.Info("node %s 已删除本地索引 %s", n.Meta.Name, name)
}

// ensureLocalShards 对路由表中本节点持有的分片创建本地分片
func (n *Node) ensureLocalShards(meta cluster.IndexMeta, routes []cluster.ShardRoute) {
	if !n.Meta.Data {
		return
	}
	var sch *schema.Schema
	if len(meta.Mapping) > 0 {
		var s schema.Schema
		if err := json.Unmarshal(meta.Mapping, &s); err == nil {
			sch = &s
		}
	}
	for shardID, r := range routes {
		hold := r.Primary == n.Meta.ID || containsNode(r.Replicas, n.Meta.ID)
		if !hold {
			continue
		}
		err := n.Mgr.EnsureShard(meta.Name, index.IndexSettings{
			NumShards:     meta.NumShards,
			NumReplicas:   meta.NumReplicas,
			DateDetection: meta.DateDetection,
		}, sch, shardID)
		if err != nil {
			mlog.Error("创建本地分片失败 %s[%d]: %v", meta.Name, shardID, err)
			continue
		}
		mlog.Info("node %s 落地分片 %s[%d] (primary=%v)", n.Meta.Name, meta.Name, shardID, r.Primary == n.Meta.ID)
	}
	// 新分片可能是副本角色，启动拉取
	n.reconcile()
}

// pollLoop data-only 节点周期拉取集群状态并落地分片（2s 间隔）
func (n *Node) pollLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.syncClusterState()
		case <-n.stopc:
			return
		}
	}
}

// syncClusterState 从 seeds 拉取最新集群状态（覆盖式更新本地视图）
func (n *Node) syncClusterState() {
	for _, seed := range n.seeds {
		data, err := n.Client.ClusterState(seed)
		if err != nil {
			continue
		}
		var snap struct {
			Nodes      map[uint64]cluster.NodeMeta                       `json:"nodes"`
			Indices    map[string]cluster.IndexMeta                      `json:"indices"`
			Routes     map[string][]cluster.ShardRoute                   `json:"routes"`
			Dead       map[uint64]bool                                   `json:"dead"`
			ShardStats map[string]map[int]map[uint64]cluster.ShardStatus `json:"shard_stats"`
		}
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}
		fresh := cluster.NewStateMachine()
		for _, nd := range snap.Nodes {
			nd := nd
			fresh.Apply(cluster.Command{Type: cluster.CmdAddNode, Node: &nd})
		}
		for name, meta := range snap.Indices {
			meta := meta
			fresh.Apply(cluster.Command{Type: cluster.CmdAddIndex, Index: &meta, Routes: snap.Routes[name]})
		}
		for id, dead := range snap.Dead {
			if dead {
				fresh.Apply(cluster.Command{Type: cluster.CmdNodeDead, NodeID: id})
			}
		}
		for _, byShard := range snap.ShardStats {
			for _, byNode := range byShard {
				for nid, st := range byNode {
					fresh.Apply(cluster.Command{Type: cluster.CmdShardStatus, NodeID: nid, Shards: []cluster.ShardStatus{st}})
				}
			}
		}
		n.mu.Lock()
		n.sm = fresh
		n.mu.Unlock()
		for name, meta := range snap.Indices {
			n.ensureLocalShards(meta, snap.Routes[name])
			// mapping 对账：CSM 为权威，本地分片缺失的字段合并进去（幂等）
			n.applyLocalMapping(name, meta.Mapping)
		}
		// 对账删除：本地有而集群快照中已不存在的索引（删索引命令的轮询侧落地）
		for _, name := range n.Mgr.Names() {
			if _, ok := snap.Indices[name]; !ok {
				n.deleteLocalIndex(name)
			}
		}
		n.reconcile()
		return
	}
}

// reconcileLoop 周期对账本地分片角色（启停副本拉取器）
func (n *Node) reconcileLoop() {
	ticker := time.NewTicker(2 * n.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.reconcile()
		case <-n.stopc:
			return
		}
	}
}

// CreateIndex 集群感知建索引：由 leader 分配路由并 propose，
// apply/轮询后各节点落地本地分片；等待本节点元数据与分片就绪后返回。
// （单机模式同样走 raft——单节点 group 的 propose/apply 本地完成，
// 行为与阶段4直建一致。）
func (n *Node) CreateIndex(name string, settings index.IndexSettings, sch *schema.Schema) error {
	rn := n.RaftNode()
	if rn == nil {
		return fmt.Errorf("node: data-only 节点不能建索引，请提交到 master 节点")
	}
	// 节点刚启动时选主可能尚未完成，先等待
	if !rn.IsLeader() {
		rn.WaitLeader(10 * time.Second)
	}
	if !rn.IsLeader() {
		return fmt.Errorf("node: 本节点不是 leader，建索引请提交到 leader（当前 leader=%d）", rn.LeaderID())
	}
	// 等自身元信息经 raft 复制应用（自举节点当选后异步提案自身，见 BootstrapMaster）
	{
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := n.state().Node(n.Meta.ID); ok {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if settings.NumShards <= 0 {
		settings.NumShards = 1
	}
	var mapping json.RawMessage
	if sch != nil {
		mapping, _ = json.Marshal(sch)
	}
	meta := cluster.IndexMeta{
		Name:          name,
		NumShards:     settings.NumShards,
		NumReplicas:   settings.NumReplicas,
		Mapping:       mapping,
		WaitAll:       settings.WaitAll == "all",
		DateDetection: settings.DateDetection,
	}
	routes, err := cluster.AllocateRoutes(n.state(), meta.NumShards, meta.NumReplicas)
	if err != nil {
		return err
	}
	if err := rn.Propose(cluster.Command{Type: cluster.CmdAddIndex, Index: &meta, Routes: routes}); err != nil {
		return err
	}

	// 等待 apply 落地：元数据可见 + （data 节点）本地分片创建完成
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := n.state().Index(name); ok {
			if !n.Meta.Data {
				return nil
			}
			if _, ok := n.Mgr.Get(name); ok {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node: 建索引 %q 等待落地超时", name)
}

// DeleteIndex 集群感知删索引：leader 提案 CmdDeleteIndex，
// apply/轮询对账后各节点清理本地分片与数据目录；
// 等待元数据清除与本节点数据删除完成后返回。
// 索引不存在返回 found=false（供 REST 层映射 404）。
func (n *Node) DeleteIndex(name string) (bool, error) {
	rn := n.RaftNode()
	if rn == nil {
		return false, fmt.Errorf("node: data-only 节点不能删索引，请提交到 master 节点")
	}
	// 与 CreateIndex 同理：节点刚启动时选主可能尚未完成，先等待
	if !rn.IsLeader() {
		rn.WaitLeader(10 * time.Second)
	}
	if !rn.IsLeader() {
		return false, fmt.Errorf("node: 本节点不是 leader，删索引请提交到 leader（当前 leader=%d）", rn.LeaderID())
	}
	if _, ok := n.state().Index(name); !ok {
		return false, nil
	}
	if err := rn.Propose(cluster.Command{Type: cluster.CmdDeleteIndex, IndexName: name}); err != nil {
		return false, err
	}

	// 等待 apply 落地：元数据清除 + （data 节点）本地索引删除完成
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := n.state().Index(name); ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if !n.Meta.Data {
			return true, nil
		}
		if _, ok := n.Mgr.Get(name); !ok {
			return true, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false, fmt.Errorf("node: 删索引 %q 等待落地超时", name)
}

// ---------- transport.Handler 实现 ----------

// Ping 探活/握手
func (n *Node) Ping(from transport.NodeMeta) (transport.NodeMeta, error) {
	return n.Meta, nil
}

// ShardSearch 本地分片查询
func (n *Node) ShardSearch(indexName string, shard int32, dsl []byte, partial bool) ([]byte, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return nil, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	res, err := ix.SearchShard(int(shard), dsl, partial)
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// ShardDoc 本地取文档
func (n *Node) ShardDoc(indexName, id string) ([]byte, bool, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return nil, false, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	raw, found, err := ix.Get(id)
	return raw, found, err
}

// ApplyOp primary 推送来的复制操作：按 LSN 对齐应用到本地副本分片
func (n *Node) ApplyOp(indexName string, shard int32, lsn int64, opData []byte) (int64, error) {
	if n.isRecovering(shardKey{indexName, int(shard)}) {
		// 恢复中副本对 waitAll 表现为"不存在"（语义同 ES initializing 分片）
		return -1, fmt.Errorf("node: 分片 %s[%d] 恢复中，暂不可用", indexName, shard)
	}
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return -1, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	var op translog.Op
	if err := json.Unmarshal(opData, &op); err != nil {
		return -1, fmt.Errorf("node: ApplyOp 解码失败: %w", err)
	}
	return ix.ApplyOp(int(shard), lsn, op)
}

// ForwardWrite 协调节点转发来的写请求：本节点应为该分片 primary
func (n *Node) ForwardWrite(indexName string, shard int32, opData []byte, waitAll bool) (int64, error) {
	// 角色校验：若路由表已变更（故障转移后），拒绝并让协调方重取路由
	route, err := n.routeOf(indexName, int(shard))
	if err != nil {
		return -1, err
	}
	if route.Primary != n.Meta.ID {
		return -1, fmt.Errorf("node: 本节点不再是 %s[%d] 的 primary", indexName, shard)
	}
	meta, _ := n.state().Index(indexName)
	var op translog.Op
	if err := json.Unmarshal(opData, &op); err != nil {
		return -1, fmt.Errorf("node: ForwardWrite 解码失败 (raw=%s): %w", string(opData), err)
	}
	return n.primaryWrite(indexName, meta, int(shard), op, opData)
}

// FetchTranslog 副本按 LSN 拉取本地 primary 分片的 translog。
// 拉取位点（fromLSN-1）即副本真实 ack，刷新保留窗口记账（retention lease）。
func (n *Node) FetchTranslog(indexName string, shard int32, fromLSN int64, limit int32, nodeID uint64) ([][]byte, int64, int64, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return nil, 0, 0, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	if nodeID != 0 {
		n.recordReplAck(shardKey{indexName, int(shard)}, nodeID, fromLSN-1, true)
	}
	ops, next, err := ix.ReadShardTranslog(int(shard), fromLSN, int(limit))
	if err != nil {
		return nil, 0, 0, err
	}
	oldest, err := ix.ShardOldestLSN(int(shard))
	if err != nil {
		return nil, 0, 0, err
	}
	out := make([][]byte, 0, len(ops))
	for _, op := range ops {
		b, err := json.Marshal(op)
		if err != nil {
			return nil, 0, 0, err
		}
		out = append(out, b)
	}
	return out, next, oldest, nil
}

// PrepareShardRecovery 副本恢复源：冻结本分片 Flush/轮替并返回一致视图
func (n *Node) PrepareShardRecovery(indexName string, shard int32) (int64, []byte, []transport.RecoveryFileInfo, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return 0, nil, nil, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	rp, err := ix.PrepareShardRecovery(int(shard))
	if err != nil {
		return 0, nil, nil, err
	}
	files := make([]transport.RecoveryFileInfo, 0, len(rp.Files))
	for _, f := range rp.Files {
		files = append(files, transport.RecoveryFileInfo{SegDir: f.SegDir, Name: f.Name, Size: f.Size})
	}
	return rp.LSNBase, rp.SchemaJSON, files, nil
}

// FetchShardFile 按恢复点分块读取本地段文件
func (n *Node) FetchShardFile(indexName string, shard int32, segDir, name string, off, limit int64) ([]byte, error) {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return nil, fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	return ix.ReadShardFile(int(shard), segDir, name, off, limit)
}

// FinishShardRecovery 结束本分片恢复源会话（解冻 Flush/轮替）
func (n *Node) FinishShardRecovery(indexName string, shard int32) error {
	ix, ok := n.Mgr.Get(indexName)
	if !ok {
		return fmt.Errorf("node: 索引 %q 不存在", indexName)
	}
	ix.FinishShardRecovery(int(shard))
	return nil
}

// RaftMessage 投递 raft 消息
func (n *Node) RaftMessage(data []byte) error {
	rn := n.RaftNode()
	if rn == nil {
		return fmt.Errorf("node: 本节点不在 raft group 中")
	}
	return rn.Step(data)
}

// JoinNode 处理节点加入：master 经 ConfChange 加入 raft group，data-only 只入元数据。
// 返回本节点（leader）元信息，供加入方预注册地址表。
//
// 陈旧成员清理（remove_node）：master 节点加入时若节点表已存在同 NodeID
// （同 grpc 地址）但不同 RaftID 的记录，说明旧成员磁盘已清（RaftID 每数据目录
// 唯一）——先提案 CmdRemoveNode 彻底摘除旧记录（节点表/路由表/分片状态），
// 再以一条 joint 成员变更原子移除旧 raft 成员并加入新成员。
// 仅清理非 leader 的陈旧成员：旧记录指向 leader 自身属运维误操作，直接报错。
func (n *Node) JoinNode(meta transport.NodeMeta) (transport.NodeMeta, error) {
	rn := n.RaftNode()
	if rn == nil {
		return transport.NodeMeta{}, fmt.Errorf("node: 本节点不是 master，无法处理加入请求")
	}
	var err error
	if meta.Master {
		if existing, ok := n.state().Node(meta.ID); ok && existing.Master &&
			cluster.RaftMemberID(existing) != cluster.RaftMemberID(meta) {
			oldRaftID := cluster.RaftMemberID(existing)
			if oldRaftID == cluster.RaftMemberID(n.Meta) {
				return transport.NodeMeta{}, fmt.Errorf(
					"node: 加入节点 %s 与 leader 自身地址冲突（同 NodeID 不同 RaftID），请检查配置（疑似两个数据目录共用同一地址）", meta.GRPCAddr)
			}
			mlog.Warn("node %s 检测到陈旧成员：NodeID=%d 旧 RaftID=%d 新 RaftID=%d，执行 remove_node 清理后重新加入",
				meta.Name, meta.ID, oldRaftID, cluster.RaftMemberID(meta))
			err = n.replaceStaleMember(rn, oldRaftID, meta)
		} else {
			err = rn.AddMaster(meta)
		}
	} else {
		err = rn.Propose(cluster.Command{Type: cluster.CmdAddNode, Node: &meta})
	}
	if err != nil {
		return transport.NodeMeta{}, err
	}
	return n.Meta, nil
}

// replaceStaleMember 清理陈旧 master 成员并完成替换：先提案 CmdRemoveNode
// 并等待旧记录从 CSM 摘除（保证先于新节点注册应用），再以一条 joint 成员
// 变更原子移除旧 raft 成员并加入新成员。
func (n *Node) replaceStaleMember(rn *cluster.RaftNode, oldRaftID uint64, meta transport.NodeMeta) error {
	if !rn.IsLeader() {
		return cluster.ErrNotLeader
	}
	if err := rn.Propose(cluster.Command{Type: cluster.CmdRemoveNode, NodeID: meta.ID}); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := n.state().Node(meta.ID); !ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, ok := n.state().Node(meta.ID); ok {
		return fmt.Errorf("node: 陈旧成员 %d 摘除超时", meta.ID)
	}
	return rn.ReplaceMaster(oldRaftID, meta)
}

// ClusterState 返回集群状态快照
func (n *Node) ClusterState() ([]byte, error) {
	return n.state().Snapshot()
}

// RaftNode 返回 raft 节点（master 才有）
func (n *Node) RaftNode() *cluster.RaftNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft
}

// Stop 停止节点集群部分
func (n *Node) Stop() {
	close(n.stopc)
	rn := n.RaftNode()
	if rn != nil {
		rn.Stop()
	}
}
