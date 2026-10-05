// Package index 实现单机索引引擎：内存缓冲 + 不可变段 + translog 崩溃恢复。
//
// 目录布局：
//
//	<dir>/schema.json        索引字段定义
//	<dir>/seg-<n>/           不可变段（见 segment 包）
//	<dir>/translog-<g>.log   预写日志（见 translog 包）
//
// 写入路径：先追加 translog（并 Sync），再写内存缓冲；Flush 将缓冲落盘为
// 新段并轮替 translog 代际。重启时加载全部段，再回放最新一代 translog。
package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
	"github.com/FalconEngine/falcon/segment"
	"github.com/FalconEngine/falcon/translog"

	// 引擎依赖内置插件（字段类型/分词器/打分器/查询子句/聚合）才能工作，
	// 在此统一引入；外部插件由使用方自行 import 注册。
	_ "github.com/FalconEngine/falcon/plugins"
)

// Engine 单个索引的引擎实例
type Engine struct {
	dir string
	// schema 以写时复制方式更新（写锁内 clone-修改-替换指针，见 inferFieldsLocked/
	// UpdateMapping）：锁外读者（查询解析/mapping 广播）拿到的始终是完整不可变的
	// 旧版本快照，避免并发读写 map；读到旧快照仅表现为新字段晚一拍可见
	schema *schema.Schema

	mu      sync.RWMutex
	buf     *memBuf
	segs    []*segment.Reader // 按段序号升序（越老越靠前）
	tlog    *translog.Log
	gen     uint64 // 当前 translog 代际
	lsnBase int64  // 当前代际的 LSN 基线（此前全部代际的记录总数）
	nextSeq int    // 下一个段序号（段与合并产物全局单调递增）
	closed  bool

	// translog 保留窗口（副本恢复用）：
	// gens 为保留的全部代际清单（升序，最后一项是当前活跃代际），
	// retentionLSN 为全局检查点——LSN <= retentionLSN 的代际允许在 Flush 时清除。
	// 默认 NoRetentionFloor（只保留当前代际，单机磁盘不膨胀）；
	// primary 节点按副本 ack 推进（见 node/replication.go）。
	gens         []translog.GenInfo
	retentionLSN int64
	recovering   bool             // 正作为恢复源被拷贝：冻结 Flush/合并/代际轮替
	recoverSnap  map[string]int64 // 恢复快照："segDir/name" -> Prepare 时刻大小
	// 段合并参数（字段形式以便测试调整）
	mergeThreshold int // 小段数量超过该值时 Flush 自动触发合并
	smallSegDocs   int // 文档数小于该值视为小段
	// 缓冲防护：缓冲内文档数达到该阈值时，写入侧持锁内联触发 flush，
	// 防止长期不 _flush 时缓冲无界增长
	maxBufferDocs int
	// dateDetection 动态 mapping 日期检测开关（索引级设置，见 IndexSettings）：
	// 开启时字符串值若可解析为日期则推断为 date 字段，否则推断为 text
	dateDetection bool
}

// NoRetentionFloor 不设置保留地板（只保留当前代际）时的 retentionLSN 取值
const NoRetentionFloor = int64(math.MaxInt64)

// 段合并默认参数
const (
	defaultMergeThreshold = 5     // 小段数量超过该值触发自动合并
	defaultSmallSegDocs   = 10000 // 文档数小于该值视为小段
	// defaultMaxBufferDocs 缓冲落盘阈值（与 smallSegDocs 对齐：
	// 自动产出的段恰好处于"非小段"边界，不额外放大合并压力）
	defaultMaxBufferDocs = 10000
)

// Open 打开（或创建）一个索引（动态 mapping 日期检测默认开启）。
// schema.json 已存在时以磁盘上的为准；否则使用 sch 并落盘，sch 为空则报错。
func Open(dir string, sch *schema.Schema) (*Engine, error) {
	return open(dir, sch, true)
}

// open 打开（或创建）一个索引；dateDetection 为动态 mapping 日期检测开关
func open(dir string, sch *schema.Schema, dateDetection bool) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("index: 创建目录失败: %w", err)
	}
	e := &Engine{
		dir:            dir,
		buf:            newMemBuf(),
		nextSeq:        1,
		retentionLSN:   NoRetentionFloor,
		mergeThreshold: defaultMergeThreshold,
		smallSegDocs:   defaultSmallSegDocs,
		maxBufferDocs:  defaultMaxBufferDocs,
		dateDetection:  dateDetection,
	}

	// 1. schema：磁盘优先
	sp := filepath.Join(dir, "schema.json")
	if b, err := os.ReadFile(sp); err == nil {
		var s schema.Schema
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, fmt.Errorf("index: 解析 schema.json 失败: %w", err)
		}
		e.schema = &s
	} else if os.IsNotExist(err) {
		if sch == nil {
			return nil, fmt.Errorf("index: %s 不存在 schema，创建索引必须提供 schema", dir)
		}
		b, err := json.Marshal(sch)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(sp, b, 0o644); err != nil {
			return nil, fmt.Errorf("index: 写入 schema.json 失败: %w", err)
		}
		e.schema = sch
	} else {
		return nil, fmt.Errorf("index: 读取 schema.json 失败: %w", err)
	}

	// 2. 加载全部段（含合并产物 seg-merge-<n>），按 meta 中的段序号升序排列
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segNames []string
	for _, ent := range entries {
		// seg-<n> 与合并产物 seg-merge-<n>（.tmp 临时目录跳过）
		if ent.IsDir() && strings.HasPrefix(ent.Name(), "seg-") && !strings.HasSuffix(ent.Name(), ".tmp") {
			segNames = append(segNames, ent.Name())
		}
	}
	for _, name := range segNames {
		r, err := segment.Open(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("index: 打开段 %s 失败: %w", name, err)
		}
		e.segs = append(e.segs, r)
		if r.Seq() >= e.nextSeq {
			e.nextSeq = r.Seq() + 1
		}
	}
	sort.Slice(e.segs, func(i, j int) bool { return e.segs[i].Seq() < e.segs[j].Seq() })

	// 3. 找到最新一代 translog 并回放（崩溃恢复）
	gen := uint64(1)
	for _, ent := range entries {
		if g, ok := translogGen(ent.Name()); ok && g >= gen {
			gen = g
		}
	}
	e.gen = gen
	// LSN 基线：跨代际单调递增的复制位点（Flush 轮替时持久化）
	e.lsnBase = readLSNBase(dir)
	// 代际保留清单（translog 保留窗口）：
	// 老数据目录无清单时按现状只认最新代际（向后兼容）；
	// 有清单时以磁盘最新代际为准做最小修复——崩溃可能发生在
	// "轮替代际"与"重写清单"之间，导致清单落后当前代际一代。
	gens, err := translog.ReadManifest(dir)
	if err != nil {
		return nil, err
	}
	if gens == nil {
		gens = []translog.GenInfo{{Gen: gen, BaseLSN: e.lsnBase}}
	} else {
		kept := gens[:0]
		for _, g := range gens {
			if g.Gen < gen {
				kept = append(kept, g)
			}
		}
		if n := len(kept); n > 0 {
			// 最新的历史代际与当前代际相邻，记录数按基线差补齐
			kept[n-1].Count = e.lsnBase - kept[n-1].BaseLSN
		}
		gens = append(kept, translog.GenInfo{Gen: gen, BaseLSN: e.lsnBase})
	}
	e.gens = gens
	if _, err := translog.Replay(dir, gen, e.lsnBase, func(lsn int64, op translog.Op) error {
		return e.applyReplay(op)
	}); err != nil {
		return nil, fmt.Errorf("index: 回放 translog 失败: %w", err)
	}
	tlog, err := translog.Open(dir, gen, e.lsnBase)
	if err != nil {
		return nil, fmt.Errorf("index: 打开 translog 失败: %w", err)
	}
	e.tlog = tlog
	return e, nil
}

// lsnBasePath LSN 基线文件路径（内容为此前全部代际的记录总数）
func lsnBasePath(dir string) string { return filepath.Join(dir, "lsn") }

// readLSNBase 读取 LSN 基线；文件不存在时为 0
func readLSNBase(dir string) int64 {
	b, err := os.ReadFile(lsnBasePath(dir))
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// translogGen 解析 translog-<g>.log 文件名中的代际号
func translogGen(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "translog-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	g, err := strconv.ParseUint(name[len("translog-"):len(name)-len(".log")], 10, 64)
	return g, err == nil
}

// applyReplay 回放一条 translog 记录（不加锁，仅 Open 时调用）
func (e *Engine) applyReplay(op translog.Op) error {
	switch op.Type {
	case translog.OpIndex:
		doc, err := e.parseDoc(op.ID, op.Doc)
		if err != nil {
			return err
		}
		e.tombstoneLocked(op.ID)
		e.buf.add(doc)
	case translog.OpDelete:
		e.tombstoneLocked(op.ID)
	}
	return nil
}

// parseDoc 按 schema 解析原始 JSON 文档为可索引的 segment.Doc。
// 嵌套对象先经 flattenDoc 展开为点路径再按字段解析。
// 每个字段的解析方式由其字段类型插件决定：倒排字段经分词器产出 terms，
// 正排字段产出数值；仅存储字段不解析（原文整体保留）。
func (e *Engine) parseDoc(id string, raw json.RawMessage) (segment.Doc, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return segment.Doc{}, fmt.Errorf("index: 文档 %q 不是合法 JSON 对象: %w", id, err)
	}
	return e.parseDocFields(id, raw, flattenDoc(fields))
}

// flattenDoc 把文档字段递归展开为点路径扁平 map：
// {"user":{"name":"x"}} → {"user.name":"x"}（对齐 ES 对嵌套 object 的处理）。
// 数组不展开（含对象数组——保持现状不索引；null 保留为叶子由上层忽略）。
func flattenDoc(fields map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(fields))
	var walk func(prefix string, m map[string]json.RawMessage)
	walk = func(prefix string, m map[string]json.RawMessage) {
		for k, v := range m {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			var sub map[string]json.RawMessage
			if err := json.Unmarshal(v, &sub); err == nil && sub != nil {
				walk(key, sub) // 嵌套对象：递归展开；空对象展开后无叶子，自然消失
				continue
			}
			out[key] = v
		}
	}
	walk("", fields)
	return out
}

// analyzerOf 取字段实际使用的分词器：优先字段级 analyzer 覆盖，
// 回落字段类型插件的默认分词器。
func analyzerOf(f schema.Field) (plugin.Analyzer, error) {
	if f.Analyzer != "" {
		return plugin.GetAnalyzer(f.Analyzer)
	}
	p, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return nil, err
	}
	return plugin.GetAnalyzer(p.Analyzer())
}

// parseDocFields 在已反序列化并 flatten 的字段集上解析文档（Index 路径复用，避免重复解析）。
// 字段查找：先精确查 f.Name；查不到且 f.Name 含 '.' 时按 multi-field 规则取
// 父路径的值（title.keyword → 取 title 的值；嵌套对象叶子的子字段同理）。
func (e *Engine) parseDocFields(id string, raw json.RawMessage, fields map[string]json.RawMessage) (segment.Doc, error) {
	doc := segment.Doc{
		ID:    id,
		Raw:   append([]byte(nil), raw...),
		Terms: make(map[string][]plugin.Token),
		Nums:  make(map[string]int64),
		Kws:   make(map[string]string),
	}
	for _, f := range e.schema.Flattened() {
		fv, ok := fields[f.Name]
		if !ok {
			if i := strings.LastIndex(f.Name, "."); i >= 0 {
				fv, ok = fields[f.Name[:i]]
			}
			if !ok {
				continue
			}
		}
		p, err := plugin.GetFieldType(f.Type)
		if err != nil {
			return segment.Doc{}, err
		}
		pv, err := p.Parse(fv)
		if err != nil {
			return segment.Doc{}, fmt.Errorf("index: 文档 %q 字段 %q: %w", id, f.Name, err)
		}
		if p.Inverted() {
			a, err := analyzerOf(f)
			if err != nil {
				return segment.Doc{}, err
			}
			doc.Terms[f.Name] = a.Analyze(pv.Text)
		}
		switch p.DocValuesKind() {
		case plugin.DVNum:
			doc.Nums[f.Name] = pv.Num
		case plugin.DVKeyword:
			doc.Kws[f.Name] = pv.Text
		}
	}
	return doc, nil
}

// inferFieldsLocked 动态 mapping：文档中未声明的字段按 JSON 值类型推断并加入 schema，
// 推断结果持久化到 schema.json。老段没有该字段的索引文件，自然视为字段缺失。
// fields 为 flattenDoc 后的扁平 map：嵌套对象的叶子逐个推断；数组/null 不推断（忽略该字段）。
func (e *Engine) inferFieldsLocked(fields map[string]json.RawMessage) error {
	var adds []schema.Field
	for name, fv := range fields {
		if _, ok := e.schema.Field(name); ok {
			continue
		}
		f, ok := schema.InferField(name, fv, e.dateDetection)
		if !ok {
			continue
		}
		adds = append(adds, f)
	}
	if len(adds) == 0 {
		return nil
	}
	// 写时复制：在副本上修改后替换指针（见 Engine.schema 注释）
	ns := e.schema.Clone()
	for _, f := range adds {
		if err := ns.AddField(f); err != nil {
			return err
		}
	}
	e.schema = ns
	return e.persistSchemaLocked()
}

// persistSchemaLocked 把当前 schema 原子持久化到 schema.json（tmp + rename）
func (e *Engine) persistSchemaLocked() error {
	b, err := json.Marshal(e.schema)
	if err != nil {
		return err
	}
	sp := filepath.Join(e.dir, "schema.json")
	if err := os.WriteFile(sp+".tmp", b, 0o644); err != nil {
		return fmt.Errorf("index: 写入 schema.json 失败: %w", err)
	}
	return os.Rename(sp+".tmp", sp)
}

// UpdateMapping 显式更新 mapping：新字段（含已有字段的新子字段）合并进 schema 并持久化；
// 与现有定义冲突（同名字段不同 Type/Analyzer）返回错误；完全一致幂等（不重复落盘）。
// 已知行为：老段没有新字段的索引文件，视为字段缺失，待段合并（merge）重建后自愈。
func (e *Engine) UpdateMapping(fields []schema.Field) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("index: 引擎已关闭")
	}
	// 写时复制：在副本上合并后替换指针（见 Engine.schema 注释）
	ns := e.schema.Clone()
	before := len(ns.Flattened())
	if err := ns.MergeFields(fields); err != nil {
		return err
	}
	if len(ns.Flattened()) == before {
		return nil // 幂等：无新字段
	}
	e.schema = ns
	return e.persistSchemaLocked()
}

// Index 写入（或更新）一篇文档。同 ID 的旧版本会被标记删除。
// 未在 schema 声明的字段按动态 mapping 推断后加入索引（嵌套对象先展开为点路径）。
// 返回该操作的 translog LSN（复制协议用）。
func (e *Engine) Index(id string, raw json.RawMessage) (int64, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return -1, fmt.Errorf("index: 文档 %q 不是合法 JSON 对象: %w", id, err)
	}
	flat := flattenDoc(fields)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return -1, fmt.Errorf("index: 引擎已关闭")
	}
	if err := e.inferFieldsLocked(flat); err != nil {
		return -1, err
	}
	doc, err := e.parseDocFields(id, raw, flat)
	if err != nil {
		return -1, err
	}
	lsn, err := e.tlog.Append(translog.Op{Type: translog.OpIndex, ID: id, Doc: raw})
	if err != nil {
		return -1, fmt.Errorf("index: 写 translog 失败: %w", err)
	}
	if err := e.tlog.Sync(); err != nil {
		return -1, fmt.Errorf("index: translog 刷盘失败: %w", err)
	}
	e.tombstoneLocked(id)
	e.buf.add(doc)
	// 缓冲达到阈值时内联落盘，防止不调用 _flush 时缓冲无界增长。
	// 已知权衡：触发时本次写入阻塞一个落盘周期（异步 flush 留待后续阶段）。
	// 恢复源拷贝期间跳过内联落盘（Flush 被冻结）：缓冲可能短时超过阈值，
	// 以恢复窗口时长为界，可接受。
	if !e.recovering && len(e.buf.docs) >= e.maxBufferDocs {
		if err := e.flushLocked(); err != nil {
			return -1, fmt.Errorf("index: 缓冲阈值触发落盘失败: %w", err)
		}
	}
	return lsn, nil
}

// Delete 按外部 ID 删除文档；文档不存在返回 found=false。
// 返回删除操作的 translog LSN（未删除时 lsn=-1）。
func (e *Engine) Delete(id string) (bool, int64, error) {
	e.mu.RLock()
	_, _, found := e.locateLocked(id)
	e.mu.RUnlock()
	if !found {
		return false, -1, nil
	}
	lsn, err := e.tlog.Append(translog.Op{Type: translog.OpDelete, ID: id})
	if err != nil {
		return false, -1, fmt.Errorf("index: 写 translog 失败: %w", err)
	}
	if err := e.tlog.Sync(); err != nil {
		return false, -1, fmt.Errorf("index: translog 刷盘失败: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, -1, fmt.Errorf("index: 引擎已关闭")
	}
	e.tombstoneLocked(id)
	return true, lsn, nil
}

// LastLSN 返回当前 translog 最后一条 LSN（无记录时为 -1）
func (e *Engine) LastLSN() int64 {
	return e.tlog.NextLSN() - 1
}

// errLimitReached 批量读取达到上限的哨兵错误（内部用）
var errLimitReached = fmt.Errorf("limit-reached")

// ReadTranslog 从指定 LSN 起批量读取 translog 操作（复制拉取用）。
// 返回操作序列与下一条可用 LSN。
// 按代际清单跨文件连读保留窗口内的历史代际（P0-2）：
// fromLSN 落在已清除区间（< OldestLSN()）或尚未有数据时返回空序列，
// 副本侧据此判断"落后出窗/历史分叉"并触发段拷贝恢复（见 node/recovery.go）。
func (e *Engine) ReadTranslog(fromLSN int64, limit int) ([]translog.Op, int64, error) {
	if limit <= 0 {
		limit = 1024
	}
	e.mu.RLock()
	gens := append([]translog.GenInfo(nil), e.gens...)
	tlog := e.tlog
	dir := e.dir
	e.mu.RUnlock()

	if len(gens) == 0 || fromLSN < gens[0].BaseLSN {
		// 落后出窗：返回空序列与当前末尾，副本走段拷贝恢复
		return nil, tlog.NextLSN(), nil
	}
	ops := make([]translog.Op, 0, limit)
	var next int64
	collect := func(lsn int64, op translog.Op) error {
		if lsn < fromLSN {
			return nil
		}
		if len(ops) >= limit {
			return errLimitReached // 提前终止扫描
		}
		ops = append(ops, op)
		next = lsn + 1
		return nil
	}
	for i, g := range gens {
		if i < len(gens)-1 && g.BaseLSN+g.Count <= fromLSN {
			continue // 整个代际在 fromLSN 之前
		}
		var err error
		if i == len(gens)-1 {
			// 当前活跃代际：经 tlog 读到调用时刻的最新数据
			err = tlog.ReadFrom(fromLSN, collect)
		} else {
			_, err = translog.Replay(dir, g.Gen, g.BaseLSN, collect)
		}
		if errors.Is(err, errLimitReached) {
			break
		}
		if err != nil {
			return nil, 0, err
		}
	}
	if len(ops) == 0 {
		next = tlog.NextLSN()
	}
	return ops, next, nil
}

// SetRetentionLSN 设置 translog 保留地板（全局检查点）：
// Flush 轮替时清除全部记录 LSN <= min 的历史代际。
// 传 NoRetentionFloor 恢复默认（只保留当前代际）。
func (e *Engine) SetRetentionLSN(min int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retentionLSN = min
}

// OldestLSN 返回保留窗口内最老可用 LSN（复制拉取下限）
func (e *Engine) OldestLSN() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.gens) == 0 {
		return e.lsnBase
	}
	return e.gens[0].BaseLSN
}

// locateLocked 定位文档所在位置：segIdx == -1 表示在内存缓冲中；未找到时 found 为 false
func (e *Engine) locateLocked(id string) (segIdx int, loc uint32, found bool) {
	if loc, ok := e.buf.id2loc[id]; ok && !e.buf.deleted[loc] {
		return -1, loc, true
	}
	for i := len(e.segs) - 1; i >= 0; i-- {
		if loc, ok := e.segs[i].LocalID(id); ok && !e.segs[i].Deleted(loc) {
			return i, loc, true
		}
	}
	return 0, 0, false
}

// tombstoneLocked 将 id 的旧版本标记删除（缓冲与所有段中至多一处存活）
func (e *Engine) tombstoneLocked(id string) {
	if loc, ok := e.buf.id2loc[id]; ok {
		e.buf.deleted[loc] = true
	}
	for _, seg := range e.segs {
		if loc, ok := seg.LocalID(id); ok {
			seg.Delete(loc)
		}
	}
}

// Flush 将内存缓冲落盘为新段，并轮替 translog 代际。
// 分片作为恢复源被拷贝期间报错（恢复会冻结 Flush/代际轮替，见 PrepareRecovery）。
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("index: 引擎已关闭")
	}
	if e.recovering {
		return fmt.Errorf("index: 分片正作为恢复源被拷贝，恢复期间禁止 _flush")
	}
	return e.flushLocked()
}

// flushLocked 缓冲落盘本体（调用方须持有写锁，closed 检查由外层完成）：
// 写新段 → 重置缓冲 → 轮替 translog 代际 → 按需触发段合并。
// Index 在缓冲达到 maxBufferDocs 阈值时也会内联调用。
func (e *Engine) flushLocked() error {
	if e.buf.empty() {
		return nil
	}
	// 过滤掉缓冲中已标记删除的文档（如同 ID 更新的旧版本），避免写入段后复活
	docs := make([]segment.Doc, 0, len(e.buf.docs))
	for i, d := range e.buf.docs {
		if !e.buf.deleted[uint32(i)] {
			docs = append(docs, d)
		}
	}
	if len(docs) > 0 {
		segDir := filepath.Join(e.dir, fmt.Sprintf("seg-%d", e.nextSeq))
		if err := segment.Write(segDir, e.nextSeq, e.schema.InvFields(), e.schema.TextFields(), e.schema.NumFields(), e.schema.KwFields(), docs); err != nil {
			return fmt.Errorf("index: 段落盘失败: %w", err)
		}
		e.nextSeq++
		r, err := segment.Open(segDir)
		if err != nil {
			return fmt.Errorf("index: 打开新段失败: %w", err)
		}
		e.segs = append(e.segs, r)
	}
	e.buf = newMemBuf()

	// 段已包含全部旧日志内容，轮替 translog 代际。
	// 旧代际不再立即删除：先重写代际清单，再按保留窗口清除
	// 全部记录 LSN <= retentionLSN 的历史代际（供落后副本跨代际拉取）。
	oldGen := e.gen
	if err := e.tlog.Close(); err != nil {
		return err
	}
	e.gen++
	e.lsnBase = e.tlog.NextLSN()
	// 基线先落盘再开新代际，保证崩溃恢复后 LSN 连续
	if err := os.WriteFile(lsnBasePath(e.dir), []byte(strconv.FormatInt(e.lsnBase, 10)), 0o644); err != nil {
		return fmt.Errorf("index: 写入 LSN 基线失败: %w", err)
	}
	tlog, err := translog.Open(e.dir, e.gen, e.lsnBase)
	if err != nil {
		return fmt.Errorf("index: 打开新 translog 失败: %w", err)
	}
	e.tlog = tlog

	// 清单定稿刚关闭的旧代际记录数，并追加新（当前）代际
	if n := len(e.gens); n > 0 && e.gens[n-1].Gen == oldGen {
		e.gens[n-1].Count = e.lsnBase - e.gens[n-1].BaseLSN
	} else {
		// 不变量被破坏（不应发生）：防御性重建为只含当前代际
		e.gens = nil
	}
	e.gens = append(e.gens, translog.GenInfo{Gen: e.gen, BaseLSN: e.lsnBase})

	// 保留窗口清除：默认（NoRetentionFloor）只留当前代际，单机磁盘不膨胀
	floor := e.retentionLSN
	if floor == NoRetentionFloor {
		floor = e.lsnBase - 1
	}
	kept := e.gens[:0]
	var pruned []translog.GenInfo
	for i, g := range e.gens {
		if i < len(e.gens)-1 && g.BaseLSN+g.Count-1 <= floor {
			pruned = append(pruned, g)
			continue
		}
		kept = append(kept, g)
	}
	e.gens = kept
	// 先落盘清单再删文件，保证重启后清单不含已删代际
	if err := translog.WriteManifest(e.dir, e.gens); err != nil {
		return err
	}
	for _, g := range pruned {
		p := filepath.Join(e.dir, fmt.Sprintf("translog-%d.log", g.Gen))
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("index: 删除已轮替 translog 失败: %w", err)
		}
	}
	// 顺带清理清单外的孤儿代际文件（老数据目录或崩溃残留）
	if entries, err := os.ReadDir(e.dir); err == nil {
		inManifest := make(map[uint64]bool, len(e.gens))
		for _, g := range e.gens {
			inManifest[g.Gen] = true
		}
		for _, ent := range entries {
			if g, ok := translogGen(ent.Name()); ok && !inManifest[g] {
				os.Remove(filepath.Join(e.dir, ent.Name()))
			}
		}
	}
	// Flush 后检查是否触发自动段合并
	return e.maybeMergeLocked()
}

// Merge 手动触发段合并：存在 2 个及以上小段时，将全部小段合并为一个新段。
// 恢复源拷贝期间同样被冻结（合并会删除快照中的段目录）。
func (e *Engine) Merge() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("index: 引擎已关闭")
	}
	if e.recovering {
		return fmt.Errorf("index: 分片正作为恢复源被拷贝，恢复期间禁止合并")
	}
	return e.mergeLocked(2)
}

// maybeMergeLocked Flush 后的自动触发入口：小段数量超过阈值才合并
func (e *Engine) maybeMergeLocked() error {
	return e.mergeLocked(e.mergeThreshold + 1)
}

// mergeLocked 当小段（文档数 < smallSegDocs）数量达到 minSmall 时，
// 将全部小段合并为一个新段。全程持有写锁，查询短暂阻塞；
// 新段先写临时目录再原子 rename 就位，之后摘除并删除旧段，
// 因此任何时刻查询读到的都是完整段。
func (e *Engine) mergeLocked(minSmall int) error {
	inMerge := make(map[int]bool)
	for i, seg := range e.segs {
		if seg.DocCount() < e.smallSegDocs {
			inMerge[i] = true
		}
	}
	if len(inMerge) < minSmall {
		return nil
	}

	// 收集全部小段中的存活文档：段从老到新、段内 docID 升序；
	// 从原文重新解析，保证倒排/正排/norm 与当前 schema 一致
	var docs []segment.Doc
	for i, seg := range e.segs {
		if !inMerge[i] {
			continue
		}
		for loc := 0; loc < seg.DocCount(); loc++ {
			if seg.Deleted(uint32(loc)) {
				continue
			}
			raw, err := seg.Stored(uint32(loc))
			if err != nil {
				return fmt.Errorf("index: 合并读取段 %s 失败: %w", seg.Dir(), err)
			}
			d, err := e.parseDoc(seg.ID(uint32(loc)), raw)
			if err != nil {
				return fmt.Errorf("index: 合并解析文档失败: %w", err)
			}
			docs = append(docs, d)
		}
	}

	// 先写临时目录，原子 rename 就位后再替换内存中的段列表
	seq := e.nextSeq
	tmpDir := filepath.Join(e.dir, fmt.Sprintf("seg-merge-%d.tmp", seq))
	finalDir := filepath.Join(e.dir, fmt.Sprintf("seg-merge-%d", seq))
	defer os.RemoveAll(tmpDir) // 失败时清理临时目录；rename 成功后为 no-op
	if err := segment.Write(tmpDir, seq, e.schema.InvFields(), e.schema.TextFields(), e.schema.NumFields(), e.schema.KwFields(), docs); err != nil {
		return fmt.Errorf("index: 合并写段失败: %w", err)
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return fmt.Errorf("index: 合并段 rename 失败: %w", err)
	}
	merged, err := segment.Open(finalDir)
	if err != nil {
		return fmt.Errorf("index: 打开合并段失败: %w", err)
	}
	e.nextSeq++

	newSegs := make([]*segment.Reader, 0, len(e.segs)-len(inMerge)+1)
	for i, seg := range e.segs {
		if !inMerge[i] {
			newSegs = append(newSegs, seg)
		}
	}
	newSegs = append(newSegs, merged)
	sort.Slice(newSegs, func(i, j int) bool { return newSegs[i].Seq() < newSegs[j].Seq() })

	oldSegs := e.segs
	e.segs = newSegs

	// 新段就位后关闭并删除旧段目录
	for i, seg := range oldSegs {
		if !inMerge[i] {
			continue
		}
		seg.Close()
		if err := os.RemoveAll(seg.Dir()); err != nil {
			return fmt.Errorf("index: 删除旧段 %s 失败: %w", seg.Dir(), err)
		}
	}
	return nil
}

// DocCount 返回存活文档总数
func (e *Engine) DocCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	n := len(e.buf.docs) - len(e.buf.deleted)
	for _, seg := range e.segs {
		n += seg.LiveCount()
	}
	return n
}

// Get 按外部 ID 取文档原文；不存在返回 found=false
func (e *Engine) Get(id string) (json.RawMessage, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, false, fmt.Errorf("index: 引擎已关闭")
	}
	segIdx, loc, found := e.locateLocked(id)
	if !found {
		return nil, false, nil
	}
	_, raw, err := e.loadDocLocked(segIdx, loc)
	if err != nil {
		return nil, false, err
	}
	// 缓冲内的 Raw 切片可能被后续写复用，拷贝后返回
	return append(json.RawMessage(nil), raw...), true, nil
}

// Schema 返回索引的字段定义
func (e *Engine) Schema() *schema.Schema { return e.schema }

// segCount 返回当前段数
func (e *Engine) segCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.segs)
}

// Close 关闭引擎（不自动 Flush，未落盘的数据由 translog 保证恢复）
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for _, seg := range e.segs {
		seg.Close()
	}
	return e.tlog.Close()
}
