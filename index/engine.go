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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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
	dir    string
	schema *schema.Schema

	mu      sync.RWMutex
	buf     *memBuf
	segs    []*segment.Reader // 按段序号升序（越老越靠前）
	tlog    *translog.Log
	gen     uint64 // 当前 translog 代际
	lsnBase int64  // 当前代际的 LSN 基线（此前全部代际的记录总数）
	nextSeq int    // 下一个段序号（段与合并产物全局单调递增）
	closed  bool

	// 段合并参数（字段形式以便测试调整）
	mergeThreshold int // 小段数量超过该值时 Flush 自动触发合并
	smallSegDocs   int // 文档数小于该值视为小段
}

// 段合并默认参数
const (
	defaultMergeThreshold = 5     // 小段数量超过该值触发自动合并
	defaultSmallSegDocs   = 10000 // 文档数小于该值视为小段
)

// Open 打开（或创建）一个索引。
// schema.json 已存在时以磁盘上的为准；否则使用 sch 并落盘，sch 为空则报错。
func Open(dir string, sch *schema.Schema) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("index: 创建目录失败: %w", err)
	}
	e := &Engine{
		dir:            dir,
		buf:            newMemBuf(),
		nextSeq:        1,
		mergeThreshold: defaultMergeThreshold,
		smallSegDocs:   defaultSmallSegDocs,
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
// 每个字段的解析方式由其字段类型插件决定：倒排字段经分词器产出 terms，
// 正排字段产出数值；仅存储字段不解析（原文整体保留）。
func (e *Engine) parseDoc(id string, raw json.RawMessage) (segment.Doc, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return segment.Doc{}, fmt.Errorf("index: 文档 %q 不是合法 JSON 对象: %w", id, err)
	}
	return e.parseDocFields(id, raw, fields)
}

// parseDocFields 在已反序列化的字段集上解析文档（Index 路径复用，避免重复解析）
func (e *Engine) parseDocFields(id string, raw json.RawMessage, fields map[string]json.RawMessage) (segment.Doc, error) {
	doc := segment.Doc{
		ID:    id,
		Raw:   append([]byte(nil), raw...),
		Terms: make(map[string][]string),
		Nums:  make(map[string]int64),
	}
	for _, f := range e.schema.Fields {
		fv, ok := fields[f.Name]
		if !ok {
			continue
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
			a, err := plugin.GetAnalyzer(p.Analyzer())
			if err != nil {
				return segment.Doc{}, err
			}
			doc.Terms[f.Name] = a.Tokenize(pv.Text)
		}
		if p.DocValues() {
			doc.Nums[f.Name] = pv.Num
		}
	}
	return doc, nil
}

// inferFieldsLocked 动态 mapping：文档中未声明的字段按 JSON 值类型推断并加入 schema，
// 推断结果持久化到 schema.json。老段没有该字段的索引文件，自然视为字段缺失。
// 对象/数组/null 不推断（忽略该字段）。
func (e *Engine) inferFieldsLocked(fields map[string]json.RawMessage) error {
	changed := false
	for name, fv := range fields {
		if _, ok := e.schema.Field(name); ok {
			continue
		}
		ft := inferFieldType(fv)
		if ft == "" {
			continue
		}
		if err := e.schema.AddField(schema.Field{Name: name, Type: ft}); err != nil {
			return err
		}
		changed = true
	}
	if !changed {
		return nil
	}
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

// inferFieldType 按 JSON 值类型推断字段类型：string->text、数字->number、bool->bool
func inferFieldType(raw json.RawMessage) string {
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return ""
	}
	switch v.(type) {
	case string:
		return "text"
	case json.Number:
		return "number"
	case bool:
		return "bool"
	}
	return ""
}

// Index 写入（或更新）一篇文档。同 ID 的旧版本会被标记删除。
// 未在 schema 声明的字段按动态 mapping 推断后加入索引。
// 返回该操作的 translog LSN（复制协议用）。
func (e *Engine) Index(id string, raw json.RawMessage) (int64, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return -1, fmt.Errorf("index: 文档 %q 不是合法 JSON 对象: %w", id, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return -1, fmt.Errorf("index: 引擎已关闭")
	}
	if err := e.inferFieldsLocked(fields); err != nil {
		return -1, err
	}
	doc, err := e.parseDocFields(id, raw, fields)
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
// 注意：Flush 轮替代际后旧代际被删除，fromLSN 落在已轮替区间时会丢数据
// （5b 简化：依赖 primary 不频繁 Flush；生产需快照同步，见 README 遗留限制）。
func (e *Engine) ReadTranslog(fromLSN int64, limit int) ([]translog.Op, int64, error) {
	if limit <= 0 {
		limit = 1024
	}
	ops := make([]translog.Op, 0, limit)
	var next int64
	err := e.tlog.ReadFrom(fromLSN, func(lsn int64, op translog.Op) error {
		if len(ops) >= limit {
			return errLimitReached // 提前终止扫描
		}
		ops = append(ops, op)
		next = lsn + 1
		return nil
	})
	if err != nil && !errors.Is(err, errLimitReached) {
		return nil, 0, err
	}
	return ops, next, nil
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

// Flush 将内存缓冲落盘为新段，并轮替 translog 代际
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("index: 引擎已关闭")
	}
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
		if err := segment.Write(segDir, e.nextSeq, e.schema.InvFields(), e.schema.TextFields(), e.schema.NumFields(), docs); err != nil {
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

	// 段已包含全部旧日志内容，轮替 translog 代际并删除旧文件
	oldPath := filepath.Join(e.dir, fmt.Sprintf("translog-%d.log", e.gen))
	if err := e.tlog.Close(); err != nil {
		return err
	}
	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("index: 删除旧 translog 失败: %w", err)
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
	// Flush 后检查是否触发自动段合并
	return e.maybeMergeLocked()
}

// Merge 手动触发段合并：存在 2 个及以上小段时，将全部小段合并为一个新段
func (e *Engine) Merge() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("index: 引擎已关闭")
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
	if err := segment.Write(tmpDir, seq, e.schema.InvFields(), e.schema.TextFields(), e.schema.NumFields(), docs); err != nil {
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
