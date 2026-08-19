package index

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/FalconEngine/falcon/plugin"
)

// defaultScorer 引擎默认打分器（可在 MatchNode.Scorer 中按查询覆盖）
const defaultScorer = "bm25"

// SortField 排序字段：仅支持建了正排的字段与 "_score"。
// 字段缺失的文档恒排在最后。
type SortField struct {
	Field string `json:"field"` // 正排字段名或 "_score"
	Desc  bool   `json:"desc"`  // true 降序，false 升序
}

// Hit 一条命中结果
type Hit struct {
	ID     string          `json:"id"`
	Score  float64         `json:"score"`
	Source json.RawMessage `json:"_source"`
}

// SearchOptions 检索选项
type SearchOptions struct {
	Sort       []SortField                  // 为空默认 _score desc
	From       int                          // 分页起始，默认 0
	Size       int                          // 分页大小，默认 10
	Aggs       map[string]plugin.Aggregator // 聚合名 -> 聚合器，作用于全部命中文档
	AggPartial bool                         // true 时聚合输出中间结果（跨分片合并用）
}

// Result 检索结果
type Result struct {
	Total int                        `json:"total"`
	Hits  []Hit                      `json:"hits"`
	Aggs  map[string]json.RawMessage `json:"aggs,omitempty"` // 聚合结果
	// 分片降级标记（跨节点查询时部分分片失败：返回部分结果而非整体报错，
	// 语义同 ES 的 partial results，见 README）
	Partial      bool  `json:"partial,omitempty"`
	FailedShards []int `json:"failed_shards,omitempty"`
}

// candidate 一个待排序的命中候选：segIdx == -1 表示在内存缓冲
type candidate struct {
	segIdx int
	loc    uint32
	score  float64
}

// Search 执行查询树检索：求值 + 删除过滤 + 排序 + 聚合 + 分页
func (e *Engine) Search(root plugin.QNode, opts *SearchOptions) (*Result, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, fmt.Errorf("index: 引擎已关闭")
	}
	if opts == nil {
		opts = &SearchOptions{}
	}
	if err := e.validateSortLocked(opts.Sort); err != nil {
		return nil, err
	}
	if root == nil {
		root = plugin.MatchAllNode{}
	}

	cands, err := e.evalLocked(root, true)
	if err != nil {
		return nil, err
	}

	// 应用删除标记
	out := make([]*candidate, 0, len(cands))
	for _, c := range cands {
		if !e.deletedLocked(c.segIdx, c.loc) {
			out = append(out, c)
		}
	}

	e.sortCandidatesLocked(out, opts.Sort)

	// 聚合作用于全部命中文档（分页之前）
	aggRes, err := e.runAggsLocked(out, opts.Aggs, opts.AggPartial)
	if err != nil {
		return nil, err
	}

	// 分页
	total := len(out)
	from := opts.From
	if from < 0 {
		from = 0
	}
	size := opts.Size
	if size <= 0 {
		size = 10
	}
	if from > total {
		from = total
	}
	end := from + size
	if end > total {
		end = total
	}

	res := &Result{Total: total, Hits: make([]Hit, 0, end-from), Aggs: aggRes}
	for _, c := range out[from:end] {
		id, source, err := e.loadDocLocked(c.segIdx, c.loc)
		if err != nil {
			return nil, err
		}
		res.Hits = append(res.Hits, Hit{ID: id, Score: c.score, Source: source})
	}
	return res, nil
}

// runAggsLocked 对全部命中文档执行局部聚合并输出结果
func (e *Engine) runAggsLocked(out []*candidate, aggs map[string]plugin.Aggregator, partial bool) (map[string]json.RawMessage, error) {
	if len(aggs) == 0 {
		return nil, nil
	}
	for _, c := range out {
		for _, ag := range aggs {
			field := ag.Field()
			switch ag.Kind() {
			case plugin.AggNum:
				if v, ok := e.numValLocked(field, c.segIdx, c.loc); ok {
					ag.CollectNum(v)
				}
			case plugin.AggString:
				if s, ok, err := e.stringValLocked(field, c.segIdx, c.loc); err != nil {
					return nil, err
				} else if ok {
					ag.CollectString(s)
				}
			}
		}
	}
	res := make(map[string]json.RawMessage, len(aggs))
	for name, ag := range aggs {
		if partial {
			// 协调层模式：输出可合并的中间结果
			b, err := ag.Marshal()
			if err != nil {
				return nil, fmt.Errorf("index: 聚合 %q 序列化中间结果失败: %w", name, err)
			}
			res[name] = b
		} else {
			res[name] = ag.Result()
		}
	}
	return res, nil
}

// stringValLocked 从原文中提取字段的字符串值（keyword 聚合用）。
// JSON 字符串去引号，其他类型取其 JSON 文本；字段缺失返回 false。
func (e *Engine) stringValLocked(field string, segIdx int, loc uint32) (string, bool, error) {
	_, raw, err := e.loadDocLocked(segIdx, loc)
	if err != nil {
		return "", false, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", false, fmt.Errorf("index: 原文不是合法 JSON 对象: %w", err)
	}
	fv, ok := m[field]
	if !ok {
		return "", false, nil
	}
	var s string
	if err := json.Unmarshal(fv, &s); err == nil {
		return s, true, nil
	}
	return string(fv), true, nil
}

// validateSortLocked 校验排序字段：必须是建了正排的字段或 "_score"
func (e *Engine) validateSortLocked(sorts []SortField) error {
	for _, s := range sorts {
		if s.Field == "_score" {
			continue
		}
		f, ok := e.schema.Field(s.Field)
		if !ok {
			return fmt.Errorf("index: 排序字段 %q 不存在", s.Field)
		}
		p, err := plugin.GetFieldType(f.Type)
		if err != nil {
			return err
		}
		if !p.DocValues() {
			return fmt.Errorf("index: 字段 %q 类型 %s 不支持排序", s.Field, f.Type)
		}
	}
	return nil
}

// sortCandidatesLocked 排序候选：按 Sort 依次比较，同分时按 _score desc、ID 升序兜底。
// 以 ID 升序兜底是为了让结果顺序与段合并无关（合并前后对拍一致）。
func (e *Engine) sortCandidatesLocked(out []*candidate, sorts []SortField) {
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		for _, s := range sorts {
			if s.Field == "_score" {
				if a.score != b.score {
					return (a.score > b.score) == s.Desc
				}
				continue
			}
			va, oka := e.numValLocked(s.Field, a.segIdx, a.loc)
			vb, okb := e.numValLocked(s.Field, b.segIdx, b.loc)
			// 字段缺失的恒排在最后
			if oka != okb {
				return oka
			}
			if oka && va != vb {
				return (va > vb) == s.Desc
			}
		}
		// 未指定排序时默认 _score desc
		if len(sorts) == 0 && a.score != b.score {
			return a.score > b.score
		}
		return e.idOfLocked(a.segIdx, a.loc) < e.idOfLocked(b.segIdx, b.loc)
	})
}

// numValLocked 读取候选的正排字段值；字段或值缺失返回 false
func (e *Engine) numValLocked(field string, segIdx int, loc uint32) (int64, bool) {
	if segIdx < 0 {
		return e.buf.num(field, loc)
	}
	return e.segs[segIdx].Num(field, loc)
}

// idOfLocked 读取候选的外部 ID（排序兜底用）
func (e *Engine) idOfLocked(segIdx int, loc uint32) string {
	if segIdx < 0 {
		return e.buf.docs[loc].ID
	}
	return e.segs[segIdx].ID(loc)
}

// srcKey 把 (segIdx, loc) 编码为 map key：缓冲占最高位
func srcKey(segIdx int, loc uint32) uint64 {
	return uint64(uint32(segIdx)+1)<<32 | uint64(loc)
}

// ---------- 查询树求值 ----------

// evalLocked 执行查询树节点；scoring=false 时命中得分恒为 1（filter 语义）
func (e *Engine) evalLocked(n plugin.QNode, scoring bool) (map[uint64]*candidate, error) {
	switch node := n.(type) {
	case plugin.MatchAllNode:
		return e.matchAllLocked(), nil
	case plugin.MatchNode:
		return e.evalMatchLocked(node, scoring)
	case plugin.TermNode:
		return e.collectTermLocked(node.Field, node.Term, 1), nil
	case plugin.RangeNode:
		return e.evalRangeLocked(node), nil
	case plugin.IDsNode:
		return e.evalIDsLocked(node), nil
	case plugin.BoolNode:
		return e.evalBoolLocked(node, scoring)
	default:
		return nil, fmt.Errorf("index: 未知查询节点类型 %T", n)
	}
}

// evalMatchLocked 分词匹配：用字段自己的分词器切分查询串，按 operator 组合；
// 有 norms 的字段用打分器打分，否则恒定 1 分；scoring=false 时恒定 1 分。
func (e *Engine) evalMatchLocked(node plugin.MatchNode, scoring bool) (map[uint64]*candidate, error) {
	f, ok := e.schema.Field(node.Field)
	if !ok {
		return nil, fmt.Errorf("index: 字段 %q 不存在", node.Field)
	}
	fp, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return nil, err
	}
	if !fp.Inverted() {
		return nil, fmt.Errorf("index: 字段 %q 类型 %s 不支持倒排检索", node.Field, f.Type)
	}
	if node.Operator != "" && node.Operator != "or" && node.Operator != "and" {
		return nil, fmt.Errorf("index: operator %q 非法，仅支持 or/and", node.Operator)
	}
	a, err := plugin.GetAnalyzer(fp.Analyzer())
	if err != nil {
		return nil, err
	}
	terms := a.Tokenize(node.Text)
	if len(terms) == 0 {
		return map[uint64]*candidate{}, nil
	}

	// 打分器：有 norms 且需要打分时从注册表取
	var scorer plugin.Scorer
	var n int
	var avgdl float64
	if scoring && fp.HasNorms() {
		name := node.Scorer
		if name == "" {
			name = defaultScorer
		}
		scorer, err = plugin.GetScorer(name)
		if err != nil {
			return nil, err
		}
		n, avgdl = e.bm25StatsLocked(node.Field)
	}

	merged := map[uint64]*candidate{}
	for ti, term := range terms {
		cur := map[uint64]*candidate{}
		df := 0
		if scorer != nil {
			df = e.docFreqLocked(node.Field, term)
		}
		add := func(segIdx int, docID uint32, tf, dl int64) {
			score := 1.0
			if scorer != nil {
				score = scorer.Score(tf, dl, df, n, avgdl)
			}
			cur[srcKey(segIdx, docID)] = &candidate{segIdx: segIdx, loc: docID, score: score}
		}
		// 缓冲
		if plist, ok := e.buf.postings(node.Field, term); ok {
			for _, p := range plist {
				add(-1, p.docID, int64(p.tf), e.buf.docLen(node.Field, p.docID))
			}
		}
		// 各段
		for si, seg := range e.segs {
			it, ok := seg.Postings(node.Field, term)
			if !ok {
				continue
			}
			for it.Next() {
				add(si, it.DocID(), int64(it.Freq()), seg.Norm(node.Field, it.DocID()))
			}
		}
		if ti == 0 || node.Operator != "and" {
			// 首个 term 直接并入；OR（默认）：并集，score 累加
			for k, c := range cur {
				if mc, ok := merged[k]; ok {
					mc.score += c.score
				} else {
					merged[k] = c
				}
			}
			continue
		}
		// AND：与已有结果求交，并累加 score
		for k, c := range merged {
			nc, ok := cur[k]
			if !ok {
				delete(merged, k)
				continue
			}
			c.score += nc.score
		}
	}
	return merged, nil
}

// evalRangeLocked 范围过滤：扫描全部文档的正排值（字段缺失的文档不匹配，ES 语义）
func (e *Engine) evalRangeLocked(node plugin.RangeNode) map[uint64]*candidate {
	out := map[uint64]*candidate{}
	scan := func(segIdx int, loc uint32) {
		v, ok := e.numValLocked(node.Field, segIdx, loc)
		if !ok {
			return
		}
		if node.HasMin && v < node.Min {
			return
		}
		if node.HasMax && v > node.Max {
			return
		}
		out[srcKey(segIdx, loc)] = &candidate{segIdx: segIdx, loc: loc, score: 1}
	}
	for loc := range e.buf.docs {
		scan(-1, uint32(loc))
	}
	for si, seg := range e.segs {
		for loc := 0; loc < seg.DocCount(); loc++ {
			scan(si, uint32(loc))
		}
	}
	return out
}

// evalIDsLocked 按外部 ID 精确命中
func (e *Engine) evalIDsLocked(node plugin.IDsNode) map[uint64]*candidate {
	out := map[uint64]*candidate{}
	for _, id := range node.IDs {
		if loc, ok := e.buf.id2loc[id]; ok {
			out[srcKey(-1, loc)] = &candidate{segIdx: -1, loc: loc, score: 1}
		}
		for si, seg := range e.segs {
			if loc, ok := seg.LocalID(id); ok {
				out[srcKey(si, loc)] = &candidate{segIdx: si, loc: loc, score: 1}
			}
		}
	}
	return out
}

// evalBoolLocked 组合子句：must/filter 求交（filter 不打分），should 并集加分，
// must_not 排除；无 must/filter 时至少命中一个 should；全部为空时匹配全部。
func (e *Engine) evalBoolLocked(node plugin.BoolNode, scoring bool) (map[uint64]*candidate, error) {
	var merged map[uint64]*candidate
	intersect := func(cur map[uint64]*candidate, addScore bool) {
		if merged == nil {
			merged = map[uint64]*candidate{}
			for k, c := range cur {
				merged[k] = c
			}
			return
		}
		for k, c := range merged {
			nc, ok := cur[k]
			if !ok {
				delete(merged, k)
				continue
			}
			if addScore {
				c.score += nc.score
			}
		}
	}

	for _, sub := range node.Must {
		cur, err := e.evalLocked(sub, scoring)
		if err != nil {
			return nil, err
		}
		intersect(cur, scoring)
	}
	for _, sub := range node.Filter {
		cur, err := e.evalLocked(sub, false)
		if err != nil {
			return nil, err
		}
		intersect(cur, false)
	}

	// should
	shoulds := make([]map[uint64]*candidate, 0, len(node.Should))
	for _, sub := range node.Should {
		cur, err := e.evalLocked(sub, scoring)
		if err != nil {
			return nil, err
		}
		shoulds = append(shoulds, cur)
	}
	if merged == nil {
		// 无 must/filter：至少命中一个 should；should 也为空时匹配全部
		if len(shoulds) == 0 {
			merged = e.matchAllLocked()
		} else {
			merged = map[uint64]*candidate{}
			for _, cur := range shoulds {
				for k, c := range cur {
					if mc, ok := merged[k]; ok {
						mc.score += c.score
					} else {
						merged[k] = c
					}
				}
			}
		}
	} else {
		// 有 must/filter：should 只加分
		for _, cur := range shoulds {
			for k, c := range cur {
				if mc, ok := merged[k]; ok {
					mc.score += c.score
				}
			}
		}
	}

	// must_not：排除
	for _, sub := range node.MustNot {
		cur, err := e.evalLocked(sub, false)
		if err != nil {
			return nil, err
		}
		for k := range cur {
			delete(merged, k)
		}
	}
	return merged, nil
}

// bm25StatsLocked 汇总 norms 字段在缓冲与各段上的全局统计：N（存活文档数）与
// 全局 avgdl（按各段/缓冲文档数加权）。
// 注：段的 df 统计（DocFreq）包含已删除文档，删除较多的段上 IDF 略有偏差，
// 段合并后自然消除；avgdl 同理按构建期文档数加权。
func (e *Engine) bm25StatsLocked(field string) (n int, avgdl float64) {
	var totalDocs, totalLen float64
	n = len(e.buf.docs) - len(e.buf.deleted)
	totalDocs = float64(len(e.buf.docs))
	totalLen = e.buf.avgDL(field) * float64(len(e.buf.docs))
	for _, seg := range e.segs {
		n += seg.LiveCount()
		totalDocs += float64(seg.DocCount())
		totalLen += seg.AvgDL(field) * float64(seg.DocCount())
	}
	if totalDocs > 0 {
		avgdl = totalLen / totalDocs
	}
	return n, avgdl
}

// docFreqLocked 汇总 term 在缓冲（仅存活）与各段（含已删除，见上注）上的 df
func (e *Engine) docFreqLocked(field, term string) int {
	df := e.buf.liveDF(field, term)
	for _, seg := range e.segs {
		df += seg.DocFreq(field, term)
	}
	return df
}

// collectTermLocked 收集单个 term 的全部命中，score 固定为 s
func (e *Engine) collectTermLocked(field, term string, s float64) map[uint64]*candidate {
	out := map[uint64]*candidate{}
	if plist, ok := e.buf.postings(field, term); ok {
		for _, p := range plist {
			out[srcKey(-1, p.docID)] = &candidate{segIdx: -1, loc: p.docID, score: s}
		}
	}
	for si, seg := range e.segs {
		it, ok := seg.Postings(field, term)
		if !ok {
			continue
		}
		for it.Next() {
			out[srcKey(si, it.DocID())] = &candidate{segIdx: si, loc: it.DocID(), score: s}
		}
	}
	return out
}

// matchAllLocked 枚举全部文档（score 恒为 1）
func (e *Engine) matchAllLocked() map[uint64]*candidate {
	out := map[uint64]*candidate{}
	for loc := range e.buf.docs {
		out[srcKey(-1, uint32(loc))] = &candidate{segIdx: -1, loc: uint32(loc), score: 1}
	}
	for si, seg := range e.segs {
		for loc := 0; loc < seg.DocCount(); loc++ {
			out[srcKey(si, uint32(loc))] = &candidate{segIdx: si, loc: uint32(loc), score: 1}
		}
	}
	return out
}

// deletedLocked 判断候选是否已被删除
func (e *Engine) deletedLocked(segIdx int, loc uint32) bool {
	if segIdx < 0 {
		return e.buf.deleted[loc]
	}
	return e.segs[segIdx].Deleted(loc)
}

// loadDocLocked 读取候选的外部 ID 与原文
func (e *Engine) loadDocLocked(segIdx int, loc uint32) (string, json.RawMessage, error) {
	if segIdx < 0 {
		d := e.buf.docs[loc]
		return d.ID, d.Raw, nil
	}
	seg := e.segs[segIdx]
	raw, err := seg.Stored(loc)
	if err != nil {
		return "", nil, err
	}
	return seg.ID(loc), raw, nil
}
