package index

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/posting"
	"github.com/FalconEngine/falcon/schema"
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
	// Sort 分片侧填充的实际排序键序列（与请求 sort 一一对应）：
	// number/date/bool→int64 JSON，keyword→字符串 JSON，_score→float JSON，缺失→null；
	// 协调层据此跨分片归并（不再从 _source 猜测排序值），无排序字段时为 nil
	Sort []json.RawMessage `json:"sort,omitempty"`
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
	sortKinds, err := e.validateSortLocked(opts.Sort)
	if err != nil {
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

	e.sortCandidatesLocked(out, opts.Sort, sortKinds)

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
		res.Hits = append(res.Hits, Hit{
			ID: id, Score: c.score, Source: source,
			Sort: e.hitSortKeysLocked(c, opts.Sort, sortKinds),
		})
	}
	return res, nil
}

// runAggsLocked 对全部命中文档执行局部聚合并输出结果
func (e *Engine) runAggsLocked(out []*candidate, aggs map[string]plugin.Aggregator, partial bool) (map[string]json.RawMessage, error) {
	if len(aggs) == 0 {
		return nil, nil
	}
	// 预解析各字符串聚合的取值路径：DVKeyword 字段走 kw 列/Kws（免原文 JSON 解析），
	// 无 kw 列的字段回落 stored 原文解析
	kwField := make(map[string]bool, len(aggs))
	for _, ag := range aggs {
		if ag.Kind() == plugin.AggString {
			kwField[ag.Field()] = e.dvKindOfLocked(ag.Field()) == plugin.DVKeyword
		}
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
				var s string
				var ok bool
				var err error
				if kwField[field] {
					s, ok, err = e.kwAggValLocked(field, c.segIdx, c.loc)
				} else {
					s, ok, err = e.stringValLocked(field, c.segIdx, c.loc)
				}
				if err != nil {
					return nil, err
				}
				if ok {
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

// kwAggValLocked keyword dv 字段的聚合取值：优先走 kw 列/Kws（免原文解析）；
// 段无 kw 列时（v1 老段、字段后加导致老段缺索引文件）回落原文 JSON 解析
// （慢但语义一致，与 exists 查询的 v1 回落同一策略）。
// 空串与 kw 列"空串即无值"语义一致，两条路径都视为缺失。
func (e *Engine) kwAggValLocked(field string, segIdx int, loc uint32) (string, bool, error) {
	if segIdx >= 0 {
		if _, hasKw := e.segs[segIdx].Kw(field); !hasKw {
			s, ok, err := e.stringValLocked(field, segIdx, loc)
			if err != nil || !ok || s == "" {
				return "", false, err
			}
			return s, true, nil
		}
	}
	s, ok := e.kwValLocked(field, segIdx, loc)
	return s, ok, nil
}

// stringValLocked 从原文中提取字段的字符串值（字符串聚合的回落取值路径：
// 无 kw 列的字段或段）。JSON 字符串去引号，其他类型取其 JSON 文本；字段缺失返回 false。
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

// validateSortLocked 校验排序字段并返回各字段的正排类型（"_score" 位为 DVNone）。
// keyword dv 字段额外要求各段都有 kw 列可读：v1 老段无 kw 列且排序器无法回落
// 原文解析，报带 _flush/merge 指引的错误（同 match_phrase 的 v1 处理策略）；
// v2 段缺个别字段的 kw 文件（字段后加）按"字段缺失"处理，merge 后自愈。
func (e *Engine) validateSortLocked(sorts []SortField) ([]plugin.DVKind, error) {
	kinds, err := validateSortFields(e.schema, sorts)
	if err != nil {
		return nil, err
	}
	for i, s := range sorts {
		if kinds[i] != plugin.DVKeyword {
			continue
		}
		for _, seg := range e.segs {
			if seg.Version() < 2 {
				return nil, fmt.Errorf("index: 段 %s 为 v1 格式不含 kw 列，无法按 keyword 字段 %q 排序，请执行 _flush 或等待 merge 重建", filepath.Base(seg.Dir()), s.Field)
			}
		}
	}
	return kinds, nil
}

// validateSortFields 按 schema 校验排序字段（协调层无本地段时同样适用）：
// 字段须存在且建了正排列（number 定长列或 keyword ord 列）或为 "_score"；
// text 本体不支持排序——本引擎明确不做 fielddata，引导改用 keyword 子字段。
// 返回各字段的正排类型（"_score" 位为 DVNone）。
func validateSortFields(sch *schema.Schema, sorts []SortField) ([]plugin.DVKind, error) {
	kinds := make([]plugin.DVKind, len(sorts))
	for i, s := range sorts {
		if s.Field == "_score" {
			continue
		}
		f, ok := sch.Field(s.Field)
		if !ok {
			return nil, fmt.Errorf("index: 排序字段 %q 不存在", s.Field)
		}
		p, err := plugin.GetFieldType(f.Type)
		if err != nil {
			return nil, err
		}
		switch p.DocValuesKind() {
		case plugin.DVNum, plugin.DVKeyword:
			kinds[i] = p.DocValuesKind()
		default:
			if p.Inverted() {
				return nil, fmt.Errorf("index: text 字段 %q 不支持排序，请使用其 keyword 子字段（如 %q）；本引擎不做 fielddata", s.Field, s.Field+".keyword")
			}
			return nil, fmt.Errorf("index: 字段 %q 类型 %s 不支持排序", s.Field, f.Type)
		}
	}
	return kinds, nil
}

// sortCandidatesLocked 排序候选：按 Sort 依次比较，同分时按 _score desc、ID 升序兜底。
// 以 ID 升序兜底是为了让结果顺序与段合并无关（合并前后对拍一致）。
// keyword dv 字段比较 term 字符串本身：kw 列的 ord 是每段局部的字典序号，
// 跨段（段与段、段与缓冲）比 ord 无意义，只能比 term 字符串。
func (e *Engine) sortCandidatesLocked(out []*candidate, sorts []SortField, kinds []plugin.DVKind) {
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		for k, s := range sorts {
			switch kinds[k] {
			case plugin.DVNum:
				va, oka := e.numValLocked(s.Field, a.segIdx, a.loc)
				vb, okb := e.numValLocked(s.Field, b.segIdx, b.loc)
				// 字段缺失的恒排在最后
				if oka != okb {
					return oka
				}
				if oka && va != vb {
					return (va > vb) == s.Desc
				}
			case plugin.DVKeyword:
				sa, oka := e.kwValLocked(s.Field, a.segIdx, a.loc)
				sb, okb := e.kwValLocked(s.Field, b.segIdx, b.loc)
				if oka != okb {
					return oka
				}
				if oka && sa != sb {
					return (sa > sb) == s.Desc
				}
			default: // "_score"
				if a.score != b.score {
					return (a.score > b.score) == s.Desc
				}
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

// kwValLocked 读取候选的 keyword dv 值（term 字符串）；字段或值缺失返回 false
func (e *Engine) kwValLocked(field string, segIdx int, loc uint32) (string, bool) {
	if segIdx < 0 {
		return e.buf.kw(field, loc)
	}
	kr, ok := e.segs[segIdx].Kw(field)
	if !ok {
		return "", false
	}
	ord, ok := kr.Ord(loc)
	if !ok {
		return "", false
	}
	return kr.Term(ord), true
}

// dvKindOfLocked 返回字段的正排类型；字段不存在或类型未注册返回 DVNone
func (e *Engine) dvKindOfLocked(field string) plugin.DVKind {
	f, ok := e.schema.Field(field)
	if !ok {
		return plugin.DVNone
	}
	p, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return plugin.DVNone
	}
	return p.DocValuesKind()
}

// hitSortKeysLocked 生成命中的排序键序列（协调层跨分片归并用，见 Hit.Sort）；
// 无排序字段时返回 nil
func (e *Engine) hitSortKeysLocked(c *candidate, sorts []SortField, kinds []plugin.DVKind) []json.RawMessage {
	if len(sorts) == 0 {
		return nil
	}
	keys := make([]json.RawMessage, len(sorts))
	for i, s := range sorts {
		var v any
		switch kinds[i] {
		case plugin.DVNum:
			if n, ok := e.numValLocked(s.Field, c.segIdx, c.loc); ok {
				v = n
			}
		case plugin.DVKeyword:
			if kw, ok := e.kwValLocked(s.Field, c.segIdx, c.loc); ok {
				v = kw
			}
		default: // "_score"
			v = c.score
		}
		if v == nil {
			keys[i] = json.RawMessage("null")
		} else {
			keys[i], _ = json.Marshal(v)
		}
	}
	return keys
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
	case plugin.MultiTermNode:
		return e.evalMultiTermLocked(node), nil
	case plugin.ExistsNode:
		return e.evalExistsLocked(node)
	case plugin.PhraseNode:
		return e.evalPhraseLocked(node, scoring)
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
	a, err := analyzerOf(f)
	if err != nil {
		return nil, err
	}
	toks := a.Analyze(node.Text)
	if len(toks) == 0 {
		return map[uint64]*candidate{}, nil
	}
	terms := make([]string, len(toks))
	for i, t := range toks {
		terms[i] = t.Term
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
	// 查询时加权（multi_match 的 field^boost 展开用）；零值/负值兜底为 1
	boost := node.Boost
	if boost <= 0 {
		boost = 1
	}
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
			cur[srcKey(segIdx, docID)] = &candidate{segIdx: segIdx, loc: docID, score: score * boost}
		}
		// 缓冲
		if plist, ok := e.buf.postings(node.Field, term); ok {
			for _, p := range plist {
				add(-1, p.docID, int64(len(p.positions)), e.buf.docLen(node.Field, p.docID))
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

// evalPhraseLocked 短语匹配（match_phrase，本期仅 Slop=0）：
// 查询串经字段分词器分析为带位置的 term 序列，各 term 取相对首个 term 的位置偏移
// （token filter 删除停用词会造成跳号，索引侧与查询侧走同一分析链、偏移同构）；
// 以全局 docFreq 最小的 term 为主驱动，其余 term 用 Advance 对齐 docID（缓冲侧直接
// map 查）；共同命中的 doc 再验证存在起始位置 p 使第 i 个 term 出现在 p+offs[i]。
// 打分简化为各 term BM25 得分之和，短语整体词频不参与——与 ES 的 phrase scorer
// 有差异（ES 以短语出现次数计 tf），此处为简化实现。
func (e *Engine) evalPhraseLocked(node plugin.PhraseNode, scoring bool) (map[uint64]*candidate, error) {
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
	if node.Slop != 0 {
		return nil, fmt.Errorf("index: match_phrase 的 slop>0 暂未支持")
	}
	a, err := analyzerOf(f)
	if err != nil {
		return nil, err
	}
	toks := a.Analyze(node.Text)
	if len(toks) == 0 {
		return map[uint64]*candidate{}, nil
	}
	terms := make([]string, len(toks))
	// 各 term 相对首个 term 的位置偏移（停用词跳号两侧同构，见函数注释）
	offs := make([]uint32, len(toks))
	for i, t := range toks {
		terms[i] = t.Term
		offs[i] = uint32(t.Position - toks[0].Position)
	}

	// 打分器：有 norms 且需要打分时启用（同 match）
	var scorer plugin.Scorer
	var n int
	var avgdl float64
	if scoring && fp.HasNorms() {
		scorer, err = plugin.GetScorer(defaultScorer)
		if err != nil {
			return nil, err
		}
		n, avgdl = e.bm25StatsLocked(node.Field)
	}

	// 各 term 的全局 df：既用于打分，也用于选主驱动
	dfs := make([]int, len(terms))
	for i, t := range terms {
		dfs[i] = e.docFreqLocked(node.Field, t)
	}
	// 主驱动取 df 最小的 term；df=0 则全局无解
	driver := 0
	for i := 1; i < len(terms); i++ {
		if dfs[i] < dfs[driver] {
			driver = i
		}
	}
	out := map[uint64]*candidate{}
	if dfs[driver] == 0 {
		return out, nil
	}

	// 命中后的得分：各 term BM25 之和；无打分器时恒 1
	scoreOf := func(tfs []int64, dl int64) float64 {
		if scorer == nil {
			return 1
		}
		s := 0.0
		for i := range terms {
			s += scorer.Score(tfs[i], dl, dfs[i], n, avgdl)
		}
		return s
	}

	// 缓冲侧：直接 map 查，逐 doc 验证位置
	bufLists := make([][]bufPosting, len(terms))
	bufOK := true
	for i, t := range terms {
		l, ok := e.buf.postings(node.Field, t)
		if !ok {
			bufOK = false
			break
		}
		bufLists[i] = l
	}
	if bufOK {
		posLists := make([][]uint32, len(terms))
		tfs := make([]int64, len(terms))
		for _, dp := range bufLists[driver] {
			posLists[driver] = dp.positions
			tfs[driver] = int64(len(dp.positions))
			aligned := true
			for i := range terms {
				if i == driver {
					continue
				}
				bp, found := findBufPosting(bufLists[i], dp.docID)
				if !found {
					aligned = false
					break
				}
				posLists[i] = bp.positions
				tfs[i] = int64(len(bp.positions))
			}
			if !aligned || !phraseMatchAt(posLists, driver, offs) {
				continue
			}
			out[srcKey(-1, dp.docID)] = &candidate{segIdx: -1, loc: dp.docID,
				score: scoreOf(tfs, e.buf.docLen(node.Field, dp.docID))}
		}
	}

	// 段侧：主驱动 Next 遍历，其余 term Advance 对齐
	for si, seg := range e.segs {
		its := make([]posting.Iterator, len(terms))
		segOK := true
		for i, t := range terms {
			it, ok := seg.Postings(node.Field, t)
			if !ok { // 某 term 整段缺失 → 该段不可能有短语命中
				segOK = false
				break
			}
			its[i] = it
		}
		if !segOK {
			continue
		}
		alignedDoc := make([]uint32, len(terms)) // 各 term 迭代器当前所在的 docID
		hasCur := make([]bool, len(terms))
		posLists := make([][]uint32, len(terms))
		tfs := make([]int64, len(terms))
		for its[driver].Next() {
			docID := its[driver].DocID()
			posLists[driver] = its[driver].Positions()
			if posLists[driver] == nil {
				return nil, fmt.Errorf("index: 段 %s 为 v1 格式不含 positions，无法执行 match_phrase，请执行 _flush 或等待 merge 重建", filepath.Base(seg.Dir()))
			}
			tfs[driver] = int64(its[driver].Freq())
			aligned := true
			for i := range terms {
				if i == driver {
					continue
				}
				// Advance 语义是严格越过当前 doc：已停在 docID 上时不能再 Advance
				if hasCur[i] && alignedDoc[i] >= docID {
					if alignedDoc[i] != docID {
						aligned = false
						break
					}
				} else {
					if !its[i].Advance(docID) || its[i].DocID() != docID {
						aligned = false
						break
					}
					alignedDoc[i], hasCur[i] = docID, true
				}
				posLists[i] = its[i].Positions() // 与 driver 同段同版本，必然非 nil
				tfs[i] = int64(its[i].Freq())
			}
			if !aligned || !phraseMatchAt(posLists, driver, offs) {
				continue
			}
			out[srcKey(si, docID)] = &candidate{segIdx: si, loc: docID,
				score: scoreOf(tfs, seg.Norm(node.Field, docID))}
		}
	}
	return out, nil
}

// findBufPosting 在缓冲倒排链（docID 递增）中二分定位 docID
func findBufPosting(list []bufPosting, docID uint32) (bufPosting, bool) {
	i := sort.Search(len(list), func(k int) bool { return list[k].docID >= docID })
	if i < len(list) && list[i].docID == docID {
		return list[i], true
	}
	return bufPosting{}, false
}

// phraseMatchAt 验证 posLists（各 term 在同一 doc 内的位置序列）是否构成短语：
// 存在起始位置 p 使第 i 个 term 出现在 p+offs[i]（p 取主驱动 term 的出现位置；
// offs 为查询侧各 term 相对偏移，停用词跳号时相邻 term 偏移可大于 1）
func phraseMatchAt(posLists [][]uint32, driver int, offs []uint32) bool {
	for _, p0 := range posLists[driver] {
		base := int64(p0) - int64(offs[driver])
		if base < 0 {
			continue
		}
		okAll := true
		for i, ps := range posLists {
			if i == driver {
				continue
			}
			if !containsPos(ps, uint32(base+int64(offs[i]))) {
				okAll = false
				break
			}
		}
		if okAll {
			return true
		}
	}
	return false
}

// containsPos 判断升序位置序列中是否含有 p
func containsPos(ps []uint32, p uint32) bool {
	i := sort.Search(len(ps), func(k int) bool { return ps[k] >= p })
	return i < len(ps) && ps[i] == p
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

// evalMultiTermLocked 多 term 并集（prefix/wildcard/fuzzy 展开产物）：
// 每个 term 复用 collectTermLocked 合并缓冲与各段，恒定 score=1（filter 语义）
func (e *Engine) evalMultiTermLocked(node plugin.MultiTermNode) map[uint64]*candidate {
	out := map[uint64]*candidate{}
	for _, term := range node.Terms {
		for k, c := range e.collectTermLocked(node.Field, term, 1) {
			out[k] = c
		}
	}
	return out
}

// expandTerms 供 ParseContext.ExpandTerms：解析期展开 multi-term 查询的词典。
// 自持读锁——DSL 解析发生在 Search 加锁之前，展开结果（term 字符串集合）在段合并
// 前后保持有效（merge 从原文重解析不丢 term），执行时按当时快照收集倒排即可。
func (e *Engine) expandTerms(field, prefix string, match func(string) bool, maxExpansions int) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil
	}
	return e.expandTermsLocked(field, prefix, match, maxExpansions)
}

// expandTermsLocked 合并缓冲内存 term map 与各段词典的枚举结果：去重、字典序、
// 最多 maxExpansions 个（超出截断字典序最小者；词典扫描为 O(词典)，不承诺大词典性能）。
// 各段按字典序各取前 maxExpansions 个即不丢全局前 maxExpansions 候选
// （若某候选不在其所在段的前 maxExpansions 内，则该段已有 maxExpansions 个更小候选）。
func (e *Engine) expandTermsLocked(field, prefix string, match func(string) bool, maxExpansions int) []string {
	if maxExpansions <= 0 {
		return nil
	}
	set := make(map[string]struct{})
	// 缓冲侧：直接遍历内存 term map（规模受 flush 阈值约束，全收不截断）
	if terms, ok := e.buf.inv[field]; ok {
		for term := range terms {
			if prefix != "" && !strings.HasPrefix(term, prefix) {
				continue
			}
			if match != nil && !match(term) {
				continue
			}
			set[term] = struct{}{}
		}
	}
	// 段侧：有序字典扫描（prefix 非空时借稀疏索引缩范围）
	for _, seg := range e.segs {
		for _, term := range seg.Terms(field, prefix, match, maxExpansions) {
			set[term] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for term := range set {
		out = append(out, term)
	}
	sort.Strings(out)
	if len(out) > maxExpansions {
		out = out[:maxExpansions]
	}
	return out
}

// evalExistsLocked 字段存在性过滤：缓冲侧直接查文档解析产物（与段 has 位同一套语义，
// 见 segment.Doc.HasField）；段侧优先走 has 位（v2 覆盖全部字段，v1 仅 number 类），
// 字段无 has 位时（v1 段、stored 类无索引结构字段、后加字段的老段）回落原文 JSON 检查
// （慢但语义一致）。空串 text 分词后 0 token、null 均视为不存在（对齐 ES）。
func (e *Engine) evalExistsLocked(node plugin.ExistsNode) (map[uint64]*candidate, error) {
	f, ok := e.schema.Field(node.Field)
	if !ok {
		return nil, fmt.Errorf("index: 字段 %q 不存在", node.Field)
	}
	fp, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return nil, err
	}
	// 有索引结构（倒排/正排）的字段可查解析产物；stored 类字段只能查原文
	indexed := fp.Inverted() || fp.DocValuesKind() != plugin.DVNone

	out := map[uint64]*candidate{}
	add := func(segIdx int, loc uint32) {
		out[srcKey(segIdx, loc)] = &candidate{segIdx: segIdx, loc: loc, score: 1}
	}
	for loc := range e.buf.docs {
		d := &e.buf.docs[loc]
		if indexed {
			if d.HasField(node.Field) {
				add(-1, uint32(loc))
			}
			continue
		}
		if ok, err := e.rawFieldExistsLocked(f, d.Raw); err != nil {
			return nil, err
		} else if ok {
			add(-1, uint32(loc))
		}
	}
	for si, seg := range e.segs {
		hasBits := indexed && seg.HasField(node.Field)
		for loc := 0; loc < seg.DocCount(); loc++ {
			if hasBits {
				if seg.Has(node.Field, uint32(loc)) {
					add(si, uint32(loc))
				}
				continue
			}
			raw, err := seg.Stored(uint32(loc))
			if err != nil {
				return nil, err
			}
			if ok, err := e.rawFieldExistsLocked(f, raw); err != nil {
				return nil, err
			} else if ok {
				add(si, uint32(loc))
			}
		}
	}
	return out, nil
}

// rawFieldExistsLocked 从原文 JSON 判断字段存在性（无 has 位时的回落路径）：
// 嵌套对象按 flattenDoc 展开、multi-field 子字段取父路径值（与 parseDocFields 一致）；
// null 不可解析为字段值视为不存在；text 经分词后 0 token（如空串）视为不存在
func (e *Engine) rawFieldExistsLocked(f schema.Field, raw json.RawMessage) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false, fmt.Errorf("index: 原文不是合法 JSON 对象: %w", err)
	}
	flat := flattenDoc(fields)
	fv, ok := flat[f.Name]
	if !ok {
		if i := strings.LastIndex(f.Name, "."); i >= 0 {
			fv, ok = flat[f.Name[:i]]
		}
		if !ok {
			return false, nil
		}
	}
	if string(fv) == "null" {
		return false, nil
	}
	p, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return false, err
	}
	pv, err := p.Parse(fv)
	if err != nil {
		// 值无法解析为该字段类型（如 number 字段上的字符串），视为不存在
		return false, nil
	}
	if p.Inverted() {
		a, err := analyzerOf(f)
		if err != nil {
			return false, err
		}
		return len(a.Analyze(pv.Text)) > 0, nil
	}
	if p.DocValuesKind() == plugin.DVKeyword {
		// keyword 空串与 kw 列"空串即无值"语义一致，视为不存在
		return pv.Text != "", nil
	}
	// number 类 Parse 成功即有值；stored 类字段原文出现且非 null 即存在
	return true, nil
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
