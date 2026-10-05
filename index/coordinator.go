package index

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
)

// ScatterPlan 跨分片 scatter-gather 的协调计划：
// 解析 DSL 后把分页窗口扩大为 from+size 供各分片执行，
// 最后由 Merge 归并各分片结果（本地或远端经 transport 均可）。
// 节点级协调器（node 包）与索引内归并（Index.SearchDSL）共用本实现。
type ScatterPlan struct {
	From        int // 原始分页参数
	Size        int
	ScatterBody []byte // 分片侧执行的 DSL（from=0, size=from+size）

	sorts    []SortField
	aggSpecs map[string]plugin.AggSpec
	aggPlugs map[string]plugin.AggPlugin
}

// NewScatterPlan 解析 DSL 并生成协调计划。sch 为该索引的 schema（协调节点
// 从集群元数据的 mapping 获得，无需本地分片）。
// 排序字段在此处一并按 schema 校验：非法排序（如 text 本体）在协调层快速失败，
// 否则各分片同样的错误会被 node 层降级语义吞成 partial 空结果（聚合解析天然
// 在本函数内发生，排序校验与之对齐）。
func NewScatterPlan(body []byte, sch *schema.Schema) (*ScatterPlan, error) {
	parsed, err := parseDSLWithSchema(body, sch)
	if err != nil {
		return nil, err
	}
	if _, err := validateSortFields(sch, parsed.opts.Sort); err != nil {
		return nil, err
	}
	from, size := parsed.opts.From, parsed.opts.Size
	if size <= 0 {
		size = 10
	}
	scatterBody, err := rewritePage(body, from+size)
	if err != nil {
		return nil, err
	}
	return &ScatterPlan{
		From: from, Size: size, ScatterBody: scatterBody,
		sorts: parsed.opts.Sort, aggSpecs: parsed.aggSpecs, aggPlugs: parsed.aggPlugs,
	}, nil
}

// Merge 归并各分片（partial 模式）结果：total 求和；hits 按与分片侧一致的
// 比较器归并后按 From/Size 截断；聚合经 Aggregator 两段式合并。
// partial 结果中缺失的聚合中间结果（分片失败降级）跳过不计。
func (p *ScatterPlan) Merge(partials []*Result) (*Result, error) {
	res := &Result{}
	var hits []Hit
	for _, r := range partials {
		res.Total += r.Total
		hits = append(hits, r.Hits...)
	}
	if res.Total == 0 && len(partials) > 0 {
		res.Hits = []Hit{}
	}

	// 归并排序（与分片侧排序器语义一致）：按分片侧上报的排序键（Hit.Sort）
	// 逐位比较——number/date/bool 为 int64 JSON，keyword 为字符串 JSON，
	// 缺失为 null 恒排最后；不再从 _source 猜测排序值（date 等字段在原文中
	// 是字符串，猜测会把有值文档误判为缺失而排到最后）
	sorts := p.sorts
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i], hits[j]
		for k, s := range sorts {
			va, oka := hitSortKey(a, k)
			vb, okb := hitSortKey(b, k)
			if oka != okb {
				return oka
			}
			if oka {
				if c := compareSortKey(va, vb); c != 0 {
					return (c > 0) == s.Desc
				}
			}
		}
		if len(sorts) == 0 && a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.ID < b.ID
	})
	from := p.From
	if from > len(hits) {
		from = len(hits)
	}
	end := from + p.Size
	if end > len(hits) {
		end = len(hits)
	}
	res.Hits = hits[from:end]

	// 聚合：两段式合并
	if len(p.aggSpecs) > 0 {
		res.Aggs = make(map[string]json.RawMessage, len(p.aggSpecs))
		for name, spec := range p.aggSpecs {
			ag := p.aggPlugs[name].New(spec)
			for _, r := range partials {
				part, ok := r.Aggs[name]
				if !ok {
					continue
				}
				if err := ag.Merge(part); err != nil {
					return nil, fmt.Errorf("index: 合并聚合 %q 失败: %w", name, err)
				}
			}
			res.Aggs[name] = ag.Result()
		}
	}
	return res, nil
}

// rewritePage 把 DSL 的分页改为 from=0, size=n（scatter 用）
func rewritePage(body []byte, n int) ([]byte, error) {
	var m map[string]json.RawMessage
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	m["from"], _ = json.Marshal(0)
	m["size"], _ = json.Marshal(n)
	return json.Marshal(m)
}

// hitSortKey 取命中的第 i 个排序键；缺失（键不存在或为 null）返回 false
func hitSortKey(h Hit, i int) (json.RawMessage, bool) {
	if i >= len(h.Sort) {
		return nil, false
	}
	k := h.Sort[i]
	if len(k) == 0 || string(k) == "null" {
		return nil, false
	}
	return k, true
}

// compareSortKey 比较两个排序键（分片侧产出：JSON 字符串或 JSON 数字）：
// 字符串按字典序，数字按数值；返回值符号与 strings.Compare 一致。
// 键类型不一致（跨分片动态推断类型分歧等异常情况）按原文兜底保证确定性。
func compareSortKey(a, b json.RawMessage) int {
	var as, bs string
	aStr := json.Unmarshal(a, &as) == nil
	bStr := json.Unmarshal(b, &bs) == nil
	if aStr && bStr {
		return strings.Compare(as, bs)
	}
	if !aStr && !bStr {
		// 数值比较：int64 排序键（unix 秒、整数）与 float 排序键（_score）
		// 在 float64 精度内均精确（|v| < 2^53）
		an, aErr := strconv.ParseFloat(string(a), 64)
		bn, bErr := strconv.ParseFloat(string(b), 64)
		if aErr == nil && bErr == nil {
			switch {
			case an < bn:
				return -1
			case an > bn:
				return 1
			}
			return 0
		}
	}
	return strings.Compare(string(a), string(b))
}
