package index

import (
	"encoding/json"
	"fmt"
	"sort"

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
func NewScatterPlan(body []byte, sch *schema.Schema) (*ScatterPlan, error) {
	parsed, err := parseDSLWithSchema(body, sch)
	if err != nil {
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

	// 归并排序（与分片侧排序器语义一致）
	sorts := p.sorts
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i], hits[j]
		for _, s := range sorts {
			if s.Field == "_score" {
				if a.Score != b.Score {
					return (a.Score > b.Score) == s.Desc
				}
				continue
			}
			va, oka := hitNumValue(a, s.Field)
			vb, okb := hitNumValue(b, s.Field)
			if oka != okb {
				return oka
			}
			if oka && va != vb {
				return (va > vb) == s.Desc
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

// hitNumValue 从命中原文中提取排序字段的数值（协调层归并用；字段缺失返回 false）
func hitNumValue(h Hit, field string) (int64, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(h.Source, &m); err != nil {
		return 0, false
	}
	fv, ok := m[field]
	if !ok {
		return 0, false
	}
	var n json.Number
	if err := json.Unmarshal(fv, &n); err != nil {
		return 0, false
	}
	v, err := n.Int64()
	return v, err == nil
}
