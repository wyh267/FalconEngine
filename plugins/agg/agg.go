// Package agg 内置聚合插件：terms / min / max / avg / sum / cardinality。
//
// 全部为两段式实现：局部聚合产生可序列化的中间结果（Marshal），
// 协调层 Merge 多个中间结果后取最终结果（Result），阶段 5 分布式复用。
// terms/cardinality 消费字段字符串值（keyword dv 字段走 kw 列，其余取自
// stored 原文；text 本体不支持聚合，解析期报错引导用 keyword 子字段）；
// min/max/avg/sum 消费 number 类正排值。
package agg

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/FalconEngine/falcon/plugin"
)

// specBody 聚合子句体 {"field": "...", "size": 10}
type specBody struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

// parseBody 解析子句体并校验字段存在；
// text 本体不支持聚合——本引擎明确不做 fielddata，引导改用 keyword 子字段
func parseBody(body json.RawMessage, ctx *plugin.ParseContext, name string) (specBody, error) {
	var b specBody
	if err := json.Unmarshal(body, &b); err != nil {
		return b, fmt.Errorf("%s: 解析失败: %w", name, err)
	}
	if b.Field == "" {
		return b, fmt.Errorf("%s: 缺少 field", name)
	}
	fp, ok := ctx.FieldPlugin(b.Field)
	if !ok {
		return b, fmt.Errorf("%s: 字段 %q 不存在", name, b.Field)
	}
	if fp.Inverted() && fp.DocValuesKind() == plugin.DVNone {
		return b, fmt.Errorf("%s: text 字段 %q 不支持聚合，请使用其 keyword 子字段（如 %q）；本引擎不做 fielddata", name, b.Field, b.Field+".keyword")
	}
	return b, nil
}

// parseNumBody 解析并校验字段必须是建了正排的字段
func parseNumBody(body json.RawMessage, ctx *plugin.ParseContext, name string) (specBody, error) {
	b, err := parseBody(body, ctx, name)
	if err != nil {
		return b, err
	}
	fp, _ := ctx.FieldPlugin(b.Field)
	if fp.DocValuesKind() != plugin.DVNum {
		return b, fmt.Errorf("%s: 字段 %q 不是正排字段", name, b.Field)
	}
	return b, nil
}

// ---------- terms ----------

type termsPlugin struct{}

func (termsPlugin) Name() string { return "terms" }

func (termsPlugin) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.AggSpec, error) {
	b, err := parseBody(body, ctx, "terms")
	if err != nil {
		return plugin.AggSpec{}, err
	}
	return plugin.AggSpec{Name: "terms", Field: b.Field, Size: b.Size}, nil
}

func (termsPlugin) New(spec plugin.AggSpec) plugin.Aggregator {
	return &termsAgg{spec: spec, counts: map[string]int64{}}
}

// termsAgg terms 聚合：局部结果是完整的精确计数表（小字段场景）
type termsAgg struct {
	spec   plugin.AggSpec
	counts map[string]int64
}

func (a *termsAgg) Kind() plugin.AggKind { return plugin.AggString }
func (a *termsAgg) Field() string        { return a.spec.Field }
func (a *termsAgg) CollectNum(int64)     {}

func (a *termsAgg) CollectString(v string) { a.counts[v]++ }

func (a *termsAgg) Marshal() (json.RawMessage, error) { return json.Marshal(a.counts) }

func (a *termsAgg) Merge(p json.RawMessage) error {
	var other map[string]int64
	if err := json.Unmarshal(p, &other); err != nil {
		return fmt.Errorf("terms: 合并中间结果失败: %w", err)
	}
	for k, v := range other {
		a.counts[k] += v
	}
	return nil
}

type bucket struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

func (a *termsAgg) Result() json.RawMessage {
	buckets := make([]bucket, 0, len(a.counts))
	for k, v := range a.counts {
		buckets = append(buckets, bucket{Key: k, Count: v})
	}
	// count 降序，同数按 key 升序
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Count != buckets[j].Count {
			return buckets[i].Count > buckets[j].Count
		}
		return buckets[i].Key < buckets[j].Key
	})
	if a.spec.Size > 0 && len(buckets) > a.spec.Size {
		buckets = buckets[:a.spec.Size]
	}
	out, _ := json.Marshal(map[string]any{"buckets": buckets})
	return out
}

// ---------- cardinality ----------

type cardinalityPlugin struct{}

func (cardinalityPlugin) Name() string { return "cardinality" }

func (cardinalityPlugin) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.AggSpec, error) {
	b, err := parseBody(body, ctx, "cardinality")
	if err != nil {
		return plugin.AggSpec{}, err
	}
	return plugin.AggSpec{Name: "cardinality", Field: b.Field}, nil
}

func (cardinalityPlugin) New(spec plugin.AggSpec) plugin.Aggregator {
	return &cardinalityAgg{spec: spec, set: map[string]struct{}{}}
}

// cardinalityAgg 精确去重计数：局部结果是去重集合
type cardinalityAgg struct {
	spec plugin.AggSpec
	set  map[string]struct{}
}

func (a *cardinalityAgg) Kind() plugin.AggKind { return plugin.AggString }
func (a *cardinalityAgg) Field() string        { return a.spec.Field }
func (a *cardinalityAgg) CollectNum(int64)     {}

func (a *cardinalityAgg) CollectString(v string) { a.set[v] = struct{}{} }

func (a *cardinalityAgg) Marshal() (json.RawMessage, error) {
	keys := make([]string, 0, len(a.set))
	for k := range a.set {
		keys = append(keys, k)
	}
	return json.Marshal(keys)
}

func (a *cardinalityAgg) Merge(p json.RawMessage) error {
	var keys []string
	if err := json.Unmarshal(p, &keys); err != nil {
		return fmt.Errorf("cardinality: 合并中间结果失败: %w", err)
	}
	for _, k := range keys {
		a.set[k] = struct{}{}
	}
	return nil
}

func (a *cardinalityAgg) Result() json.RawMessage {
	out, _ := json.Marshal(map[string]any{"value": len(a.set)})
	return out
}

// ---------- min / max / avg / sum ----------

// numAgg 数值聚合的中间结果：min/max/avg/sum 共用
type numAgg struct {
	field string
	op    string // min/max/avg/sum
	min   int64
	max   int64
	sum   int64
	count int64
}

func (a *numAgg) Kind() plugin.AggKind { return plugin.AggNum }
func (a *numAgg) Field() string        { return a.field }
func (a *numAgg) CollectString(string) {}

func (a *numAgg) CollectNum(v int64) {
	if a.count == 0 || v < a.min {
		a.min = v
	}
	if a.count == 0 || v > a.max {
		a.max = v
	}
	a.sum += v
	a.count++
}

type numPartial struct {
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	Sum   int64 `json:"sum"`
	Count int64 `json:"count"`
}

func (a *numAgg) Marshal() (json.RawMessage, error) {
	return json.Marshal(numPartial{Min: a.min, Max: a.max, Sum: a.sum, Count: a.count})
}

func (a *numAgg) Merge(p json.RawMessage) error {
	var o numPartial
	if err := json.Unmarshal(p, &o); err != nil {
		return fmt.Errorf("%s: 合并中间结果失败: %w", a.op, err)
	}
	if o.Count == 0 {
		return nil
	}
	if a.count == 0 || o.Min < a.min {
		a.min = o.Min
	}
	if a.count == 0 || o.Max > a.max {
		a.max = o.Max
	}
	a.sum += o.Sum
	a.count += o.Count
	return nil
}

func (a *numAgg) Result() json.RawMessage {
	var v any
	switch a.op {
	case "min":
		if a.count > 0 {
			v = a.min
		}
	case "max":
		if a.count > 0 {
			v = a.max
		}
	case "sum":
		v = a.sum
	case "avg":
		if a.count > 0 {
			v = float64(a.sum) / float64(a.count)
		}
	}
	out, _ := json.Marshal(map[string]any{"value": v})
	return out
}

// numPlugin min/max/avg/sum 共用的插件骨架
type numPlugin struct{ op string }

func (p numPlugin) Name() string { return p.op }

func (p numPlugin) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.AggSpec, error) {
	b, err := parseNumBody(body, ctx, p.op)
	if err != nil {
		return plugin.AggSpec{}, err
	}
	return plugin.AggSpec{Name: p.op, Field: b.Field}, nil
}

func (p numPlugin) New(spec plugin.AggSpec) plugin.Aggregator {
	return &numAgg{field: spec.Field, op: spec.Name}
}

func init() {
	plugin.RegisterAgg(termsPlugin{})
	plugin.RegisterAgg(cardinalityPlugin{})
	plugin.RegisterAgg(numPlugin{op: "min"})
	plugin.RegisterAgg(numPlugin{op: "max"})
	plugin.RegisterAgg(numPlugin{op: "avg"})
	plugin.RegisterAgg(numPlugin{op: "sum"})
}
