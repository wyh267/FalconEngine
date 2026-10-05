// Package query 内置查询子句解析器插件：
// match / term / terms / range / bool / match_all / ids /
// match_phrase / multi_match / prefix / wildcard / fuzzy / exists。
//
// 解析产物是 plugin.QNode 查询树，由 index 引擎执行。
package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
)

// fieldPlugin 查询字段的类型插件，字段不存在时返回错误
func fieldPlugin(ctx *plugin.ParseContext, field string) (plugin.FieldTypePlugin, error) {
	p, ok := ctx.FieldPlugin(field)
	if !ok {
		return nil, fmt.Errorf("字段 %q 不存在", field)
	}
	return p, nil
}

// oneField 解析 {"<field>": body} 形式的子句体
func oneField(body json.RawMessage) (string, json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return "", nil, fmt.Errorf("子句体应为 {\"<field>\": ...} 形式: %w", err)
	}
	if len(m) != 1 {
		return "", nil, fmt.Errorf("子句体应恰好包含一个字段，实际 %d 个", len(m))
	}
	for field, v := range m {
		return field, v, nil
	}
	return "", nil, fmt.Errorf("空子句体")
}

// ---------- match ----------

type matchParser struct{}

func (matchParser) Name() string { return "match" }

func (matchParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("match: %w", err)
	}
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return nil, fmt.Errorf("match: %w", err)
	}
	if !fp.Inverted() {
		return nil, fmt.Errorf("match: 字段 %q 不是倒排字段", field)
	}

	node := plugin.MatchNode{Field: field}
	// 支持简写 {"content": "雅礼"} 与完整 {"content": {"query": "雅礼", "operator": "or"}}
	var text string
	if err := json.Unmarshal(v, &text); err == nil {
		node.Text = text
	} else {
		var opts struct {
			Query    string `json:"query"`
			Operator string `json:"operator"`
			Scorer   string `json:"scorer"`
		}
		if err := json.Unmarshal(v, &opts); err != nil {
			return nil, fmt.Errorf("match: 字段 %q 的值应为字符串或对象: %w", field, err)
		}
		node.Text, node.Operator, node.Scorer = opts.Query, opts.Operator, opts.Scorer
	}
	if node.Operator != "" && node.Operator != "or" && node.Operator != "and" {
		return nil, fmt.Errorf("match: operator %q 非法，仅支持 or/and", node.Operator)
	}
	return node, nil
}

// ---------- term / terms ----------

type termParser struct{}

func (termParser) Name() string { return "term" }

// parseTermValue 按字段类型解析 term 值：倒排字段产出 TermNode，正排字段产出等值 RangeNode
func parseTermValue(field string, raw json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return nil, err
	}
	pv, err := fp.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("字段 %q 的值非法: %w", field, err)
	}
	switch {
	case fp.Inverted():
		return plugin.TermNode{Field: field, Term: pv.Text}, nil
	case fp.DocValuesKind() == plugin.DVNum:
		return plugin.RangeNode{Field: field, Min: pv.Num, Max: pv.Num, HasMin: true, HasMax: true}, nil
	default:
		return nil, fmt.Errorf("字段 %q 类型 %q 不支持 term 查询", field, fp.Name())
	}
}

func (termParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("term: %w", err)
	}
	return parseTermValue(field, v, ctx)
}

type termsParser struct{}

func (termsParser) Name() string { return "terms" }

func (termsParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("terms: %w", err)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(v, &values); err != nil {
		return nil, fmt.Errorf("terms: 字段 %q 的值应为数组: %w", field, err)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("terms: 字段 %q 的值数组不能为空", field)
	}
	node := plugin.BoolNode{}
	for _, raw := range values {
		sub, err := parseTermValue(field, raw, ctx)
		if err != nil {
			return nil, fmt.Errorf("terms: %w", err)
		}
		node.Should = append(node.Should, sub)
	}
	return node, nil
}

// ---------- range ----------

type rangeParser struct{}

func (rangeParser) Name() string { return "range" }

func (rangeParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("range: %w", err)
	}
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return nil, fmt.Errorf("range: %w", err)
	}
	if fp.DocValuesKind() != plugin.DVNum {
		return nil, fmt.Errorf("range: 字段 %q 不是正排字段", field)
	}
	var bounds map[string]json.RawMessage
	if err := json.Unmarshal(v, &bounds); err != nil {
		return nil, fmt.Errorf("range: 字段 %q 的值应为 {\"gte\":..,\"lte\":..} 形式: %w", field, err)
	}
	node := plugin.RangeNode{Field: field}
	for op, raw := range bounds {
		pv, err := fp.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("range: 字段 %q 边界 %q 非法: %w", field, op, err)
		}
		switch op {
		case "gte":
			node.Min, node.HasMin = pv.Num, true
		case "lte":
			node.Max, node.HasMax = pv.Num, true
		case "gt":
			node.Min, node.HasMin = pv.Num+1, true
		case "lt":
			node.Max, node.HasMax = pv.Num-1, true
		default:
			return nil, fmt.Errorf("range: 不支持的边界操作符 %q", op)
		}
	}
	if !node.HasMin && !node.HasMax {
		return nil, fmt.Errorf("range: 至少需要一个边界（gte/lte/gt/lt）")
	}
	return node, nil
}

// ---------- bool ----------

type boolParser struct{}

func (boolParser) Name() string { return "bool" }

func (boolParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	var raw struct {
		Must    []json.RawMessage `json:"must"`
		Filter  []json.RawMessage `json:"filter"`
		Should  []json.RawMessage `json:"should"`
		MustNot []json.RawMessage `json:"must_not"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("bool: 解析失败: %w", err)
	}
	node := plugin.BoolNode{}
	parseEach := func(list []json.RawMessage) ([]plugin.QNode, error) {
		out := make([]plugin.QNode, 0, len(list))
		for _, c := range list {
			n, err := plugin.ParseClause(c, ctx)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return out, nil
	}
	var err error
	if node.Must, err = parseEach(raw.Must); err != nil {
		return nil, fmt.Errorf("bool.must: %w", err)
	}
	if node.Filter, err = parseEach(raw.Filter); err != nil {
		return nil, fmt.Errorf("bool.filter: %w", err)
	}
	if node.Should, err = parseEach(raw.Should); err != nil {
		return nil, fmt.Errorf("bool.should: %w", err)
	}
	if node.MustNot, err = parseEach(raw.MustNot); err != nil {
		return nil, fmt.Errorf("bool.must_not: %w", err)
	}
	return node, nil
}

// ---------- match_phrase ----------

type matchPhraseParser struct{}

func (matchPhraseParser) Name() string { return "match_phrase" }

func (matchPhraseParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("match_phrase: %w", err)
	}
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return nil, fmt.Errorf("match_phrase: %w", err)
	}
	if !fp.Inverted() {
		return nil, fmt.Errorf("match_phrase: 字段 %q 不是倒排字段", field)
	}

	node := plugin.PhraseNode{Field: field}
	// 支持简写 {"content": "quick brown"} 与完整 {"content": {"query": "quick brown", "slop": 0}}
	var text string
	if err := json.Unmarshal(v, &text); err == nil {
		node.Text = text
	} else {
		var opts struct {
			Query string `json:"query"`
			Slop  int    `json:"slop"`
		}
		if err := json.Unmarshal(v, &opts); err != nil {
			return nil, fmt.Errorf("match_phrase: 字段 %q 的值应为字符串或对象: %w", field, err)
		}
		node.Text, node.Slop = opts.Query, opts.Slop
	}
	if node.Slop < 0 {
		return nil, fmt.Errorf("match_phrase: slop 不能为负，got %d", node.Slop)
	}
	if node.Slop > 0 {
		return nil, fmt.Errorf("match_phrase: slop>0 暂未支持")
	}
	return node, nil
}

// ---------- multi_match ----------

type multiMatchParser struct{}

func (multiMatchParser) Name() string { return "multi_match" }

// Parse 把 {"multi_match": {"query": "...", "fields": ["title^3", "content"]}}
// 展开为 BoolNode{Should: [MatchNode{...Boost}...]}：should 并集加分近似 ES 的
// best_fields（ES 取各字段最高分即 dis_max，本实现为分数累加，差异见注释）；
// type 参数（most_fields/cross_fields/phrase 等）暂不支持。
func (multiMatchParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	var v struct {
		Query    string   `json:"query"`
		Fields   []string `json:"fields"`
		Operator string   `json:"operator"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, fmt.Errorf("multi_match: 解析失败: %w", err)
	}
	if v.Query == "" {
		return nil, fmt.Errorf("multi_match: query 不能为空")
	}
	if len(v.Fields) == 0 {
		return nil, fmt.Errorf("multi_match: fields 不能为空")
	}
	if v.Operator != "" && v.Operator != "or" && v.Operator != "and" {
		return nil, fmt.Errorf("multi_match: operator %q 非法，仅支持 or/and", v.Operator)
	}
	node := plugin.BoolNode{}
	for _, spec := range v.Fields {
		field, boost, err := parseFieldBoost(spec)
		if err != nil {
			return nil, fmt.Errorf("multi_match: %w", err)
		}
		fp, err := fieldPlugin(ctx, field)
		if err != nil {
			return nil, fmt.Errorf("multi_match: %w", err)
		}
		if !fp.Inverted() {
			return nil, fmt.Errorf("multi_match: 字段 %q 不是倒排字段", field)
		}
		node.Should = append(node.Should, plugin.MatchNode{
			Field: field, Text: v.Query, Operator: v.Operator, Boost: boost,
		})
	}
	return node, nil
}

// parseFieldBoost 解析 "title^3" 形式的字段加权；无 "^" 时 boost 为 1
func parseFieldBoost(spec string) (string, float64, error) {
	field, boostStr, found := strings.Cut(spec, "^")
	if field == "" {
		return "", 0, fmt.Errorf("fields 元素 %q 字段名为空", spec)
	}
	if !found {
		return field, 1, nil
	}
	boost, err := strconv.ParseFloat(boostStr, 64)
	if err != nil || boost <= 0 {
		return "", 0, fmt.Errorf("fields 元素 %q 的 boost 应为正数, got %q", spec, boostStr)
	}
	return field, boost, nil
}

// ---------- exists ----------

type existsParser struct{}

func (existsParser) Name() string { return "exists" }

func (existsParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	var v struct {
		Field string `json:"field"`
	}
	if err := json.Unmarshal(body, &v); err != nil || v.Field == "" {
		return nil, fmt.Errorf("exists: 子句体应形如 {\"field\": \"<字段名>\"}")
	}
	if _, err := fieldPlugin(ctx, v.Field); err != nil {
		return nil, fmt.Errorf("exists: %w", err)
	}
	return plugin.ExistsNode{Field: v.Field}, nil
}

// ---------- match_all / ids ----------

type matchAllParser struct{}

func (matchAllParser) Name() string { return "match_all" }

func (matchAllParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	return plugin.MatchAllNode{}, nil
}

type idsParser struct{}

func (idsParser) Name() string { return "ids" }

func (idsParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	var v struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, fmt.Errorf("ids: 解析失败: %w", err)
	}
	return plugin.IDsNode{IDs: v.Values}, nil
}

func init() {
	plugin.RegisterQuery(matchParser{})
	plugin.RegisterQuery(termParser{})
	plugin.RegisterQuery(termsParser{})
	plugin.RegisterQuery(rangeParser{})
	plugin.RegisterQuery(boolParser{})
	plugin.RegisterQuery(matchAllParser{})
	plugin.RegisterQuery(idsParser{})
	plugin.RegisterQuery(matchPhraseParser{})
	plugin.RegisterQuery(multiMatchParser{})
	plugin.RegisterQuery(existsParser{})
	plugin.RegisterQuery(prefixParser{})
	plugin.RegisterQuery(wildcardParser{})
	plugin.RegisterQuery(fuzzyParser{})
}
