package index

import (
	"encoding/json"
	"fmt"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/schema"
)

// dslRequest 查询 DSL 的顶层结构
type dslRequest struct {
	Query json.RawMessage              `json:"query"` // 查询子句，缺省为 match_all
	From  *int                         `json:"from"`
	Size  *int                         `json:"size"`
	Sort  []map[string]json.RawMessage `json:"sort"` // [{"field": "desc"|"asc"}]
	Aggs  map[string]json.RawMessage   `json:"aggs"` // {聚合名: {"<类型>": {...}}}
}

// parseContext 构建本引擎的查询/聚合解析上下文：按字段名查类型插件，
// 并提供词典展开能力（multi-term 查询解析期展开用，自持读锁见 expandTerms）
func (e *Engine) parseContext() *plugin.ParseContext {
	ctx := parseContextOf(e.schema)
	ctx.ExpandTerms = e.expandTerms
	return ctx
}

// parseContextOf 按 schema 构建解析上下文（协调层无本地引擎时直接用；
// 无词典数据，ExpandTerms 留空——multi-term 节点在分片侧重新解析时才展开）
func parseContextOf(sch *schema.Schema) *plugin.ParseContext {
	return &plugin.ParseContext{
		FieldPlugin: func(field string) (plugin.FieldTypePlugin, bool) {
			f, ok := sch.Field(field)
			if !ok {
				return nil, false
			}
			p, err := plugin.GetFieldType(f.Type)
			if err != nil {
				return nil, false
			}
			return p, true
		},
	}
}

// SearchDSL 执行 JSON 查询 DSL。
//
//	{
//	  "query": {"match": {"content": {"query": "雅礼", "operator": "or"}}, ...},
//	  "from": 0, "size": 10,
//	  "sort": [{"datetime": "desc"}, {"_score": "desc"}],
//	  "aggs": {"by_level": {"terms": {"field": "level", "size": 10}}}
//	}
//
// dslParsed DSL 解析产物（协调层合并聚合时复用其中的聚合规格）
type dslParsed struct {
	root     plugin.QNode
	opts     *SearchOptions
	aggSpecs map[string]plugin.AggSpec // 聚合名 -> 规格（用于协调层 New+Merge）
	aggPlugs map[string]plugin.AggPlugin
}

// parseDSL 解析 DSL 顶层：查询子句 + 排序 + 分页 + 聚合规格
func (e *Engine) parseDSL(body []byte) (*dslParsed, error) {
	return parseDSLWithContext(body, e.parseContext())
}

// parseDSLWithSchema 用给定 schema 解析 DSL（协调层用：无本地引擎，词典展开留空）
func parseDSLWithSchema(body []byte, sch *schema.Schema) (*dslParsed, error) {
	return parseDSLWithContext(body, parseContextOf(sch))
}

// parseDSLWithContext 用给定解析上下文解析 DSL 顶层
func parseDSLWithContext(body []byte, ctx *plugin.ParseContext) (*dslParsed, error) {
	var req dslRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("index: DSL 解析失败: %w", err)
	}

	// 查询子句
	root := plugin.QNode(plugin.MatchAllNode{})
	if len(req.Query) > 0 {
		node, err := plugin.ParseClause(req.Query, ctx)
		if err != nil {
			return nil, err
		}
		root = node
	}

	// 排序
	opts := &SearchOptions{}
	for _, item := range req.Sort {
		if len(item) != 1 {
			return nil, fmt.Errorf("index: sort 元素应恰好包含一个字段")
		}
		for field, dirRaw := range item {
			var dir string
			if err := json.Unmarshal(dirRaw, &dir); err != nil {
				return nil, fmt.Errorf("index: sort 字段 %q 的方向应为 \"asc\"/\"desc\"", field)
			}
			switch dir {
			case "asc":
				opts.Sort = append(opts.Sort, SortField{Field: field})
			case "desc":
				opts.Sort = append(opts.Sort, SortField{Field: field, Desc: true})
			default:
				return nil, fmt.Errorf("index: sort 字段 %q 方向 %q 非法，仅支持 asc/desc", field, dir)
			}
		}
	}
	if req.From != nil {
		opts.From = *req.From
	}
	if req.Size != nil {
		opts.Size = *req.Size
	}

	// 聚合规格（协调层需要规格来 Merge 各分片的中间结果）
	parsed := &dslParsed{root: root, opts: opts}
	if len(req.Aggs) > 0 {
		parsed.aggSpecs = make(map[string]plugin.AggSpec, len(req.Aggs))
		parsed.aggPlugs = make(map[string]plugin.AggPlugin, len(req.Aggs))
		for name, body := range req.Aggs {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(body, &m); err != nil || len(m) != 1 {
				return nil, fmt.Errorf("index: 聚合 %q 应形如 {\"<类型>\": {...}}", name)
			}
			for aggName, aggBody := range m {
				p, err := plugin.GetAgg(aggName)
				if err != nil {
					return nil, err
				}
				spec, err := p.Parse(aggBody, ctx)
				if err != nil {
					return nil, fmt.Errorf("index: 聚合 %q: %w", name, err)
				}
				parsed.aggSpecs[name] = spec
				parsed.aggPlugs[name] = p
			}
		}
	}
	return parsed, nil
}

// SearchDSL 执行 JSON 查询 DSL（本 shard 最终结果）
func (e *Engine) SearchDSL(body []byte) (*Result, error) {
	return e.SearchShardDSL(body, false)
}

// SearchShardDSL 执行 JSON 查询 DSL；partial=true 时聚合字段返回
// 可合并的中间结果（Aggregator.Marshal），供协调层跨分片合并（5b scatter-gather 用）
func (e *Engine) SearchShardDSL(body []byte, partial bool) (*Result, error) {
	parsed, err := e.parseDSL(body)
	if err != nil {
		return nil, err
	}
	if len(parsed.aggSpecs) > 0 {
		parsed.opts.Aggs = make(map[string]plugin.Aggregator, len(parsed.aggSpecs))
		for name, spec := range parsed.aggSpecs {
			parsed.opts.Aggs[name] = parsed.aggPlugs[name].New(spec)
		}
	}
	parsed.opts.AggPartial = partial
	return e.Search(parsed.root, parsed.opts)
}
