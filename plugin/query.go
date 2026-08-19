package plugin

import (
	"encoding/json"
	"fmt"
)

// 查询树节点（IR）：DSL 子句解析的产物，由 index 引擎执行。
// 每种节点一个具体类型，引擎按类型 switch 执行（这是执行层对 IR 的分派，
// 不属于"按类型名分发"的插件查找）。

// QNode 查询树节点标记接口
type QNode interface{ qnode() }

// MatchNode 分词匹配（text 字段）：查询串经字段分词器切分后按 Operator 组合，Scorer 打分
type MatchNode struct {
	Field    string
	Text     string
	Operator string // "or"（默认）或 "and"
	Scorer   string // 打分器名，空表示引擎默认
}

// TermNode 倒排精确匹配（keyword 等字段）：字段 + 精确 term
type TermNode struct {
	Field string
	Term  string
}

// RangeNode number 类字段（number/date/bool）的范围过滤：走 docvalues。
// HasMin/HasMax 标记边界是否存在，Min/Max 为闭区间端点。
type RangeNode struct {
	Field  string
	Min    int64
	Max    int64
	HasMin bool
	HasMax bool
}

// MatchAllNode 匹配全部文档
type MatchAllNode struct{}

// IDsNode 按外部文档 ID 精确命中
type IDsNode struct {
	IDs []string
}

// BoolNode 组合子句：must/filter 取交集（filter 不打分），should 取并集加分，
// must_not 排除。无 must/filter 时至少命中一个 should。
type BoolNode struct {
	Must    []QNode
	Filter  []QNode
	Should  []QNode
	MustNot []QNode
}

func (MatchNode) qnode()    {}
func (TermNode) qnode()     {}
func (RangeNode) qnode()    {}
func (MatchAllNode) qnode() {}
func (IDsNode) qnode()      {}
func (BoolNode) qnode()     {}

// ParseClause 解析一个 DSL 查询子句：{"<子句名>": <body>}。
// 子句名必须已注册，这是注册表查找而非硬编码分发。
func ParseClause(raw json.RawMessage, ctx *ParseContext) (QNode, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("plugin: 查询子句应为对象: %w", err)
	}
	if len(m) != 1 {
		return nil, fmt.Errorf("plugin: 查询子句应恰好包含一个键，实际 %d 个", len(m))
	}
	for name, body := range m {
		p, err := GetQuery(name)
		if err != nil {
			return nil, err
		}
		return p.Parse(body, ctx)
	}
	return nil, fmt.Errorf("plugin: 空查询子句")
}
