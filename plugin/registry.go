// Package plugin 定义 FalconEngine 的插件注册表与扩展点接口。
//
// 采用进程内注册模式（同 database/sql 驱动）：插件包在 init() 中调用
// Register* 注册实现，上层只通过 Get* 按名字取实现，不允许 switch-case
// 按类型名分发。重复注册 panic，Get 未注册返回明确错误。
//
// 本包处于依赖链底层：只定义接口与查询/聚合的中间表示（IR），
// 不依赖 index/segment 等执行层包。
package plugin

import (
	"encoding/json"
	"fmt"
	"sync"
)

// ---------- 分词器 ----------

// Analyzer 分词器扩展点
type Analyzer interface {
	// Tokenize 将文本切分为 token 序列，保持出现顺序（允许重复，用于统计词频）
	Tokenize(s string) []string
}

// ---------- 字段类型 ----------

// Value 字段值解析后的内部表示
type Value struct {
	Text string // 倒排字段：待分词文本（引擎会交给 Analyzer() 指定的分词器处理）
	Num  int64  // 正排字段：数值（date 已转为 unix 秒，bool 为 1/0）
}

// FieldTypePlugin 字段类型扩展点
type FieldTypePlugin interface {
	Name() string
	// Analyzer 倒排字段使用的分词器名；非倒排字段返回 ""
	Analyzer() string
	// Inverted 是否建倒排索引
	Inverted() bool
	// DocValues 是否建 number 类正排（支持过滤与排序）
	DocValues() bool
	// HasNorms 是否记录文档长度 norms（BM25 用）；true 时 Inverted 必须为 true
	HasNorms() bool
	// Parse 将 JSON 原始值解析为内部表示；非法值返回错误
	Parse(v json.RawMessage) (Value, error)
}

// ---------- 打分器 ----------

// Scorer 相关度打分扩展点
type Scorer interface {
	Name() string
	// Score 计算单文档单 term 得分。
	// tf 词频；dl 文档长度（term 数）；df 命中该 term 的文档数；n 总文档数；avgdl 平均文档长度。
	Score(tf, dl int64, df, n int, avgdl float64) float64
}

// ---------- 查询子句解析器 ----------

// ParseContext 子句解析上下文：提供字段 -> 字段类型插件的查询能力
type ParseContext struct {
	// FieldPlugin 按字段名返回其类型插件；字段不存在时 ok=false
	FieldPlugin func(field string) (FieldTypePlugin, bool)
}

// QueryParser DSL 查询子句解析器扩展点：把子句 JSON 解析为查询树节点（QNode）
type QueryParser interface {
	Name() string
	Parse(body json.RawMessage, ctx *ParseContext) (QNode, error)
}

// ---------- 聚合解析器 ----------

// AggSpec 聚合规格（解析产物，可跨节点传输）
type AggSpec struct {
	Name  string `json:"name"`  // 聚合类型名（terms/avg/...）
	Field string `json:"field"` // 作用字段
	Size  int    `json:"size"`  // terms 聚合返回的桶数上限
}

// AggKind 聚合消费的值类型
type AggKind int

const (
	AggNum    AggKind = iota // number 类正排值
	AggString                // 字段原始字符串值（取自 stored 原文）
)

// Aggregator 两段式聚合器：引擎/分片局部聚合产生中间结果（Marshal），
// 协调层 Merge 多个中间结果后取 Result。阶段 5 分布式协调层复用同一接口。
type Aggregator interface {
	// Kind 该聚合消费的值类型
	Kind() AggKind
	// Field 该聚合作用的字段名
	Field() string
	// CollectNum Kind==AggNum 时对每篇命中文档调用（字段缺失的文档不调用）
	CollectNum(v int64)
	// CollectString Kind==AggString 时对每篇命中文档调用（字段缺失的文档不调用）
	CollectString(v string)
	// Marshal 序列化局部中间结果（可跨节点传输）
	Marshal() (json.RawMessage, error)
	// Merge 合并另一个局部中间结果
	Merge(p json.RawMessage) error
	// Result 输出最终结果（JSON）
	Result() json.RawMessage
}

// AggPlugin 聚合扩展点：解析 DSL 子句 + 构建 Aggregator
type AggPlugin interface {
	Name() string
	Parse(body json.RawMessage, ctx *ParseContext) (AggSpec, error)
	// New 按规格构建一个聚合器实例
	New(spec AggSpec) Aggregator
}

// ---------- 注册表 ----------

var (
	mu         sync.RWMutex
	analyzers  = map[string]Analyzer{}
	fieldTypes = map[string]FieldTypePlugin{}
	scorers    = map[string]Scorer{}
	queries    = map[string]QueryParser{}
	aggs       = map[string]AggPlugin{}
)

// RegisterAnalyzer 注册分词器；重复注册 panic
func RegisterAnalyzer(name string, a Analyzer) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := analyzers[name]; dup {
		panic(fmt.Sprintf("plugin: 分词器 %q 重复注册", name))
	}
	analyzers[name] = a
}

// GetAnalyzer 按名取分词器；未注册返回错误
func GetAnalyzer(name string) (Analyzer, error) {
	mu.RLock()
	defer mu.RUnlock()
	a, ok := analyzers[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 分词器 %q 未注册", name)
	}
	return a, nil
}

// RegisterFieldType 注册字段类型插件；重复注册 panic
func RegisterFieldType(p FieldTypePlugin) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := fieldTypes[p.Name()]; dup {
		panic(fmt.Sprintf("plugin: 字段类型 %q 重复注册", p.Name()))
	}
	fieldTypes[p.Name()] = p
}

// GetFieldType 按名取字段类型插件；未注册返回错误
func GetFieldType(name string) (FieldTypePlugin, error) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := fieldTypes[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 字段类型 %q 未注册", name)
	}
	return p, nil
}

// RegisterScorer 注册打分器；重复注册 panic
func RegisterScorer(s Scorer) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := scorers[s.Name()]; dup {
		panic(fmt.Sprintf("plugin: 打分器 %q 重复注册", s.Name()))
	}
	scorers[s.Name()] = s
}

// GetScorer 按名取打分器；未注册返回错误
func GetScorer(name string) (Scorer, error) {
	mu.RLock()
	defer mu.RUnlock()
	s, ok := scorers[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 打分器 %q 未注册", name)
	}
	return s, nil
}

// RegisterQuery 注册查询子句解析器；重复注册 panic
func RegisterQuery(p QueryParser) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := queries[p.Name()]; dup {
		panic(fmt.Sprintf("plugin: 查询子句 %q 重复注册", p.Name()))
	}
	queries[p.Name()] = p
}

// GetQuery 按名取查询子句解析器；未注册返回错误
func GetQuery(name string) (QueryParser, error) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := queries[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 查询子句 %q 未注册", name)
	}
	return p, nil
}

// RegisterAgg 注册聚合插件；重复注册 panic
func RegisterAgg(p AggPlugin) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := aggs[p.Name()]; dup {
		panic(fmt.Sprintf("plugin: 聚合 %q 重复注册", p.Name()))
	}
	aggs[p.Name()] = p
}

// GetAgg 按名取聚合插件；未注册返回错误
func GetAgg(name string) (AggPlugin, error) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := aggs[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 聚合 %q 未注册", name)
	}
	return p, nil
}
