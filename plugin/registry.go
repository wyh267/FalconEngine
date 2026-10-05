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

// ---------- 分析链（char filter → tokenizer → token filter） ----------

// Token 分词单元：Term 为词元文本；Position 为该词元在原文中的位置
// （短语查询依赖；token filter 删除词元时保留其余词元的 Position 不变，
// 允许跳号）；Start/End 为原文偏移预留（offsets，本期可不填）
type Token struct {
	Term     string
	Position int
	Start    int
	End      int
}

// CharFilter 字符过滤器扩展点：分词前对原文做变换（如字符映射）
type CharFilter interface {
	Name() string
	Filter(string) string
}

// Tokenizer 切词器扩展点：把文本切分为带位置的 token 序列
// （Position 单调不减、Term 非空；转小写等归一化交给 token filter）
type Tokenizer interface {
	Name() string
	Tokenize(string) []Token
}

// TokenFilter 词元过滤器扩展点：对 token 序列做变换（转小写、删停用词等）。
// 删除词元时必须保持剩余词元的 Position 不变（允许跳号），
// 输出 Position 单调不减、Term 非空——索引/查询两侧位置同构是短语查询正确的前提
type TokenFilter interface {
	Name() string
	Filter([]Token) []Token
}

// Analyzer 分词器扩展点：完整分析链（char filter → tokenizer → token filter），
// 通常由 NewChainAnalyzer 组合三级组件得到
type Analyzer interface {
	// Analyze 将文本分析为带位置的 token 序列：
	// 保持出现顺序（允许重复，用于统计词频），Position 单调不减，Term 非空
	Analyze(string) []Token
}

// chainAnalyzer 由三级组件依次组合而成的标准分析链
type chainAnalyzer struct {
	cfs []CharFilter
	tk  Tokenizer
	tfs []TokenFilter
}

// NewChainAnalyzer 组合 char filter → tokenizer → token filter 为完整分词器。
// tk 为 nil 时 panic（切词器是链的必需级）；cfs/tfs 可为空
func NewChainAnalyzer(cfs []CharFilter, tk Tokenizer, tfs []TokenFilter) Analyzer {
	if tk == nil {
		panic("plugin: NewChainAnalyzer 的 tokenizer 不能为 nil")
	}
	return chainAnalyzer{cfs: cfs, tk: tk, tfs: tfs}
}

// Analyze 依次应用 char filters → tokenizer → token filters
func (c chainAnalyzer) Analyze(s string) []Token {
	for _, cf := range c.cfs {
		s = cf.Filter(s)
	}
	toks := c.tk.Tokenize(s)
	for _, tf := range c.tfs {
		toks = tf.Filter(toks)
	}
	return toks
}

// ---------- 字段类型 ----------

// Value 字段值解析后的内部表示
type Value struct {
	Text string // 倒排字段：待分词文本（引擎会交给 Analyzer() 指定的分词器处理）
	Num  int64  // 正排字段：数值（date 已转为 unix 秒——精度到秒，无毫秒/时区偏移；bool 为 1/0）
}

// FieldTypePlugin 字段类型扩展点
type FieldTypePlugin interface {
	Name() string
	// Analyzer 倒排字段使用的分词器名；非倒排字段返回 ""
	Analyzer() string
	// Inverted 是否建倒排索引
	Inverted() bool
	// DocValuesKind 建哪种正排列（不支持过滤与排序时返回 DVNone）
	DocValuesKind() DVKind
	// HasNorms 是否记录文档长度 norms（BM25 用）；true 时 Inverted 必须为 true
	HasNorms() bool
	// Parse 将 JSON 原始值解析为内部表示；非法值返回错误
	Parse(v json.RawMessage) (Value, error)
}

// DVKind 字段的正排列（docvalues）类型
type DVKind int

const (
	DVNone    DVKind = iota // 不建正排列
	DVNum                   // number 定长列（number/date/bool），支持过滤与排序
	DVKeyword               // keyword ord 列（keyword 字段）
)

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
	// ExpandTerms 枚举字段词典中满足条件的 term（prefix/wildcard/fuzzy 的解析期展开用）：
	// 合并内存缓冲与各段词典，结果去重、按字典序、最多 maxExpansions 个（超出截断）；
	// prefix 非空时借词典稀疏索引缩小扫描范围；match 为 nil 表示全收。
	// 纯 schema 解析场景（协调层无本地索引数据）为 nil：multi-term 类解析器此时
	// 产出未展开的节点（Terms 为空），分片侧重新解析时才真正展开。
	ExpandTerms func(field, prefix string, match func(term string) bool, maxExpansions int) []string
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
	AggString                // 字段字符串值（keyword dv 字段取自 kw 列，其余取自 stored 原文）
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
	mu           sync.RWMutex
	analyzers    = map[string]Analyzer{}
	charFilters  = map[string]CharFilter{}
	tokenizers   = map[string]Tokenizer{}
	tokenFilters = map[string]TokenFilter{}
	fieldTypes   = map[string]FieldTypePlugin{}
	scorers      = map[string]Scorer{}
	queries      = map[string]QueryParser{}
	aggs         = map[string]AggPlugin{}
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

// RegisterCharFilter 注册字符过滤器；重复注册 panic
func RegisterCharFilter(f CharFilter) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := charFilters[f.Name()]; dup {
		panic(fmt.Sprintf("plugin: 字符过滤器 %q 重复注册", f.Name()))
	}
	charFilters[f.Name()] = f
}

// GetCharFilter 按名取字符过滤器；未注册返回错误
func GetCharFilter(name string) (CharFilter, error) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := charFilters[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 字符过滤器 %q 未注册", name)
	}
	return f, nil
}

// RegisterTokenizer 注册切词器；重复注册 panic
func RegisterTokenizer(t Tokenizer) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := tokenizers[t.Name()]; dup {
		panic(fmt.Sprintf("plugin: 切词器 %q 重复注册", t.Name()))
	}
	tokenizers[t.Name()] = t
}

// GetTokenizer 按名取切词器；未注册返回错误
func GetTokenizer(name string) (Tokenizer, error) {
	mu.RLock()
	defer mu.RUnlock()
	t, ok := tokenizers[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 切词器 %q 未注册", name)
	}
	return t, nil
}

// RegisterTokenFilter 注册词元过滤器；重复注册 panic
func RegisterTokenFilter(f TokenFilter) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := tokenFilters[f.Name()]; dup {
		panic(fmt.Sprintf("plugin: 词元过滤器 %q 重复注册", f.Name()))
	}
	tokenFilters[f.Name()] = f
}

// GetTokenFilter 按名取词元过滤器；未注册返回错误
func GetTokenFilter(name string) (TokenFilter, error) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := tokenFilters[name]
	if !ok {
		return nil, fmt.Errorf("plugin: 词元过滤器 %q 未注册", name)
	}
	return f, nil
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
