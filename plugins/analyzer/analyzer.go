// Package analyzer 内置分析链组件与分词器插件。
//
// 切词器（tokenizer）：
//
//	standard   标准切词器（analysis.Tokenize：ASCII 成词，CJK 单字）
//	keyword    整词不切分
//	whitespace 按空白切分
//
// 词元过滤器（token filter）：
//
//	lowercase  转小写（Unicode 感知）
//	stop       移除英文停用词，保留其余 token 的 Position（跳号）
//
// 字符过滤器（char filter）：
//
//	mapping    字符映射样板（"ph"→"f"、"qu"→"kw"），可用 NewMappingFilter 自定义
//
// 分词器（analyzer，由 NewChainAnalyzer 组合三级组件）：
//
//	standard    standard 切词器 + lowercase（与历史行为一致）
//	keyword     keyword 切词器（整词，不转小写）
//	whitespace  whitespace 切词器 + lowercase
//	stop        standard 切词器 + lowercase + stop（去停用词后位置跳号）
package analyzer

import (
	"strings"

	"github.com/FalconEngine/falcon/analysis"
	"github.com/FalconEngine/falcon/plugin"
)

// ---------- 切词器 ----------

// standardTokenizer 标准切词器：ASCII 字母/数字成词，CJK 单字（实现见 analysis 包）
type standardTokenizer struct{}

func (standardTokenizer) Name() string { return "standard" }

func (standardTokenizer) Tokenize(s string) []plugin.Token {
	words := analysis.Tokenize(s)
	toks := make([]plugin.Token, 0, len(words))
	for i, w := range words {
		toks = append(toks, plugin.Token{Term: w, Position: i})
	}
	return toks
}

// keywordTokenizer 整词切词器：整个输入作为单个 token
type keywordTokenizer struct{}

func (keywordTokenizer) Name() string { return "keyword" }

func (keywordTokenizer) Tokenize(s string) []plugin.Token {
	if s == "" {
		return nil
	}
	return []plugin.Token{{Term: s, Position: 0}}
}

// whitespaceTokenizer 空白切词器：按空白切分，不做任何归一化
type whitespaceTokenizer struct{}

func (whitespaceTokenizer) Name() string { return "whitespace" }

func (whitespaceTokenizer) Tokenize(s string) []plugin.Token {
	words := strings.Fields(s)
	toks := make([]plugin.Token, 0, len(words))
	for i, w := range words {
		toks = append(toks, plugin.Token{Term: w, Position: i})
	}
	return toks
}

// ---------- 词元过滤器 ----------

// lowercaseFilter 转小写词元过滤器（Unicode 感知；CJK 等无大小写字符不受影响）
type lowercaseFilter struct{}

func (lowercaseFilter) Name() string { return "lowercase" }

func (lowercaseFilter) Filter(toks []plugin.Token) []plugin.Token {
	for i := range toks {
		toks[i].Term = strings.ToLower(toks[i].Term)
	}
	return toks
}

// stopFilter 停用词词元过滤器：移除停用词，保留其余 token 的 Position（跳号），
// 使短语查询能感知被删除词元占用的位置（与 ES 的 stop filter 语义一致）
type stopFilter struct {
	words map[string]bool
}

func (stopFilter) Name() string { return "stop" }

func (f stopFilter) Filter(toks []plugin.Token) []plugin.Token {
	out := toks[:0] // 原地过滤：保持相对顺序与 Position 不变
	for _, t := range toks {
		if !f.words[t.Term] {
			out = append(out, t)
		}
	}
	return out
}

// NewStopFilter 以指定停用词表构造停用词词元过滤器
func NewStopFilter(words []string) plugin.TokenFilter {
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return stopFilter{words: m}
}

// englishStopWords 内置英文停用词表（Lucene/ES 默认英语停用词集）
var englishStopWords = []string{
	"a", "an", "and", "are", "as", "at", "be", "but", "by",
	"for", "if", "in", "into", "is", "it",
	"no", "not", "of", "on", "or", "such",
	"that", "the", "their", "then", "there", "these",
	"they", "this", "to", "was", "will", "with",
}

// ---------- 字符过滤器 ----------

// mappingFilter 字符映射字符过滤器：分词前把原文中的 key 子串替换为 value
type mappingFilter struct {
	replacer *strings.Replacer
}

func (mappingFilter) Name() string { return "mapping" }

func (f mappingFilter) Filter(s string) string { return f.replacer.Replace(s) }

// NewMappingFilter 以指定映射表构造字符映射字符过滤器
func NewMappingFilter(mappings map[string]string) plugin.CharFilter {
	pairs := make([]string, 0, 2*len(mappings))
	for old, new := range mappings {
		pairs = append(pairs, old, new)
	}
	return mappingFilter{replacer: strings.NewReplacer(pairs...)}
}

func init() {
	// 三级组件注册
	plugin.RegisterTokenizer(standardTokenizer{})
	plugin.RegisterTokenizer(keywordTokenizer{})
	plugin.RegisterTokenizer(whitespaceTokenizer{})
	plugin.RegisterTokenFilter(lowercaseFilter{})
	plugin.RegisterTokenFilter(NewStopFilter(englishStopWords))
	plugin.RegisterCharFilter(NewMappingFilter(map[string]string{"ph": "f", "qu": "kw"}))

	// 组合分词器注册（standard 与历史行为一致：切词 + 转小写，只是拆成了链）
	std := standardTokenizer{}
	lower := lowercaseFilter{}
	plugin.RegisterAnalyzer("standard", plugin.NewChainAnalyzer(nil, std, []plugin.TokenFilter{lower}))
	plugin.RegisterAnalyzer("keyword", plugin.NewChainAnalyzer(nil, keywordTokenizer{}, nil))
	plugin.RegisterAnalyzer("whitespace", plugin.NewChainAnalyzer(nil, whitespaceTokenizer{}, []plugin.TokenFilter{lower}))
	plugin.RegisterAnalyzer("stop", plugin.NewChainAnalyzer(nil, std,
		[]plugin.TokenFilter{lower, NewStopFilter(englishStopWords)}))
}
