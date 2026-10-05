// Package revtext 是一个演示用的外部插件：把每个词按字符翻转后再建索引。
//
// 演示进程内插件模式（同 database/sql 驱动）：
//  1. 实现 plugin.Tokenizer 等分析链组件接口，用 plugin.NewChainAnalyzer 组合分词器
//  2. 在 init() 中注册
//  3. 使用方 import 本包即生效（见 examples/custom-plugin/main.go）
//
// 注册内容：
//   - 切词器 "reverse"：按空白切词（保留原词，不做归一化）
//   - 分词器 "reverse"：reverse 切词器 + 翻转词元过滤器（"hello" -> "olleh"）
//   - 字段类型 "reverse_text"：倒排 + norms，使用 "reverse" 分词器
package revtext

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
)

// reverseTokenizer 空白切词器（演示自定义切词器组件）
type reverseTokenizer struct{}

func (reverseTokenizer) Name() string { return "reverse" }

func (reverseTokenizer) Tokenize(s string) []plugin.Token {
	words := strings.Fields(s)
	toks := make([]plugin.Token, 0, len(words))
	for i, w := range words {
		toks = append(toks, plugin.Token{Term: w, Position: i})
	}
	return toks
}

// reverseFilter 翻转词元过滤器：把每个词元按字符翻转（Position 不变）
type reverseFilter struct{}

func (reverseFilter) Name() string { return "reverse" }

func (reverseFilter) Filter(toks []plugin.Token) []plugin.Token {
	for i := range toks {
		r := []rune(toks[i].Term)
		for lo, hi := 0, len(r)-1; lo < hi; lo, hi = lo+1, hi-1 {
			r[lo], r[hi] = r[hi], r[lo]
		}
		toks[i].Term = string(r)
	}
	return toks
}

// reverseTextType 翻转文本字段类型
type reverseTextType struct{}

func (reverseTextType) Name() string                 { return "reverse_text" }
func (reverseTextType) Analyzer() string             { return "reverse" }
func (reverseTextType) Inverted() bool               { return true }
func (reverseTextType) DocValuesKind() plugin.DVKind { return plugin.DVNone }
func (reverseTextType) HasNorms() bool               { return true }

func (reverseTextType) Parse(v json.RawMessage) (plugin.Value, error) {
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return plugin.Value{}, fmt.Errorf("reverse_text 字段应为字符串: %w", err)
	}
	return plugin.Value{Text: s}, nil
}

func init() {
	plugin.RegisterTokenizer(reverseTokenizer{})
	plugin.RegisterTokenFilter(reverseFilter{})
	plugin.RegisterAnalyzer("reverse", plugin.NewChainAnalyzer(nil, reverseTokenizer{}, []plugin.TokenFilter{reverseFilter{}}))
	plugin.RegisterFieldType(reverseTextType{})
}
