// Package revtext 是一个演示用的外部插件：把每个词按字符翻转后再建索引。
//
// 演示进程内插件模式（同 database/sql 驱动）：
//  1. 实现 plugin.Analyzer / plugin.FieldTypePlugin 接口
//  2. 在 init() 中注册
//  3. 使用方 import 本包即生效（见 examples/custom-plugin/main.go）
//
// 注册内容：
//   - 分词器 "reverse"：按空白切词后把每个词翻转（"hello" -> "olleh"）
//   - 字段类型 "reverse_text"：倒排 + norms，使用 "reverse" 分词器
package revtext

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
)

// reverseAnalyzer 翻转分词器
type reverseAnalyzer struct{}

func (reverseAnalyzer) Tokenize(s string) []string {
	words := strings.Fields(s)
	out := make([]string, 0, len(words))
	for _, w := range words {
		r := []rune(w)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		out = append(out, string(r))
	}
	return out
}

// reverseTextType 翻转文本字段类型
type reverseTextType struct{}

func (reverseTextType) Name() string     { return "reverse_text" }
func (reverseTextType) Analyzer() string { return "reverse" }
func (reverseTextType) Inverted() bool   { return true }
func (reverseTextType) DocValues() bool  { return false }
func (reverseTextType) HasNorms() bool   { return true }

func (reverseTextType) Parse(v json.RawMessage) (plugin.Value, error) {
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return plugin.Value{}, fmt.Errorf("reverse_text 字段应为字符串: %w", err)
	}
	return plugin.Value{Text: s}, nil
}

func init() {
	plugin.RegisterAnalyzer("reverse", reverseAnalyzer{})
	plugin.RegisterFieldType(reverseTextType{})
}
