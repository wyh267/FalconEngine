// multi-term 类查询（prefix/wildcard）：解析期经 ParseContext.ExpandTerms 对字段词典
// （内存缓冲 + 各段有序字典）展开为 term 列表，产出 plugin.MultiTermNode（并集、恒定 1 分、
// filter 语义）。词典扫描为 O(词典)，超出 maxMultiTermExpansions 截断，
// 不承诺大词典性能（与 ES 对高基数字典的取舍一致）。
package query

import (
	"encoding/json"
	"fmt"

	"github.com/FalconEngine/falcon/plugin"
)

// maxMultiTermExpansions prefix/wildcard 的词典展开上限（fuzzy 另有更小的默认值）
const maxMultiTermExpansions = 1024

// expandMultiTerm 调用解析上下文的词典展开能力；上下文无展开能力（协调层纯 schema 解析）
// 时返回 nil——该产物不会被协调层执行，分片侧重新解析时才真正展开
func expandMultiTerm(ctx *plugin.ParseContext, field, prefix string, match func(string) bool, maxExpansions int) []string {
	if ctx.ExpandTerms == nil {
		return nil
	}
	return ctx.ExpandTerms(field, prefix, match, maxExpansions)
}

// multiTermField 解析 {"<field>": "<值>"} 形式的子句体并校验字段为倒排字段
func multiTermField(clause string, body json.RawMessage, ctx *plugin.ParseContext) (string, string, error) {
	field, v, err := oneField(body)
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", clause, err)
	}
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", clause, err)
	}
	if !fp.Inverted() {
		return "", "", fmt.Errorf("%s: 字段 %q 不是倒排字段", clause, field)
	}
	var value string
	if err := json.Unmarshal(v, &value); err != nil {
		return "", "", fmt.Errorf("%s: 字段 %q 的值应为字符串: %w", clause, field, err)
	}
	return field, value, nil
}

// ---------- prefix ----------

type prefixParser struct{}

func (prefixParser) Name() string { return "prefix" }

func (prefixParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, prefix, err := multiTermField("prefix", body, ctx)
	if err != nil {
		return nil, err
	}
	// 前缀本身即扫描范围（段侧借稀疏索引定位起始块），无需额外匹配条件
	terms := expandMultiTerm(ctx, field, prefix, nil, maxMultiTermExpansions)
	return plugin.MultiTermNode{Field: field, Terms: terms}, nil
}

// ---------- wildcard ----------

type wildcardParser struct{}

func (wildcardParser) Name() string { return "wildcard" }

func (wildcardParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, pattern, err := multiTermField("wildcard", body, ctx)
	if err != nil {
		return nil, err
	}
	if pattern == "" {
		return nil, fmt.Errorf("wildcard: 字段 %q 的模式不能为空", field)
	}
	// 首字符非通配符时取字面前缀借稀疏索引缩小扫描范围，否则全词典扫描
	terms := expandMultiTerm(ctx, field, literalPrefix(pattern), func(term string) bool {
		return wildcardMatch(pattern, term)
	}, maxMultiTermExpansions)
	return plugin.MultiTermNode{Field: field, Terms: terms}, nil
}

// literalPrefix 取模式中首个通配符（* 或 ?）之前的字面前缀（按 rune 截取，UTF-8 安全）
func literalPrefix(pattern string) string {
	for i, r := range pattern {
		if r == '*' || r == '?' {
			return pattern[:i]
		}
	}
	return pattern
}

// wildcardMatch 通配符匹配：* 匹配任意（含空）字符序列，? 匹配单个字符（按 rune 计）。
// 双指针 + 星号回溯的线性摊还实现。
func wildcardMatch(pattern, s string) bool {
	p, t := []rune(pattern), []rune(s)
	px, tx := 0, 0
	star, starTx := -1, 0 // 最近一个 * 的位置及它当时吞到的文本位置
	for tx < len(t) {
		if px < len(p) && (p[px] == '?' || p[px] == t[tx]) {
			px++
			tx++
			continue
		}
		if px < len(p) && p[px] == '*' {
			star, starTx = px, tx
			px++
			continue
		}
		if star >= 0 {
			// 回溯：让最近的 * 多吞一个字符后重试
			starTx++
			px, tx = star+1, starTx
			continue
		}
		return false
	}
	for px < len(p) && p[px] == '*' {
		px++
	}
	return px == len(p)
}
