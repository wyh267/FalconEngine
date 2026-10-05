// Package analysis 提供极简分词实现（标准切词器的纯实现库，不依赖 plugin 包）。
//
// 规则：连续的 ASCII 字母/数字合并为一个词（保留原始大小写，
// 转小写由分析链中的 lowercase 词元过滤器负责）；
// 其余非空白、非标点的字符（如 CJK 表意文字）按单字切分；
// 空白与标点仅作为分隔符，不产生 token。
package analysis

import "unicode"

// Tokenize 将文本切分为词序列，保持出现顺序（允许重复，用于统计词频）
func Tokenize(s string) []string {
	var tokens []string
	var word []rune // 当前累积的 ASCII 词

	flush := func() {
		if len(word) > 0 {
			tokens = append(tokens, string(word))
			word = word[:0]
		}
	}

	for _, r := range s {
		switch {
		case r < 128 && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			word = append(word, r)
		case unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r):
			flush()
		default:
			// CJK 等表意文字：先收尾 ASCII 词，再按单字输出
			flush()
			tokens = append(tokens, string(r))
		}
	}
	flush()
	return tokens
}
