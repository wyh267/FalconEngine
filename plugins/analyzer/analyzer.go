// Package analyzer 内置分词器插件。
//
//	standard   默认分词器（analysis.Tokenize：ASCII 成词小写化，CJK 单字）
//	keyword    整词不分词
//	whitespace 按空白切分
package analyzer

import (
	"strings"

	"github.com/FalconEngine/falcon/analysis"
	"github.com/FalconEngine/falcon/plugin"
)

type standard struct{}

func (standard) Tokenize(s string) []string { return analysis.Tokenize(s) }

type keyword struct{}

func (keyword) Tokenize(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

type whitespace struct{}

func (whitespace) Tokenize(s string) []string { return strings.Fields(s) }

func init() {
	plugin.RegisterAnalyzer("standard", standard{})
	plugin.RegisterAnalyzer("keyword", keyword{})
	plugin.RegisterAnalyzer("whitespace", whitespace{})
}
