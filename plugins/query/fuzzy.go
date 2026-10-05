// fuzzy 查询：字段词典全量扫描 + Levenshtein 编辑距离过滤（上限 2，对齐 ES）。
// 编辑距离实现内联于本文件，不引外部依赖；扫描为 O(词典)，
// 以 max_expansions（默认 50）截断兜底，不承诺大词典性能。
package query

import (
	"encoding/json"
	"fmt"

	"github.com/FalconEngine/falcon/plugin"
)

// defaultFuzzyMaxExpansions fuzzy 的默认展开上限（比 prefix/wildcard 小：
// 全词典扫描 + 逐 term 距离计算更贵）
const defaultFuzzyMaxExpansions = 50

type fuzzyParser struct{}

func (fuzzyParser) Name() string { return "fuzzy" }

func (fuzzyParser) Parse(body json.RawMessage, ctx *plugin.ParseContext) (plugin.QNode, error) {
	field, v, err := oneField(body)
	if err != nil {
		return nil, fmt.Errorf("fuzzy: %w", err)
	}
	fp, err := fieldPlugin(ctx, field)
	if err != nil {
		return nil, fmt.Errorf("fuzzy: %w", err)
	}
	if !fp.Inverted() {
		return nil, fmt.Errorf("fuzzy: 字段 %q 不是倒排字段", field)
	}

	// 支持简写 {"tag": "ERR"} 与完整
	// {"tag": {"value": "ERR", "fuzziness": 2, "max_expansions": 50}}
	value := ""
	fuzziness := 2
	maxExp := defaultFuzzyMaxExpansions
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		value = s
	} else {
		var opts struct {
			Value         string `json:"value"`
			Fuzziness     *int   `json:"fuzziness"`
			MaxExpansions *int   `json:"max_expansions"`
		}
		if err := json.Unmarshal(v, &opts); err != nil {
			return nil, fmt.Errorf("fuzzy: 字段 %q 的值应为字符串或对象: %w", field, err)
		}
		value = opts.Value
		if opts.Fuzziness != nil {
			fuzziness = *opts.Fuzziness
		}
		if opts.MaxExpansions != nil {
			maxExp = *opts.MaxExpansions
		}
	}
	if fuzziness < 0 || fuzziness > 2 {
		return nil, fmt.Errorf("fuzzy: fuzziness 仅支持 0~2（对齐 ES 上限）, got %d", fuzziness)
	}
	if maxExp <= 0 {
		return nil, fmt.Errorf("fuzzy: max_expansions 应 > 0, got %d", maxExp)
	}
	terms := expandMultiTerm(ctx, field, "", func(term string) bool {
		return levenshteinWithin(value, term, fuzziness)
	}, maxExp)
	return plugin.MultiTermNode{Field: field, Terms: terms}, nil
}

// levenshteinWithin 判断 a 与 b 的 Levenshtein 编辑距离是否 ≤ maxDist。
// 按 rune 计（CJK term 以字符为单位）；两行动态规划，某行最小值超过 maxDist 即提前退出。
func levenshteinWithin(a, b string, maxDist int) bool {
	ra, rb := []rune(a), []rune(b)
	// 长度差超过 maxDist 必然不命中
	if d := len(ra) - len(rb); d > maxDist || -d > maxDist {
		return false
	}
	// prev[j] = a 的前 i 个字符与 b 的前 j 个字符的编辑距离（i=0 时即 j）
	prev := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur := make([]int, len(rb)+1)
		cur[0] = i
		rowMin := i
		for j := 1; j <= len(rb); j++ {
			cost := 0
			if ra[i-1] != rb[j-1] {
				cost = 1
			}
			cur[j] = min(cur[j-1]+1, min(prev[j]+1, prev[j-1]+cost))
			rowMin = min(rowMin, cur[j])
		}
		if rowMin > maxDist {
			return false
		}
		prev = cur
	}
	return prev[len(rb)] <= maxDist
}
