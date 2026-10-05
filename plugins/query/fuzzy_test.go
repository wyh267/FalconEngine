package query

import (
	"reflect"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
)

// TestLevenshteinWithin 编辑距离边界：距离 0/1/2/3 与阈值的关系（含 CJK rune 语义）
func TestLevenshteinWithin(t *testing.T) {
	cases := []struct {
		a, b    string
		maxDist int
		want    bool
	}{
		{"hello", "hello", 0, true},     // 距离 0
		{"hello", "hellp", 0, false},    // 距离 1 > 0
		{"hello", "hellp", 1, true},     // 替换，距离 1
		{"hello", "helo", 1, true},      // 删除，距离 1
		{"hello", "hexlp", 1, false},    // 距离 2 > 1
		{"hello", "hexlp", 2, true},     // 两次替换，距离 2
		{"kitten", "sitting", 2, false}, // 经典距离 3 > 2
		{"kitten", "sitting", 3, true},
		{"hxxpp", "hello", 2, false}, // 距离 4
		{"", "", 0, true},
		{"", "ab", 2, true}, // 空串：距离 = 长度差
		{"ab", "", 1, false},
		{"abc", "abcdef", 2, false}, // 长度差 3 > 2 提前剪枝
		{"abc", "abcdef", 3, true},
		// CJK 按 rune 计
		{"你好世界", "你好世", 1, true},
		{"你好世界", "你坏世", 2, true},
		{"你好世界", "你坏界", 1, false},
	}
	for _, c := range cases {
		if got := levenshteinWithin(c.a, c.b, c.maxDist); got != c.want {
			t.Errorf("levenshteinWithin(%q, %q, %d) = %v, want %v", c.a, c.b, c.maxDist, got, c.want)
		}
	}
}

// TestFuzzyParserExpand fuzzy 解析期展开：词典全量扫描 + 距离过滤 + 默认上限
func TestFuzzyParserExpand(t *testing.T) {
	var call expandCall
	canned := []string{"hello", "hellp", "hexlp", "world"}
	ctx := hookCtx(canned, &call)
	p, _ := plugin.GetQuery("fuzzy")

	// 简写：默认 fuzziness=2、max_expansions=50
	n, err := p.Parse(json_raw(`{"tag":"hello"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	mt, ok := n.(plugin.MultiTermNode)
	if !ok || mt.Field != "tag" {
		t.Fatalf("fuzzy 应解析为 MultiTermNode, got %#v", n)
	}
	if !reflect.DeepEqual(mt.Terms, []string{"hello", "hellp", "hexlp"}) {
		t.Errorf("默认 fuzziness=2 展开 = %v, want [hello hellp hexlp]", mt.Terms)
	}
	if call.prefix != "" || call.maxExpansions != defaultFuzzyMaxExpansions {
		t.Errorf("ExpandTerms 调用参数 = %+v, want 空前缀 + 默认上限 50", call)
	}

	// 完整形态：fuzziness=1 收窄
	n, err = p.Parse(json_raw(`{"tag":{"value":"hello","fuzziness":1,"max_expansions":10}}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if mt = n.(plugin.MultiTermNode); !reflect.DeepEqual(mt.Terms, []string{"hello", "hellp"}) {
		t.Errorf("fuzziness=1 展开 = %v, want [hello hellp]", mt.Terms)
	}
	if call.maxExpansions != 10 {
		t.Errorf("max_expansions 透传 = %d, want 10", call.maxExpansions)
	}
}
