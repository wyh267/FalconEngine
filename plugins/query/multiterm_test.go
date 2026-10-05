package query

import (
	"reflect"
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
)

// expandCall 记录一次 ExpandTerms 调用的参数
type expandCall struct {
	field         string
	prefix        string
	maxExpansions int
}

// hookCtx 在 testCtx 基础上挂词典展开钩子：canned 为模拟词典，
// 行为与引擎 expandTermsLocked 契约一致（prefix 缩范围 + match 过滤 + 上限截断）
func hookCtx(canned []string, call *expandCall) *plugin.ParseContext {
	ctx := testCtx()
	ctx.ExpandTerms = func(field, prefix string, match func(string) bool, maxExpansions int) []string {
		*call = expandCall{field: field, prefix: prefix, maxExpansions: maxExpansions}
		var out []string
		for _, term := range canned {
			if prefix != "" && !strings.HasPrefix(term, prefix) {
				continue
			}
			if match != nil && !match(term) {
				continue
			}
			out = append(out, term)
			if len(out) >= maxExpansions {
				break
			}
		}
		return out
	}
	return ctx
}

// TestPrefixParserExpand prefix 解析期展开：前缀透传为扫描范围，产出 MultiTermNode
func TestPrefixParserExpand(t *testing.T) {
	var call expandCall
	ctx := hookCtx([]string{"ERR-01", "ERR-02", "WARN-01"}, &call)
	p, _ := plugin.GetQuery("prefix")

	n, err := p.Parse(json_raw(`{"tag":"ERR"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	mt, ok := n.(plugin.MultiTermNode)
	if !ok || mt.Field != "tag" {
		t.Fatalf("prefix 应解析为 MultiTermNode, got %#v", n)
	}
	if !reflect.DeepEqual(mt.Terms, []string{"ERR-01", "ERR-02"}) {
		t.Errorf("展开 terms = %v, want [ERR-01 ERR-02]", mt.Terms)
	}
	if call.field != "tag" || call.prefix != "ERR" || call.maxExpansions != maxMultiTermExpansions {
		t.Errorf("ExpandTerms 调用参数 = %+v", call)
	}

	// 无展开钩子（协调层纯 schema 解析）：产出未展开节点而非报错
	n, err = p.Parse(json_raw(`{"tag":"ERR"}`), testCtx())
	if err != nil {
		t.Fatal(err)
	}
	if mt := n.(plugin.MultiTermNode); len(mt.Terms) != 0 {
		t.Errorf("无钩子时 Terms 应为空, got %v", mt.Terms)
	}
}

// TestWildcardParserExpand wildcard：首字符非通配走字面前缀缩范围，否则全词典扫描
func TestWildcardParserExpand(t *testing.T) {
	var call expandCall
	canned := []string{"ERR-01", "ERR-02", "ERR-11", "WARN-01"}
	ctx := hookCtx(canned, &call)
	p, _ := plugin.GetQuery("wildcard")

	// 字面前缀缩范围
	n, err := p.Parse(json_raw(`{"tag":"ERR-0*"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	mt := n.(plugin.MultiTermNode)
	if !reflect.DeepEqual(mt.Terms, []string{"ERR-01", "ERR-02"}) {
		t.Errorf("ERR-0* 展开 = %v, want [ERR-01 ERR-02]", mt.Terms)
	}
	if call.prefix != "ERR-0" {
		t.Errorf("字面前缀应为 ERR-0, got %q", call.prefix)
	}

	// 首字符为通配符：全词典扫描（空前缀）+ 模式过滤
	n, err = p.Parse(json_raw(`{"tag":"*-01"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	mt = n.(plugin.MultiTermNode)
	if !reflect.DeepEqual(mt.Terms, []string{"ERR-01", "WARN-01"}) {
		t.Errorf("*-01 展开 = %v, want [ERR-01 WARN-01]", mt.Terms)
	}
	if call.prefix != "" {
		t.Errorf("首字符通配时前缀应为空（全词典扫描）, got %q", call.prefix)
	}

	// ? 单字符通配
	n, err = p.Parse(json_raw(`{"tag":"ERR-0?"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if mt = n.(plugin.MultiTermNode); !reflect.DeepEqual(mt.Terms, []string{"ERR-01", "ERR-02"}) {
		t.Errorf("ERR-0? 展开 = %v, want [ERR-01 ERR-02]", mt.Terms)
	}
}

// TestWildcardMatch 通配符匹配单元测试（含 CJK rune 语义）
func TestWildcardMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"", "", true},
		{"", "a", false},
		{"*", "", true},
		{"*", "anything", true},
		{"**", "ab", true},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "ab", false},
		{"a*c", "abbc", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"a?c", "abbc", false},
		{"*a*b*", "xaYbZ", true},
		{"*a*b*", "xay", false},
		{"ERR-*", "ERR-01", true},
		{"ERR-0?", "ERR-01", true},
		{"ERR-0?", "ERR-1", false},
		// CJK 按 rune 匹配
		{"你好*", "你好世界", true},
		{"你?界", "你好界", true},
		{"你?界", "你好世界", false},
	}
	for _, c := range cases {
		if got := wildcardMatch(c.pattern, c.s); got != c.want {
			t.Errorf("wildcardMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

// TestLiteralPrefix 字面前缀提取
func TestLiteralPrefix(t *testing.T) {
	cases := map[string]string{
		"ERR*":   "ERR",
		"*ERR":   "",
		"ER?R*":  "ER",
		"abc":    "abc",
		"你好*":    "你好",
		"?":      "",
		"a*b?c*": "a",
	}
	for pattern, want := range cases {
		if got := literalPrefix(pattern); got != want {
			t.Errorf("literalPrefix(%q) = %q, want %q", pattern, got, want)
		}
	}
}
