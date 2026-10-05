package query

import (
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"

	// 解析依赖字段类型与分词器注册表
	_ "github.com/FalconEngine/falcon/plugins/analyzer"
	_ "github.com/FalconEngine/falcon/plugins/fieldtype"
)

// testCtx 构造测试用解析上下文：content(text)、tag(keyword)、level(number)、ts(date)
func testCtx() *plugin.ParseContext {
	fields := map[string]string{
		"content": "text",
		"tag":     "keyword",
		"level":   "number",
		"ts":      "date",
	}
	return &plugin.ParseContext{
		FieldPlugin: func(field string) (plugin.FieldTypePlugin, bool) {
			ft, ok := fields[field]
			if !ok {
				return nil, false
			}
			p, err := plugin.GetFieldType(ft)
			if err != nil {
				return nil, false
			}
			return p, true
		},
	}
}

func TestBuiltinQueryParsers(t *testing.T) {
	ctx := testCtx()
	cases := []struct {
		name    string
		valid   []string
		invalid []string
	}{
		{"match", []string{`{"content":"雅礼"}`, `{"content":{"query":"雅礼","operator":"and"}}`},
			[]string{`{"level":"x"}`, `{"content":{"operator":"xor"}}`, `{"nope":"x"}`}},
		{"term", []string{`{"tag":"黄V"}`, `{"level":18}`, `{"ts":"2024-01-02"}`},
			[]string{`{"tag":123}`, `{"level":"abc"}`, `{"nope":"x"}`}},
		{"terms", []string{`{"tag":["黄V","蓝V"]}`, `{"level":[1,2]}`},
			[]string{`{"tag":[]}`, `{"tag":"黄V"}`}},
		{"range", []string{`{"level":{"gte":1,"lte":9}}`, `{"ts":{"gte":"2013-08-18"}}`, `{"level":{"gt":1}}`},
			[]string{`{"level":{}}`, `{"tag":{"gte":1}}`, `{"level":{"between":1}}`}},
		{"bool", []string{`{"must":[{"match":{"content":"雅礼"}}],"filter":[{"term":{"tag":"黄V"}}]}`},
			[]string{`{"must":[{"nope":{}}]}`}},
		{"match_all", []string{`{}`}, nil},
		{"ids", []string{`{"values":["a","b"]}`}, []string{`{"values":"a"}`}},
		{"match_phrase", []string{`{"content":"雅礼 中学"}`, `{"content":{"query":"雅礼 中学","slop":0}}`},
			[]string{`{"level":{"query":"1"}}`, `{"content":{"query":"a b","slop":1}}`, `{"content":{"query":"a","slop":-1}}`, `{"nope":"x"}`}},
		{"multi_match", []string{`{"query":"x","fields":["content^3","tag"]}`, `{"query":"x","fields":["content"],"operator":"and"}`},
			[]string{`{"query":"","fields":["content"]}`, `{"query":"x","fields":[]}`, `{"query":"x","fields":["level"]}`,
				`{"query":"x","fields":["content^0"]}`, `{"query":"x","fields":["content^z"]}`, `{"query":"x","fields":["nope"]}`,
				`{"query":"x","fields":["content"],"operator":"xor"}`}},
		{"exists", []string{`{"field":"content"}`},
			[]string{`{}`, `{"field":""}`, `{"field":"nope"}`}},
		{"prefix", []string{`{"tag":"ERR"}`, `{"content":"hel"}`},
			[]string{`{"level":"1"}`, `{"tag":123}`, `{"nope":"x"}`}},
		{"wildcard", []string{`{"tag":"ERR*"}`, `{"tag":"*E?R"}`},
			[]string{`{"tag":""}`, `{"level":"1*"}`, `{"tag":1}`}},
		{"fuzzy", []string{`{"tag":"ERR"}`, `{"tag":{"value":"ERR","fuzziness":1,"max_expansions":10}}`},
			[]string{`{"tag":{"value":"ERR","fuzziness":3}}`, `{"tag":{"value":"ERR","max_expansions":0}}`, `{"level":"x"}`, `{"tag":123}`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := plugin.GetQuery(c.name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunQueryParserSuite(t, p, ctx, c.valid, c.invalid)
		})
	}
}

func TestTermNodeShapes(t *testing.T) {
	ctx := testCtx()
	p, _ := plugin.GetQuery("term")

	// keyword -> TermNode
	n, err := p.Parse(json_raw(`{"tag":"黄V"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tn, ok := n.(plugin.TermNode); !ok || tn.Field != "tag" || tn.Term != "黄V" {
		t.Errorf("keyword term 应解析为 TermNode, got %#v", n)
	}

	// number -> 等值 RangeNode
	n, err = p.Parse(json_raw(`{"level":18}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	rn, ok := n.(plugin.RangeNode)
	if !ok || rn.Field != "level" || rn.Min != 18 || rn.Max != 18 || !rn.HasMin || !rn.HasMax {
		t.Errorf("number term 应解析为等值 RangeNode, got %#v", n)
	}
}

func TestBoolRecursion(t *testing.T) {
	ctx := testCtx()
	p, _ := plugin.GetQuery("bool")
	n, err := p.Parse(json_raw(`{"must":[{"bool":{"should":[{"term":{"tag":"a"}},{"term":{"tag":"b"}}]}}]}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	bn := n.(plugin.BoolNode)
	inner, ok := bn.Must[0].(plugin.BoolNode)
	if !ok || len(inner.Should) != 2 {
		t.Fatalf("bool 递归解析失败: %#v", n)
	}
}

func json_raw(s string) []byte { return []byte(s) }

// TestMatchPhraseShapes match_phrase 解析形态：简写/完整/slop 校验
func TestMatchPhraseShapes(t *testing.T) {
	ctx := testCtx()
	p, _ := plugin.GetQuery("match_phrase")

	n, err := p.Parse(json_raw(`{"content":"quick brown"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	pn, ok := n.(plugin.PhraseNode)
	if !ok || pn.Field != "content" || pn.Text != "quick brown" || pn.Slop != 0 {
		t.Errorf("简写应解析为 PhraseNode, got %#v", n)
	}

	n, err = p.Parse(json_raw(`{"content":{"query":"a b","slop":0}}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pn := n.(plugin.PhraseNode); pn.Text != "a b" || pn.Slop != 0 {
		t.Errorf("完整形态解析错误: %#v", pn)
	}

	if _, err := p.Parse(json_raw(`{"content":{"query":"a b","slop":2}}`), ctx); err == nil ||
		!strings.Contains(err.Error(), "暂未支持") {
		t.Errorf("slop>0 应报\"暂未支持\", got %v", err)
	}
}

// TestMultiMatchShapes multi_match 展开为 BoolNode{Should: [MatchNode...Boost]}
func TestMultiMatchShapes(t *testing.T) {
	ctx := testCtx()
	p, _ := plugin.GetQuery("multi_match")

	n, err := p.Parse(json_raw(`{"query":"apple","fields":["content^3","tag"],"operator":"and"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	bn, ok := n.(plugin.BoolNode)
	if !ok || len(bn.Should) != 2 {
		t.Fatalf("multi_match 应展开为含 2 个 should 的 BoolNode, got %#v", n)
	}
	m0 := bn.Should[0].(plugin.MatchNode)
	m1 := bn.Should[1].(plugin.MatchNode)
	if m0.Field != "content" || m0.Boost != 3 || m0.Text != "apple" || m0.Operator != "and" {
		t.Errorf("content^3 展开错误: %#v", m0)
	}
	if m1.Field != "tag" || m1.Boost != 1 {
		t.Errorf("无 ^ 后缀 boost 应为 1: %#v", m1)
	}
}

// TestParseFieldBoost "field^boost" 解析
func TestParseFieldBoost(t *testing.T) {
	cases := []struct {
		spec  string
		field string
		boost float64
		ok    bool
	}{
		{"title", "title", 1, true},
		{"title^3", "title", 3, true},
		{"title^0.5", "title", 0.5, true},
		{"title^", "", 0, false},
		{"title^0", "", 0, false},
		{"title^-1", "", 0, false},
		{"title^x", "", 0, false},
		{"^2", "", 0, false},
	}
	for _, c := range cases {
		f, b, err := parseFieldBoost(c.spec)
		if (err == nil) != c.ok {
			t.Errorf("parseFieldBoost(%q) err = %v, ok 应为 %v", c.spec, err, c.ok)
			continue
		}
		if c.ok && (f != c.field || b != c.boost) {
			t.Errorf("parseFieldBoost(%q) = %q, %v，期望 %q, %v", c.spec, f, b, c.field, c.boost)
		}
	}
}

// TestExistsShape exists 解析形态
func TestExistsShape(t *testing.T) {
	ctx := testCtx()
	p, _ := plugin.GetQuery("exists")
	n, err := p.Parse(json_raw(`{"field":"tag"}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if en, ok := n.(plugin.ExistsNode); !ok || en.Field != "tag" {
		t.Errorf("exists 应解析为 ExistsNode, got %#v", n)
	}
}
