package query

import (
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
