package agg

import (
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"

	_ "github.com/FalconEngine/falcon/plugins/fieldtype"
)

// testCtx 构造测试用解析上下文：tag(keyword)、level(number)
func testCtx() *plugin.ParseContext {
	fields := map[string]string{"tag": "keyword", "level": "number"}
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

func TestBuiltinAggs(t *testing.T) {
	ctx := testCtx()

	// 字符串类聚合：terms / cardinality（两段式合并一致性由套件验证）
	for _, name := range []string{"terms", "cardinality"} {
		t.Run(name, func(t *testing.T) {
			p, err := plugin.GetAgg(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunAggSuite(t, p, ctx, `{"field":"tag"}`, nil,
				[]string{"a", "b", "a", "c", "b", "a"})
		})
	}

	// 数值类聚合：min / max / avg / sum
	for _, name := range []string{"min", "max", "avg", "sum"} {
		t.Run(name, func(t *testing.T) {
			p, err := plugin.GetAgg(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunAggSuite(t, p, ctx, `{"field":"level"}`,
				[]int64{3, 1, 4, 1, 5, 9, 2, 6}, nil)
		})
	}

	// 数值聚合拒绝非正排字段
	p, _ := plugin.GetAgg("avg")
	if _, err := p.Parse(json_raw(`{"field":"tag"}`), ctx); err == nil {
		t.Error("avg 作用于 keyword 字段应报错")
	}
}

func TestNumAggResults(t *testing.T) {
	ctx := testCtx()
	cases := []struct {
		name string
		want string
	}{
		{"min", `{"value":1}`},
		{"max", `{"value":9}`},
		{"sum", `{"value":27}`},
		{"avg", `{"value":4.5}`},
	}
	for _, c := range cases {
		p, _ := plugin.GetAgg(c.name)
		spec, err := p.Parse(json_raw(`{"field":"level"}`), ctx)
		if err != nil {
			t.Fatal(err)
		}
		ag := p.New(spec)
		for _, v := range []int64{3, 1, 4, 9, 5, 5} {
			ag.CollectNum(v)
		}
		if got := string(ag.Result()); got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
	}

	// 空数据集：min/max/avg 为 null，sum 为 0
	p, _ := plugin.GetAgg("min")
	spec, _ := p.Parse(json_raw(`{"field":"level"}`), ctx)
	if got := string(p.New(spec).Result()); got != `{"value":null}` {
		t.Errorf("空 min = %s, want null", got)
	}
	p, _ = plugin.GetAgg("sum")
	spec, _ = p.Parse(json_raw(`{"field":"level"}`), ctx)
	if got := string(p.New(spec).Result()); got != `{"value":0}` {
		t.Errorf("空 sum = %s, want 0", got)
	}
}

func json_raw(s string) []byte { return []byte(s) }
