package fieldtype

import (
	"encoding/json"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"

	// 字段类型校验依赖分词器注册表
	_ "github.com/FalconEngine/falcon/plugins/analyzer"
)

func TestBuiltinFieldTypes(t *testing.T) {
	cases := []struct {
		name    string
		valid   []string
		invalid []string
	}{
		{"text", []string{`"abc"`, `"中文 文本"`}, []string{`123`, `true`, `{}`}},
		{"keyword", []string{`"黄V"`, `""`}, []string{`1.5`, `[]`}},
		{"number", []string{`18`, `-3`, `0`}, []string{`"abc"`, `1.5`, `true`}},
		{"date", []string{`"2024-01-02 03:04:05"`, `"2024-01-02"`}, []string{`"2024/01/01"`, `123`, `"abc"`}},
		{"bool", []string{`true`, `false`}, []string{`"true"`, `1`}},
		{"stored", []string{`"anything"`, `123`, `{"a":1}`}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := plugin.GetFieldType(c.name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunFieldTypeSuite(t, p, c.valid, c.invalid)
		})
	}

	// 索引能力声明检查
	want := map[string]struct {
		inv   bool
		dv    plugin.DVKind
		norms bool
	}{
		"text":    {true, plugin.DVNone, true},
		"keyword": {true, plugin.DVKeyword, false},
		"number":  {false, plugin.DVNum, false},
		"date":    {false, plugin.DVNum, false},
		"bool":    {false, plugin.DVNum, false},
		"stored":  {false, plugin.DVNone, false},
	}
	for name, w := range want {
		p, _ := plugin.GetFieldType(name)
		if p.Inverted() != w.inv || p.DocValuesKind() != w.dv || p.HasNorms() != w.norms {
			t.Errorf("%s 能力声明 = (%v,%v,%v), want %v", name, p.Inverted(), p.DocValuesKind(), p.HasNorms(), w)
		}
	}

	// date 解析为 unix 秒
	d, _ := plugin.GetFieldType("date")
	v1, _ := d.Parse(json.RawMessage(`"2024-01-02"`))
	v2, _ := d.Parse(json.RawMessage(`"2024-01-02 03:04:05"`))
	if v2.Num-v1.Num != 3*3600+4*60+5 {
		t.Errorf("date 解析差值 = %d, want %d", v2.Num-v1.Num, 3*3600+4*60+5)
	}
}
