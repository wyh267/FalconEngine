package revtext

import (
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"
)

// TestReversePlugin 用契约套件验证 demo 插件（分词器 + 各级组件 + 字段类型）
func TestReversePlugin(t *testing.T) {
	a, err := plugin.GetAnalyzer("reverse")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunAnalyzerSuite(t, a)

	tk, err := plugin.GetTokenizer("reverse")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunTokenizerSuite(t, tk)

	tf, err := plugin.GetTokenFilter("reverse")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunTokenFilterSuite(t, tf)

	// 翻转行为本身
	got := a.Analyze("hello 世界")
	if len(got) != 2 || got[0].Term != "olleh" || got[1].Term != "界世" ||
		got[0].Position != 0 || got[1].Position != 1 {
		t.Errorf("reverse 分析 = %v, want [olleh@0 界世@1]", got)
	}

	p, err := plugin.GetFieldType("reverse_text")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunFieldTypeSuite(t, p, []string{`"abc"`}, []string{`123`})
}
