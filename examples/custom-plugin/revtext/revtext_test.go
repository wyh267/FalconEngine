package revtext

import (
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"
)

// TestReversePlugin 用契约套件验证 demo 插件
func TestReversePlugin(t *testing.T) {
	a, err := plugin.GetAnalyzer("reverse")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunAnalyzerSuite(t, a)

	// 翻转行为本身
	got := a.Tokenize("hello 世界")
	if len(got) != 2 || got[0] != "olleh" || got[1] != "界世" {
		t.Errorf("reverse 分词 = %v, want [olleh 界世]", got)
	}

	p, err := plugin.GetFieldType("reverse_text")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunFieldTypeSuite(t, p, []string{`"abc"`}, []string{`123`})
}
