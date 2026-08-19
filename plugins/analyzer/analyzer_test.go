package analyzer

import (
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"
)

func TestBuiltinAnalyzers(t *testing.T) {
	for _, name := range []string{"standard", "keyword", "whitespace"} {
		t.Run(name, func(t *testing.T) {
			a, err := plugin.GetAnalyzer(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunAnalyzerSuite(t, a)
		})
	}

	// keyword 整词不分词
	kw, _ := plugin.GetAnalyzer("keyword")
	got := kw.Tokenize("hello world 世界")
	if len(got) != 1 || got[0] != "hello world 世界" {
		t.Errorf("keyword 应整词返回, got %v", got)
	}

	// whitespace 只按空白切
	ws, _ := plugin.GetAnalyzer("whitespace")
	got = ws.Tokenize("a b\tc\nd")
	if len(got) != 4 {
		t.Errorf("whitespace 应切出 4 词, got %v", got)
	}
}
