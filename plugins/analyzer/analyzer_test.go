package analyzer

import (
	"reflect"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"
)

// termsOf 提取 token 序列的 Term 序列（便于对拍）
func termsOf(toks []plugin.Token) []string {
	if len(toks) == 0 {
		return nil
	}
	out := make([]string, 0, len(toks))
	for _, t := range toks {
		out = append(out, t.Term)
	}
	return out
}

func TestBuiltinAnalyzers(t *testing.T) {
	for _, name := range []string{"standard", "keyword", "whitespace", "stop"} {
		t.Run(name, func(t *testing.T) {
			a, err := plugin.GetAnalyzer(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunAnalyzerSuite(t, a)
		})
	}

	// keyword 整词不分词、不转小写
	kw, _ := plugin.GetAnalyzer("keyword")
	if got := kw.Analyze("Hello World 世界"); len(got) != 1 || got[0].Term != "Hello World 世界" {
		t.Errorf("keyword 应整词返回, got %v", got)
	}

	// whitespace 只按空白切，且经 lowercase 转小写
	ws, _ := plugin.GetAnalyzer("whitespace")
	if got := termsOf(ws.Analyze("A b\tc\nd")); !reflect.DeepEqual(got, []string{"a", "b", "c", "d"}) {
		t.Errorf("whitespace 应切出 4 个小写词, got %v", got)
	}

	// stop 分词器：停用词被移除且保留位置跳号
	st, _ := plugin.GetAnalyzer("stop")
	got := st.Analyze("the quick fox")
	if len(got) != 2 || got[0].Term != "quick" || got[1].Term != "fox" ||
		got[0].Position != 1 || got[1].Position != 2 {
		t.Errorf("stop 应产 [quick@1 fox@2], got %v", got)
	}
}

// TestStandardRegression standard 分词器回归：lowercase 拆为独立 token filter 后，
// 产出的 term 序列与历史单体式实现一致
func TestStandardRegression(t *testing.T) {
	std, _ := plugin.GetAnalyzer("standard")
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"hello world", []string{"hello", "world"}},
		{"Hello, World!", []string{"hello", "world"}},
		{"Go语言1.24发布", []string{"go", "语", "言", "1", "24", "发", "布"}},
		{"看山东，赞山东", []string{"看", "山", "东", "赞", "山", "东"}},
		{"  a\tb\nc  ", []string{"a", "b", "c"}},
	}
	for _, c := range cases {
		if got := termsOf(std.Analyze(c.in)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("standard.Analyze(%q) terms = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestBuiltinTokenizers(t *testing.T) {
	for _, name := range []string{"standard", "keyword", "whitespace"} {
		t.Run(name, func(t *testing.T) {
			tk, err := plugin.GetTokenizer(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunTokenizerSuite(t, tk)
		})
	}

	// 切词器不做归一化（保留原始大小写，转小写由 token filter 负责）
	tk, _ := plugin.GetTokenizer("standard")
	if got := termsOf(tk.Tokenize("Hello World")); !reflect.DeepEqual(got, []string{"Hello", "World"}) {
		t.Errorf("standard 切词器应保留大小写, got %v", got)
	}
	tk, _ = plugin.GetTokenizer("whitespace")
	if got := termsOf(tk.Tokenize("A b")); !reflect.DeepEqual(got, []string{"A", "b"}) {
		t.Errorf("whitespace 切词器应保留大小写, got %v", got)
	}
}

func TestBuiltinTokenFilters(t *testing.T) {
	for _, name := range []string{"lowercase", "stop"} {
		t.Run(name, func(t *testing.T) {
			f, err := plugin.GetTokenFilter(name)
			if err != nil {
				t.Fatal(err)
			}
			plugintest.RunTokenFilterSuite(t, f)
		})
	}

	// lowercase 行为：ASCII 与 Unicode 均转小写，Position 不变
	lower, _ := plugin.GetTokenFilter("lowercase")
	got := lower.Filter([]plugin.Token{{Term: "Hello", Position: 0}, {Term: "ÄBC", Position: 1}})
	if got[0].Term != "hello" || got[1].Term != "äbc" || got[0].Position != 0 || got[1].Position != 1 {
		t.Errorf("lowercase 结果错误: %v", got)
	}

	// stop 行为：移除停用词且保留位置跳号（positions 的核心用例）
	stop, _ := plugin.GetTokenFilter("stop")
	got = stop.Filter([]plugin.Token{
		{Term: "the", Position: 0},
		{Term: "quick", Position: 1},
		{Term: "fox", Position: 2},
	})
	want := []plugin.Token{{Term: "quick", Position: 1}, {Term: "fox", Position: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("stop 过滤 = %v, want %v", got, want)
	}
}

func TestBuiltinCharFilters(t *testing.T) {
	cf, err := plugin.GetCharFilter("mapping")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunCharFilterSuite(t, cf)

	if got := cf.Filter("phone queen"); got != "fone kween" {
		t.Errorf("mapping 字符替换 = %q, want %q", got, "fone kween")
	}
}

// TestChainWithCharFilter char filter 生效用例：mapping → standard → lowercase 组合链
func TestChainWithCharFilter(t *testing.T) {
	cf, _ := plugin.GetCharFilter("mapping")
	tk, _ := plugin.GetTokenizer("standard")
	tf, _ := plugin.GetTokenFilter("lowercase")
	a := plugin.NewChainAnalyzer([]plugin.CharFilter{cf}, tk, []plugin.TokenFilter{tf})

	// "ph" 在分词前被映射为 "f"（映射表大小写敏感，"Phone" 不含小写 "ph" 不受影响），
	// 切词后 lowercase 统一转小写
	got := a.Analyze("Phone ph")
	want := []plugin.Token{{Term: "phone", Position: 0}, {Term: "f", Position: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("含 char filter 的链 = %v, want %v", got, want)
	}
}
