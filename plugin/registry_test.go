package plugin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeAnalyzer 测试用分词器
type fakeAnalyzer struct{}

func (fakeAnalyzer) Analyze(s string) []Token {
	parts := strings.Split(s, ",")
	toks := make([]Token, 0, len(parts))
	for i, p := range parts {
		toks = append(toks, Token{Term: p, Position: i})
	}
	return toks
}

func TestRegisterDuplicatePanics(t *testing.T) {
	// 注册表为进程级全局态，-count=N 重复跑时同名首次注册也会 panic，
	// 故每次运行取唯一名（重复注册断言不受影响）
	name := fmt.Sprintf("fake_for_test_%d", time.Now().UnixNano())
	RegisterAnalyzer(name, fakeAnalyzer{})

	defer func() {
		if recover() == nil {
			t.Fatal("重复注册应 panic")
		}
	}()
	RegisterAnalyzer(name, fakeAnalyzer{})
}

func TestGetUnregistered(t *testing.T) {
	if _, err := GetAnalyzer("不存在"); err == nil || !strings.Contains(err.Error(), "未注册") {
		t.Errorf("GetAnalyzer 未注册应返回明确错误, got %v", err)
	}
	if _, err := GetCharFilter("不存在"); err == nil {
		t.Error("GetCharFilter 未注册应报错")
	}
	if _, err := GetTokenizer("不存在"); err == nil {
		t.Error("GetTokenizer 未注册应报错")
	}
	if _, err := GetTokenFilter("不存在"); err == nil {
		t.Error("GetTokenFilter 未注册应报错")
	}
	if _, err := GetFieldType("不存在"); err == nil {
		t.Error("GetFieldType 未注册应报错")
	}
	if _, err := GetScorer("不存在"); err == nil {
		t.Error("GetScorer 未注册应报错")
	}
	if _, err := GetQuery("不存在"); err == nil {
		t.Error("GetQuery 未注册应报错")
	}
	if _, err := GetAgg("不存在"); err == nil {
		t.Error("GetAgg 未注册应报错")
	}
}

func TestParseClauseDispatch(t *testing.T) {
	// 注册一个测试用子句解析器（注册表为进程级全局态，-count=N 重跑时已注册则跳过）
	if _, err := GetQuery("test_clause"); err != nil {
		RegisterQuery(testClauseParser{})
	}
	ctx := &ParseContext{}

	n, err := ParseClause(json.RawMessage(`{"test_clause":{"x":1}}`), ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := n.(MatchAllNode); !ok {
		t.Errorf("分派结果类型错误: %T", n)
	}

	// 多键子句应报错
	if _, err := ParseClause(json.RawMessage(`{"a":{},"b":{}}`), ctx); err == nil {
		t.Error("多键子句应报错")
	}
	// 未注册子句应报错
	if _, err := ParseClause(json.RawMessage(`{"nope":{}}`), ctx); err == nil {
		t.Error("未注册子句应报错")
	}
}

type testClauseParser struct{}

func (testClauseParser) Name() string { return "test_clause" }
func (testClauseParser) Parse(body json.RawMessage, ctx *ParseContext) (QNode, error) {
	return MatchAllNode{}, nil
}

// ---------- 分析链组合 helper 测试 ----------

type bangCharFilter struct{}

func (bangCharFilter) Name() string           { return "bang" }
func (bangCharFilter) Filter(s string) string { return s + ",!" }

type commaTokenizer struct{}

func (commaTokenizer) Name() string { return "comma" }
func (commaTokenizer) Tokenize(s string) []Token {
	parts := strings.Split(s, ",")
	toks := make([]Token, 0, len(parts))
	for i, p := range parts {
		toks = append(toks, Token{Term: p, Position: i})
	}
	return toks
}

type upperTokenFilter struct{}

func (upperTokenFilter) Name() string { return "upper" }
func (upperTokenFilter) Filter(toks []Token) []Token {
	for i := range toks {
		toks[i].Term = strings.ToUpper(toks[i].Term)
	}
	return toks
}

// TestChainAnalyzer 验证三级组件的应用顺序：char filter → tokenizer → token filter
func TestChainAnalyzer(t *testing.T) {
	a := NewChainAnalyzer([]CharFilter{bangCharFilter{}}, commaTokenizer{}, []TokenFilter{upperTokenFilter{}})
	got := a.Analyze("a,b")
	want := []Token{
		{Term: "A", Position: 0},
		{Term: "B", Position: 1},
		{Term: "!", Position: 2}, // char filter 追加的 "," 进入了切词（顺序证明）
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("链式分析 = %v, want %v", got, want)
	}

	defer func() {
		if recover() == nil {
			t.Error("tokenizer 为 nil 应 panic")
		}
	}()
	NewChainAnalyzer(nil, nil, nil)
}
