package plugin

import (
	"encoding/json"
	"strings"
	"testing"
)

// fakeAnalyzer 测试用分词器
type fakeAnalyzer struct{}

func (fakeAnalyzer) Tokenize(s string) []string { return strings.Split(s, ",") }

func TestRegisterDuplicatePanics(t *testing.T) {
	RegisterAnalyzer("fake_for_test", fakeAnalyzer{})

	defer func() {
		if recover() == nil {
			t.Fatal("重复注册应 panic")
		}
	}()
	RegisterAnalyzer("fake_for_test", fakeAnalyzer{})
}

func TestGetUnregistered(t *testing.T) {
	if _, err := GetAnalyzer("不存在"); err == nil || !strings.Contains(err.Error(), "未注册") {
		t.Errorf("GetAnalyzer 未注册应返回明确错误, got %v", err)
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
	// 注册一个测试用子句解析器
	RegisterQuery(testClauseParser{})
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
