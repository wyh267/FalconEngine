// Package plugintest 提供插件契约测试套件。
// 自定义插件的测试应调用对应的 Run*Suite 验证契约行为，
// 内置插件（plugins/ 下）的测试同样基于本套件。
package plugintest

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/FalconEngine/falcon/plugin"
)

// RunAnalyzerSuite 验证分词器契约：
// 空输入安全、确定性（同输入同输出）、不产出空 token
func RunAnalyzerSuite(t *testing.T, a plugin.Analyzer) {
	t.Helper()

	if got := a.Tokenize(""); len(got) != 0 {
		t.Errorf("空输入应产出空结果, got %v", got)
	}

	in := "Hello 世界 foo-bar"
	first := a.Tokenize(in)
	second := a.Tokenize(in)
	if !reflect.DeepEqual(first, second) {
		t.Errorf("分词不确定: %v vs %v", first, second)
	}
	for _, tok := range first {
		if tok == "" {
			t.Errorf("不允许产出空 token, 全部结果: %v", first)
		}
	}
}

// RunFieldTypeSuite 验证字段类型插件契约：
// Name 非空；validSamples 全部解析成功；invalidSamples 全部报错；
// 倒排字段必须声明已注册的分词器；HasNorms 必须是倒排字段
func RunFieldTypeSuite(t *testing.T, p plugin.FieldTypePlugin, validSamples, invalidSamples []string) {
	t.Helper()

	if p.Name() == "" {
		t.Error("Name() 不能为空")
	}
	if p.Inverted() {
		if p.Analyzer() == "" {
			t.Error("倒排字段必须声明分词器")
		} else if _, err := plugin.GetAnalyzer(p.Analyzer()); err != nil {
			t.Errorf("声明的分词器 %q 未注册: %v", p.Analyzer(), err)
		}
	}
	if p.HasNorms() && !p.Inverted() {
		t.Error("HasNorms 的字段必须是倒排字段")
	}
	for _, s := range validSamples {
		if _, err := p.Parse(json.RawMessage(s)); err != nil {
			t.Errorf("合法样本 %s 解析失败: %v", s, err)
		}
	}
	for _, s := range invalidSamples {
		if _, err := p.Parse(json.RawMessage(s)); err == nil {
			t.Errorf("非法样本 %s 应报错", s)
		}
	}
}

// RunScorerSuite 验证打分器契约：
// 得分非负、确定性；tf 单调不减；df 单调不增（越稀有越高分）；dl 单调不增（越长分越低）
func RunScorerSuite(t *testing.T, s plugin.Scorer) {
	t.Helper()

	if s.Name() == "" {
		t.Error("Name() 不能为空")
	}
	base := s.Score(1, 10, 5, 100, 10)
	if base < 0 {
		t.Errorf("得分不应为负: %v", base)
	}
	if got := s.Score(1, 10, 5, 100, 10); got != base {
		t.Errorf("打分不确定: %v vs %v", got, base)
	}
	if s.Score(3, 10, 5, 100, 10) < base {
		t.Error("tf 增大得分不应降低")
	}
	if s.Score(1, 10, 50, 100, 10) > s.Score(1, 10, 5, 100, 10) {
		t.Error("df 增大得分不应升高（稀有 term 应更高分）")
	}
	if s.Score(1, 100, 5, 100, 10) > s.Score(1, 1, 5, 100, 10) {
		t.Error("dl 增大得分不应升高（长文档应被抑制）")
	}
}

// RunQueryParserSuite 验证查询子句解析器契约：
// validBodies 全部解析成功且返回非空节点；invalidBodies 全部报错
func RunQueryParserSuite(t *testing.T, p plugin.QueryParser, ctx *plugin.ParseContext, validBodies, invalidBodies []string) {
	t.Helper()

	if p.Name() == "" {
		t.Error("Name() 不能为空")
	}
	for _, s := range validBodies {
		n, err := p.Parse(json.RawMessage(s), ctx)
		if err != nil {
			t.Errorf("合法子句 %s 解析失败: %v", s, err)
		} else if n == nil {
			t.Errorf("合法子句 %s 返回了空节点", s)
		}
	}
	for _, s := range invalidBodies {
		if _, err := p.Parse(json.RawMessage(s), ctx); err == nil {
			t.Errorf("非法子句 %s 应报错", s)
		}
	}
}

// RunAggSuite 验证聚合插件契约：
// Parse 合法/非法子句；两段式语义——数据分两路收集再 Marshal/Merge 合并，
// 与单路全量收集的 Result 必须一致
func RunAggSuite(t *testing.T, p plugin.AggPlugin, ctx *plugin.ParseContext, validBody string, nums []int64, strs []string) {
	t.Helper()

	if p.Name() == "" {
		t.Error("Name() 不能为空")
	}
	spec, err := p.Parse(json.RawMessage(validBody), ctx)
	if err != nil {
		t.Fatalf("合法聚合子句 %s 解析失败: %v", validBody, err)
	}
	if _, err := p.Parse(json.RawMessage(`{}`), ctx); err == nil {
		t.Error("空子句体应报错")
	}

	// 单路全量收集
	full := p.New(spec)
	feedAgg(t, full, nums, strs)

	// 两路收集 + Marshal/Merge 合并
	left, right := p.New(spec), p.New(spec)
	half := len(nums) / 2
	feedAgg(t, left, nums[:half], strs[:len(strs)/2])
	feedAgg(t, right, nums[half:], strs[len(strs)/2:])
	partial, err := right.Marshal()
	if err != nil {
		t.Fatalf("Marshal 失败: %v", err)
	}
	if err := left.Merge(partial); err != nil {
		t.Fatalf("Merge 失败: %v", err)
	}

	if !reflect.DeepEqual(full.Result(), left.Result()) {
		t.Errorf("两段式结果不一致:\n单路: %s\n合并: %s", full.Result(), left.Result())
	}
}

// feedAgg 按聚合器 Kind 投喂数值或字符串序列
func feedAgg(t *testing.T, ag plugin.Aggregator, nums []int64, strs []string) {
	t.Helper()
	switch ag.Kind() {
	case plugin.AggNum:
		for _, v := range nums {
			ag.CollectNum(v)
		}
	case plugin.AggString:
		for _, v := range strs {
			ag.CollectString(v)
		}
	default:
		t.Fatalf("未知 AggKind: %v", ag.Kind())
	}
}
