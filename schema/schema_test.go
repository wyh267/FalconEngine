package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/schema"

	// schema 包自身不注册字段类型/分词器，测试引入内置插件填充注册表
	// （plugins/fieldtype init 同时注入日期检测器）
	_ "github.com/FalconEngine/falcon/plugins"
)

func mustNew(t *testing.T, fields []schema.Field) *schema.Schema {
	t.Helper()
	s, err := schema.New(fields)
	if err != nil {
		t.Fatalf("schema.New error: %v", err)
	}
	return s
}

// TestMultiFieldsExpansion multi-fields 子字段展开为点路径：byName 可查、
// Flattened/各字段选择器（倒排/keyword 列）均包含子字段
func TestMultiFieldsExpansion(t *testing.T) {
	s := mustNew(t, []schema.Field{
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
		{Name: "level", Type: "number"},
	})

	if _, ok := s.Field("title"); !ok {
		t.Fatal("父字段 title 不可查")
	}
	sub, ok := s.Field("title.keyword")
	if !ok || sub.Type != "keyword" {
		t.Fatalf("子字段 title.keyword = %+v, ok=%v, want keyword/true", sub, ok)
	}
	flat := s.Flattened()
	if len(flat) != 3 || flat[0].Name != "title" || flat[1].Name != "title.keyword" || flat[2].Name != "level" {
		names := make([]string, 0, len(flat))
		for _, f := range flat {
			names = append(names, f.Name)
		}
		t.Fatalf("Flattened = %v, want [title title.keyword level]", names)
	}
	// 序列化保持嵌套形态并可无损回读
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"name":"title.keyword"`) {
		t.Fatalf("序列化应保持嵌套形态而非点路径展开: %s", b)
	}
	var s2 schema.Schema
	if err := json.Unmarshal(b, &s2); err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if _, ok := s2.Field("title.keyword"); !ok {
		t.Fatal("回读后 title.keyword 不可查")
	}
	// kw 列字段选择包含子字段
	kws := s2.KwFields()
	if len(kws) != 1 || kws[0] != "title.keyword" {
		t.Fatalf("KwFields = %v, want [title.keyword]", kws)
	}
}

// TestFieldNameDotRejected 显式声明的字段名禁止含 '.'（先行校验，避免与对象展开歧义）
func TestFieldNameDotRejected(t *testing.T) {
	if _, err := schema.New([]schema.Field{{Name: "a.b", Type: "text"}}); err == nil {
		t.Fatal("字段名含 '.' 应被拒绝")
	}
}

// TestSubFieldNestingRejected multi-fields 子字段不允许再嵌套（仅一层）
func TestSubFieldNestingRejected(t *testing.T) {
	_, err := schema.New([]schema.Field{
		{Name: "title", Type: "text", Fields: []schema.Field{
			{Name: "keyword", Type: "keyword", Fields: []schema.Field{{Name: "x", Type: "keyword"}}},
		}},
	})
	if err == nil {
		t.Fatal("子字段再嵌套应被拒绝")
	}
}

// TestAnalyzerOverride 字段级 analyzer 覆盖校验：
// 非倒排类型不能指定；未注册的分词器报错；合法覆盖通过
func TestAnalyzerOverride(t *testing.T) {
	if _, err := schema.New([]schema.Field{{Name: "n", Type: "number", Analyzer: "standard"}}); err == nil {
		t.Fatal("非倒排类型指定 analyzer 应被拒绝")
	}
	if _, err := schema.New([]schema.Field{{Name: "t", Type: "text", Analyzer: "nope"}}); err == nil {
		t.Fatal("未注册 analyzer 应被拒绝")
	}
	s := mustNew(t, []schema.Field{{Name: "t", Type: "text", Analyzer: "whitespace"}})
	f, _ := s.Field("t")
	if f.Analyzer != "whitespace" {
		t.Fatalf("Analyzer = %q, want whitespace", f.Analyzer)
	}
}

// TestInferField 动态推断规则：数字->number；bool->bool；日期串（开启检测）->date；
// 其余字符串->text+keyword 子字段；对象/数组/null 不推断
func TestInferField(t *testing.T) {
	cases := []struct {
		raw  string
		want string // 期望类型；"" 表示不推断
	}{
		{`42`, "number"},
		{`true`, "bool"},
		{`"2024-01-01"`, "date"},
		{`"2024-01-01 10:00:00"`, "date"},
		{`"abc"`, "text"},
		{`"2024-13-45"`, "text"}, // 形似日期但解析失败
		{`{"a":1}`, ""},
		{`[1,2]`, ""},
		{`null`, ""},
	}
	for _, c := range cases {
		f, ok := schema.InferField("x", json.RawMessage(c.raw), true)
		if c.want == "" {
			if ok {
				t.Fatalf("InferField(%s) 应不推断, got %+v", c.raw, f)
			}
			continue
		}
		if !ok || f.Type != c.want {
			t.Fatalf("InferField(%s) = %+v, ok=%v, want type=%s", c.raw, f, ok, c.want)
		}
		if c.want == "text" {
			if len(f.Fields) != 1 || f.Fields[0].Name != "keyword" || f.Fields[0].Type != "keyword" {
				t.Fatalf("text 推断应附带 keyword 子字段: %+v", f.Fields)
			}
		}
	}
	// date_detection=false：日期串推断为 text
	f, ok := schema.InferField("x", json.RawMessage(`"2024-01-01"`), false)
	if !ok || f.Type != "text" {
		t.Fatalf("date_detection=false 时应推断为 text, got %+v", f)
	}
}

// TestMergeFields mapping 显式更新：新字段（含已有字段的新子字段）追加；
// 已有字段 Type/Analyzer 不一致报错；完全一致幂等
func TestMergeFields(t *testing.T) {
	s := mustNew(t, []schema.Field{
		{Name: "title", Type: "text"},
		{Name: "level", Type: "number"},
	})

	// 新字段 + 已有字段的新子字段
	err := s.MergeFields([]schema.Field{
		{Name: "tag", Type: "keyword"},
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
	})
	if err != nil {
		t.Fatalf("MergeFields error: %v", err)
	}
	if _, ok := s.Field("tag"); !ok {
		t.Fatal("新字段 tag 未合并")
	}
	if _, ok := s.Field("title.keyword"); !ok {
		t.Fatal("已有字段的新子字段 title.keyword 未合并")
	}

	// 幂等：完全一致重复合并
	if err := s.MergeFields([]schema.Field{
		{Name: "tag", Type: "keyword"},
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
	}); err != nil {
		t.Fatalf("幂等合并应通过: %v", err)
	}

	// 冲突：已有字段改类型
	if err := s.MergeFields([]schema.Field{{Name: "level", Type: "keyword"}}); err == nil {
		t.Fatal("修改已有字段类型应报错")
	}
	// 冲突：已有子字段改类型
	if err := s.MergeFields([]schema.Field{
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "text"}}},
	}); err == nil {
		t.Fatal("修改已有子字段类型应报错")
	}
	// 冲突后无副作用（全部预检通过才应用）
	if err := s.MergeFields([]schema.Field{{Name: "level", Type: "keyword"}, {Name: "ghost", Type: "text"}}); err == nil {
		t.Fatal("含冲突项的合并应整体报错")
	}
	if _, ok := s.Field("ghost"); ok {
		t.Fatal("冲突的合并不应留下部分字段")
	}
}

// TestDiffFields 增量计算：整体新字段原样返回；已有字段的新子字段以父骨架返回
func TestDiffFields(t *testing.T) {
	base := mustNew(t, []schema.Field{{Name: "title", Type: "text"}})
	local := mustNew(t, []schema.Field{
		{Name: "title", Type: "text", Fields: []schema.Field{{Name: "keyword", Type: "keyword"}}},
	})
	// 点路径字段（动态推断产物）经 AddField 注册
	if err := local.AddField(schema.Field{Name: "user.name", Type: "text"}); err != nil {
		t.Fatal(err)
	}
	delta := local.DiffFields(base)
	if len(delta) != 2 {
		t.Fatalf("delta = %+v, want 2 项", delta)
	}
	// 顺序按 local 定义：title 骨架（仅新子字段） + user.name 整体
	if delta[0].Name != "title" || len(delta[0].Fields) != 1 || delta[0].Fields[0].Name != "keyword" {
		t.Fatalf("delta[0] = %+v, want title 骨架 + keyword 子字段", delta[0])
	}
	if delta[1].Name != "user.name" {
		t.Fatalf("delta[1] = %+v, want user.name", delta[1])
	}
	// base 的字段在 local 中全部存在（且无新子字段）时为空
	if d := base.DiffFields(local); len(d) != 0 {
		t.Fatalf("base 相对 local 应无增量, got %+v", d)
	}
	if d := local.DiffFields(local); len(d) != 0 {
		t.Fatalf("自身 delta 应为空, got %+v", d)
	}
}

// TestMergeMappingJSON CSM 侧只增合并（纯结构）：新字段并入、已有字段以原定义为准、
// 子字段只增合并
func TestMergeMappingJSON(t *testing.T) {
	base := json.RawMessage(`{"fields":[{"name":"title","type":"text"}]}`)
	delta := json.RawMessage(`{"fields":[{"name":"title","type":"keyword","fields":[{"name":"raw","type":"keyword"}]},{"name":"age","type":"number"}]}`)
	merged, err := schema.MergeMappingJSON(base, delta)
	if err != nil {
		t.Fatal(err)
	}
	var s schema.Schema
	if err := json.Unmarshal(merged, &s); err != nil {
		t.Fatal(err)
	}
	// title 以 base 为准（只增不改），但新子字段 raw 并入；age 为新字段
	f, ok := s.Field("title")
	if !ok || f.Type != "text" {
		t.Fatalf("title = %+v, want text", f)
	}
	if _, ok := s.Field("title.raw"); !ok {
		t.Fatal("子字段 title.raw 未合并")
	}
	if _, ok := s.Field("age"); !ok {
		t.Fatal("新字段 age 未合并")
	}
	// 空 base
	merged, err = schema.MergeMappingJSON(nil, delta)
	if err != nil {
		t.Fatal(err)
	}
	var s2 schema.Schema
	if err := json.Unmarshal(merged, &s2); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Field("title"); !ok {
		t.Fatal("空 base 合并失败")
	}
}
