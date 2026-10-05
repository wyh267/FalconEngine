// Package schema 定义索引的字段结构（mapping）。
// 字段类型的合法性由 plugin 注册表决定（见 plugins/fieldtype）；
// 各类型进入哪种索引（倒排/正排/norms）同样查询注册表而非硬编码。
//
// 字段模型：Field.Fields 承载 multi-fields 子字段（一个字段同时建多种索引，
// 如 title(text) + title.keyword，仅支持一层）；嵌套对象不建类型化父字段，
// 文档解析时展开为点路径（见 index.flattenDoc），动态推断产生的点路径字段
// （如 user.name）作为顶层条目存储。byName 统一以点路径为键。
package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/FalconEngine/falcon/plugin"
)

// Field 一个字段的定义
type Field struct {
	Name     string  `json:"name" yaml:"name"`
	Type     string  `json:"type" yaml:"type"`                             // 已注册的字段类型插件名
	Analyzer string  `json:"analyzer,omitempty" yaml:"analyzer,omitempty"` // 覆盖类型默认分词器
	Fields   []Field `json:"fields,omitempty" yaml:"fields,omitempty"`     // multi-fields 子字段，一层为止
}

// Schema 一个索引的全部字段定义
type Schema struct {
	Fields []Field `json:"fields" yaml:"fields"`

	byName map[string]Field // 点路径（含 multi-fields 子字段）-> 字段定义
}

// New 创建并校验 Schema：字段名唯一且不含 '.'、类型已在注册表中注册。
// 允许空字段列表（纯动态 mapping，字段在首次写入时推断）。
func New(fields []Field) (*Schema, error) {
	return newSchema(fields, false)
}

// newSchema 构建 Schema；allowDot 允许点路径字段名（动态推断产物与磁盘恢复用，
// 用户显式声明一律经 New 禁止含 '.'，避免与嵌套对象的点路径展开歧义）
func newSchema(fields []Field, allowDot bool) (*Schema, error) {
	s := &Schema{byName: make(map[string]Field, len(fields))}
	for _, f := range fields {
		if err := s.add(f, allowDot); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// checkFieldShape 字段形状校验：名非空；按上下文禁止含 '.'
func checkFieldShape(f Field, allowDot bool) error {
	if f.Name == "" {
		return fmt.Errorf("schema: 字段名不能为空")
	}
	if !allowDot && strings.Contains(f.Name, ".") {
		return fmt.Errorf("schema: 字段名 %q 不允许含 '.'（避免与嵌套对象的点路径展开歧义）", f.Name)
	}
	return nil
}

// validateFieldType 字段类型与分词器校验（依赖插件注册表）：
// 类型已注册；声明 norms 的类型必须是倒排字段；显式 analyzer 已注册且类型为倒排字段；
// 未显式指定时倒排字段的类型默认分词器必须已注册。
func validateFieldType(f Field) error {
	p, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return fmt.Errorf("schema: 字段 %q: %w", f.Name, err)
	}
	if p.HasNorms() && !p.Inverted() {
		return fmt.Errorf("schema: 字段 %q 类型 %q 声明了 norms 但不是倒排字段", f.Name, f.Type)
	}
	if f.Analyzer != "" {
		if !p.Inverted() {
			return fmt.Errorf("schema: 字段 %q 类型 %q 不是倒排类型，不能指定 analyzer", f.Name, f.Type)
		}
		if _, err := plugin.GetAnalyzer(f.Analyzer); err != nil {
			return fmt.Errorf("schema: 字段 %q: %w", f.Name, err)
		}
	} else if p.Inverted() {
		if _, err := plugin.GetAnalyzer(p.Analyzer()); err != nil {
			return fmt.Errorf("schema: 字段 %q 类型 %q 依赖的: %w", f.Name, f.Type, err)
		}
	}
	return nil
}

// add 校验并追加一个字段（含 multi-fields 子字段展开，不持久化，持久化由调用方负责）。
// 先全量校验再注册，失败不留中间状态。
func (s *Schema) add(f Field, allowDot bool) error {
	if err := checkFieldShape(f, allowDot); err != nil {
		return err
	}
	if err := validateFieldType(f); err != nil {
		return err
	}
	for _, sub := range f.Fields {
		if len(sub.Fields) > 0 {
			return fmt.Errorf("schema: 字段 %q 的子字段不允许再嵌套 fields（multi-fields 仅支持一层）", f.Name)
		}
		if err := checkFieldShape(sub, allowDot); err != nil {
			return fmt.Errorf("schema: 字段 %q 的子字段: %w", f.Name, err)
		}
		if err := validateFieldType(sub); err != nil {
			return fmt.Errorf("schema: 字段 %q 的子字段: %w", f.Name, err)
		}
	}
	if _, dup := s.byName[f.Name]; dup {
		return fmt.Errorf("schema: 字段名重复: %q", f.Name)
	}
	for _, sub := range f.Fields {
		if _, dup := s.byName[f.Name+"."+sub.Name]; dup {
			return fmt.Errorf("schema: 字段名重复: %q", f.Name+"."+sub.Name)
		}
	}
	s.Fields = append(s.Fields, f)
	s.byName[f.Name] = f
	for _, sub := range f.Fields {
		sub.Name = f.Name + "." + sub.Name
		sub.Fields = nil
		s.byName[sub.Name] = sub
	}
	return nil
}

// AddField 动态追加字段（动态 mapping 用）；显式声明的字段优先，重名返回错误。
// 允许点路径字段名（嵌套对象叶子经 flatten 后的推断产物，如 user.name）。
func (s *Schema) AddField(f Field) error {
	return s.add(f, true)
}

// Field 按名查找字段定义（点路径，含 multi-fields 子字段）
func (s *Schema) Field(name string) (Field, bool) {
	f, ok := s.byName[name]
	return f, ok
}

// Clone 深拷贝（Fields 与子字段切片均复制，byName 重建）。
// 引擎以写时复制方式更新 schema（clone-修改-替换指针），
// 锁外读者看到的始终是完整不可变的旧版本。
func (s *Schema) Clone() *Schema {
	ns := &Schema{
		Fields: make([]Field, len(s.Fields)),
		byName: make(map[string]Field, len(s.byName)),
	}
	for i, f := range s.Fields {
		ns.Fields[i] = cloneField(f)
	}
	for k, f := range s.byName {
		ns.byName[k] = cloneField(f)
	}
	return ns
}

func cloneField(f Field) Field {
	if f.Fields != nil {
		f.Fields = append([]Field(nil), f.Fields...)
	}
	return f
}

// Flattened 返回扁平点路径视角的全部字段（父字段 + multi-fields 子字段，保持定义顺序）
func (s *Schema) Flattened() []Field {
	out := make([]Field, 0, len(s.byName))
	for _, f := range s.Fields {
		out = append(out, f)
		for _, sub := range f.Fields {
			sub.Name = f.Name + "." + sub.Name
			sub.Fields = nil
			out = append(out, sub)
		}
	}
	return out
}

// selectBy 按字段类型插件的某个性质筛选字段名（含子字段，保持定义顺序）
func (s *Schema) selectBy(pred func(p plugin.FieldTypePlugin) bool) []string {
	var names []string
	for _, f := range s.Flattened() {
		p, err := plugin.GetFieldType(f.Type)
		if err == nil && pred(p) {
			names = append(names, f.Name)
		}
	}
	return names
}

// NumFields 返回所有建 number 类正排的字段名（支持过滤与排序，保持定义顺序）
func (s *Schema) NumFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.DocValuesKind() == plugin.DVNum })
}

// KwFields 返回所有建 keyword ord 列的字段名（保持定义顺序）
func (s *Schema) KwFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.DocValuesKind() == plugin.DVKeyword })
}

// InvFields 返回所有倒排字段名（保持定义顺序）
func (s *Schema) InvFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.Inverted() })
}

// TextFields 返回所有记录 norms 的字段名（保持定义顺序），BM25 用
func (s *Schema) TextFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.HasNorms() })
}

// MergeFields mapping 显式更新：新字段（含已有字段的新子字段）追加合并；
// 已存在字段的 Type/Analyzer 不一致返回错误；与现有定义完全一致幂等通过。
// 先全量预检再应用，失败不留部分合并的中间状态。
// 允许点路径字段名（集群同步/动态推断产物经此合并进各分片 schema）。
func (s *Schema) MergeFields(fields []Field) error {
	return s.mergeFields(fields, true)
}

// MergeFieldsUnchecked 同 MergeFields 但跳过预检中的插件注册表校验
// （新增字段注册时仍会经 add 校验类型——本方法用于多分片 schema 并集等
// 字段来源已可信、仅需合并去重的场景，避免重复的预检开销）。
func (s *Schema) MergeFieldsUnchecked(fields []Field) error {
	return s.mergeFields(fields, false)
}

func (s *Schema) mergeFields(fields []Field, validate bool) error {
	for _, f := range fields {
		if err := s.checkMergeField(f, validate); err != nil {
			return err
		}
	}
	for _, f := range fields {
		if _, ok := s.byName[f.Name]; !ok {
			// 整个字段（含全部子字段）都是新的
			if err := s.add(f, true); err != nil {
				return err
			}
			continue
		}
		for _, sub := range f.Fields {
			if _, ok := s.byName[f.Name+"."+sub.Name]; ok {
				continue
			}
			if err := s.addSubField(f.Name, sub); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkMergeField 合并预检：形状/嵌套层数/类型校验 + 与已有定义冲突检查
func (s *Schema) checkMergeField(f Field, validate bool) error {
	if f.Name == "" {
		return fmt.Errorf("schema: 字段名不能为空")
	}
	if validate {
		if err := validateFieldType(f); err != nil {
			return err
		}
	}
	if old, ok := s.byName[f.Name]; ok && (old.Type != f.Type || old.Analyzer != f.Analyzer) {
		return fmt.Errorf("schema: 字段 %q 已存在且定义不一致（%s/%s -> %s/%s），mapping 不允许修改已有字段",
			f.Name, old.Type, old.Analyzer, f.Type, f.Analyzer)
	}
	for _, sub := range f.Fields {
		if len(sub.Fields) > 0 {
			return fmt.Errorf("schema: 字段 %q 的子字段不允许再嵌套 fields（multi-fields 仅支持一层）", f.Name)
		}
		if validate {
			if err := validateFieldType(sub); err != nil {
				return fmt.Errorf("schema: 字段 %q 的子字段: %w", f.Name, err)
			}
		}
		full := f.Name + "." + sub.Name
		if old, ok := s.byName[full]; ok && (old.Type != sub.Type || old.Analyzer != sub.Analyzer) {
			return fmt.Errorf("schema: 字段 %q 已存在且定义不一致（%s/%s -> %s/%s），mapping 不允许修改已有字段",
				full, old.Type, old.Analyzer, sub.Type, sub.Analyzer)
		}
	}
	return nil
}

// addSubField 给已有顶层字段追加一个 multi-fields 子字段（预检已通过）
func (s *Schema) addSubField(parentName string, sub Field) error {
	for i := range s.Fields {
		if s.Fields[i].Name != parentName {
			continue
		}
		s.Fields[i].Fields = append(s.Fields[i].Fields, sub)
		sub.Name = parentName + "." + sub.Name
		sub.Fields = nil
		s.byName[sub.Name] = sub
		// byName 中父字段是追加了子字段前的拷贝，同步刷新
		s.byName[parentName] = s.Fields[i]
		return nil
	}
	return fmt.Errorf("schema: 父字段 %q 不存在", parentName)
}

// DiffFields 返回 s 有而 base 没有的字段（mapping 增量广播用）：
// base 中不存在的顶层字段整体返回；已存在顶层字段的新子字段以
// "父骨架（Name/Type/Analyzer）+ 新子字段"形式返回，可直接交给 MergeFields 合并。
func (s *Schema) DiffFields(base *Schema) []Field {
	var out []Field
	for _, f := range s.Fields {
		if _, ok := base.byName[f.Name]; !ok {
			out = append(out, f)
			continue
		}
		var subs []Field
		for _, sub := range f.Fields {
			if _, ok := base.byName[f.Name+"."+sub.Name]; !ok {
				subs = append(subs, sub)
			}
		}
		if len(subs) > 0 {
			out = append(out, Field{Name: f.Name, Type: f.Type, Analyzer: f.Analyzer, Fields: subs})
		}
	}
	return out
}

// MergeMappingJSON 把 delta（{"fields":[...]}）只增合并进 base（同形），返回合并后的 JSON。
// 纯结构合并：同名字段（含子字段）以 base 为准，不做字段类型/分词器校验——
// 供 raft FSM apply 等要求确定性、不依赖插件注册表的场景使用（校验由提案边界负责）。
func MergeMappingJSON(base, delta json.RawMessage) (json.RawMessage, error) {
	var b, d struct {
		Fields []Field `json:"fields"`
	}
	if len(base) > 0 {
		if err := json.Unmarshal(base, &b); err != nil {
			return nil, fmt.Errorf("schema: mapping base 解析失败: %w", err)
		}
	}
	if err := json.Unmarshal(delta, &d); err != nil {
		return nil, fmt.Errorf("schema: mapping delta 解析失败: %w", err)
	}
	idx := make(map[string]int, len(b.Fields))
	for i, f := range b.Fields {
		idx[f.Name] = i
	}
	for _, f := range d.Fields {
		i, ok := idx[f.Name]
		if !ok {
			idx[f.Name] = len(b.Fields)
			b.Fields = append(b.Fields, f)
			continue
		}
		// 已有顶层字段：只增合并其子字段
		sub := make(map[string]bool, len(b.Fields[i].Fields))
		for _, sf := range b.Fields[i].Fields {
			sub[sf.Name] = true
		}
		for _, sf := range f.Fields {
			if !sub[sf.Name] {
				b.Fields[i].Fields = append(b.Fields[i].Fields, sf)
				sub[sf.Name] = true
			}
		}
	}
	return json.Marshal(struct {
		Fields []Field `json:"fields"`
	}{b.Fields})
}

// UnmarshalJSON 反序列化后重建索引。
// 磁盘/集群上持久化的 schema 可能含动态推断产生的点路径字段名，故放开 '.' 限制。
func (s *Schema) UnmarshalJSON(b []byte) error {
	type alias Schema
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	ns, err := newSchema(a.Fields, true)
	if err != nil {
		return err
	}
	*s = *ns
	return nil
}
