// Package schema 定义索引的字段结构（mapping）。
// 字段类型的合法性由 plugin 注册表决定（见 plugins/fieldtype）；
// 各类型进入哪种索引（倒排/正排/norms）同样查询注册表而非硬编码。
package schema

import (
	"encoding/json"
	"fmt"

	"github.com/FalconEngine/falcon/plugin"
)

// Field 一个字段的定义
type Field struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"` // 已注册的字段类型插件名
}

// Schema 一个索引的全部字段定义
type Schema struct {
	Fields []Field `json:"fields" yaml:"fields"`

	byName map[string]Field
}

// New 创建并校验 Schema：字段名唯一、类型已在注册表中注册。
// 允许空字段列表（纯动态 mapping，字段在首次写入时推断）。
func New(fields []Field) (*Schema, error) {
	s := &Schema{byName: make(map[string]Field, len(fields))}
	for _, f := range fields {
		if err := s.add(f); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// add 校验并追加一个字段（不持久化，持久化由调用方负责）
func (s *Schema) add(f Field) error {
	if f.Name == "" {
		return fmt.Errorf("schema: 字段名不能为空")
	}
	p, err := plugin.GetFieldType(f.Type)
	if err != nil {
		return fmt.Errorf("schema: 字段 %q: %w", f.Name, err)
	}
	if p.Inverted() {
		if _, err := plugin.GetAnalyzer(p.Analyzer()); err != nil {
			return fmt.Errorf("schema: 字段 %q 类型 %q 依赖的: %w", f.Name, f.Type, err)
		}
	}
	if p.HasNorms() && !p.Inverted() {
		return fmt.Errorf("schema: 字段 %q 类型 %q 声明了 norms 但不是倒排字段", f.Name, f.Type)
	}
	if _, dup := s.byName[f.Name]; dup {
		return fmt.Errorf("schema: 字段名重复: %q", f.Name)
	}
	s.Fields = append(s.Fields, f)
	s.byName[f.Name] = f
	return nil
}

// AddField 动态追加字段（动态 mapping 用）；显式声明的字段优先，重名返回错误
func (s *Schema) AddField(f Field) error {
	return s.add(f)
}

// Field 按名查找字段定义
func (s *Schema) Field(name string) (Field, bool) {
	f, ok := s.byName[name]
	return f, ok
}

// selectBy 按字段类型插件的某个性质筛选字段名（保持定义顺序）
func (s *Schema) selectBy(pred func(p plugin.FieldTypePlugin) bool) []string {
	var names []string
	for _, f := range s.Fields {
		p, err := plugin.GetFieldType(f.Type)
		if err == nil && pred(p) {
			names = append(names, f.Name)
		}
	}
	return names
}

// NumFields 返回所有建正排的字段名（支持过滤与排序，保持定义顺序）
func (s *Schema) NumFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.DocValues() })
}

// InvFields 返回所有倒排字段名（保持定义顺序）
func (s *Schema) InvFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.Inverted() })
}

// TextFields 返回所有记录 norms 的字段名（保持定义顺序），BM25 用
func (s *Schema) TextFields() []string {
	return s.selectBy(func(p plugin.FieldTypePlugin) bool { return p.HasNorms() })
}

// UnmarshalJSON 反序列化后重建索引
func (s *Schema) UnmarshalJSON(b []byte) error {
	type alias Schema
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	ns, err := New(a.Fields)
	if err != nil {
		return err
	}
	*s = *ns
	return nil
}
