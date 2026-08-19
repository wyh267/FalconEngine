// Package fieldtype 内置字段类型插件：text / keyword / number / date / bool / stored。
//
//	text     全文检索：倒排 + norms，分词器 standard
//	keyword  精确匹配：倒排（无 norms），分词器 keyword（整词）
//	number   整数：正排，支持过滤与排序
//	date     日期：解析 "2006-01-02 15:04:05" 或 "2006-01-02" 为 unix 秒，正排
//	bool     布尔：true/false 按 1/0 存储，正排
//	stored   仅存储原文，不可检索
package fieldtype

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/FalconEngine/falcon/plugin"
)

// dateLayouts 支持的日期格式（按本地时区解析）
var dateLayouts = []string{"2006-01-02 15:04:05", "2006-01-02"}

// ParseDate 解析日期字符串为 unix 秒（本地时区），供查询侧复用
func ParseDate(s string) (int64, error) {
	for _, layout := range dateLayouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("非法日期格式 %q，仅支持 %q 与 %q", s, dateLayouts[0], dateLayouts[1])
}

// base 公共骨架：各字段类型嵌入后按需覆盖
type base struct {
	name     string
	analyzer string
	inverted bool
	docvals  bool
	norms    bool
}

func (b base) Name() string     { return b.name }
func (b base) Analyzer() string { return b.analyzer }
func (b base) Inverted() bool   { return b.inverted }
func (b base) DocValues() bool  { return b.docvals }
func (b base) HasNorms() bool   { return b.norms }

// parseString 提取 JSON 字符串值
func parseString(v json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", err
	}
	return s, nil
}

// ---------- text / keyword ----------

type textType struct{ base }

func (textType) Parse(v json.RawMessage) (plugin.Value, error) {
	s, err := parseString(v)
	if err != nil {
		return plugin.Value{}, fmt.Errorf("text 字段应为字符串: %w", err)
	}
	return plugin.Value{Text: s}, nil
}

type keywordType struct{ base }

func (keywordType) Parse(v json.RawMessage) (plugin.Value, error) {
	s, err := parseString(v)
	if err != nil {
		return plugin.Value{}, fmt.Errorf("keyword 字段应为字符串: %w", err)
	}
	return plugin.Value{Text: s}, nil
}

// ---------- number ----------

type numberType struct{ base }

func (numberType) Parse(v json.RawMessage) (plugin.Value, error) {
	var n json.Number
	if err := json.Unmarshal(v, &n); err != nil {
		return plugin.Value{}, fmt.Errorf("number 字段应为数字: %w", err)
	}
	num, err := n.Int64()
	if err != nil {
		return plugin.Value{}, fmt.Errorf("number 字段应为整数: %w", err)
	}
	return plugin.Value{Num: num}, nil
}

// ---------- date ----------

type dateType struct{ base }

func (dateType) Parse(v json.RawMessage) (plugin.Value, error) {
	s, err := parseString(v)
	if err != nil {
		return plugin.Value{}, fmt.Errorf("date 字段应为日期字符串: %w", err)
	}
	num, err := ParseDate(s)
	if err != nil {
		return plugin.Value{}, err
	}
	return plugin.Value{Num: num}, nil
}

// ---------- bool ----------

type boolType struct{ base }

func (boolType) Parse(v json.RawMessage) (plugin.Value, error) {
	var b bool
	if err := json.Unmarshal(v, &b); err != nil {
		return plugin.Value{}, fmt.Errorf("bool 字段应为布尔值: %w", err)
	}
	if b {
		return plugin.Value{Num: 1}, nil
	}
	return plugin.Value{Num: 0}, nil
}

// ---------- stored ----------

type storedType struct{ base }

func (storedType) Parse(v json.RawMessage) (plugin.Value, error) {
	// 仅存储，无需解析（原文整体保留）
	return plugin.Value{}, nil
}

func init() {
	plugin.RegisterFieldType(textType{base{name: "text", analyzer: "standard", inverted: true, norms: true}})
	plugin.RegisterFieldType(keywordType{base{name: "keyword", analyzer: "keyword", inverted: true}})
	plugin.RegisterFieldType(numberType{base{name: "number", docvals: true}})
	plugin.RegisterFieldType(dateType{base{name: "date", docvals: true}})
	plugin.RegisterFieldType(boolType{base{name: "bool", docvals: true}})
	plugin.RegisterFieldType(storedType{base{name: "stored"}})
}
