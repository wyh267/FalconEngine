package schema

import (
	"bytes"
	"encoding/json"
)

// DateDetector 日期检测函数：判断字符串是否为可识别的日期格式。
// 由字段类型插件包（plugins/fieldtype）在 init 时注入——schema 包处于依赖链
// 底层，不能反向 import 具体字段类型实现（循环依赖），故以注册方式解耦。
// 未注入时日期检测不生效（字符串一律推断为 text）。
var DateDetector func(string) bool

// RegisterDateDetector 注册日期检测器（字段类型插件包 init 调用，重复注册覆盖）
func RegisterDateDetector(d func(string) bool) { DateDetector = d }

// InferField 动态 mapping 推断：按 JSON 原始值推断字段定义。
// 返回 ok=false 表示该值不推断（对象/数组/null——对象叶子由调用方 flatten 后
// 逐个调用本函数；数组与 null 对齐现状不索引）。
//
// 推断规则（对齐 ES 动态 mapping）：
//   - 数字 -> number；bool -> bool
//   - string：开启 dateDetection 且日期检测命中 -> date；
//     否则 -> text，并附带 keyword 子字段（multi-fields，兼顾全文与精确匹配/排序聚合）
func InferField(name string, raw json.RawMessage, dateDetection bool) (Field, bool) {
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return Field{}, false
	}
	switch val := v.(type) {
	case string:
		if dateDetection && DateDetector != nil && DateDetector(val) {
			return Field{Name: name, Type: "date"}, true
		}
		return Field{Name: name, Type: "text", Fields: []Field{{Name: "keyword", Type: "keyword"}}}, true
	case json.Number:
		return Field{Name: name, Type: "number"}, true
	case bool:
		return Field{Name: name, Type: "bool"}, true
	}
	return Field{}, false
}
