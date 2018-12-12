/**
基础结构定义
 */
package util

import "errors"

var (
	ERR_NOT_ENCODER = errors.New("val is not encoder")
	ERR_NOT_DECODER = errors.New("val is not decoder")


	TFileStore string = "file_store"
)

type UInt32 uint32

func (u UInt32) Equal(o interface{}) bool {

	if ou, ok := o.(UInt32); ok {
		return ou == u
	}
	return false
}

// 字典元素
type DictItem struct {
}




