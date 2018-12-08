/**
基础接口定义
 */
package util

// 可以比较的类型
type Comparable interface {
	// 相等
	Equal(o interface{}) bool
}


// 可编码
type FalconEncoder interface {
	FalconEncoding() ([]byte, error)
}

// 可解码
type FalconDecoder interface {
	FalconDecoding(bytes []byte) error
}


// 比较任意两个变量
func Equal(a, b interface{}) bool {

	if cmpa, ok := a.(Comparable); ok {
		return cmpa.Equal(b)
	} else if cmpb, ok := b.(Comparable); ok {
		return cmpb.Equal(a)
	} else {
		return a == b
	}
}



// 编码任意数据
func Encoding(e interface{}) ([]byte, error) {

	if encoder, ok := e.(FalconEncoder); ok {
		return encoder.FalconEncoding()
	}
	return nil, ERR_NOT_ENCODER

}

// 解码数据
func Decoding(b []byte, d interface{}) error {

	if decoder, ok := d.(FalconDecoder); ok {
		return decoder.FalconDecoding(b)
	}
	return ERR_NOT_DECODER
}








