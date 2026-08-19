/**
基础接口定义
 */
package util

import (
	"io"
)

// 可以比较的类型
type Comparable interface {
	// 相等
	Equal(o interface{}) bool
}


// 可编码
type FalconEncoder interface {
	FalconEncoding() ([]byte, error)
	FalconStreamEncoding() (FalconWriter,error)
}

// 可解码
type FalconDecoder interface {
	FalconDecoding(bytes []byte) error
	FalconStreamDecoding(in FalconReader) error
}



// 存储写入接口[只能顺序写入]
type FalconWriter interface {

	io.WriteCloser
	WriteUint64(val uint64) error
	WriteInt64(val int64) error
	WriteUVarInt(val uint64) error  // 写入可变长度的数据
	WriteVarInt(val int64) error
	Destroy() error
	Sync() error
}


type FalconRandomReader interface {
	ReadUint64(offset int) (uint64,error)
	ReadInt64(offset int) (int64,error)
	ReadUVarInt(offset int) (uint64,error)
	ReadVarInt(offset int) (int64,error)
	SubReader(offset int,lens int) (FalconReader,error)
	SubRandomReader(offset int,lens int) (FalconRandomReader,error)
	Destroy() error


}

// 存储顺序读取
type FalconReader interface {

	io.ByteReader
	ReadUint64() (uint64,error)
	ReadInt64() (int64,error)
	ReadUVarInt() (uint64,error)
	ReadVarInt() (int64,error)
	io.ReadCloser
	ReadBytes(readLen int) ([]byte,error)
	SubReader(offset int,lens int) (FalconReader,error)
	SubRandomReader(offset int,lens int) (FalconRandomReader,error)
	Destroy() error

}


// 拷贝流
func CopyStream(in FalconReader,out FalconWriter) (int64,error) {
	return io.Copy(out,in)
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


// 编码数据流
func StreamEncoding(e interface{}) (FalconWriter, error) {

	if encoder, ok := e.(FalconEncoder); ok {
		return encoder.FalconStreamEncoding()
	}
	return nil, ERR_NOT_ENCODER

}

// 解码数据流
func StreamDecoding(in FalconReader, d interface{}) error {

	if decoder, ok := d.(FalconDecoder); ok {
		return decoder.FalconStreamDecoding(in)
	}
	return ERR_NOT_DECODER
}


// 编码数据
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








