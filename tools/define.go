package tools

import (
	"encoding/binary"
	"fmt"
)

type FalconSearchEncoder interface {
	FalconEncoding() ([]byte, error)
	ToString() string
}

type FalconSearchDecoder interface {
	FalconDecoding(bytes []byte) error
	ToString() string
	// FalconPrepare(length int64) (int64,error)
}

type FalconCoder interface {
	FalconEncoding() ([]byte, error)
	FalconDecoding(bytes []byte) error
	ToString() string
}


type DictValue struct {
	Val uint64
	ExtVal uint64
}

func NewDicValue() *DictValue{
	return &DictValue{}
}

func (dv *DictValue) FalconEncoding() ([]byte,error) {
	b:=make([]byte,24)
	binary.LittleEndian.PutUint64(b[:8],uint64(16))
	binary.LittleEndian.PutUint64(b[8:16],dv.Val)
	binary.LittleEndian.PutUint64(b[16:],dv.ExtVal)
	return b,nil

}

func (dv *DictValue) FalconDecoding(src []byte) error {
	if len(src)!=24{
		return fmt.Errorf("Length is not 24 byte")
	}
	dv.Val=binary.LittleEndian.Uint64(src[8:16])
	dv.ExtVal=binary.LittleEndian.Uint64(src[16:])
	return nil
}

func (dv *DictValue) ToString() string {
	return fmt.Sprintf(`{ "Val": %d , "ExtVal"：%d }`,dv.Val,dv.ExtVal)
}



type DocId struct{
	DocID uint32
	Weight uint32
}

func (di *DocId) ToString() string {
	return fmt.Sprintf(`{"id":%d,"weight":%d}`,di.DocID,di.Weight)
}


// 字段类型
type FalconFieldType uint32

const (
	// 字符串类型
	TFalconString FalconFieldType = 0x0001
)


// 字符串信息
type FalconFieldInfo struct {
	Name string
	Type FalconFieldType
	Offset int64

}



func (ffi *FalconFieldInfo) FalconEncoding() ([]byte, error) {
	b:=make([]byte,8)
	lensBytes:=make([]byte,4)
	binary.LittleEndian.PutUint32(lensBytes,uint32(len(ffi.Name)))
	b=append(b,lensBytes...)
	b=append(b,[]byte(ffi.Name)...)
	binary.LittleEndian.PutUint32(lensBytes,uint32(ffi.Type))
	b=append(b,lensBytes...)

	lensBytes=make([]byte,8)
	binary.LittleEndian.PutUint64(lensBytes,uint64(ffi.Offset))
	b=append(b,lensBytes...)
	binary.LittleEndian.PutUint64(b[:8],uint64(len(b)))
	return b,nil

}

func (ffi *FalconFieldInfo) FalconDecoding(bytes []byte) error {

	lens:=binary.LittleEndian.Uint32(bytes[8:8+4])
	ffi.Name = string(bytes[12:lens+12])
	ffi.Type = FalconFieldType(binary.LittleEndian.Uint32(bytes[lens+12:lens+12+4]))
	ffi.Offset = int64(binary.LittleEndian.Uint64(bytes[lens+12+4:]))
	return nil

}

func (ffi *FalconFieldInfo) ToString() string {
	return fmt.Sprintf(`{"name":"%s","type":"%d","offset":%d}`,ffi.Name,ffi.Type,ffi.Offset)
}


// 读写模式
type FalconMode uint32

const (
	TWriteMode FalconMode = 0x0001
	TReadMode FalconMode = 0x0002
	TRWMode FalconMode = 0x0003
)