package message

import (
	"github.com/golang/protobuf/proto"
	"encoding/binary"
	"fmt"
)

func (bl *BinlogMessage) FalconEncoding() ([]byte, error) {
	lensbytes:=make([]byte,4)
	bj,_:=proto.Marshal(bl)
	lens := len(bj)
	binary.LittleEndian.PutUint64(lensbytes,uint64(lens))
	lensbytes = append(lensbytes,bj...)
	return lensbytes,nil
}

//type FalconSearchStoreInfo struct{
//	StoreName string
//	StoreLength int64
//}

func NewDicValue() *DictValue{
	return &DictValue{}
}

func (dv *DictValue) FalconEncoding() ([]byte,error) {
	b:=make([]byte,16)
	binary.LittleEndian.PutUint64(b[:8],dv.Offset)
	binary.LittleEndian.PutUint64(b[8:],dv.Length)
	return b,nil

}

func (dv *DictValue) FalconDecoding(src []byte) error {
	if len(src)!=16{
		return fmt.Errorf("Length is not 24 byte")
	}
	dv.Offset=binary.LittleEndian.Uint64(src[:8])
	dv.Length=binary.LittleEndian.Uint64(src[8:])
	return nil
}

func (dv *DictValue) ToString() string {
	return fmt.Sprintf(`{ "offset": %d , "length"：%d }`,dv.Offset,dv.Length)
}



func (di *DocId) ToString() string {
	return fmt.Sprintf(`{"id":%d,"weight":%d}`,di.DocID,di.Weight)
}