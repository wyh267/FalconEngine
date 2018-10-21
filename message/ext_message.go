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



func (di *DocId) ToString() string {
	return fmt.Sprintf(`{"id":%d,"weight":%d}`,di.DocID,di.Weight)
}