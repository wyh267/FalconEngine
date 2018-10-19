package message

import (
	"github.com/golang/protobuf/proto"
	"encoding/binary"
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