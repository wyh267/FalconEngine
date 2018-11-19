package invert

import (
	"github.com/FalconEngine/index/dict"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
)

type InvertReader struct {
	name string
	path string

	dicReader dict.FalconStringDictReadService
	ivtReader store.FalconSearchStoreReadService

}



func NewStringInvertReader(name string,offset int64,dicStore,ivtReader store.FalconSearchStoreReadService) FalconStringInvertReadService {

	reader := &InvertReader{name: name}
	reader.dicReader = dict.NewFalconReadMap()
	if err:=reader.dicReader.LoadDic(dicStore,offset);err!=nil{
		mlog.Error("Load Error : %v",err)
	}
	reader.ivtReader = ivtReader

	//reader.ivtReader = store.NewFalconFileStoreReadService(path + "/" +name + ".ivt")
	return reader

}


func (ir *InvertReader) Fetch(key string) (FalconDocList, bool, error) {

	dv,found:=ir.dicReader.Get(key)
	if !found {
		return nil,false,nil
	}
	docList := NewMemoryFalconDocList()
	// delete by wuyinghao
	//by:=make([]byte,dv.Length*8)
	//if err:=ir.ivtReader.ReadFullBytesAt(int64(dv.Offset),by);err!=nil{
	//	return nil,false,err
	//}
	by,err:=ir.ivtReader.ReadFullBytes(int64(dv.Offset),int64(dv.Length*8))
	if err!=nil{
		return nil,false,err
	}

	if err:=docList.FalconDecoding(by);err!=nil{
		return nil,false,err
	}
	//_,err:=ir.ivtReader.ReadMessage(int64(dv.Offset),docList)
	return docList,found,nil

}