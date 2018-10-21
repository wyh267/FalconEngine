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
	_,err:=ir.ivtReader.ReadMessage(int64(dv.Val),docList)
	return docList,found,err

}