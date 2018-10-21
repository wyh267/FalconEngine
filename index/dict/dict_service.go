package dict

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/message"
)

// 字典服务
type FalconStringDictService interface {
	Put(key string,dv *message.DictValue) error
	Get(key string) (*message.DictValue,bool)
	WriteDic(storeService store.FalconSearchStoreWriteService) (int64,error)
	tools.FalconSearchEncoder
}

type FalconStringDictWriteService interface {
	Put(key string,dv *message.DictValue) error
	WriteDic(storeService store.FalconSearchStoreWriteService) (int64,error)
	tools.FalconSearchEncoder
}

type FalconStringDictReadService interface {
	Get(key string) (*message.DictValue,bool)
	LoadDic(storeService store.FalconSearchStoreReadService,offset int64) error
	tools.FalconSearchDecoder
}