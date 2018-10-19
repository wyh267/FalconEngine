package dict

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/store"
)

// 字典服务
type FalconStringDictService interface {
	Put(key string,dv *tools.DictValue) error
	Get(key string) (*tools.DictValue,bool)
	WriteDic(storeService store.FalconSearchStoreWriteService) (int64,error)
	tools.FalconSearchEncoder
}

type FalconStringDictWriteService interface {
	Put(key string,dv *tools.DictValue) error
	WriteDic(storeService store.FalconSearchStoreWriteService) (int64,error)
	tools.FalconSearchEncoder
}

type FalconStringDictReadService interface {
	Get(key string) (*tools.DictValue,bool)
	LoadDic(storeService store.FalconSearchStoreReadService) error
	tools.FalconSearchDecoder
}