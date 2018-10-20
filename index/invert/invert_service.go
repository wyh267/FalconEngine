package invert

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/store"
)


// 倒排链
type FalconDocList interface {
	GetLength() int
	GetDoc(idx int) (*tools.DocId,error)
	Push(docid *tools.DocId) error
	tools.FalconCoder
}


// 字符型倒排索引写服务
type FalconStringInvertWriteService interface {
	Put(key string,docid *tools.DocId) error
	//Store() error
	Store(invertListStore,dictStore store.FalconSearchStoreWriteService) (int64,error)
	ToString() string
}

// 字符型倒排索引读服务
type FalconStringInvertReadService interface {
	Fetch(key string) (FalconDocList,bool,error)
}


// 倒排索引服务集合[多个字段]
type FalconInvertSetService interface {
	// 新增一个字段
	AddField(field string,fieldType tools.FalconFieldType) error
	// 写入
	PutString(field,key string,docid *tools.DocId) error
	// 搜索
	FetchString(field,key string) (FalconDocList,bool,error)
	// 持久化
	Persistence() error

	ToString() string
}