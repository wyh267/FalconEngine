package invert

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/store"
	"io"
	"github.com/FalconEngine/message"
)


// 倒排链
type FalconDocList interface {
	GetLength() int
	GetDoc(idx int) (*message.DocId,error)
	Push(docid *message.DocId) error
	tools.FalconCoder
}


// 字符型倒排索引写服务
type FalconStringInvertWriteService interface {
	Put(key string,docid *message.DocId) error
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
	PutString(field,key string,docid *message.DocId) error
	// 搜索
	FetchString(field,key string) (FalconDocList,bool,error)
	// 持久化
	Persistence() error
	// 关闭
	io.Closer

	ToString() string
}