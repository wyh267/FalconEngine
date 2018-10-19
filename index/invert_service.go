package index

import (
	"github.com/FalconEngine/tools"
)

type FalconDocList interface {
	GetLength() int
	GetDoc(idx int) (*tools.DocId,error)
	Push(docid *tools.DocId) error
	tools.FalconCoder
}


type FalconStringInvertWriteService interface {
	Put(key string,docid *tools.DocId) error
	Store() error
	ToString() string
}


type FalconStringInvertReadService interface {
	Fetch(key string) (FalconDocList,bool,error)
}