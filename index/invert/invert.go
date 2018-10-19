package invert

import (
	"github.com/FalconEngine/tools"
	"sync"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/index/dict"
	"fmt"
)

type InvertWriter struct {
	name string
	path string

	tmpInvert    map[string]FalconDocList
	invertLocker *sync.RWMutex
}

func NewStringInvertWriter(name string, path string) FalconStringInvertWriteService {
	writer := &InvertWriter{name: name, path: path, tmpInvert: make(map[string]FalconDocList), invertLocker: new(sync.RWMutex)}

	return writer
}

func (iw *InvertWriter) Put(key string, docid *tools.DocId) error {
	iw.invertLocker.Lock()
	defer iw.invertLocker.Unlock()
	if _, ok := iw.tmpInvert[key]; !ok {
		iw.tmpInvert[key] = NewMemoryFalconDocList()
	}

	return iw.tmpInvert[key].Push(docid)

}

func (iw *InvertWriter) Store() error {

	invertListStore := store.NewFalconFileStoreWriteService(iw.path + "/" + iw.name + ".ivt")
	dictStore := store.NewFalconFileStoreWriteService(iw.path + "/" + iw.name + ".dic")
	dictMap := dict.NewFalconWriteMap()

	for key,v := range iw.tmpInvert {
		pos,err:=invertListStore.AppendMessage(v)
		if err != nil {
			mlog.Error("Write Error : %v",err)
			return err
		}
		dictMap.Put(key,&tools.DictValue{Val:uint64(pos)})
	}
	_,err:=dictStore.AppendMessage(dictMap)
	if err != nil {
		mlog.Error("Write Error : %v",err)
		return err
	}

	invertListStore.Close()
	dictStore.Close()
	return nil
}


func (iw *InvertWriter) ToString() string {

	result := "\n"
	for key,v := range iw.tmpInvert {
		result = result + fmt.Sprintf("[ %s ] >> %s \n",key,v.ToString())
	}
	return result
}