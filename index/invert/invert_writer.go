package invert

import (
	"sync"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/index/dict"
	"fmt"
	"github.com/FalconEngine/message"
)

type InvertWriter struct {
	name string

	tmpInvert    map[string]FalconDocList
	invertLocker *sync.RWMutex
}

func NewStringInvertWriter(name string) FalconStringInvertWriteService {
	writer := &InvertWriter{name: name, tmpInvert: make(map[string]FalconDocList), invertLocker: new(sync.RWMutex)}

	return writer
}

func (iw *InvertWriter) Put(key string, docid *message.DocId) error {
	iw.invertLocker.Lock()
	defer iw.invertLocker.Unlock()
	if _, ok := iw.tmpInvert[key]; !ok {
		iw.tmpInvert[key] = NewMemoryFalconDocList()
	}

	return iw.tmpInvert[key].Push(docid)

}

func (iw *InvertWriter) Store(invertListStore,dictStore store.FalconSearchStoreWriteService) (int64,error) {

	//invertListStore := store.NewFalconFileStoreWriteService(iw.path + "/" + iw.name + ".ivt")
	//dictStore := store.NewFalconFileStoreWriteService(iw.path + "/" + iw.name + ".dic")
	dictMap := dict.NewFalconWriteMap()

	for key,v := range iw.tmpInvert {
		pos,err:=invertListStore.AppendMessage(v)
		if err != nil {
			mlog.Error("Write Error : %v",err)
			return -1,err
		}
		dictMap.Put(key,&message.DictValue{Val:uint64(pos)})
	}

	return dictStore.AppendMessage(dictMap)

	//offset,err:=dictStore.AppendMessage(dictMap)
	//if err != nil {
	//	mlog.Error("Write Error : %v",err)
	//	return -1,err
	//}

	//invertListStore.Close()
	//dictStore.Close()
	//return offset,err
}


func (iw *InvertWriter) ToString() string {

	result := "\n"
	for key,v := range iw.tmpInvert {
		result = result + fmt.Sprintf("[ %s ] >> %s \n",key,v.ToString())
	}
	return result
}