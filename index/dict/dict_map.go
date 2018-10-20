package dict

import (
	"fmt"
	"github.com/FalconEngine/tools"
	"encoding/binary"
	"sync"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
)

type FalconString string

func (fs FalconString) FalconEncoding() ([]byte, error) {
	b:=make([]byte,8)
	binary.LittleEndian.PutUint64(b[:8],uint64(len(fs)))
	by:=[]byte(string(fs))
	b = append(b,by...)
	return b,nil
}

func (fs FalconString) FalconDecoding(bytes []byte) error {
	fs = FalconString(string(bytes[8:]))
	return nil
}


type FalconMap struct {
	dic map[string]*tools.DictValue
	locker *sync.RWMutex
}

func NewFalconWriteMap() FalconStringDictWriteService {
	return &FalconMap{dic:make(map[string]*tools.DictValue),locker:new(sync.RWMutex)}
}

func NewFalconReadMap() FalconStringDictReadService {
	return &FalconMap{dic:make(map[string]*tools.DictValue),locker:new(sync.RWMutex)}
}

func NewFalconMap() FalconStringDictService {
	return &FalconMap{dic:make(map[string]*tools.DictValue),locker:new(sync.RWMutex)}
}

func (fm *FalconMap) LoadDic(storeService store.FalconSearchStoreReadService,offset int64) error{
	_,err:=storeService.ReadMessage(offset,fm)
	return err
}


func (fm *FalconMap) WriteDic(storeService store.FalconSearchStoreWriteService) (int64,error) {
	mlog.Info("Write To Store Service ...")
	pos,err:= storeService.AppendMessage(fm)
	if err != nil {
		return pos,err
	}
	storeService.Sync()
	return pos,err
}


func (fm *FalconMap) FalconEncoding() ([]byte, error) {

	fmBytes:=make([]byte,8)
	for k,v := range fm.dic {
		keyBytes,_:=FalconString(k).FalconEncoding()
		fmBytes = append(fmBytes,keyBytes...)
		valBytes,_:=v.FalconEncoding()
		fmBytes = append(fmBytes,valBytes...)
	}
	binary.LittleEndian.PutUint64(fmBytes[:8],uint64(len(fmBytes)))
	return fmBytes,nil
}

func (fm *FalconMap) FalconDecoding(bytes []byte) error {
	end := len(bytes)
	for pos:=8;pos<end; {
		keyLen := int(binary.LittleEndian.Uint64(bytes[pos:pos+8]))
		key := string(bytes[pos+8:pos+8+keyLen])
		valLen := int(binary.LittleEndian.Uint64(bytes[pos+8+keyLen:pos+8+keyLen+8]))
		val := tools.NewDicValue()
		val.FalconDecoding(bytes[pos+8+keyLen:pos+8+keyLen+8+valLen])
		fm.dic[key] = val
		pos = pos + (8+keyLen+8+valLen)
	}
	return nil
}

func (fm *FalconMap) ToString() string {

	result := ""
	for k,v:=range fm.dic {
		s := fmt.Sprintf("%s >> %s \n",k,v.ToString())
		result = result + s
	}
	return result
}

func (fm *FalconMap) Put(key string, dv *tools.DictValue) error {
	fm.locker.Lock()
	defer fm.locker.Unlock()
	fm.dic[key] = dv
	return nil
}

func (fm *FalconMap) Get(key string) (*tools.DictValue, bool) {
	fm.locker.RLock()
	defer fm.locker.RUnlock()
	v,ok:=fm.dic[key]
	return v,ok

}


