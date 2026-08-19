package dict

import (
	"fmt"
	"encoding/binary"
	"sync"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/message"
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

	storeBodyLength uint64
	startOffset int64
	dic map[string]*message.DictValue
	locker *sync.RWMutex
}

func NewFalconWriteMap() FalconStringDictWriteService {
	return &FalconMap{dic:make(map[string]*message.DictValue),locker:new(sync.RWMutex)}
}

func NewFalconReadMap() FalconStringDictReadService {
	return &FalconMap{dic:make(map[string]*message.DictValue),locker:new(sync.RWMutex)}
}


func (fm *FalconMap) storeHeader(storeService store.FalconSearchStoreWriteService) error {

	blength := make([]byte, 8)
	binary.LittleEndian.PutUint64(blength, fm.storeBodyLength)
	_, err := storeService.AppendBytes(blength)
	//mlog.Info("lens : %d",fm.storeBodyLength)
	return err

}

func (fm *FalconMap) loadHeader(storeService store.FalconSearchStoreReadService) error {

	// delete by wuyinghao
	//blength := make([]byte,8)
	//if err:=storeService.ReadFullBytesAt(fm.startOffset,blength);err!=nil{
	//	return err
	//}

	//blength := make([]byte,8)
	if blength,err:=storeService.ReadFullBytes(fm.startOffset,8);err!=nil{
		return err
	}else{
		fm.storeBodyLength = binary.LittleEndian.Uint64(blength)
		return nil
	}


}

func (fm *FalconMap) LoadDic(storeService store.FalconSearchStoreReadService,offset int64) error{

	fm.startOffset = offset
	fm.loadHeader(storeService)

	// delete by wuyinghao
	//mapStoreBody := make([]byte,fm.storeBodyLength)
	//if err:=storeService.ReadFullBytesAt(offset+8,mapStoreBody);err!=nil{
	//	return err
	//}
	//
	//return fm.FalconDecoding(mapStoreBody)

	if mapStoreBody,err:=storeService.ReadFullBytes(offset+8,int64(fm.storeBodyLength));err!=nil{
		return err
	}else{
		return fm.FalconDecoding(mapStoreBody)
	}



}





// 写入文件
func (fm *FalconMap) Persistence(storeService store.FalconSearchStoreWriteService) (int64,error) {
	//mlog.Info("Persistence dict to store service ...")
	// 编码
	encodeBytes,_:=fm.FalconEncoding()
	fm.storeBodyLength = uint64(len(encodeBytes))

	//保存头
	fm.storeHeader(storeService)
	//保存内容
	pos,err:=storeService.AppendBytes(encodeBytes)
	if err != nil {
		return pos,err
	}
	storeService.Sync()
	//mlog.Info("map store pos %d",pos)
	return pos-8,err
}


func (fm *FalconMap) FalconEncoding() ([]byte, error) {

	fmBytes:=make([]byte,0)
	for k,v := range fm.dic {
		keyBytes,_:=FalconString(k).FalconEncoding()
		fmBytes = append(fmBytes,keyBytes...)
		valBytes,_:=v.FalconEncoding()
		fmBytes = append(fmBytes,valBytes...)
	}
	return fmBytes,nil
}

func (fm *FalconMap) FalconDecoding(bytes []byte) error {
	end := len(bytes)
	for pos:=0;pos<end; {
		//mlog.Info("pos %d %d",pos,end)
		keyLen := int(binary.LittleEndian.Uint64(bytes[pos:pos+8]))
		//mlog.Info("pos %d ll %d",pos,keyLen)

		key := string(bytes[pos+8:pos+8+keyLen])
		pos = pos+8+keyLen
		val := message.NewDicValue()
		val.FalconDecoding(bytes[pos:pos+16])
		fm.dic[key] = val
		pos = pos + 16
		//mlog.Info("posn %d %s",pos,val.ToString())

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

func (fm *FalconMap) Put(key string, dv *message.DictValue) error {
	fm.locker.Lock()
	defer fm.locker.Unlock()
	fm.dic[key] = dv
	return nil
}

func (fm *FalconMap) Get(key string) (*message.DictValue, bool) {
	fm.locker.RLock()
	defer fm.locker.RUnlock()
	v,ok:=fm.dic[key]
	return v,ok

}


