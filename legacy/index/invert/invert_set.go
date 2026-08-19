package invert


import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
	"fmt"
	"github.com/FalconEngine/message"
	"strings"
	"encoding/json"
	"encoding/binary"
)

// 倒排索引集合
type InvertSet struct {
	name string
	path string
	mode tools.FalconMode
	fieldInformations map[string]*tools.FalconFieldInfo

	// 字符串倒排写服务
	stringInvertWriteServices map[string]FalconStringInvertWriteService
	// 字符串倒排读服务
	stringInvertReadServices map[string]FalconStringInvertReadService



	//invertListStoreWriter store.FalconSearchStoreWriteService
	//dictStoreWriter store.FalconSearchStoreWriteService


	invertListStoreReader store.FalconSearchStoreReadService
	dictStoreReader store.FalconSearchStoreReadService



}



func NewInvertSet(name string,path string) FalconInvertSetService {

	is := &InvertSet{name:name,path:path,
	fieldInformations:make(map[string]*tools.FalconFieldInfo),
	stringInvertReadServices:make(map[string]FalconStringInvertReadService),
	stringInvertWriteServices:make(map[string]FalconStringInvertWriteService)}

	metaFile := path + "/" + name + ".mt"
	ivtFile := path + "/" + name + ".ivt"
	dicFile := path + "/" + name + ".dic"

	if tools.Exists(metaFile) && tools.Exists(ivtFile) && tools.Exists(dicFile) {
		is.invertListStoreReader = store.NewFalconSearchStoreReadService(ivtFile)
		is.dictStoreReader = store.NewFalconSearchStoreReadService(dicFile)
		if err:=is.init();err!=nil{
			return nil
		}
		mlog.Trace("Load InvertSetService with read mode success ...")
		return is
	}


	is.mode = tools.TWriteMode

	mlog.Trace("Start InvertSetService with write mode success ...")

	return is



}

func (is *InvertSet) init() error {

	if err:=is.loadMeta();err!=nil{
		mlog.Error("load meta error : %v",err)
		return err
	}
	is.mode = tools.TReadMode
	return nil
}


func (is *InvertSet) AddField(field string, fieldType tools.FalconFieldType) error {

	if _,ok:=is.fieldInformations[field];ok{
		return fmt.Errorf("Field [ %s ] is already exist",field)
	}

	is.fieldInformations[field] = &tools.FalconFieldInfo{Name:field,Type:fieldType,Offset:0}

	switch fieldType {
	case tools.TFalconString:
		is.stringInvertWriteServices[field] = NewStringInvertWriter(is.name)
	default:
		mlog.Error("unkown field type %d",fieldType)
		return fmt.Errorf("unkown field type")
	}


	return nil

}

func (is *InvertSet) PutString(field, key string, docid *message.DocId) error {

	if is.mode & tools.TWriteMode != tools.TWriteMode {
		mlog.Error("not write mode ...")
		return fmt.Errorf("not write mode")
	}

	if ivtWriter ,ok := is.stringInvertWriteServices[field];!ok {
		return fmt.Errorf("Field is not string mode or not found ...")
	}else{
		return ivtWriter.Put(key,docid)
	}

}

func (is *InvertSet) FetchString(field, key string) (FalconDocList, bool, error) {

	if is.mode & tools.TReadMode != tools.TReadMode {
		mlog.Error("not write mode ...")
		return nil,false,fmt.Errorf("not write mode")
	}

	if ivtReader,ok := is.stringInvertReadServices[field];!ok{
		return nil,false,fmt.Errorf("Field is not string mode or not found ...")
	}else{
		return ivtReader.Fetch(key)
	}


}

func (is *InvertSet) Persistence() error {

	// 模式判断
	if is.mode & tools.TWriteMode != tools.TWriteMode {
		mlog.Error("not write mode ...")
		return fmt.Errorf("not write mode")
	}

	ivtFile := is.path + "/" + is.name + ".ivt"
	dicFile := is.path + "/" + is.name + ".dic"


	invertListStoreWriter := store.NewFalconFileStoreWriteService(ivtFile)
	dictStoreWriter := store.NewFalconFileStoreWriteService(dicFile)

	// 持久化数据
	for fieldName,ivtWriter := range is.stringInvertWriteServices {
		offset,err:=ivtWriter.Persistence(invertListStoreWriter,dictStoreWriter)
		if err != nil {
			mlog.Error("Persistence [ %s ] failure : %v",fieldName,err)
			return err
		}
		//mlog.Info("field : %s dic offset : %d",fieldName,offset)
		is.fieldInformations[fieldName].Offset = offset
	}
	invertListStoreWriter.Close()
	dictStoreWriter.Close()

	// 持久化元数据
	is.storeMeta()


	// 重新以只读方式读取所有数据
	is.invertListStoreReader = store.NewFalconSearchStoreReadService(is.path + "/" + is.name + ".ivt")
	is.dictStoreReader = store.NewFalconSearchStoreReadService(is.path + "/" + is.name + ".dic")


	for _,fi := range is.fieldInformations {
		//mlog.Info("Field Information : %s",fi.ToString())
		is.stringInvertReadServices[fi.Name] = NewStringInvertReader(fi.Name,fi.Offset,is.dictStoreReader,is.invertListStoreReader)
	}
	is.mode = tools.TReadMode


	return nil

}

func (is *InvertSet) Close() error {

	// TODO 判断
	is.dictStoreReader.Close()
	is.invertListStoreReader.Close()

	return nil
}

func (is *InvertSet) ToString() string {

	info := make([]string,0)
	for _,fi:=range is.fieldInformations {
		info = append(info,fi.ToString())
	}


	return fmt.Sprintf("[[\n%s\n]]",strings.Join(info,"\n"))
}

func (is *InvertSet) FalconEncoding() ([]byte, error) {
	//encBytes := make([]byte, 0)

	bj,err:=json.Marshal(is.fieldInformations)
	if err!=nil {
		mlog.Error("json : %v",err)
		return nil,err
	}
	//encBytes = append(encBytes,bj...)
	//binary.LittleEndian.PutUint64(encBytes[:8],uint64(len(encBytes)))

	return bj,nil

}

func (is *InvertSet) FalconDecoding(bytes []byte) error {

	json.Unmarshal(bytes,&is.fieldInformations)

	for _,fi:=range is.fieldInformations {
		is.stringInvertReadServices[fi.Name] = NewStringInvertReader(fi.Name,fi.Offset,is.dictStoreReader,is.invertListStoreReader)
	}

	return nil


}


func (is *InvertSet) loadMeta() error {

	metaFile := is.path + "/" + is.name + ".mt"

	metaReader := store.NewFalconSearchStoreReadService(metaFile)
	// delete by wuyinghao
	//lensBytes := make([]byte,8)
	//if err:=metaReader.ReadFullBytesAt(0,lensBytes);err!=nil{
	//	return err
	//}
	//metaBytes := make([]byte,binary.LittleEndian.Uint64(lensBytes))
	//if err:=metaReader.ReadFullBytesAt(8,metaBytes);err!=nil{
	//	mlog.Error("read meta bytes error : %v",err)
	//	return err
	//}

	lensBytes,err:=metaReader.ReadFullBytes(0,8)
	if err!=nil{
		return err
	}
	metaBytes,err:=metaReader.ReadFullBytes(8,int64(binary.LittleEndian.Uint64(lensBytes)))
	if err!=nil{
		mlog.Error("read meta bytes error : %v",err)
		return err
	}


	if err:=is.FalconDecoding(metaBytes);err!=nil{
		mlog.Error("decoding error : %v",err)
		return err
	}

	return metaReader.Close()

}

func (is *InvertSet) storeMeta() error {

	metaFile := is.path + "/" + is.name + ".mt"

	// 持久化元数据
	metaStoreWriter := store.NewFalconFileStoreWriteService(metaFile)
	metaBytes,err:=is.FalconEncoding()
	if err != nil {
		mlog.Error(" encoding meta error : %v",err)
		return err
	}
	// 写入长度
	metaLensBytes := make([]byte,8)
	binary.LittleEndian.PutUint64(metaLensBytes,uint64(len(metaBytes)))
	metaStoreWriter.AppendBytes(metaLensBytes)
	metaStoreWriter.AppendBytes(metaBytes)
	metaStoreWriter.Close()

	return nil

}