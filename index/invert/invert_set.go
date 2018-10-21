package invert

import (
	"github.com/FalconEngine/tools"
	"fmt"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/store"
	"encoding/binary"
	"encoding/json"
	"strings"
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



	invertListStoreWriter store.FalconSearchStoreWriteService
	dictStoreWriter store.FalconSearchStoreWriteService


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
		is.invertListStoreReader = store.NewFalconFileStoreReadService(ivtFile)
		is.dictStoreReader = store.NewFalconFileStoreReadService(dicFile)
		if err:=is.init(metaFile);err!=nil{
			return nil
		}
		mlog.Info("Load InvertSetService with read mode success ...")
		return is
	}


	is.mode = tools.TWriteMode
	is.invertListStoreWriter = store.NewFalconFileStoreWriteService(ivtFile)
	is.dictStoreWriter = store.NewFalconFileStoreWriteService(dicFile)
	mlog.Info("Start InvertSetService with write mode success ...")

	return is



}

func (is *InvertSet) init(metaFile string) error {

	metaReader := store.NewFalconFileStoreReadService(metaFile)
	if _,err:=metaReader.ReadMessage(0,is);err!=nil{
		return err
	}
	is.mode = tools.TReadMode
	return metaReader.Close()
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

func (is *InvertSet) PutString(field, key string, docid *tools.DocId) error {

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

	// 持久化数据
	for fieldName,ivtWriter := range is.stringInvertWriteServices {
		offset,err:=ivtWriter.Store(is.invertListStoreWriter,is.dictStoreWriter)
		if err != nil {
			mlog.Error("Persistence [ %s ] failure : %v",fieldName,err)
			return err
		}
		is.fieldInformations[fieldName].Offset = offset
	}
	is.invertListStoreWriter.Close()
	is.dictStoreWriter.Close()

	// 持久化元数据
	metaStoreWriter := store.NewFalconFileStoreWriteService(is.path + "/" + is.name + ".mt")
	metaStoreWriter.AppendMessage(is)
	metaStoreWriter.Close()


	// 重新以只读方式读取所有数据
	is.invertListStoreReader = store.NewFalconFileStoreReadService(is.path + "/" + is.name + ".ivt")
	is.dictStoreReader = store.NewFalconFileStoreReadService(is.path + "/" + is.name + ".dic")


	for _,fi := range is.fieldInformations {
		mlog.Info("Field Information : %s",fi.ToString())
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
	encBytes := make([]byte, 8)

	bj,err:=json.Marshal(is.fieldInformations)
	if err!=nil {
		mlog.Error("json : %v",err)
		return nil,err
	}
	encBytes = append(encBytes,bj...)
	binary.LittleEndian.PutUint64(encBytes[:8],uint64(len(encBytes)))

	return encBytes,nil

}

func (is *InvertSet) FalconDecoding(bytes []byte) error {

	json.Unmarshal(bytes[8:],&is.fieldInformations)

	for _,fi:=range is.fieldInformations {
		is.stringInvertReadServices[fi.Name] = NewStringInvertReader(fi.Name,fi.Offset,is.dictStoreReader,is.invertListStoreReader)
	}

	return nil


}