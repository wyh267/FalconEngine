package segment

import (
	"github.com/FalconEngine/index/invert"
	"fmt"
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/message"
)

type FalconSegment struct{
	name string
	indexName string
	path string
	segmentNumber uint32
	docCount uint32
	fieldMappings *map[string]*tools.FalconMapping

	invterService invert.FalconInvertSetService

}


func NewFalconSegment(num uint32,indexName string,path string,mappings *map[string]*tools.FalconMapping) FalconSegmentService {

	fs := &FalconSegment{indexName:indexName,
	name:fmt.Sprintf("%s_%d",indexName,num),
	path:path,
	fieldMappings:mappings,
	segmentNumber:num}


	fs.invterService = invert.NewInvertSet(fs.name,path)
	for k,v := range *fs.fieldMappings {
		finfo,err:=v.GetFieldInfo()
		if err != nil {
			return nil
		}
		fs.invterService.AddField(k,finfo.Type)
	}
	mlog.Info("Create [ %s ] Segment [ %s ] success ...",fs.indexName,fs.name)
	return fs
}



func (fs *FalconSegment) AddField(mapping *tools.FalconMapping) error {

	finfo,_:=mapping.GetFieldInfo()
	err:= fs.invterService.AddField(finfo.Name,finfo.Type)
	if err != nil {
		return err
	}
	(*fs.fieldMappings)[mapping.FieldName] = mapping
	return nil

}

func (fs *FalconSegment) UpdateDocument(document map[string]interface{}) error {

	docId := &message.DocId{DocID:fs.docCount,Weight:0}
	for field,value := range document {

		fieldMapping,ok := (*fs.fieldMappings)[field]
		if !ok {
			mlog.Error("Field [ %s ] Mapping not found",field)
			return fmt.Errorf("Field mapping not found...")
		}


		switch value.(type){
		case string:
			if fieldMapping.FieldType == tools.TKeywordType {
				realValue,_ := value.(string)
				if err:=fs.invterService.PutString(field,realValue,docId);err!=nil{
					return err
				}
				continue
			}
			mlog.Error("field [ %s ] type is wrong ...",field)
			return fmt.Errorf("field [ %s ] type is wrong ...",field)
		case []string:
			if fieldMapping.FieldType == tools.TKeywordType {
				realValues,_ := value.([]string)
				for _,realValue:=range realValues {
					if err:=fs.invterService.PutString(field,realValue,docId);err!=nil{
						return err
					}
				}
				continue
			}
			mlog.Error("field [ %s ] type is wrong ...",field)
			return fmt.Errorf("field [ %s ] type is wrong ...",field)

		default:
			panic("unknown type")

		}

	}
	fs.docCount ++
	mlog.Info("Update Document ... %v",document)
	return nil


}

func (fs *FalconSegment)SimpleSearch(field,keyword string) (invert.FalconDocList,bool,error){

	return fs.invterService.FetchString(field,keyword)
}

func (fs *FalconSegment) Persistence() error {

	return fs.invterService.Persistence()


}

func (fs *FalconSegment) Close() error {

	return fs.invterService.Close()

}

func (fs *FalconSegment) ToString() string {
	panic("implement me")


}