package segment

import (
	"github.com/FalconEngine/index/invert"
	"fmt"
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/message"
	"os"
)

type FalconSegment struct{
	name string
	indexName string
	path string
	segmentNumber uint32
	docCount uint32
	fieldMappings *tools.FalconIndexMappings

	invertService invert.FalconInvertSetService

}

func LoadFalconSegment(num uint32,indexName string,path string,mappings *tools.FalconIndexMappings) FalconSegmentService{
	fs := &FalconSegment{indexName:indexName,
		name:fmt.Sprintf("%s_%010d",indexName,num),
		path:path,
		fieldMappings:mappings,
		segmentNumber:num}
	os.MkdirAll(path,0777)

	fs.invertService = invert.NewInvertSet(fs.name,path)

	mlog.Info("Load [ %s ] Segment [ %s ] success ...",fs.indexName,fs.name)
	return fs

}

func NewFalconSegment(num uint32,indexName string,path string,mappings *tools.FalconIndexMappings) FalconSegmentService {

	fs := &FalconSegment{indexName:indexName,
	name:fmt.Sprintf("%s_%010d",indexName,num),
	path:path,
	fieldMappings:mappings,
	segmentNumber:num}

	os.MkdirAll(path,0777)

	fs.invertService = invert.NewInvertSet(fs.name,path)

	for _,v := range fs.fieldMappings.GetMappings() {

		finfo,err:=v.GetFieldInfo()
		if err != nil {
			return nil
		}
		fs.invertService.AddField(v.FieldName,finfo.Type)
	}
	mlog.Info("Create [ %s ] Segment [ %s ] success ...",fs.indexName,fs.name)
	return fs
}

func (fs *FalconSegment) Number() uint32{
	return fs.segmentNumber
}

func (fs *FalconSegment) DocumentCount() uint32 {
	return fs.docCount
}

func (fs *FalconSegment) Name() string {
	return fs.name
}

func (fs *FalconSegment) AddField(mapping *tools.FalconMapping) error {


	//if err:=fs.fieldMappings.AddFieldMapping(mapping);err!=nil{
	//	mlog.Error("add mappings [ %s ] error %v",mapping.FieldName,err)
	//	return err
	//}

	finfo,_:=mapping.GetFieldInfo()
	err:= fs.invertService.AddField(finfo.Name,finfo.Type)
	if err != nil {
		return err
	}
	return nil

}

func (fs *FalconSegment) UpdateDocument(document map[string]interface{}) error {

	docId := &message.DocId{DocID:fs.docCount,Weight:0}
	for field,value := range document {

		fieldMapping,ok := fs.fieldMappings.GetFieldMapping(field)//(*fs.fieldMappings)[field]
		if !ok {
			mlog.Error("Field [ %s ] Mapping not found",field)
			return fmt.Errorf("Field mapping not found...")
		}


		switch value.(type){
		case string:
			if fieldMapping.FieldType == tools.TKeywordType {
				realValue,_ := value.(string)
				if err:=fs.invertService.PutString(field,realValue,docId);err!=nil{
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
					if err:=fs.invertService.PutString(field,realValue,docId);err!=nil{
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
	//mlog.Info("Update Document ... %v",document)
	return nil


}

func (fs *FalconSegment)SimpleSearch(field,keyword string) (invert.FalconDocList,bool,error){

	return fs.invertService.FetchString(field,keyword)
}

func (fs *FalconSegment) Persistence() error {

	return fs.invertService.Persistence()


}

func (fs *FalconSegment) Close() error {

	return fs.invertService.Close()

}

func (fs *FalconSegment) ToString() string {
	panic("implement me")


}