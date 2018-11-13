package index

import (
	"testing"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/tools"
	"fmt"
	"time"
)

func Test_Index(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "idx.log")

	mappings:=tools.NewFalconIndexMappings()
	mappings.AddFieldMapping(&tools.FalconMapping{FieldName:"field1",FieldType:tools.TKeywordType})
	mappings.AddFieldMapping(&tools.FalconMapping{FieldName:"field2",FieldType:tools.TKeywordType})


	idx := NewIndex("test_index","./data")
	idx.CreateMappings(mappings)

	for i:=0;i<10000;i++{
		doc := make(map[string]interface{})
		doc["field1"]=fmt.Sprintf("dfdsf%d",i)
		doc["field2"]=[]string{fmt.Sprintf("1112"),fmt.Sprintf("2222f%d",i),fmt.Sprintf("3333%d",i)}

		idx.UpdateDocument("yyyhhh",doc)
		time.Sleep(time.Millisecond*200)

	}

	return

}