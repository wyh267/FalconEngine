package segment

import (
	"testing"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/tools"
	"fmt"
)

func Test_Segment(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "iw.log")

	mappings:=make(map[string]*tools.FalconMapping)

	mappings["field1"] = &tools.FalconMapping{FieldName:"field1",FieldType:tools.TKeywordType}
	mappings["field2"] = &tools.FalconMapping{FieldName:"field2",FieldType:tools.TKeywordType}


	sg := NewFalconSegment(uint32(0),"test_index",".",&mappings)

	for i:=0;i<100;i++{
		doc := make(map[string]interface{})
		doc["field1"]=fmt.Sprintf("dfdsf%d",i)
		doc["field2"]=[]string{fmt.Sprintf("1112"),fmt.Sprintf("2222f%d",i),fmt.Sprintf("3333%d",i)}

		sg.UpdateDocument(doc)

	}

	sg.Persistence()
	l,_,_:=sg.SimpleSearch("field2","1112")
	mlog.Info("%s",l.ToString())
}