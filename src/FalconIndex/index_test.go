
package FalconIndex


import (
    //"fmt"
    "testing"
    "utils"
   // "math/rand"
    //"tree"
    //fi "FalconIndex"
)


func Test_NewIndex(t *testing.T) {
    logger,_:=utils.New("test_log")

    var err error
     idx := NewEmptyIndex("test_index", "./" , logger)
     
     idx.AddField(utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})
     
     if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
     
}


func Test_LoadIndex(t *testing.T) {
    logger,_:=utils.New("test_log")

    var err error
     idx := NewIndexWithLocalFile("test_index", "./" , logger)
     
     idx.AddField(utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
     idx.AddField(utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})
     
     if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}