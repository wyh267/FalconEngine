package FalconIndex


import (
    "fmt"
    "testing"
    "utils"
    //fi "FalconIndex"
)



func Test_NewEmptySegmentAndAddDocument(t *testing.T) {
    fmt.Printf("============================  Test_NewEmptySegmentAndAddDocument ======================== ====\n")
    logger,_:=utils.New("test_log")
    var err error
    
    fields := make([]utils.SimpleFieldInfo, 0)
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})

   
    segment := NewEmptySegmentWithFieldsInfo("test_segment_111",0,fields,logger)
    
    for docid := uint32(0); docid < 10; docid++ {
		content := make(map[string]string)

		content["AAA"] = "1"
		content["BBB"] = "98"
		content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", docid)

		err := segment.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
    
    segment.AddField(utils.SimpleFieldInfo{FieldName: "YYY", FieldType: utils.IDX_TYPE_STRING})
    
    for docid := uint32(10); docid < 20; docid++ {
		content := make(map[string]string)

		content["AAA"] = "1"
		content["BBB"] = "98"
		content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", docid)

		err := segment.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
    
    segment.DeleteField("ZZZ")
    
    segment.Serialization()
    
    segment.Close()
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}


func Test_LoadSegment(t *testing.T) {
    fmt.Printf("============================  Test_LoadSegment ======================== ====\n")
    logger,_:=utils.New("test_log")
    var err error
    segment := NewSegmentWithLocalFile("test_segment_111",logger)
    
    res,match:=segment.findField("98","BBB",nil)
    
    if match {
       fmt.Printf("res BBB :: %v \n",res)
    }
    
    
    res,match=segment.findField("98","ZZZ",nil)
    
    if match {
       fmt.Printf("res ZZZ:: %v \n",res)
    }
    
    res,match=segment.findField("4","YYY",nil)
    
    if match {
       fmt.Printf("res YYY:: %v \n",res)
    }
    
    segment.DeleteField("AAA")
    
    res,match=segment.findField("1","AAA",nil)
    
    if match {
       fmt.Printf("res AAA :: %v \n",res)
    }
    
    segment.Close()
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}

func Test_LoadSegmentQuery(t *testing.T) {
    fmt.Printf("======================== Test_LoadSegmentSecond ======================== ====\n")
    logger,_:=utils.New("test_log")
    var err error
    segment := NewSegmentWithLocalFile("test_segment_111",logger)
    
    res,match:=segment.Query("BBB","98")
    
    if match {
       fmt.Printf("res BBB :: %v \n",res)
    }
    
    v6 := segment.Filter("CCC", utils.FILT_OVER, 5, 0, res)
    
    fmt.Printf("res CCC Fliter :: %v \n",v6)
    
    
    
    
    segment.Close()
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}



func Test_GetValue(t *testing.T) {
    fmt.Printf("======================== Test_GetValue ======================== ====\n")
    logger,_:=utils.New("test_log")
    var err error
    segment := NewSegmentWithLocalFile("test_segment_111",logger)
    
    res,_:=segment.GetDocument(12)
    fmt.Printf(" docudment[12] : %v\n",res)
    
    resstr,_:=segment.GetFieldValue(12,"BBB")
    fmt.Printf(" docudment[12] : %v\n",res)
    
    
    resstr,_=segment.GetFieldValue(12,"HHH")
    fmt.Printf(" docudment[12] : %v\n",resstr)
    
    res,_=segment.GetValueWithFields(12,[]string{"AAA","BBB","CCC"})
    fmt.Printf(" docudment[12] : %v\n",res)
    
    
    segment.Close()
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}







