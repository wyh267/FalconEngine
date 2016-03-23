package segment

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"utils"
	//fi "FalconIndex"
)

func Test_NewEmptySegmentAndAddDocument(t *testing.T) {
	fmt.Printf("============================  Test_NewEmptySegmentAndAddDocument ============================\n")
	logger, _ := utils.New("test_log")
	var err error

	fields := make([]utils.SimpleFieldInfo, 0)
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})

	segment := NewEmptySegmentWithFieldsInfo("test_segment_111", 0, fields, logger)

	for docid := uint32(0); docid < 100; docid++ {
		content := make(map[string]string)

		content["AAA"] = fmt.Sprintf("%v", rand.Intn(100))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(100))
		content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", docid)

		err := segment.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}

	segment.AddField(utils.SimpleFieldInfo{FieldName: "YYY", FieldType: utils.IDX_TYPE_STRING})

	for docid := uint32(100); docid < 2000; docid++ {
		content := make(map[string]string)

		content["AAA"] = fmt.Sprintf("%v", rand.Intn(1000))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(1000))
		content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", rand.Intn(1000))

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
	fmt.Printf("============================  Test_LoadSegment ============================\n")
	logger, _ := utils.New("test_log")
	var err error
	segment := NewSegmentWithLocalFile("test_segment_111", logger)

	res, match := segment.findField("98", "BBB", nil)

	if match {
		fmt.Printf("[TEST] >>>>> res BBB :: %v \n", res)
	}

	res, match = segment.findField("98", "ZZZ", nil)

	if match {
		fmt.Printf("[TEST] >>>>> res ZZZ:: %v \n", res)
	}

	res, match = segment.findField("4", "YYY", nil)

	if match {
		fmt.Printf("[TEST] >>>>> res YYY:: %v \n", res)
	}

	segment.DeleteField("AAA")

	res, match = segment.findField("1", "AAA", nil)

	if match {
		fmt.Printf("[TEST] >>>>> res AAA :: %v \n", res)
	}

	segment.Close()

	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}

}

func Test_LoadSegmentQuery(t *testing.T) {
	fmt.Printf("======================== Test_LoadSegmentSecond ============================\n")
	logger, _ := utils.New("test_log")
	var err error
	segment := NewSegmentWithLocalFile("test_segment_111", logger)

	res, match := segment.Query("BBB", "98")

	if match {
		fmt.Printf("[TEST] >>>>> res BBB :: %v \n", res)
	}

	v6 := segment.Filter("CCC", utils.FILT_OVER, 990, 0, res)

	fmt.Printf("[TEST] >>>>> res CCC Fliter :: %v \n", v6)

	segment.Close()

	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("Test_LoadSegment OK")
	}

}

func Test_GetValue(t *testing.T) {
	fmt.Printf("======================== Test_GetValue ============================\n")
	logger, _ := utils.New("test_log")
	var err error
	segment := NewSegmentWithLocalFile("test_segment_111", logger)

	res, _ := segment.GetDocument(12)
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	resstr, _ := segment.GetFieldValue(12, "BBB")
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	resstr, _ = segment.GetFieldValue(12, "HHH")
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", resstr)

	res, _ = segment.GetValueWithFields(12, []string{"AAA", "BBB", "CCC"})
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	docids, _ := segment.SearchUnitDocIds([]utils.FSSearchQuery{utils.FSSearchQuery{FieldName: "BBB", Value: "98"}},
		[]utils.FSSearchFilted{utils.FSSearchFilted{FieldName: "CCC", Start: 990, Type: utils.FILT_OVER}},
		nil, nil)
	fmt.Printf("[TEST] >>>>>  SearchUnitDocIds[12] : %v\n", docids)

	segment.Close()

	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("Test_GetValue OK")
	}

}

func Test_MergeSegment(t *testing.T) {
	fmt.Printf("======================== Test_MergeSegment ============================\n")
	logger, _ := utils.New("test_log")
	var err error
	fields := make([]utils.SimpleFieldInfo, 0)
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})

	segment1 := NewEmptySegmentWithFieldsInfo("test_segment_1", 0, fields, logger)

	for docid := uint32(0); docid < 10000; docid++ {
		content := make(map[string]string)
		content["AAA"] = fmt.Sprintf("%v", rand.Intn(100))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(100))
		//content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", docid)
		err := segment1.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}

	segment1.AddField(utils.SimpleFieldInfo{FieldName: "YYY", FieldType: utils.IDX_TYPE_STRING})

	for docid := uint32(10000); docid < 100000; docid++ {
		content := make(map[string]string)
		content["AAA"] = fmt.Sprintf("%v", rand.Intn(200))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(200))
		//content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", rand.Intn(2000))
		err := segment1.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
	segment1.DeleteField("ZZZ")
	segment1.Serialization()
	segment1.Close()
	segment1 = NewSegmentWithLocalFile("test_segment_1", logger)
    fields = append(fields, utils.SimpleFieldInfo{FieldName: "YYY", FieldType: utils.IDX_TYPE_STRING})
	
	segment2 := NewEmptySegmentWithFieldsInfo("test_segment_2", 100000, fields, logger)
	for docid := uint32(100000); docid < 120000; docid++ {
		content := make(map[string]string)
		content["AAA"] = fmt.Sprintf("%v", rand.Intn(100))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(100))
		content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", docid)
		err := segment2.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}

	//segment2.AddField(utils.SimpleFieldInfo{FieldName: "YYY", FieldType: utils.IDX_TYPE_STRING})

	for docid := uint32(120000); docid < 800000; docid++ {
		content := make(map[string]string)
		content["AAA"] = fmt.Sprintf("%v", rand.Intn(200))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(200))
		//content["YYY"] = "4"
		content["CCC"] = fmt.Sprintf("%v", rand.Intn(2000))
		err := segment2.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
    
    
    for docid := uint32(800000); docid < 801000; docid++ {
		content := make(map[string]string)
		content["AAA"] = fmt.Sprintf("%v", rand.Intn(200))
		content["BBB"] = fmt.Sprintf("%v", rand.Intn(200))
		content["YYY"] = fmt.Sprintf("%v", rand.Intn(10))
		content["CCC"] = fmt.Sprintf("%v", rand.Intn(2000))
		err := segment2.AddDocument(docid, content)
		if err != nil {
			fmt.Errorf("%v", err)
		}
	}
    
	segment2.DeleteField("ZZZ")
	segment2.Serialization()
	segment2.Close()
	segment2 = NewSegmentWithLocalFile("test_segment_2", logger)

	start := time.Now()
    
	segment3 := NewEmptySegmentWithFieldsInfo("test_segment_merge_3", 0, fields, logger)
	segment3.MergeSegments([]*Segment{segment1, segment2})

	fmt.Printf(">>>>>>>>>>MERGE COST TIME:%v <<<<<<<<<\n", time.Now().Sub(start))

	segment3 = NewSegmentWithLocalFile("test_segment_merge_3", logger)

	res, _ := segment3.GetDocument(12)
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	resstr, _ := segment3.GetFieldValue(12, "BBB")
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	resstr, _ = segment3.GetFieldValue(12, "HHH")
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", resstr)

	res, _ = segment3.GetValueWithFields(12, []string{"AAA", "BBB", "CCC"})
	fmt.Printf("[TEST] >>>>>  docudment[12] : %v\n", res)

	docids, _ := segment3.SearchUnitDocIds([]utils.FSSearchQuery{utils.FSSearchQuery{FieldName: "BBB", Value: "50"}},
		[]utils.FSSearchFilted{utils.FSSearchFilted{FieldName: "CCC", Start: 1997, Type: utils.FILT_OVER}},
		nil, nil)
	fmt.Printf("[TEST] >>>>>  BBB[12] : %v\n", docids)
    
    
    
    docids, _ = segment3.SearchUnitDocIds([]utils.FSSearchQuery{utils.FSSearchQuery{FieldName: "YYY", Value: "6"}},
		[]utils.FSSearchFilted{utils.FSSearchFilted{FieldName: "CCC", Start: 1000, Type: utils.FILT_OVER}},
		nil, nil)
	fmt.Printf("[TEST] >>>>>  YYY[12] : %v\n", docids)


    res, _ = segment3.GetValueWithFields(12, []string{"AAA", "BBB", "CCC","YYY"})
	fmt.Printf("[TEST] >>>>>  docudment with YYY[12] : %v\n", res)
    
     res, _ = segment3.GetValueWithFields(800010, []string{"AAA", "BBB", "CCC","YYY"})
	fmt.Printf("[TEST] >>>>>  docudment with YYY[800010] : %v\n", res)

	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("Test_GetValue OK")
	}

}
