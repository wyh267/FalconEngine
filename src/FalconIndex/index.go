package FalconIndex

import (
	"fmt"
	"math/rand"
	"utils"
    fis "FalconIndex/segment"
    
)

func hello() {

	fmt.Printf("============================  Test_NewEmptySegmentAndAddDocument ======================== ====\n")
	logger, _ := utils.New("test_log")
	//var err error

	fields := make([]utils.SimpleFieldInfo, 0)
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "AAA", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "BBB", FieldType: utils.IDX_TYPE_STRING})
	fields = append(fields, utils.SimpleFieldInfo{FieldName: "CCC", FieldType: utils.IDX_TYPE_NUMBER})

	segment := fis.NewEmptySegmentWithFieldsInfo("test_segment_111", 0, fields, logger)

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

	for docid := uint32(100); docid < 2000000; docid++ {
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

}
