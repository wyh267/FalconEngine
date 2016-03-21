package FalconIndex


import (
    "fmt"
    "testing"
    "utils"
    "math/rand"
    "tree"
    //fi "FalconIndex"
)


var pflOffset1,pflOffset2 int64
var pflLen1,pflLen2 int

func Test_AddDocumentField(t *testing.T) {
    logger,_:=utils.New("test_log")
    filed1:=newEmptyField("test_field1",0,utils.IDX_TYPE_STRING,logger)
    filed2:=newEmptyField("test_field2",0,utils.IDX_TYPE_NUMBER,logger)
    var err error
    
    for i:=uint32(0);i<100000;i++{
        filed1.addDocument(i,fmt.Sprintf("%v",rand.Intn(1000)))
        filed2.addDocument(i,fmt.Sprintf("%v",i))
    }
    btree := tree.NewBTDB("test_segment_field.bt")
    err=filed1.serialization("test_segment_field",btree)
    err=filed2.serialization("test_segment_field",btree)
    
    pflOffset1=filed1.pflOffset
    pflLen1=filed1.pflLen
    
    pflOffset2=filed2.pflOffset
    pflLen2=filed2.pflLen
    
    btree.Close()
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}


func Test_LoadField(t *testing.T) {
    fmt.Println("=======TESTING Test_LoadField")
    
    logger,_:=utils.New("test_log")
    
    idxMmap, err := utils.NewMmap("test_segment_field.idx", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	idxMmap.SetFileEnd(0)
	fmt.Printf("[INFO] Read Invert File : segment.idx\n ")
    
    pflmmap, err := utils.NewMmap("test_segment_field.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflmmap.SetFileEnd(0)
	fmt.Printf("[INFO] Read Invert File : segment.idx\n ")
    
    dtlmmap, err := utils.NewMmap("test_segment_field.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	dtlmmap.SetFileEnd(0)
    
    btree := tree.NewBTDB("test_segment_field.bt")
    
                                 
    field1:=newFieldWithLocalFile("test_field1","test_segment_field",0,100000,
                        utils.IDX_TYPE_STRING,pflOffset1,pflLen1,idxMmap,pflmmap,dtlmmap,false,
                        btree,logger)
                        
                        
    field2:=newFieldWithLocalFile("test_field2","test_segment_field",0,100000,
                        utils.IDX_TYPE_NUMBER,pflOffset2,pflLen2,idxMmap,pflmmap,dtlmmap,false,
                        btree,logger)
    
    fmt.Println("=======TESTING Test_LoadField Query Field1")
    
    v1,_:=field1.query("45")
    v2,_:=field1.query("hello")
    v3,_:=field1.getValue(3)
    v4,_:=field1.getValue(20)
    
    fmt.Printf("field1[45]:%v \nfield1[hello]:%v\n",v1,v2)
    fmt.Printf("field1[3]:%v \nfield1[20]:%v\n",v3,v4)
    
    
    fmt.Println("=======TESTING Test_LoadField Query Field2")
    
    v11,_:=field2.query("450")
    v12,_:=field2.query("hello")
    v13,_:=field2.getValue(3)
    v14,_:=field2.getValue(20)
    
    fmt.Printf("field1[450]:%v \nfield1[hello]:%v\n",v11,v12)
    fmt.Printf("field1[3]:%v \nfield1[20]:%v\n",v13,v14)
   
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}
