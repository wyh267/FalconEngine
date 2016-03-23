package segment


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
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
	}
	idxMmap.SetFileEnd(0)
	fmt.Printf("[TEST] >>>>> [INFO] Read Invert File : segment.idx\n ")
    
    pflmmap, err := utils.NewMmap("test_segment_field.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
	}
	pflmmap.SetFileEnd(0)
	fmt.Printf("[TEST] >>>>> [INFO] Read Invert File : segment.idx\n ")
    
    dtlmmap, err := utils.NewMmap("test_segment_field.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
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
    
    fmt.Printf("[TEST] >>>>> field1[45]:%v \n[TEST] >>>>> field1[hello]:%v\n",v1,v2)
    fmt.Printf("[TEST] >>>>> field1[3]:%v \n[TEST] >>>>> field1[20]:%v\n",v3,v4)
    
    
    fmt.Println("=======TESTING Test_LoadField Query Field2")
    
    v11,_:=field2.query("450")
    v12,_:=field2.query("hello")
    v13,_:=field2.getValue(3)
    v14,_:=field2.getValue(20)
    
    fmt.Printf("[TEST] >>>>> field1[450]:%v \n[TEST] >>>>> field1[hello]:%v\n",v11,v12)
    fmt.Printf("[TEST] >>>>> field1[3]:%v \n[TEST] >>>>> field1[20]:%v\n",v13,v14)
   
   
  
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}



func Test_MergeField(t *testing.T) {
    fmt.Println("=======TESTING Test_MergeField")
    
    logger,_:=utils.New("test_log")

   fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
   fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
   fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
   
   filed1:=newEmptyField("test_field",0,utils.IDX_TYPE_STRING,logger)
   filed2:=newEmptyField("test_field",100,utils.IDX_TYPE_STRING,logger)
    var err error
    
    for i:=uint32(0);i<100;i++{
        filed1.addDocument(i,fmt.Sprintf("%v",rand.Intn(10)))
    }
    
    for i:=uint32(100);i<200;i++{
        filed2.addDocument(i,fmt.Sprintf("%v",rand.Intn(10)))
    }
    btree1 := tree.NewBTDB("test_segment_field1.bt")
    err=filed1.serialization("test_segment_field1",btree1)
    
    btree2 := tree.NewBTDB("test_segment_field2.bt")
    err=filed2.serialization("test_segment_field2",btree2)
    
    pflOffset1=filed1.pflOffset
    pflLen1=filed1.pflLen
    
    pflOffset2=filed2.pflOffset
    pflLen2=filed2.pflLen
    
    btree1.Close()
    btree2.Close()
    
    idxmmap1, _ := utils.NewMmap("test_segment_field1.idx", utils.MODE_APPEND)
    pflmmap1, _ := utils.NewMmap("test_segment_field1.pfl", utils.MODE_APPEND)
    dtlmmap1, _ := utils.NewMmap("test_segment_field1.dtl", utils.MODE_APPEND)
	
    idxmmap2, _ := utils.NewMmap("test_segment_field2.idx", utils.MODE_APPEND)
    pflmmap2, _ := utils.NewMmap("test_segment_field2.pfl", utils.MODE_APPEND)
    dtlmmap2, _ := utils.NewMmap("test_segment_field2.dtl", utils.MODE_APPEND)
	
    
    
    btree1 = tree.NewBTDB("test_segment_field1.bt")
    btree2 = tree.NewBTDB("test_segment_field2.bt")
    
                                 
    filed1=newFieldWithLocalFile("test_field","test_segment_field1",0,100,
                        utils.IDX_TYPE_STRING,pflOffset1,pflLen1,idxmmap1,pflmmap1,dtlmmap1,false,
                        btree1,logger)
                        
                        
    filed2=newFieldWithLocalFile("test_field","test_segment_field2",100,200,
                        utils.IDX_TYPE_STRING,pflOffset2,pflLen2,idxmmap2,pflmmap2,dtlmmap2,false,
                        btree2,logger)
    
    
   
   
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
    mergefield:=newEmptyField("test_field",0,utils.IDX_TYPE_STRING,logger)
    btree := tree.NewBTDB("test_segment_merge_field.bt")
    if err:=btree.AddBTree("test_field");err!=nil{
        fmt.Printf("[TEST] >>>>> [ERROR] invert --> Create BTree Error %v\n",err)
    }
    pflOffset2,pflLen2,_:=mergefield.mergeField([]*FSField{filed1,filed2},"test_segment_merge_field",btree)
    
    
    
    idxmmap, _ := utils.NewMmap("test_segment_merge_field.idx", utils.MODE_APPEND)
    pflmmap, _ := utils.NewMmap("test_segment_merge_field.pfl", utils.MODE_APPEND)
    dtlmmap, _ := utils.NewMmap("test_segment_merge_field.dtl", utils.MODE_APPEND)
	
    
    
    btree = tree.NewBTDB("test_segment_merge_field.bt")
    mergefield=newFieldWithLocalFile("test_field","test_segment_merge_field",0,200,
                        utils.IDX_TYPE_STRING,pflOffset2,pflLen2,idxmmap,pflmmap,dtlmmap,false,
                        btree,logger)
    
    
    
    
    
    v1,_:=mergefield.query("45")
    v2,_:=mergefield.query("5")
    v3,_:=mergefield.getValue(3)
    v4,_:=mergefield.getValue(20)
    
    fmt.Printf("[TEST] >>>>> mergefield[45]:%v \n mergefield[5]:%v\n",v1,v2)
    fmt.Printf("[TEST] >>>>> mergefield[3]:%v \n mergefield[20]:%v\n",v3,v4)
    
     fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
   fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
   fmt.Println("=======TESTING Test_MergeField  merge ....... fields ")
}