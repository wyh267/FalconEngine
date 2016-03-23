package segment


import (
    "fmt"
    "testing"
    "utils"
    "tree"
    "math/rand" 
    //fi "FalconIndex"
)


var offset1 int64
var offset2 int64
var lens1 int
var lens2 int


func Test_AddDocumentStringIvt(t *testing.T) {
    logger,_:=utils.New("test_log")
    
    
    ivt := newEmptyInvert(utils.IDX_TYPE_STRING, 0, "testfield", logger)
    
    var err error
    for i:=uint32(0);i<1000000;i++{
        //fmt.Printf("[TEST] >>>>> docid:%v value:%v\n",i,i)
        ivt.addDocument(i,fmt.Sprintf("%v",rand.Intn(5000)))
    }
    
    v1,_:=ivt.query("5")
    fmt.Printf("[TEST] >>>>> 5:::%v\n",len(v1))
    
    btree := tree.NewBTDB("test_segment_ivt.bt")
    if err:=btree.AddBTree("testfield");err!=nil{
        fmt.Printf("[TEST] >>>>> [ERROR] invert --> Create BTree Error %v\n",err)
    }
    
    
    err=ivt.serialization("test_segment_ivt",btree)
    fmt.Printf("[TEST] >>>>> %v,%v,%v,%v\n",offset1,offset2,lens1,lens2)
    err=btree.Close()
    
    
    
    
    if err != nil {
		t.Error("Test_AddDocumentStringIvt Fail...", err)
	} else {
		t.Log("Test_AddDocumentStringIvt OK...")
	}
    
    
}




func Test_SearchStringIvt(t *testing.T) {
    logger,_:=utils.New("test_log")
    idxMmap, err := utils.NewMmap("test_segment_ivt.idx", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
	}
	idxMmap.SetFileEnd(0)
    btree := tree.NewBTDB("test_segment_ivt.bt")
    
    ivt:=newInvertWithLocalFile(btree,utils.IDX_TYPE_STRING,"testfield","test_segment_ivt",idxMmap,logger)
    
    v1,_:=ivt.query("5")
    fmt.Printf("[TEST] >>>>> 5:::%v\n",len(v1))
    v2,_:=ivt.query("11")
    fmt.Printf("[TEST] >>>>> 11::%v\n",len(v2))
    
    
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
    
    
}



func Test_MergeIvt(t *testing.T) {
    
    fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++ Test_MergeIvt ++++++++++++++++++++++++++++++++++++++++++++++++\n")
    fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++ Test_MergeIvt ++++++++++++++++++++++++++++++++++++++++++++++++\n")
    fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++ Test_MergeIvt ++++++++++++++++++++++++++++++++++++++++++++++++\n")
   
    logger,_:=utils.New("test_log")
    ivt1 := newEmptyInvert(utils.IDX_TYPE_STRING, 0, "testfield", logger)
    var err error
    for i:=uint32(0);i<20;i++{
        ivt1.addDocument(i,fmt.Sprintf("%v",rand.Intn(4)))
    }
    
    btree1 := tree.NewBTDB("test_segment_ivt1.bt")
    if err:=btree1.AddBTree("testfield");err!=nil{
        fmt.Printf("[TEST] >>>>> [ERROR] invert --> Create BTree Error %v\n",err)
    }
    err=ivt1.serialization("test_segment_ivt1",btree1)
    err=btree1.Close()
    
    
    
    ivt2 := newEmptyInvert(utils.IDX_TYPE_STRING, 20, "testfield", logger)
    for i:=uint32(20);i<30;i++{
        ivt2.addDocument(i,fmt.Sprintf("%v",rand.Intn(2)))
    }
    
    btree2 := tree.NewBTDB("test_segment_ivt2.bt")
    if err:=btree2.AddBTree("testfield");err!=nil{
        fmt.Printf("[TEST] >>>>> [ERROR] invert --> Create BTree Error %v\n",err)
    }
    err=ivt2.serialization("test_segment_ivt2",btree2)
    err=btree2.Close()
    
    
    
    
    idxMmap1, err := utils.NewMmap("test_segment_ivt1.idx", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
	}
	idxMmap1.SetFileEnd(0)
    nbtree1 := tree.NewBTDB("test_segment_ivt1.bt")
    
    nivt1:=newInvertWithLocalFile(nbtree1,utils.IDX_TYPE_STRING,"testfield","test_segment_ivt1",idxMmap1,logger)
    
    idxMmap2, err := utils.NewMmap("test_segment_ivt2.idx", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("[TEST] >>>>> mmap error : %v \n", err)
	}
	idxMmap2.SetFileEnd(0)
    nbtree2 := tree.NewBTDB("test_segment_ivt2.bt")
    
    nivt2:=newInvertWithLocalFile(nbtree2,utils.IDX_TYPE_STRING,"testfield","test_segment_ivt2",idxMmap2,logger)
    
    
    
    merge := newEmptyInvert(utils.IDX_TYPE_STRING, 0, "testfield", logger)
    mergetree := tree.NewBTDB("test_segment_ivt_merge.bt")
    if err:=mergetree.AddBTree("testfield");err!=nil{
        fmt.Printf("[TEST] >>>>> [TEST]>>>>> [ERROR] invert --> Create BTree Error %v\n",err)
    }
    merge.mergeInvert([]*invert{nivt1,nivt2},"test_segment_ivt_merge",mergetree)
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
    mergetree.Close()
    
    
    idxMmap3, err := utils.NewMmap("test_segment_ivt_merge.idx", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	idxMmap3.SetFileEnd(0)
    nbtree3 := tree.NewBTDB("test_segment_ivt_merge.bt")
    
    nmerge:=newInvertWithLocalFile(nbtree3,utils.IDX_TYPE_STRING,"testfield","test_segment_ivt_merge",idxMmap3,logger)
    
    v1,_:=nmerge.query("1")
    fmt.Printf("[TEST] >>>>> 1:::::::::%v\n",v1)
    v1,_=nmerge.query("0")
    fmt.Printf("[TEST] >>>>> 0:::::::::%v\n",v1)
    v1,_=nmerge.query("2")
    fmt.Printf("[TEST] >>>>> 2:::::::::%v\n",v1)
    v1,_=nmerge.query("3")
    fmt.Printf("[TEST] >>>>> 3:::::::::%v\n",v1)
    
    fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++ Test_MergeIvt END ++++++++++++++++++++++++++++++++++++++++++++++++\n")
   fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++ Test_MergeIvt END++++++++++++++++++++++++++++++++++++++++++++++++\n")
   
    
}

