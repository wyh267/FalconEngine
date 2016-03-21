package FalconIndex


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
        //fmt.Printf("docid:%v value:%v\n",i,i)
        ivt.addDocument(i,fmt.Sprintf("%v",rand.Intn(5000)))
    }
    
    v1,_:=ivt.query("5")
    fmt.Printf("5:::%v\n",len(v1))
    
    btree := tree.NewBTDB("test_segment_ivt.bt")
    if err:=btree.AddBTree("testfield");err!=nil{
        fmt.Printf("[ERROR] invert --> Create BTree Error %v\n",err)
    }
    
    
    err=ivt.serialization("test_segment_ivt",btree)
    fmt.Printf("%v,%v,%v,%v\n",offset1,offset2,lens1,lens2)
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
		fmt.Printf("mmap error : %v \n", err)
	}
	idxMmap.SetFileEnd(0)
    btree := tree.NewBTDB("test_segment_ivt.bt")
    
    ivt:=newInvertWithLocalFile(btree,utils.IDX_TYPE_STRING,"testfield","test_segment_ivt",idxMmap,logger)
    
    v1,_:=ivt.query("5")
    fmt.Printf("5:::%v\n",len(v1))
    v2,_:=ivt.query("11")
    fmt.Printf("11::%v\n",len(v2))
    
    
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
    
    
}


