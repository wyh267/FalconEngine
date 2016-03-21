package FalconIndex


import (
    "fmt"
    "testing"
    "utils"
    //fi "FalconIndex"
)



func Test_AddDocumentNumberPfl(t *testing.T) {
    logger,_:=utils.New("test_log")
    numpfl:=newEmptyProfile(utils.IDX_TYPE_NUMBER,0,"numberfield", 0, logger)
    var err error
    for i:=uint32(0);i<100;i++{
        numpfl.addDocument(i,fmt.Sprintf("%v",i))
    }
    poffset,plens,err:=numpfl.serialization("test_segment_pfl")
    pflMmap, err := utils.NewMmap("test_segment_pfl.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflMmap.SetFileEnd(0)
    
    
    numpfl1:=newProfileWithLocalFile(utils.IDX_TYPE_NUMBER,0,"test_segment_pfl",pflMmap,nil,poffset,uint64(plens),false,logger)
    
    res,_:=numpfl1.getValue(32)
	fmt.Printf("res::: %v \n",res)
    
    numpfl1.updateDocument(32,"567")
    
    res2,_:=numpfl1.getValue(32)
	fmt.Printf("res::: %v \n",res2)
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}




func Test_AddDocumentStringPfl(t *testing.T) {
    logger,_:=utils.New("test_log")
    strpfl:=newEmptyProfile(utils.IDX_TYPE_STRING,0,"stringfield", 0, logger)
    var err error
    for i:=uint32(0);i<100;i++{
        strpfl.addDocument(i,fmt.Sprintf("%v",i))
    }
    poffset,plens,err:=strpfl.serialization("test_segment_pfl")
    pflMmap, err := utils.NewMmap("test_segment_pfl.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflMmap.SetFileEnd(0)
    
    dtlMmap, err := utils.NewMmap("test_segment_pfl.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	dtlMmap.SetFileEnd(0)
    
    
    strpfl1:=newProfileWithLocalFile(utils.IDX_TYPE_STRING,0,"test_segment_pfl",pflMmap,dtlMmap,poffset,uint64(plens),false,logger)
    
    res,_:=strpfl1.getValue(32)
	fmt.Printf("res::: %v \n",res)
    
    strpfl1.updateDocument(32,"hello")
    
    res2,_:=strpfl1.getValue(32)
	fmt.Printf("res::: %v \n",res2)
    
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}
