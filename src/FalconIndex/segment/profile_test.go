package segment


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




func Test_MergePfl(t *testing.T) {
    fmt.Printf("=============Test_MergePfl=================\n")
    fmt.Printf("=============Test_MergePfl=================\n")
    fmt.Printf("=============Test_MergePfl=================\n")
    fmt.Printf("=============Test_MergePfl=================\n")
    fmt.Printf("=============Test_MergePfl=================\n")
    logger,_:=utils.New("test_log")
    strpfl1:=newEmptyProfile(utils.IDX_TYPE_STRING,0,"stringfield", 0, logger)
    var err error
    for i:=uint32(0);i<1000;i++{
        strpfl1.addDocument(i,fmt.Sprintf("%v",i))
    }
    poffset1,plens1,err:=strpfl1.serialization("test_segment_pfl1")
    pflMmap1, err := utils.NewMmap("test_segment_pfl1.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflMmap1.SetFileEnd(0)
    
    dtlMmap1, err := utils.NewMmap("test_segment_pfl1.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	dtlMmap1.SetFileEnd(0)
     
    nstrpfl1:=newProfileWithLocalFile(utils.IDX_TYPE_STRING,0,"test_segment_pfl1",pflMmap1,dtlMmap1,poffset1,uint64(plens1),false,logger)
    
    
    
    strpfl2:=newEmptyProfile(utils.IDX_TYPE_STRING,0,"stringfield", 1000, logger)

    for i:=uint32(1000);i<3000;i++{
        strpfl2.addDocument(i,fmt.Sprintf("%v",i))
    }
    poffset2,plens2,err:=strpfl2.serialization("test_segment_pfl2")
    pflMmap2, err := utils.NewMmap("test_segment_pfl2.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflMmap2.SetFileEnd(0)
    
    dtlMmap2, err := utils.NewMmap("test_segment_pfl2.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	dtlMmap2.SetFileEnd(0)
     
    nstrpfl2:=newProfileWithLocalFile(utils.IDX_TYPE_STRING,0,"test_segment_pfl2",pflMmap2,dtlMmap2,poffset2,uint64(plens2),false,logger)
    
    
    
    merge:=newEmptyProfile(utils.IDX_TYPE_STRING,0,"stringfield", 0, logger)
    //merge.mergeProfiles(nstrpfl1,"test_merge_segment")
    
    poffset,plens,err:=merge.mergeProfiles([]*profile{nstrpfl1,nstrpfl2},"test_merge_segment")
    
    pflMmap, err := utils.NewMmap("test_merge_segment.pfl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	pflMmap.SetFileEnd(0)
    
    dtlMmap, err := utils.NewMmap("test_merge_segment.dtl", utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	dtlMmap.SetFileEnd(0)
    nmerge:=newProfileWithLocalFile(utils.IDX_TYPE_STRING,0,"test_merge_segment",pflMmap,dtlMmap,poffset,uint64(plens),false,logger)
    
    res,_:=nmerge.getValue(32)
	fmt.Printf("res::: %v \n",res)
    
    
    nmerge.updateDocument(32,"hello")
    
    res2,_:=nmerge.getValue(32)
	fmt.Printf("res::: %v \n",res2)
    
    res,_=nmerge.getValue(1232)
	fmt.Printf("res::: %v \n",res)
    
    res,_=nmerge.getValue(329)
	fmt.Printf("res::: %v \n",res)
    
    
    
    if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}
    
}