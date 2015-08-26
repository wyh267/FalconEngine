/*****************************************************************************
 *  file name : InvertIndex.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引基础结构
 *
******************************************************************************/

package utils

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"os"
	"syscall"
	//"unsafe"
)

//
// DocId的最小结构体，包括DocId本身和权重，权重目前都是0
//
type DocIdInfo struct {
	DocId  int64
	//Weight int64
}

//
//静态倒排索引的最小单位，包含一个docid链和这个链的元信息(这个链的对应key[可能是任何类型])
//
type InvertDocIdList struct {
	Key       interface{}
	DocIdList []DocIdInfo
	StartPos  int64
	EndPos	  int64
	IncDocIdList	[]DocIdInfo
}

func NewInvertDocIdList(key interface{}) *InvertDocIdList {
	this := &InvertDocIdList{key, make([]DocIdInfo, 0),0,0,make([]DocIdInfo, 0)}
	return this
}

func (this *InvertDocIdList) Display() {

	fmt.Printf(" KEY: [ %v ] ==> [ ", this.Key)
	for _, v := range this.DocIdList {
		fmt.Printf(" %v ", v.DocId)
	}
	fmt.Printf(" ] \n")

}

//
//
//倒排索引
//IdxType    倒排索引类型，string,int64,float.....
//
//
const TYPE_TEXT int64 = 1
const TYPE_NUM int64 = 2
const TYPE_BOOL int64 = 3
const TYPE_FLOAT int64 = 4

type InvertIdx struct {
	IdxType       int64
	IdxName       string
	IdxLen        int64
	MmapBytes	  []byte
	IsMaped		  bool
	KeyInvertList []InvertDocIdList
}

func NewInvertIdx(idx_type int64, name string) *InvertIdx {

	list := make([]InvertDocIdList, 1)
	list[0] = InvertDocIdList{"nil", make([]DocIdInfo, 0),0,0,make([]DocIdInfo, 0)}
	this := &InvertIdx{IdxType: idx_type, IdxName: name, IdxLen: 0, KeyInvertList: list,MmapBytes : nil,IsMaped : false}
	return this

}

func (this *InvertIdx) GetInvertIndex(index int64) ([]DocIdInfo, bool) {

	if index > this.IdxLen || index < 1 {
		return nil, false
	}
	functime := InitTime()
	lens := int(this.KeyInvertList[index].EndPos)
	//./index/%v_idx.
	if this.IsMaped == false {
		
	
	fmt.Printf("Cost Time : %v \n",functime("Start"))
	f,_ := os.Open(fmt.Sprintf("./index/%v_idx.idx",this.IdxName))
	//fmt.Printf("Start : %v   Lens : %v   file_name : %v  \n",this.KeyInvertList[index].StartPos,this.KeyInvertList[index].EndPos*8,fmt.Sprintf("./index/%v_idx.idx",this.IdxName))
	defer f.Close()
	

	fi, err := f.Stat()
	if err != nil{
		fmt.Printf("ERR:%v",err)
	}
	//start:=int(this.KeyInvertList[index].StartPos)/4096
	//page_offset:=int(this.KeyInvertList[index].StartPos) % 4096
	//resultSize := int(page_offset+lens*8)

	this.MmapBytes,err = syscall.Mmap(int(f.Fd()),0,int(fi.Size()),syscall.PROT_READ,syscall.MAP_PRIVATE)

	if err != nil{
		fmt.Printf("MAPPING ERROR  %v \n",err)
		return nil,false
	}
	
	//defer syscall.Munmap(b)
	fmt.Printf("Cost Time : %v \n",functime("mmap"))
	
	//fmt.Printf("%x\n",b)
	//var doc_list *DocIdInfo
	//this.KeyInvertList[index].DocIdList=(*DocIdInfo)b
	//index_len:=int(this.KeyInvertList[index].EndPos)
	//p:=(*[20]DocIdInfo)(unsafe.Pointer(&b))

	//fmt.Printf("%v \n",p)
	this.IsMaped=true
	}
	reader := bytes.NewReader(this.MmapBytes[int(this.KeyInvertList[index].StartPos):int(this.KeyInvertList[index].StartPos)+lens*8])
	fmt.Printf("Cost Time : %v \n",functime("reader"))
	this.KeyInvertList[index].DocIdList = make([]DocIdInfo,lens)
	fmt.Printf("Cost Time : %v \n",functime("make"))
	binary.Read(reader,binary.LittleEndian,this.KeyInvertList[index].DocIdList)
	fmt.Printf("Cost Time : %v \n",functime("read map byte"))
	this.KeyInvertList[index].DocIdList=append(this.KeyInvertList[index].DocIdList,this.KeyInvertList[index].IncDocIdList...)
	fmt.Printf("Cost Time : %v \n",functime("append op"))
	testb := make([]byte,int(this.KeyInvertList[index].StartPos)+lens*8)
	fmt.Printf("Cost Time : %v \n",functime("make testb"))
	reader := bytes.NewReader(testb)
	fmt.Printf("Cost Time : %v \n",functime("reader testb"))
	binary.Read(reader,binary.LittleEndian,this.KeyInvertList[index].DocIdList)
	fmt.Printf("Cost Time : %v \n",functime("read test byte"))
	//fmt.Printf("DOC_IDS : %v \n",this.KeyInvertList[index].DocIdList)
	return this.KeyInvertList[index].DocIdList, true

}

func (this *InvertIdx) Display() {
	var idxtype string
	switch this.IdxType {
	case TYPE_TEXT:
		idxtype = "TEXT INDEX"
	case TYPE_NUM:
		idxtype = "NUMBER INDEX"
	case TYPE_BOOL:
	case TYPE_FLOAT:
	}

	fmt.Printf("\n=========== [ %v ] [ TYPE : %v ] [ LEN : %v ]==============\n", this.IdxName, idxtype, this.IdxLen)
	for index, value := range this.KeyInvertList {
		fmt.Printf("INDEX : [ %v ] ::: ", index)
		value.Display()
	}

}

