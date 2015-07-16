/*****************************************************************************
 *  file name : InvertIndex.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引
 *
******************************************************************************/

package utils

import (
	"fmt"
)

//
// DocId的最小结构体，包括DocId本身和权重
//
type DocIdInfo struct {
	DocId 	int64		
	Weight	int64 
}


//
//静态倒排索引的最小单位，包含一个docid链和这个链的元信息(这个链的对应key[可能是任何类型])
//
type InvertDocIdList struct {
	Key 		interface{}
	DocIdList	[]DocIdInfo
}

func NewInvertDocIdList(key interface{}) *InvertDocIdList{
	this := &InvertDocIdList{key,make([]DocIdInfo,0)}
	return this
}


func (this *InvertDocIdList) Display(){
	
	fmt.Printf(" KEY: [ %v ] ==> [ ",this.Key)
	for _,v := range this.DocIdList{
		fmt.Printf(" %v ",v.DocId)
	}
	fmt.Printf(" ] \n")
	
}

//
//
//倒排索引
//IdxType    倒排索引类型，string,int64,float.....
//
//
const TYPE_TEXT	int64 = 1	
const TYPE_NUM	int64 = 2
const TYPE_BOOL int64 = 3
const TYPE_FLOAT int64 = 4
type InvertIdx struct {
	IdxType			int64
	IdxName			string
	IdxLen			int64
	KeyInvertList	[]InvertDocIdList 
}



func NewInvertIdx(idx_type int64,name string) *InvertIdx{

	list:=make([]InvertDocIdList,1)
	list[0]=InvertDocIdList{"nil",make([]DocIdInfo,0)}
	this := &InvertIdx{IdxType:idx_type,IdxName:name,IdxLen:0,KeyInvertList:list}
	return this
	
}




func (this *InvertIdx) GetInvertIndex(index int64)([]DocIdInfo,bool){
	
	
	if index > this.IdxLen || index < 1{
		return nil,false
	}
	
	return this.KeyInvertList[index].DocIdList , true
	
}



func (this *InvertIdx) Display(){
	var idxtype string
	switch this.IdxType{
		case TYPE_TEXT:
			idxtype = "TEXT INDEX"
		case TYPE_NUM:
		case TYPE_BOOL:
		case TYPE_FLOAT:
	}
	
	fmt.Printf("\n=========== [ %v ] [ TYPE : %v ] [ LEN : %v ]==============\n",this.IdxName,idxtype,this.IdxLen)
	for index,value := range this.KeyInvertList{
		fmt.Printf("INDEX : [ %v ] ::: ",index)
		value.Display()
	}
	
}





/*****************************************************************************
*  function name : 
*  params : 
*  return : 
*
*  description : 求交集
*
******************************************************************************/











