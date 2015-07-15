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

//
//
//倒排索引
//IdxType    倒排索引类型，string,int64,float.....
//
//
type InvertIdx struct {
	IdxType			int64
	KeyInvertList	[]InvertDocIdList 
}




/*****************************************************************************
*  function name : 
*  params : 
*  return : 
*
*  description : 求交集
*
******************************************************************************/











