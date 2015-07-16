/*****************************************************************************
 *  file name : NumberIndex.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数字类型的倒排索引
 *
******************************************************************************/


package indexer


import (
	u "utils"
)


type NumberIndex struct{
	Name  		string
	ivtIndex	*u.InvertIdx
	dicIndex	*u.NumberIdxDic
}


func NewNumberIndex(name string,ivt *u.InvertIdx,dic *u.NumberIdxDic) *NumberIndex{
	
	this := &NumberIndex{name,ivt,dic}
	return this
	
}


func (this *NumberIndex)FindNumber(term int64) ([]u.DocIdInfo,bool) {
	
	term_id,_ := this.dicIndex.Find(term)
	if term_id == -1 {
		return nil,false
	}
	return this.ivtIndex.GetInvertIndex(term_id)
	
}






