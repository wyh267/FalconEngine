/*****************************************************************************
 *  file name : TextIndex.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 文本索引操作
 *
******************************************************************************/

package indexer


import (
	u "utils"
	"strings"
)


type TextIndex struct{
	Name  		string
	ivtIndex	*u.InvertIdx
	dicIndex	*u.StringIdxDic
}


func NewTextIndex(name string,ivt *u.InvertIdx,dic *u.StringIdxDic) *TextIndex{
	
	this := &TextIndex{name,ivt,dic}
	return this
	
}


func (this *TextIndex)FindTerm(term string) ([]u.DocIdInfo,bool) {
	
	term_id := this.dicIndex.Find(strings.ToLower(term))
	if term_id == -1 {
		return nil,false
	}
	return this.ivtIndex.GetInvertIndex(term_id)
}


