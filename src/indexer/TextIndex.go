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
	"fmt"
	"strings"
	u "utils"
)

type TextIndex struct {
	*Index
	dicIndex *u.StringIdxDic
}

func NewTextIndex(name string, ivt *u.InvertIdx, dic *u.StringIdxDic) *TextIndex {
	index := &Index{name, 1, ivt}
	this := &TextIndex{index, dic}
	return this

}

func (this *TextIndex) FindTerm(term string) ([]u.DocIdInfo, bool) {

	term_id := this.dicIndex.Find(strings.ToLower(term))
	if term_id == -1 {
		return nil, false
	}
	return this.ivtIndex.GetInvertIndex(term_id)
}

func (this *TextIndex) Find(term interface{}) ([]u.DocIdInfo, bool) {

	term_str, ok := term.(string)
	if !ok {
		return nil, false
	}

	return this.FindTerm(term_str)
}

func (this *TextIndex) Display() {
	fmt.Printf("\n============================= %v =============================\n", this.Name)
	this.dicIndex.Display()
	this.ivtIndex.Display()
	fmt.Printf("\n===============================================================\n")
}

func (this *TextIndex) GetType() int64 {
	return this.Type
}




func (this *TextIndex) GetIvtIndex() *u.InvertIdx {
	return this.ivtIndex
}



func (this *TextIndex)GetNumDic() *u.NumberIdxDic{
	return nil
}
func (this *TextIndex)GetStrDic() *u.StringIdxDic{
	return this.dicIndex
}
