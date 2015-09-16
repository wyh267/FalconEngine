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
	"fmt"
	u "utils"
)

type NumberIndex struct {
	*Index
	dicIndex *u.NumberIdxDic
}

func NewNumberIndex(name string, ivt *u.InvertIdx, dic *u.NumberIdxDic) *NumberIndex {
	index := &Index{Name:name, Type:2, ivtIndex:ivt}
	this := &NumberIndex{index, dic}
	return this

}

func (this *NumberIndex) FindNumber(term int64) ([]u.DocIdInfo, bool) {

	term_id, _ := this.dicIndex.Find(term)
	if term_id == -1 {
		return nil, false
	}
	return this.ivtIndex.GetInvertIndex(term_id)

}

func (this *NumberIndex) Find(term interface{}) ([]u.DocIdInfo, bool) {

	term_num, ok := term.(int64)
	if !ok {
		return nil, false
	}

	return this.FindNumber(term_num)
}

func (this *NumberIndex) Display() {
	fmt.Printf("\n============================= %v =============================\n", this.Name)
	this.dicIndex.Display()
	this.ivtIndex.Display()
	fmt.Printf("\n===============================================================\n")
}

func (this *NumberIndex) GetType() int64 {
	return this.Type
}

func (this *NumberIndex) GetIvtIndex() *u.InvertIdx {
	return this.ivtIndex
}

func (this *NumberIndex) GetNumDic() *u.NumberIdxDic {
	return this.dicIndex
}
func (this *NumberIndex) GetStrDic() *u.StringIdxDic {
	return nil
}


func (this *NumberIndex) SetCustomInterface(inter u.CustomInterface) error {
	this.customeInter=inter
	return nil
}

func (this *NumberIndex) GetCustomInterface() u.CustomInterface {
	return this.customeInter
}