/*****************************************************************************
 *  file name : IndexBuilder
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 生成倒排索引和正排索引的工具类，可考虑变成函数而不是一个类
 *
******************************************************************************/
package utils

import (
	"errors"
	"strings"
	//"fmt"
)

type IndexBuilder struct {
	Segmenter *Segmenter
}

/*****************************************************************************
*  function name : BuildTextIndex
*  params :
*  return :
*
*  description :
*
******************************************************************************/
const RULE_EN int64 = 1
const RULE_CHN int64 = 2

func (this *IndexBuilder) BuildTextIndex(doc_id int64, content string, ivt_idx *InvertIdx, ivt_dic *StringIdxDic,split_type int64) error {

	if ivt_idx.IdxType != TYPE_TEXT {
		return errors.New("Wrong Type")
	}

	if len(strings.TrimSpace(content)) == 0 {
		return nil //errors.New("nothing")
	}
	
	var terms []string
	
	switch split_type{
		case 1:	//正常切词
			terms = RemoveDuplicatesAndEmpty(this.Segmenter.Segment(content, true))
		case 2: //按单个字符进行切词
			terms = RemoveDuplicatesAndEmpty(strings.Split(content, ""))
		case 3: //按规定的分隔符进行切词
			terms = RemoveDuplicatesAndEmpty(strings.Split(content, ";"))
	}


	

	for _, term := range terms {
		len := ivt_dic.Length()
		key_id := ivt_dic.Put(term)
		if key_id == -1 {
			return errors.New("Text Bukets full")
		}
		//新增
		if key_id > len {
			invertList := NewInvertDocIdList(term)
			invertList.DocIdList = append(invertList.DocIdList, DocIdInfo{DocId:doc_id})
			ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
			ivt_idx.IdxLen++
		} else { //更新
			ivt_idx.KeyInvertList[key_id].DocIdList = append(ivt_idx.KeyInvertList[key_id].DocIdList, DocIdInfo{DocId:doc_id})
		}

	}
	return nil
}

func (this *IndexBuilder) BuildNumberIndex(doc_id int64, content int64, ivt_idx *InvertIdx, ivt_dic *NumberIdxDic) error {

	len := ivt_dic.Length()
	//fmt.Println("len ",len)
	//fmt.Printf("doc_id : %v  content : %v \n",doc_id,content)
	key_id := ivt_dic.Put(content)
	if key_id == -1 {
		//fmt.Println("Bukent full")
		return errors.New("Number Bukets full")
	}
	//新增
	if key_id > len {
		//fmt.Println("Add Bukent full")
		invertList := NewInvertDocIdList(content)
		invertList.DocIdList = append(invertList.DocIdList, DocIdInfo{DocId:doc_id})
		ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
		ivt_idx.IdxLen++
	} else { //更新
		//fmt.Println("Update Bukent full")
		ivt_idx.KeyInvertList[key_id].DocIdList = append(ivt_idx.KeyInvertList[key_id].DocIdList, DocIdInfo{DocId:doc_id})
	}
	//ivt_idx.Display()
	//ivt_dic.Display()
	return nil
}
