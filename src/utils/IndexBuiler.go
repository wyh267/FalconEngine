
/*****************************************************************************
 *  file name : 
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 
 *
******************************************************************************/
package utils

import (
	"strings"
	"errors"
	//"fmt"
)


/*****************************************************************************
*  function name : BuildTextIndex
*  params : 
*  return : 
*
*  description : 
*
******************************************************************************/
const RULE_EN	int64 = 1
const RULE_CHN	int64 = 2
func BuildTextIndex(doc_id int64,content string,rule int64,ivt_idx *InvertIdx,ivt_dic *StringIdxDic) error {
	
	if ivt_idx.IdxType != TYPE_TEXT {
		return errors.New("Wrong Type")
	}
	
	var terms []string 
	//英文直接按照空格切割
	if rule == RULE_EN {
		terms = RemoveDuplicatesAndEmpty(strings.Fields(strings.ToLower(content)))
		if len(terms) == 0{
			return errors.New("Empty content")
		}
	}
	
	
	for _,term := range terms {
		len:=ivt_dic.Length()
		key_id := ivt_dic.Put(term)
		if key_id == -1 {
			return errors.New("Bukets full")
		}
		//新增
		if key_id > len {
			invertList := NewInvertDocIdList(term)
			invertList.DocIdList = append(invertList.DocIdList,DocIdInfo{doc_id,0})
			ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList,*invertList)
			ivt_idx.IdxLen++
		}else{//更新
			ivt_idx.KeyInvertList[key_id].DocIdList= append(ivt_idx.KeyInvertList[key_id].DocIdList,DocIdInfo{doc_id,0})
		}
		
	}
	return nil
}





func BuildNumberIndex(doc_id int64,content int64,ivt_idx *InvertIdx,ivt_dic *NumberIdxDic) error {
	
	len:=ivt_dic.Length()
	key_id := ivt_dic.Put(content)
	if key_id == -1 {
		return errors.New("Bukets full")
	}
	//新增
	if key_id > len {
		invertList := NewInvertDocIdList(content)
		invertList.DocIdList = append(invertList.DocIdList,DocIdInfo{doc_id,0})
		ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList,*invertList)
		ivt_idx.IdxLen++
	}else{//更新
		ivt_idx.KeyInvertList[key_id].DocIdList= append(ivt_idx.KeyInvertList[key_id].DocIdList,DocIdInfo{doc_id,0})
	}
	
	return nil
}

