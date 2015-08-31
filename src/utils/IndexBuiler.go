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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

type SortByKeyId []TmpIdx

func (a SortByKeyId) Len() int      { return len(a) }
func (a SortByKeyId) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByKeyId) Less(i, j int) bool {

	if a[i].KeyId == a[j].KeyId {
		return a[i].DocId < a[j].DocId
	}

	return a[i].KeyId < a[j].KeyId
}

type TmpIdx struct {
	KeyId int64
	DocId int64
}

type IndexBuilder struct {
	Segmenter    *Segmenter
	TempIndex    map[string][]TmpIdx
	TempIndexNum map[string]int64
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

func (this *IndexBuilder) BuildTextIndex(doc_id int64, content string, ivt_idx *InvertIdx, ivt_dic *StringIdxDic, split_type int64, is_inc bool) error {

	if ivt_idx.IdxType != TYPE_TEXT {
		return errors.New("Wrong Type")
	}

	if len(strings.TrimSpace(content)) == 0 {
		return nil //errors.New("nothing")
	}

	var terms []string

	switch split_type {
	case 1: //正常切词
		terms = RemoveDuplicatesAndEmpty(this.Segmenter.Segment(content, true))
	case 2: //按单个字符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ""))
	case 3: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ";"))
	case 4: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, "@"))
	}

	for _, term := range terms {
		len := ivt_dic.Length()
		key_id := ivt_dic.Put(term)
		if key_id == -1 {
			return errors.New("Text Bukets full")
		}
		//新增
		if is_inc == false {
			if key_id > len {
				invertList := NewInvertDocIdList(term)
				invertList.DocIdList = append(invertList.DocIdList, DocIdInfo{DocId: doc_id})
				ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
				ivt_idx.IdxLen++
			} else { //更新
				ivt_idx.KeyInvertList[key_id].DocIdList = append(ivt_idx.KeyInvertList[key_id].DocIdList, DocIdInfo{DocId: doc_id})
			}
		} else {
			if key_id > len {
				invertList := NewInvertDocIdList(term)
				invertList.IncDocIdList = append(invertList.IncDocIdList, DocIdInfo{DocId: doc_id})
				ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
				ivt_idx.IdxLen++
			} else { //更新
				ivt_idx.KeyInvertList[key_id].IncDocIdList = append(ivt_idx.KeyInvertList[key_id].IncDocIdList, DocIdInfo{DocId: doc_id})
			}
		}

		//将key_id,doc_id写入临时内存中

	}
	return nil
}

func (this *IndexBuilder) BuildNumberIndex(doc_id int64, content int64, ivt_idx *InvertIdx, ivt_dic *NumberIdxDic, is_inc bool) error {

	len := ivt_dic.Length()
	//fmt.Println("len ",len)
	//fmt.Printf("doc_id : %v  content : %v \n",doc_id,content)
	key_id := ivt_dic.Put(content)
	if key_id == -1 {
		//fmt.Println("Bukent full")
		return errors.New("Number Bukets full")
	}
	//新增
	if is_inc == false {
		if key_id > len {
			invertList := NewInvertDocIdList(content)
			invertList.DocIdList = append(invertList.DocIdList, DocIdInfo{DocId: doc_id})
			ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
			ivt_idx.IdxLen++
		} else { //更新
			//fmt.Println("Update Bukent full")
			ivt_idx.KeyInvertList[key_id].DocIdList = append(ivt_idx.KeyInvertList[key_id].DocIdList, DocIdInfo{DocId: doc_id})
		}

	} else {
		if key_id > len {
			invertList := NewInvertDocIdList(content)
			invertList.IncDocIdList = append(invertList.IncDocIdList, DocIdInfo{DocId: doc_id})
			ivt_idx.KeyInvertList = append(ivt_idx.KeyInvertList, *invertList)
			ivt_idx.IdxLen++
		} else { //更新
			ivt_idx.KeyInvertList[key_id].IncDocIdList = append(ivt_idx.KeyInvertList[key_id].IncDocIdList, DocIdInfo{DocId: doc_id})
		}
	}

	//ivt_idx.Display()
	ivt_dic.Display()
	return nil
}

func (this *IndexBuilder) BuildTextIndexTemp(doc_id int64, content string, ivt_idx *InvertIdx, ivt_dic *StringIdxDic, split_type int64, index_name string) error {

	if ivt_idx.IdxType != TYPE_TEXT {
		return errors.New("Wrong Type")
	}

	if len(strings.TrimSpace(content)) == 0 {
		return nil //errors.New("nothing")
	}

	var terms []string

	switch split_type {
	case 1: //正常切词
		terms = RemoveDuplicatesAndEmpty(this.Segmenter.Segment(content, true))
	case 2: //按单个字符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ""))
	case 3: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ";"))
	case 4: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, "@"))
	}

	for _, term := range terms {
		//len := ivt_dic.Length()
		key_id := ivt_dic.Put(term)
		if key_id == -1 {
			return errors.New("Text Bukets full")
		}

		err := this.writeTempIndex(key_id, doc_id, index_name)
		if err != nil {
			return err
		}

	}
	return nil
}

func (this *IndexBuilder) BuildNumberIndexTemp(doc_id int64, content int64, ivt_idx *InvertIdx, ivt_dic *NumberIdxDic, index_name string) error {

	//len := ivt_dic.Length()
	//fmt.Println("len ",len)
	//fmt.Printf("doc_id : %v  content : %v \n",doc_id,content)
	key_id := ivt_dic.Put(content)
	if key_id == -1 {
		//fmt.Println("Bukent full")
		return errors.New("Number Bukets full")
	}

	err := this.writeTempIndex(key_id, doc_id, index_name)
	if err != nil {
		return err
	}

	//ivt_dic.Display()
	return nil
}

func (this *IndexBuilder) WriteAllTempIndexToFile() error {

	for index_name, _ := range this.TempIndex {
		err := this.writeTempIndexToFile(index_name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *IndexBuilder) writeTempIndexToFile(index_name string) error {
	file_name := fmt.Sprintf("./index_tmp/%v_%03d.idx.tmp", index_name, this.TempIndexNum[index_name])
	fmt.Printf("Write index[%v] to File [%v]...\n", index_name, file_name)
	buf := new(bytes.Buffer)
	sort.Sort(SortByKeyId(this.TempIndex[index_name]))
	for _, tmp := range this.TempIndex[index_name] {
		err := binary.Write(buf, binary.LittleEndian, tmp)
		if err != nil {
			fmt.Printf("Write Error ..%v\n", err)
		}
	}
	//fmt.Printf("%x\n", buf.Bytes())
	//fmt.Printf("%v\n", this.TempIndex[index_name])
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}
	fout.Write(buf.Bytes())
	return nil
}

func (this *IndexBuilder) writeTempIndex(key_id, doc_id int64, index_name string) error {
	//将key_id,doc_id写入临时内存中
	_, ok := this.TempIndex[index_name]
	//第一次遇到这个索引
	if !ok {
		_, num_ok := this.TempIndexNum[index_name]
		if !num_ok {
			this.TempIndexNum[index_name] = 0
		} else {
			this.TempIndexNum[index_name] = this.TempIndexNum[index_name] + 1
		}

		this.TempIndex[index_name] = make([]TmpIdx, 0)
	}
	this.TempIndex[index_name] = append(this.TempIndex[index_name], TmpIdx{key_id, doc_id})

	if len(this.TempIndex[index_name]) == 1000 {
		err := this.writeTempIndexToFile(index_name)
		if err != nil {
			return err
		}
		delete(this.TempIndex, index_name)
	}
	return nil
}
