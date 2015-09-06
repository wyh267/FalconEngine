/*****************************************************************************
 *  file name : JsonFileIO.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 读写json文件
 *
******************************************************************************/

package utils

import (
	"encoding/json"
	"os"
	//"bufio"
	//"bytes"
	"bytes"
	"encoding/binary"
	"io/ioutil"
	//"sort"
	"fmt"
)

func WriteIndexDataToFileWithChan(invertIdx *InvertIdx, dic_data interface{}, index_name string, wchan chan string) error {

	WriteToIndexFile(invertIdx, fmt.Sprintf("./index/%v_idx.idx", index_name))
	WriteToJson(invertIdx, fmt.Sprintf("./index/%v_idx.json", index_name))
	WriteToJson(dic_data, fmt.Sprintf("./index/%v_dic.json", index_name))
	wchan <- index_name
	return nil
}

func WriteToJsonWithChan(data interface{}, file_name string, wchan chan string) error {

	WriteToJson(data, file_name)
	wchan <- file_name
	return nil
}

func WriteToIndexFileWithChan(invertIdx *InvertIdx, file_name string, wchan chan string) error {

	WriteToIndexFile(invertIdx, file_name)
	wchan <- file_name
	return nil
}

/*
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


type InvertIdx struct {
	IdxType       int64
	IdxName       string
	IdxLen        int64
	KeyInvertList []InvertDocIdList
}
*/
func WriteToIndexFile(invertIdx *InvertIdx, file_name string) error {

	//file_name := fmt.Sprintf("./index_tmp/%v_%03d.idx",index_name,this.TempIndexNum[index_name])
	fmt.Printf("Write index[%v] to File [%v]...\n", file_name, file_name)
	buf := new(bytes.Buffer)
	//sort.Sort(SortByKeyId(this.TempIndex[index_name]))
	var start_pos int64 = 0
	for index, KeyIdList := range invertIdx.KeyInvertList {
		//var doc_lens int64 = 0
		invertIdx.KeyInvertList[index].StartPos = start_pos
		invertIdx.KeyInvertList[index].EndPos = int64(len(KeyIdList.DocIdList))
		for _, DocIdInfo := range KeyIdList.DocIdList {
			err := binary.Write(buf, binary.LittleEndian, DocIdInfo)
			start_pos = start_pos + 8
			if err != nil {
				fmt.Printf("Write Error ..%v\n", err)
			}
		}
		invertIdx.KeyInvertList[index].DocIdList = nil

	}

	//fmt.Printf("%x\n", buf.Bytes())
	//fmt.Printf("%v\n", this.TempIndex[index_name])
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		fmt.Printf("Create Error %v\n", file_name)
		return err
	}
	fout.Write(buf.Bytes())
	return nil

}

/*****************************************************************************
*  function name : WriteToJson
*  params : interface to tran json ,file_name
*  return : error
*
*  description :  write any struct to json and store to the disk
*
******************************************************************************/
func WriteToJson(data interface{}, file_name string) error {

	fmt.Printf("Writing to File [%v]...\n", file_name)
	info_json, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Marshal err %v\n", file_name)
		return err
	}
	//fmt.Printf("%v\n",info_json)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}
	fout.Write(info_json)
	return nil

}

/*****************************************************************************
*  function name : ReadFromJson
*  params : file name
*  return : all the file content for bytes
*
*  description : read bytes in file
*
******************************************************************************/
func ReadFromJson(file_name string) ([]byte, error) {

	fin, err := os.Open(file_name)
	defer fin.Close()
	if err != nil {
		return nil, err
	}

	buffer, err := ioutil.ReadAll(fin)
	if err != nil {
		return nil, err
	}
	return buffer, nil

}
