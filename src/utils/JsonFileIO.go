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
	"io/ioutil"
	//"sort"
	"fmt"
)


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

	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {

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
