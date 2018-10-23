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
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
	//"sort"
	"fmt"
)

// WriteToJson function description : 写入json文件
// params :
// return :
func WriteToJson(data interface{}, file_name string) error {

	//fmt.Printf("Writing to File [%v]...\n", file_name)
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

// ReadFromJson function description : 读取json文件
// params :
// return :
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

func ReadFile(path string) (string, error) {
	fi, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	return string(fd), nil
}

func WriteFile(path, content string) error {

	f, err := os.Create(path) //创建文件
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f) //创建新的 Writer 对象
	_, err1 := w.WriteString(content)
	if err1 != nil {
		return err
	}
	w.Flush()
	return nil

}
