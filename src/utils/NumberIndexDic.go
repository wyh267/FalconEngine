/*****************************************************************************
 *  file name : NumberIndexDic.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数字到ID的hash映射文件，可以为每个数字生成唯一的ID，用于后续的
 *   				   倒排索引
 *
******************************************************************************/

package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

type NumberIdxDic struct {
	Lens   int64
	IntMap map[string]int64
	Index  int64
	Name   string
}

/*****************************************************************************
*  function name : NewNumberIdxDic
*  params : buket_type ===> 1: 2: 3:
*  return : NumberIdxDic
*
*  description :
*
******************************************************************************/
func NewNumberIdxDic(name string) *NumberIdxDic {

	this := &NumberIdxDic{IntMap: make(map[string]int64), Lens: 0, Index: 1, Name: name}
	return this
}

func (this *NumberIdxDic) Display() {
	fmt.Printf("========================= Bukets : %v  EntityCount :%v =========================\n", this.Lens, this.Index)

	for k, v := range this.IntMap {
		fmt.Printf("Key : %v \t\t--- Value : %v  \n", k, v)
	}
	fmt.Printf("===============================================================================\n")
}

func (this *NumberIdxDic) Put(key int64) int64 {

	key_str := fmt.Sprintf("%v", key)
	id, _ := this.Find(key)
	if id != -1 {
		return id
	}

	this.IntMap[key_str] = this.Index
	this.Index++
	this.Lens++

	return this.IntMap[key_str]

}

/*****************************************************************************
*  function name : Length
*  params : nil
*  return : int64
*
*  description : 返回哈希表长度
*
******************************************************************************/
func (this *NumberIdxDic) Length() int64 {

	return this.Lens

}

func (this *NumberIdxDic) Find(key int64) (int64, int64) {

	key_str := fmt.Sprintf("%v", key)
	value, has_key := this.IntMap[key_str]
	if has_key {
		return value, 0
	}
	return -1, 0

}

func (this *NumberIdxDic) WriteToFile() error {

	fmt.Printf("Writing to File [%v]...\n", this.Name)
	file_name := fmt.Sprintf("./index/%v.dic", this.Name)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, this.Lens)
	err = binary.Write(buf, binary.LittleEndian, this.Index)
	if err != nil {
		fmt.Printf("Lens ERROR :%v \n", err)
	}
	for k, v := range this.IntMap {
		err := binary.Write(buf, binary.LittleEndian, int64(len(k)))
		if err != nil {
			fmt.Printf("Write Lens Error :%v \n", err)
		}
		err = binary.Write(buf, binary.LittleEndian, []byte(k))
		if err != nil {
			fmt.Printf("Write Key Error :%v \n", err)
		}
		err = binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			fmt.Printf("Write Value Error :%v \n", err)
		}
	}
	fout.Write(buf.Bytes())
	return nil
}

func (this *NumberIdxDic) ReadFromFile() error {

	file_name := fmt.Sprintf("./index/%v.dic", this.Name)
	f, err := os.Open(file_name)
	defer f.Close()
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}

	MmapBytes, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)

	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil
	}

	defer syscall.Munmap(MmapBytes)

	this.Lens = int64(binary.LittleEndian.Uint64(MmapBytes[:8]))
	this.Index = int64(binary.LittleEndian.Uint64(MmapBytes[8:16]))
	var start int64 = 16
	var i int64 = 0
	for i = 0; i < this.Lens; i++ {
		lens := int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		key := string(MmapBytes[start : start+lens])
		start += lens
		value := int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		this.IntMap[key] = value
	}

	return nil

}
