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
	//"syscall"
)

type NumberIdxDic struct {
	Lens     int64
	IntMap   map[string]int64
	Index    int64
	Name     string
	mmap     *Mmap
	isSearch bool
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

	this := &NumberIdxDic{IntMap: make(map[string]int64), Lens: 0, Index: 1, Name: name, mmap: nil, isSearch: false}
	/*
		file_name := fmt.Sprintf("./index/%v.dic", this.Name)

		var err error
		this.mmap,err = NewMmap(file_name,MODE_CREATE)
		if err !=nil {
			fmt.Printf("mmap error : %v \n",err)
			return this
		}
	*/
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

	if this.isSearch {
		//fmt.Printf("updating key_value : %v index:%v lens:%v \n",this.IntMap[key_str],this.Index,this.Lens)
		this.mmap.WriteInt64(0, this.Lens)
		this.mmap.WriteInt64(8, this.Index)
		this.mmap.AppendStringWithLen(key_str)
		this.mmap.AppendInt64(this.Index - 1)
	}

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

	var err error
	this.mmap, err = NewMmap(file_name, MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return err
	}

	this.Lens = this.mmap.ReadInt64(0)
	this.Index = this.mmap.ReadInt64(8)
	//fmt.Printf("lens : %v index : %v \n",this.Lens,this.Index)
	var start int64 = 16
	var i int64 = 0
	for i = 0; i < this.Lens; i++ {
		lens := this.mmap.ReadInt64(start)
		//fmt.Printf("lens : %v \n",lens)
		start += 8
		key := this.mmap.ReadString(start, lens)
		start += lens
		value := this.mmap.ReadInt64(start)
		start += 8
		this.IntMap[key] = value
	}

	this.mmap.SetFileEnd(start)
	this.isSearch = true
	return nil

}
