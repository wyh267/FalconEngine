/*****************************************************************************
 *  file name : InvertIndex.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引基础结构
 *
******************************************************************************/

package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	//"unsafe"
)

//
// DocId的最小结构体，包括DocId本身和权重，权重目前都是0
//
type DocIdInfo struct {
	DocId int64
	//Weight int64
}

//
//静态倒排索引的最小单位，包含一个docid链和这个链的元信息(这个链的对应key[可能是任何类型])
//
type InvertDocIdList struct {
	//Key          interface{}
	DocIdList    []DocIdInfo
	StartPos     int64
	EndPos       int64
	IncDocIdList []DocIdInfo
}

func NewInvertDocIdList(key interface{}) *InvertDocIdList {
	this := &InvertDocIdList{ /*key,*/ make([]DocIdInfo, 0), 0, 0, make([]DocIdInfo, 0)}
	return this
}

func (this *InvertDocIdList) Display() {

	fmt.Printf(" KEY: [  ] ==> [ " /*, this.Key*/)
	for _, v := range this.DocIdList {
		fmt.Printf(" %v ", v.DocId)
	}
	fmt.Printf(" ] \n")

}

//
//
//倒排索引
//IdxType    倒排索引类型，string,int64,float.....
//
//
const TYPE_TEXT int64 = 1
const TYPE_NUM int64 = 2
const TYPE_BOOL int64 = 3
const TYPE_FLOAT int64 = 4

type InvertIdx struct {
	IdxType   int64
	IdxName   string
	IdxLen    int64
	MmapBytes []byte
	IsMaped   bool

	KeyInvertList []InvertDocIdList
}

func NewInvertIdx(idx_type int64, name string) *InvertIdx {

	list := make([]InvertDocIdList, 1)
	list[0] = InvertDocIdList{ /*"nil",*/ make([]DocIdInfo, 0), 0, 0, make([]DocIdInfo, 0)}
	this := &InvertIdx{IdxType: idx_type, IdxName: name, IdxLen: 0, KeyInvertList: list, MmapBytes: nil, IsMaped: false}
	return this

}

func NewInvertIdxWithName(name string) *InvertIdx {

	list := make([]InvertDocIdList, 0)
	this := &InvertIdx{IdxType: 0, IdxName: name, IdxLen: 0, KeyInvertList: list, MmapBytes: nil, IsMaped: false}
	return this

}

func (this *InvertIdx) GetInvertIndex(index int64) ([]DocIdInfo, bool) {

	if index > this.IdxLen || index < 1 {
		return nil, false
	}

	lens := int(this.KeyInvertList[index].EndPos)

	f, _ := os.Open(fmt.Sprintf("./index/%v_idx.idx", this.IdxName))
	//fmt.Printf("Start : %v   Lens : %v   file_name : %v  \n",this.KeyInvertList[index].StartPos,this.KeyInvertList[index].EndPos*8,fmt.Sprintf("./index/%v_idx.idx",this.IdxName))
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}
	//start:=int(this.KeyInvertList[index].StartPos)/4096
	//page_offset:=int(this.KeyInvertList[index].StartPos) % 4096
	//resultSize := int(page_offset+lens*8)

	MmapBytes, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)

	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil, false
	}

	defer syscall.Munmap(MmapBytes)

	StartPos := int(this.KeyInvertList[index].StartPos)

	this.KeyInvertList[index].DocIdList = make([]DocIdInfo, lens)

	for i := 0; i < lens; i++ {
		start := StartPos + i*8
		end := StartPos + (i+1)*8
		this.KeyInvertList[index].DocIdList[i].DocId = int64(binary.LittleEndian.Uint64(MmapBytes[start:end]))
	}

	this.KeyInvertList[index].DocIdList = append(this.KeyInvertList[index].DocIdList, this.KeyInvertList[index].IncDocIdList...)

	return this.KeyInvertList[index].DocIdList, true

}

func (this *InvertIdx) Display() {
	var idxtype string
	switch this.IdxType {
	case TYPE_TEXT:
		idxtype = "TEXT INDEX"
	case TYPE_NUM:
		idxtype = "NUMBER INDEX"
	case TYPE_BOOL:
	case TYPE_FLOAT:
	}

	fmt.Printf("\n=========== [ %v ] [ TYPE : %v ] [ LEN : %v ]==============\n", this.IdxName, idxtype, this.IdxLen)
	for index, value := range this.KeyInvertList {
		fmt.Printf("INDEX : [ %v ] ::: ", index)
		value.Display()
	}

}

func (this *InvertIdx) ReadFromFile() error {

	file_name := fmt.Sprintf("./index/%v.idx.dic", this.IdxName)
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

	this.IdxType = int64(binary.LittleEndian.Uint64(MmapBytes[:8]))
	this.IdxLen = int64(binary.LittleEndian.Uint64(MmapBytes[8:16]))
	name_lens := int64(binary.LittleEndian.Uint64(MmapBytes[16:24]))
	this.IdxName = string(MmapBytes[24 : 24+name_lens])
	var start int64 = 24 + name_lens
	var i int64 = 0
	for i = 0; i <= this.IdxLen; i++ {
		start_pos := int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		end_pos := int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		this.KeyInvertList = append(this.KeyInvertList, InvertDocIdList{nil, start_pos, end_pos, nil})

	}

	return nil

}

func (this *InvertIdx) WriteToFile() error {

	fmt.Printf("Writing to File [%v]...\n", this.IdxName)
	file_name := fmt.Sprintf("./index/%v.idx.dic", this.IdxName)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, this.IdxType)
	err = binary.Write(buf, binary.LittleEndian, this.IdxLen)
	if err != nil {
		fmt.Printf("Lens ERROR :%v \n", err)
	}
	err = binary.Write(buf, binary.LittleEndian, int64(len(this.IdxName)))
	if err != nil {
		fmt.Printf("Lens IdxName ERROR :%v \n", err)
	}
	err = binary.Write(buf, binary.LittleEndian, []byte(this.IdxName))
	if err != nil {
		fmt.Printf("IdxName ERROR :%v \n", err)
	}

	for _, v := range this.KeyInvertList {

		err = binary.Write(buf, binary.LittleEndian, v.StartPos)
		if err != nil {
			fmt.Printf("Write StartPos Error :%v \n", err)
		}
		err = binary.Write(buf, binary.LittleEndian, v.EndPos)
		if err != nil {
			fmt.Printf("Write EndPos Error :%v \n", err)
		}
	}
	fout.Write(buf.Bytes())
	return nil

}
