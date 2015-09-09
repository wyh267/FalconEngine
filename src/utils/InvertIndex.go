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
	IncPos		 int64
	IncDocIdList []DocIdInfo
}

func NewInvertDocIdList(key interface{}) *InvertDocIdList {
	this := &InvertDocIdList{ /*key,*/ make([]DocIdInfo, 0), 0, 0,0, make([]DocIdInfo, 0)}
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
	posMmap   *Mmap
	idxMmap	  *Mmap
	upMmap	  *Mmap
}

func NewInvertIdx(idx_type int64, name string) *InvertIdx {

	list := make([]InvertDocIdList, 1)
	list[0] = InvertDocIdList{ /*"nil",*/ make([]DocIdInfo, 0), 0, 0,0, make([]DocIdInfo, 0)}
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


/*
	f, _ := os.Open(fmt.Sprintf("./index/%v.idx", this.IdxName))
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
*/
	StartPos := int(this.KeyInvertList[index].StartPos)

	this.KeyInvertList[index].DocIdList = make([]DocIdInfo, lens)

	for i := 0; i < lens; i++ {
		start := StartPos + i*8
		//end := StartPos + (i+1)*8
		this.KeyInvertList[index].DocIdList[i].DocId = this.idxMmap.ReadInt64(int64(start))//int64(binary.LittleEndian.Uint64(MmapBytes[start:end]))
	}

	this.KeyInvertList[index].DocIdList = append(this.KeyInvertList[index].DocIdList, this.KeyInvertList[index].IncDocIdList...)
	fmt.Printf("DOC_ID : %v \n",this.KeyInvertList[index].DocIdList)
	return this.KeyInvertList[index].DocIdList, true

}



func (this *InvertIdx) UpdateInvert(key_id,doc_id int64) error {
	
	var next int64 = 0
	inc_start:=this.upMmap.GetPointer()
	this.upMmap.AppendInt64(doc_id)
	this.upMmap.AppendInt64(next)
	this.upMmap.WriteInt64(0,this.upMmap.GetPointer())
	
	if key_id > this.IdxLen {
		invertList := NewInvertDocIdList("term")
		invertList.IncDocIdList = append(invertList.IncDocIdList, DocIdInfo{DocId: doc_id})
		this.KeyInvertList = append(this.KeyInvertList, *invertList)
		this.IdxLen++
		//更新pos文件的inc_start位
		//全新的数据
		this.posMmap.AppendInt64(next)
		this.posMmap.AppendInt64(next)
		this.posMmap.AppendInt64(inc_start)
		this.posMmap.WriteInt64(8,this.IdxLen)
		
	} else { //更新
		if len(this.KeyInvertList[key_id].IncDocIdList) == 0 {//第一个新数据
			//更新pos文件的inc_start位
			pos:=(24+128)+(key_id)*24+16
			this.posMmap.WriteInt64(pos,inc_start)
		}else{
			//更新up文件的上一个节点的next位
			next_pos := this.KeyInvertList[key_id].IncPos + 8
			this.upMmap.WriteInt64(next_pos,inc_start)
		}
		
		this.KeyInvertList[key_id].IncDocIdList = append(this.KeyInvertList[key_id].IncDocIdList, DocIdInfo{DocId: doc_id})
		
	}
	this.KeyInvertList[key_id].IncPos=inc_start
	
	return nil
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
	
	var err error
	//mmap增量索引数据
	file_name := fmt.Sprintf("./index/%v.up", this.IdxName)
	this.upMmap,err = NewMmap(file_name,MODE_APPEND)
	if err !=nil {
		fmt.Printf("mmap error : %v \n",err)
		return err
	}
	end:=this.upMmap.ReadInt64(0)
	if end == 0 {
		this.upMmap.SetFileEnd(8)
	}else{
		this.upMmap.SetFileEnd(end)
	}
	
	

	file_name = fmt.Sprintf("./index/%v.pos", this.IdxName)
	
	this.posMmap,err = NewMmap(file_name,MODE_APPEND)
	if err !=nil {
		fmt.Printf("mmap error : %v \n",err)
		return err
	}
	this.IdxType = this.posMmap.ReadInt64(0)
	this.IdxLen = this.posMmap.ReadInt64(8)
	name_lens := this.posMmap.ReadInt64(16)
	this.IdxName = this.posMmap.ReadString(24,name_lens)//string(MmapBytes[24 : 24+name_lens])
	byte_len:=128-len(this.IdxName)
	var start int64 = 24 + name_lens + int64(byte_len)
	var i int64 = 0
	for i = 0; i <= this.IdxLen; i++ {
		start_pos := this.posMmap.ReadInt64(start)
		start += 8
		end_pos := this.posMmap.ReadInt64(start)
		start += 8
		inc_pos := this.posMmap.ReadInt64(start)
		start += 8
		this.KeyInvertList = append(this.KeyInvertList, InvertDocIdList{nil, start_pos, end_pos,inc_pos, nil})
		//将增量读入内存中
		if inc_pos > 0 {
			var it int64 = inc_pos
			for it != 0 {
				this.KeyInvertList[i].IncPos=it
				doc_id:=this.upMmap.ReadInt64(it)
				it=this.upMmap.ReadInt64(it+8)
				this.KeyInvertList[i].IncDocIdList = append(this.KeyInvertList[i].IncDocIdList, DocIdInfo{DocId: doc_id})
			}
			
			
		}
	}
	this.posMmap.SetFileEnd(start)
	
	
	//mmap主索引数据
	file_name = fmt.Sprintf("./index/%v.idx", this.IdxName)
	this.idxMmap,err = NewMmap(file_name,MODE_APPEND)
	if err !=nil {
		fmt.Printf("mmap error : %v \n",err)
		return err
	}
	
	
	
	
	return nil

}

func (this *InvertIdx) WriteToFile() error {
	
	
	if err:=this.WriteToIndexFile();err!=nil{
		return err
	}
	if err:=this.WriteToIndexPosFile();err!=nil{
		return err
	}
	
	if err:=this.WriteUpIndexFile();err!=nil{
		return err
	}
	
	return nil
	

}



func (this *InvertIdx) WriteUpIndexFile() error {
	
	file_name := fmt.Sprintf("./index/%v.up", this.IdxName)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}
	err=syscall.Ftruncate(int(fout.Fd()),APPEND_DATA)
	if err != nil {
		fmt.Printf("ftruncate error : %v\n",err)
		return err
	}
	
	return nil
	
}


func (this *InvertIdx) WriteToIndexPosFile() error {
	fmt.Printf("Writing to File [%v]...\n", this.IdxName)
	file_name := fmt.Sprintf("./index/%v.pos", this.IdxName)
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
	byte_len:=128-len(this.IdxName)
	null_byte := make([]byte,byte_len)
	err = binary.Write(buf, binary.LittleEndian, null_byte)
	if err != nil {
		fmt.Printf("IdxName ERROR :%v \n", err)
	}
	
	var null_data int64 = 0
	for _, v := range this.KeyInvertList {

		err = binary.Write(buf, binary.LittleEndian, v.StartPos)
		if err != nil {
			fmt.Printf("Write StartPos Error :%v \n", err)
		}
		err = binary.Write(buf, binary.LittleEndian, v.EndPos)
		if err != nil {
			fmt.Printf("Write EndPos Error :%v \n", err)
		}
		
		err = binary.Write(buf, binary.LittleEndian,null_data)
		if err != nil {
			fmt.Printf("Write null_data Error :%v \n", err)
		}
	}
	fout.Write(buf.Bytes())
	return nil
}



func  (this *InvertIdx) WriteToIndexFile() error {


	file_name := fmt.Sprintf("./index/%v.idx", this.IdxName)
	fmt.Printf("Write index[%v] to File [%v]...\n", file_name, file_name)
	buf := new(bytes.Buffer)
	var start_pos int64 = 0
	for index, KeyIdList := range this.KeyInvertList {
		this.KeyInvertList[index].StartPos = start_pos
		this.KeyInvertList[index].EndPos = int64(len(KeyIdList.DocIdList))
		for _, DocIdInfo := range KeyIdList.DocIdList {
			err := binary.Write(buf, binary.LittleEndian, DocIdInfo)
			start_pos = start_pos + 8
			if err != nil {
				fmt.Printf("Write Error ..%v\n", err)
			}
		}
		this.KeyInvertList[index].DocIdList = nil
	}
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		fmt.Printf("Create Error %v\n", file_name)
		return err
	}
	fout.Write(buf.Bytes())
	return nil

}