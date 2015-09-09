/*****************************************************************************
 *  file name : Detail.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : Detail文件
 *
******************************************************************************/

package indexer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	"utils"
)

type DetailInfo struct {
	DetailBytes []byte
	ByteStart   int64
	ByteLen     int64
	IsInc       bool
}

type Detail struct {
	MaxDocId      int64
	Offset        int64
	DetailList    []DetailInfo
	IncDetailList []DetailInfo
}

func NewDetail() *Detail {

	this := &Detail{MaxDocId: 0, DetailList: make([]DetailInfo, 1), IncDetailList: make([]DetailInfo, 1), Offset: 0}

	return this
}

func (this *Detail) GetDocInfo(doc_id int64) (map[string]string, error) {

	if doc_id > this.MaxDocId {
		return nil, errors.New("DocId Wrong")
	}

	if this.DetailList[doc_id].IsInc == true {
		var info_detail map[string]string
		err := json.Unmarshal(this.DetailList[doc_id].DetailBytes, &info_detail)
		if err != nil {
			fmt.Printf("Unmarshal ERROR  %v \n", err)
			return nil, err
		}
		return info_detail, nil
	}

	//functime := utils.InitTime()

	//fmt.Printf("Cost Time : %v \n",functime("Start"))
	f, _ := os.Open("./index/detail.dat")
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}
	MmapBytes, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil, err
	}
	defer syscall.Munmap(MmapBytes)
	//fmt.Printf("Cost Time : %v \n",functime("mmap"))

	StartPos := int(this.DetailList[doc_id].ByteStart)
	EndPos := int(this.DetailList[doc_id].ByteLen) + StartPos
	this.DetailList[doc_id].DetailBytes = MmapBytes[StartPos:EndPos]
	//fmt.Printf("Cost Time : %v \n",functime("MmapBytes"))

	var info_detail map[string]string
	err = json.Unmarshal(this.DetailList[doc_id].DetailBytes, &info_detail)
	if err != nil {
		fmt.Printf("Unmarshal ERROR  %v \n", err)
		return nil, err
	}
	return info_detail, nil

}

func (this *Detail) PutDocInfo(doc_id int64, info map[string]string) error {
	if doc_id != this.MaxDocId+1 {
		return errors.New("DocID Wrong")
	}

	info_byte, err := json.Marshal(info)
	if err != nil {
		return err
	}
	var detail_info DetailInfo
	detail_info.DetailBytes = info_byte
	detail_info.ByteLen = int64(len(info_byte))
	detail_info.ByteStart = this.Offset
	detail_info.IsInc = false
	this.Offset += int64(len(info_byte))
	this.MaxDocId++
	this.DetailList = append(this.DetailList, detail_info)

	return nil
}

func (this *Detail) SetNewValue(doc_id int64, info map[string]string) error {

	binfo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	if doc_id > this.MaxDocId {
		var detail_info DetailInfo
		detail_info.DetailBytes = binfo
		detail_info.IsInc = true
		this.MaxDocId++
		this.DetailList = append(this.DetailList, detail_info)

	} else {
		this.DetailList[int(doc_id)].DetailBytes = binfo
	}

	this.DetailList[doc_id].IsInc = true
	return nil

}

func (this *Detail) WriteDetailToFile() error {

	buf := new(bytes.Buffer)

	//file_name := fmt.Sprintf("./index/detail.dat")
	fout, err := os.Create(fmt.Sprintf("./index/detail.dat"))
	if err != nil {
		fmt.Printf("Create Error %v\n", err)
		return err
	}
	defer fout.Close()

	for index, _ := range this.DetailList {

		err := binary.Write(buf, binary.LittleEndian, this.DetailList[index].DetailBytes)
		if err != nil {
			fmt.Printf("Write Error ..%v\n", err)
		}
		this.DetailList[index].DetailBytes = nil
	}

	fout.Write(buf.Bytes())
	utils.WriteToJson(this, "./index/detail.idx.json")
	return nil

}
