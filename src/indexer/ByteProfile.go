/*****************************************************************************
 *  file name : ByteProfile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description :
 *
******************************************************************************/

package indexer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	u "utils"
)

type ByteNode struct {
	Data     []byte
	Start    int64
	DataLen  int
	InMomory bool
}

type ByteProfile struct {
	*Profile
	ProfileList []ByteNode
}

func NewByteProfile(name string) *ByteProfile {
	profile := &Profile{name, 2, 1, false}
	this := &ByteProfile{profile, make([]ByteNode, 1)}
	return this
}

func (this *ByteProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, v.DataLen)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *ByteProfile) PutProfile(doc_id int64, value []byte) error {
	//fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ] [ DOC_ID : %v ]============\n", this.Name, this.Len,doc_id)
	if doc_id > this.Len || doc_id < 1 {
		return errors.New("docid is wrong")
	}

	var byte_node ByteNode
	byte_node.Data = value
	byte_node.Start = 0
	byte_node.DataLen = len(value)
	byte_node.InMomory = true

	if doc_id == this.Len {
		this.ProfileList = append(this.ProfileList, byte_node)
		this.Len++
		return nil
	}

	this.ProfileList[doc_id] = byte_node
	return nil

}

func (this *ByteProfile) FindValue(doc_id int64) ([]byte, error) {
	if doc_id >= this.Len || doc_id < 1 {

		return nil, errors.New("docid is wrong")
	}

	if this.ProfileList[doc_id].InMomory == true {
		return this.ProfileList[doc_id].Data, nil
	}

	f, _ := os.Open(fmt.Sprintf("./index/%v_pfl.dat", this.Name))
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

	StartPos := int(this.ProfileList[doc_id].Start)
	EndPos := this.ProfileList[doc_id].DataLen + StartPos
	this.ProfileList[doc_id].Data = make([]byte, this.ProfileList[doc_id].DataLen)
	copy(this.ProfileList[doc_id].Data, MmapBytes[StartPos:EndPos])
	//fmt.Printf("Cost Time : %v \n",functime("MmapBytes"))

	this.ProfileList[doc_id].InMomory = true
	//fmt.Printf("list : %v\n", string(this.ProfileList[doc_id].Data))
	return this.ProfileList[doc_id].Data, nil

}

func (this *ByteProfile) FilterValue(doc_ids []u.DocIdInfo, value string, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	values := strings.Split(value, ",")
	value_num, _ := strconv.ParseInt(values[0], 0, 0)
	index_start, _ := strconv.ParseInt(values[1], 0, 0)
	index_end, _ := strconv.ParseInt(values[2], 0, 0)
	switch filt_type {
	case FILT_TYPE_LESS:
		//fmt.Printf("FILT_TYPE_LESS\n")
	OUTER_LESS:
		for i, _ := range doc_ids {
			byte_value, _ := this.FindValue(doc_ids[i].DocId)
			items := strings.Split(string(byte_value), ",")
			lens := len(items)
			//fmt.Printf("value : %v , start : %v , end : %v , lens_items : %v \n", value_num, index_start, index_end, len(items))
			if int(index_start) >= lens || int(index_end) >= lens || index_start >= index_end || lens < 1 || index_start < 0 || index_end < 1 {
				fmt.Printf("byteprofile info Error ... ")
				continue
			}
			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data
				if total >= value_num {
					continue OUTER_LESS
				}
			}
			if total < value_num {
				res = append(res, doc_ids[i])
			}

		}

	case FILT_TYPE_ABOVE:
		//fmt.Printf("FILT_TYPE_LESS\n")
		for i, _ := range doc_ids {
			byte_value, _ := this.FindValue(doc_ids[i].DocId)
			items := strings.Split(string(byte_value), ",")
			lens := len(items)
			//fmt.Printf("value : %v , start : %v , end : %v , lens_items : %v \n", value_num, index_start, index_end, len(items))
			if int(index_start) >= lens || int(index_end) >= lens || index_start >= index_end || lens < 1 || index_start < 0 || index_end < 1 {
				fmt.Printf("byteprofile info Error ... ")
				continue
			}
			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data
			}
			if total > value_num {
				res = append(res, doc_ids[i])
			}

		}
	case FILT_TYPE_EQUAL:
		//fmt.Printf("FILT_TYPE_LESS\n")
	OUTER_EQUAL:
		for i, _ := range doc_ids {
			byte_value, _ := this.FindValue(doc_ids[i].DocId)
			items := strings.Split(string(byte_value), ",")
			lens := len(items)
			//fmt.Printf("value : %v , start : %v , end : %v , lens_items : %v \n", value_num, index_start, index_end, len(items))
			if int(index_start) >= lens || int(index_end) >= lens || index_start >= index_end || lens < 1 || index_start < 0 || index_end < 1 {
				fmt.Printf("byteprofile info Error ... ")
				continue
			}
			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data
				if total > value_num {
					continue OUTER_EQUAL
				}
			}
			if total == value_num {
				res = append(res, doc_ids[i])
			}

		}
	case FILT_TYPE_UNEQUAL:
		//fmt.Printf("FILT_TYPE_LESS\n")
		for i, _ := range doc_ids {
			byte_value, _ := this.FindValue(doc_ids[i].DocId)
			items := strings.Split(string(byte_value), ",")
			lens := len(items)
			//fmt.Printf("value : %v , start : %v , end : %v , lens_items : %v \n", value_num, index_start, index_end, len(items))
			if int(index_start) >= lens || int(index_end) >= lens || index_start >= index_end || lens < 1 || index_start < 0 || index_end < 1 {
				fmt.Printf("byteprofile info Error ... ")
				continue
			}
			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data

			}
			if total != value_num {
				res = append(res, doc_ids[i])
			}

		}
	}

	return res, nil
}

func (this *ByteProfile) Put(doc_id int64, value interface{}) error {
	value_num, ok := value.([]byte)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_num)

}

func (this *ByteProfile) Find(doc_id int64) (interface{}, error) {

	return this.FindValue(doc_id)
}

func (this *ByteProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	if doc_ids == nil {
		return nil, nil
	}

	value_str, ok := value.(string)
	if !ok {
		return doc_ids, nil
	}

	return this.FilterValue(doc_ids, value_str, is_forward, filt_type)

}

func (this *ByteProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	return nil, nil
}

func (this *ByteProfile) GetType() int64 {
	return this.Type
}

func (this *ByteProfile) WriteToFile() error {

	buf := new(bytes.Buffer)

	//file_name := fmt.Sprintf("./index/detail.dat")
	fout, err := os.Create(fmt.Sprintf("./index/%v_pfl.dat", this.Name))
	if err != nil {
		fmt.Printf("Create Error %v\n", err)
		return err
	}
	defer fout.Close()
	var start int64 = 0
	for index, _ := range this.ProfileList {

		err := binary.Write(buf, binary.LittleEndian, this.ProfileList[index].Data)
		if err != nil {
			fmt.Printf("Write Error ..%v\n", err)
		}
		this.ProfileList[index].Start = start
		this.ProfileList[index].DataLen = len(this.ProfileList[index].Data)
		this.ProfileList[index].InMomory = false
		this.ProfileList[index].Data = nil
		start += int64(this.ProfileList[index].DataLen)
	}

	fout.Write(buf.Bytes())
	u.WriteToJson(this, fmt.Sprintf("./index/%v_pfl.json", this.Name))
	return nil

}

func (this *ByteProfile) ReadFromFile() error {

	return nil
}

func (this *ByteProfile) WriteToFileWithChan(wchan chan string) error {

	this.WriteToFile()
	wchan <- fmt.Sprintf("./index/%v_pfl.dat", this.Name)
	return nil
}
