/*****************************************************************************
 *  file name : TextProfile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 文本正牌索引
 *
******************************************************************************/

package indexer

import (
	//"encoding/json"
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

type TextProfile struct {
	*Profile
	ProfileList []string
}

func NewTextProfile(name string) *TextProfile {
	profile := &Profile{name, 1, 1, false}
	this := &TextProfile{profile, make([]string, 1)}
	return this
}

func (this *TextProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, v)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *TextProfile) PutProfile(doc_id int64, value string) error {
	if doc_id > this.Len || doc_id < 1 {
		return errors.New("docid is wrong")
	}

	if doc_id == this.Len {
		this.ProfileList = append(this.ProfileList, value)
		this.Len++
		return nil
	}

	this.ProfileList[doc_id] = value
	return nil

}

func (this *TextProfile) FindValue(doc_id int64) (string, error) {
	if doc_id >= this.Len || doc_id < 1 {
		return "", errors.New("docid is wrong")
	}

	return this.ProfileList[doc_id], nil

}

func (this *TextProfile) FilterValue(doc_ids []u.DocIdInfo, value string, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	switch filt_type {
	case FILT_TYPE_LESS:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] < value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_ABOVE:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] > value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_EQUAL:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] == value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_UNEQUAL:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] != value {
				res = append(res, doc_ids[i])
			}
		}

	case FILT_TYPE_LESS_DATERANGE:
		fmt.Printf("FILT_TYPE_LESS_DATERANGE\n")
		values := strings.Split(value, ",")
		value_num, _ := strconv.ParseInt(values[0], 0, 0)
		index_start, _ := strconv.ParseInt(values[1], 0, 0)
		index_end, _ := strconv.ParseInt(values[2], 0, 0)
		for i, _ := range doc_ids {
			items := strings.Split(this.ProfileList[doc_ids[i].DocId], ",")

			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data
			}
			if total < value_num {
				res = append(res, doc_ids[i])
			}

		}
	case FILT_TYPE_ABOVE_DATERANGE:
		values := strings.Split(value, ",")
		value_num, _ := strconv.ParseInt(values[0], 0, 0)
		index_start, _ := strconv.ParseInt(values[1], 0, 0)
		index_end, _ := strconv.ParseInt(values[2], 0, 0)
		for i, _ := range doc_ids {
			items := strings.Split(this.ProfileList[doc_ids[i].DocId], ",")

			var total int64 = 0
			for pos := int(index_start); pos < int(index_end); pos++ {
				data, _ := strconv.ParseInt(items[pos], 0, 0)
				total += data
			}
			if total > value_num {
				res = append(res, doc_ids[i])
			}
		}

	case FILT_TYPE_INCLUDE:
		for i, _ := range doc_ids {
			if strings.Contains(this.ProfileList[doc_ids[i].DocId], value) {
				res = append(res, doc_ids[i])
			}

		}

	default:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] == value {
				res = append(res, doc_ids[i])
			}
		}
	}

	return res, nil
}

func (this *TextProfile) Put(doc_id int64, value interface{}) error {
	value_str, ok := value.(string)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_str)

}

func (this *TextProfile) Find(doc_id int64) (interface{}, error) {

	return this.FindValue(doc_id)
}

func (this *TextProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	if doc_ids == nil {
		return nil, nil
	}

	value_str, ok := value.(string)
	if !ok {
		return doc_ids, nil
	}

	return this.FilterValue(doc_ids, value_str, is_forward, filt_type)

}

func (this *TextProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	for i, _ := range doc_ids {
		if cf(value, this.ProfileList[doc_ids[i].DocId]) == r {
			res = append(res, doc_ids[i])
		}
	}

	return res, nil

}

func (this *TextProfile) GetType() int64 {
	return this.Type
}

func (this *TextProfile) WriteToFile() error {

	file_name := fmt.Sprintf("./index/%v_plf.plf", this.Name)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, this.Len)
	if err != nil {
		fmt.Printf("Lens ERROR :%v \n", err)
	}
	err = binary.Write(buf, binary.LittleEndian, this.Type)
	if err != nil {
		fmt.Printf("Type ERROR :%v \n", err)
	}
	err = binary.Write(buf, binary.LittleEndian, int64(len(this.Name)))
	if err != nil {
		fmt.Printf("Write Name Lens Error :%v \n", err)
	}
	err = binary.Write(buf, binary.LittleEndian, []byte(this.Name))
	if err != nil {
		fmt.Printf("Write Name Error :%v \n", err)
	}
	for _, value := range this.ProfileList {
		err := binary.Write(buf, binary.LittleEndian, int64(len(value)))
		if err != nil {
			fmt.Printf("Write value Error :%v \n", err)
		}
		err = binary.Write(buf, binary.LittleEndian, []byte(value))
		if err != nil {
			fmt.Printf("Write value Error :%v \n", err)
		}
	}
	fout.Write(buf.Bytes())
	return nil

}

func (this *TextProfile) ReadFromFile() error {

	file_name := fmt.Sprintf("./index/%v_plf.plf", this.Name)
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

	this.Len = int64(binary.LittleEndian.Uint64(MmapBytes[:8]))
	this.Type = int64(binary.LittleEndian.Uint64(MmapBytes[8:16]))
	name_lens := int64(binary.LittleEndian.Uint64(MmapBytes[16:24]))
	this.Name = string(MmapBytes[24 : 24+name_lens])
	var start int64 = 24 + name_lens
	var i int64 = 0
	for i = 1; i < this.Len; i++ {
		value_lens := int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		value := string(MmapBytes[start : start+value_lens])
		start += value_lens
		this.ProfileList = append(this.ProfileList, value)
	}

	return nil
}
