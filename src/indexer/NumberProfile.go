/*****************************************************************************
 *  file name : NumberProfile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数字正排文件
 *
******************************************************************************/

package indexer

import (
	//"encoding/json"
	"errors"
	"fmt"
	"strconv"
	u "utils"
	"os"
	"bytes"
	"syscall"
	"encoding/binary"
)

type NumberProfile struct {
	*Profile
	ProfileList []int64
}

func NewNumberProfile(name string) *NumberProfile {
	profile := &Profile{name, 2, 1, false}
	this := &NumberProfile{profile, make([]int64, 1)}
	return this
}

func (this *NumberProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, v)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *NumberProfile) PutProfile(doc_id, value int64) error {
	//fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ] [ DOC_ID : %v ]============\n", this.Name, this.Len,doc_id)
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

func (this *NumberProfile) FindValue(doc_id int64) (int64, error) {
	if doc_id >= this.Len || doc_id < 1 {
		return 0, errors.New("docid is wrong")
	}

	return this.ProfileList[doc_id], nil

}

func (this *NumberProfile) FilterValue(doc_ids []u.DocIdInfo, value int64, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

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
	default:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] == value {
				res = append(res, doc_ids[i])
			}
		}
	}

	/*
		if is_forward == true {

			for i, _ := range doc_ids {
				if this.ProfileList[doc_ids[i].DocId] == value {
					res = append(res, doc_ids[i])
				}
			}

		} else {
			for i, _ := range doc_ids {
				if this.ProfileList[doc_ids[i].DocId] != value {
					res = append(res, doc_ids[i])
				}
			}
		}
	*/
	return res, nil
}

func (this *NumberProfile) Put(doc_id int64, value interface{}) error {
	value_num, ok := value.(int64)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_num)

}

func (this *NumberProfile) Find(doc_id int64) (interface{}, error) {

	return this.FindValue(doc_id)
}

func (this *NumberProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	if doc_ids == nil {
		return nil, nil
	}

	value_str, ok := value.(string)
	if ok {
		v, err := strconv.ParseInt(value_str, 0, 0)
		if err != nil {
			fmt.Printf("Error %v \n", value)
			return doc_ids, nil
		}
		return this.FilterValue(doc_ids, v, is_forward, filt_type)
	}

	value_num, ok := value.(int64)
	if ok {
		return this.FilterValue(doc_ids, value_num, is_forward, filt_type)
	}

	value_num_float, ok := value.(float64)
	if ok {
		return this.FilterValue(doc_ids, int64(value_num_float), is_forward, filt_type)
	}

	return doc_ids, nil

}

func (this *NumberProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	for i, _ := range doc_ids {
		if cf(value, this.ProfileList[doc_ids[i].DocId]) == r {
			res = append(res, doc_ids[i])
		}
	}

	return res, nil
}

func (this *NumberProfile) GetType() int64 {
	return this.Type
}

func (this *NumberProfile) WriteToFile() error {

	file_name := fmt.Sprintf("./index/%v_plf.plf", this.Name)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		return err
	}
	
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, this.Len)
	if err != nil {
			fmt.Printf("Lens ERROR :%v \n",err)
	}
	err = binary.Write(buf, binary.LittleEndian, this.Type)
	if err != nil {
			fmt.Printf("Type ERROR :%v \n",err)
	}
	//err = binary.Write(buf, binary.LittleEndian, int64(this.IsMmap))
	//if err != nil {
	//		fmt.Printf("IsMmap ERROR :%v \n",err)
	//}
	err = binary.Write(buf, binary.LittleEndian, int64(len(this.Name)))
	if err != nil {
		fmt.Printf("Write Name Lens Error :%v \n",err)
	}
	err = binary.Write(buf, binary.LittleEndian, []byte(this.Name))
	if err != nil {
		fmt.Printf("Write Name Error :%v \n",err)
	}
	for _,value := range this.ProfileList {
		err := binary.Write(buf, binary.LittleEndian, value)
		if err != nil {
			fmt.Printf("Write value Error :%v \n",err)
		}
	}
	fout.Write(buf.Bytes())
	return nil
	
}

func (this *NumberProfile) ReadFromFile() error {
	
	file_name := fmt.Sprintf("./index/%v_plf.plf",this.Name)
	f, err := os.Open(file_name)
	defer f.Close()
	if err != nil {
		return  err
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
	
	
	this.Len=int64(binary.LittleEndian.Uint64(MmapBytes[:8]))
	this.Type=int64(binary.LittleEndian.Uint64(MmapBytes[8:16]))
	//this.IsMmap=bool(binary.LittleEndian.Uint64(MmapBytes[16:24]))
	name_lens:=int64(binary.LittleEndian.Uint64(MmapBytes[16:24]))
	this.Name = string(MmapBytes[24:24+name_lens])
	var start int64 = 24+name_lens
	var i int64 = 0
	for i=1;i<this.Len;i++{
		value:=int64(binary.LittleEndian.Uint64(MmapBytes[start:start+8]))
		start+=8
		this.ProfileList=append(this.ProfileList,value)
	}
	

	return nil
}
