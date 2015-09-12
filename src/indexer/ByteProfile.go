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
	u "utils"
)

type ByteProfile struct {
	*Detail
	customeInter u.CustomInterface
}

func NewByteProfile(name string) *ByteProfile {
	profile_name := fmt.Sprintf("%v.pfl", name)
	detail := NewDetailWithName(profile_name)
	return &ByteProfile{Detail: detail}
}

func (this *ByteProfile) PutProfile(doc_id int64, value []byte) error {

	if this.IsSearch == true {

		return this.SetNewValueByte(doc_id, value)

	} else {

		return this.PutDocInfoByte(doc_id, value)
	}

}

func (this *ByteProfile) Put(doc_id int64, value interface{}) error {
	value_byte, ok := value.([]byte)

	if !ok {
		return errors.New("Wrong type..")
	}
	return this.PutProfile(doc_id, value_byte)
}

func (this *ByteProfile) Find(doc_id int64) (interface{}, error) {

	if doc_id > this.MaxDocId {
		return nil, errors.New("DocId Wrong")
	}

	if this.DetailList[doc_id].IsInc == true {
		return this.DetailList[doc_id].DetailBytes, nil
	}

	StartPos := int(this.DetailList[doc_id].ByteStart)
	EndPos := int(this.DetailList[doc_id].ByteLen) + StartPos
	this.DetailList[doc_id].DetailBytes = make([]byte, int(this.DetailList[doc_id].ByteLen))

	if this.DetailList[doc_id].InInc == 0 {
		copy(this.DetailList[doc_id].DetailBytes, this.detailMmap.Read(int64(StartPos), int64(EndPos)))
	} else {
		copy(this.DetailList[doc_id].DetailBytes, this.upMmap.Read(int64(StartPos), int64(EndPos)))
	}

	return this.DetailList[doc_id].DetailBytes, nil
}

func (this *ByteProfile) FilterValue(doc_ids []u.DocIdInfo, value []byte, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	return doc_ids, nil
}

func (this *ByteProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	if is_forward == true {
		if bytes_value, ok := value.([]byte); ok {
			return this.FilterValue(doc_ids, bytes_value, is_forward, filt_type)
		}
		return doc_ids, nil
	} else {

		return this.CustomFilterInterface(doc_ids, value)

	}

	return doc_ids, nil
}

func (this *ByteProfile) Display() {

}

func (this *ByteProfile) GetType() int64 {

	return PflByte
}

func (this *ByteProfile) GetMaxDocId() int64 {

	return this.MaxDocId
}

func (this *ByteProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	return doc_ids, nil
}

func (this *ByteProfile) CustomFilterInterface(doc_ids []u.DocIdInfo, value interface{}) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	for _, doc_id := range doc_ids {
		pfl_value, err := this.GetDocInfoByte(doc_id.DocId)
		if err != nil {
			continue
		}
		if this.customeInter.CustomeFunction(value, pfl_value) == true {
			res = append(res, doc_id)
		}
	}

	return res, nil
}

func (this *ByteProfile) WriteToFile() error {

	return this.WriteDetailToFile()
}

func (this *ByteProfile) ReadFromFile() error {

	this.IsSearch = true
	return this.ReadDetailFromFile()
}

func (this *ByteProfile) GetDocInfoByte(doc_id int64) ([]byte, error) {

	if doc_id > this.MaxDocId {
		return nil, errors.New("DocId Wrong")
	}

	if this.DetailList[doc_id].IsInc == true {
		return this.DetailList[doc_id].DetailBytes, nil
	}

	StartPos := int(this.DetailList[doc_id].ByteStart)
	EndPos := int(this.DetailList[doc_id].ByteLen) + StartPos
	this.DetailList[doc_id].DetailBytes = make([]byte, int(this.DetailList[doc_id].ByteLen))

	if this.DetailList[doc_id].InInc == 0 {
		copy(this.DetailList[doc_id].DetailBytes, this.detailMmap.Read(int64(StartPos), int64(EndPos)))
	} else {
		copy(this.DetailList[doc_id].DetailBytes, this.upMmap.Read(int64(StartPos), int64(EndPos)))
	}

	return this.DetailList[doc_id].DetailBytes, nil

}

func (this *ByteProfile) PutDocInfoByte(doc_id int64, info_byte []byte) error {

	if doc_id != this.MaxDocId+1 {
		return errors.New("DocID Wrong")
	}

	var detail_info DetailInfo
	detail_info.DetailBytes = info_byte
	detail_info.ByteLen = int64(len(info_byte))
	detail_info.ByteStart = this.Offset
	detail_info.IsInc = false
	detail_info.InInc = 0
	this.Offset += int64(len(info_byte))
	this.MaxDocId++
	this.DetailList = append(this.DetailList, detail_info)

	return nil
}

func (this *ByteProfile) SetNewValueByte(doc_id int64, binfo []byte) error {
	//只要是新增的，都需要写入up文件中
	info_start := this.upMmap.GetPointer()
	info_lens := int64(len(binfo))
	this.upMmap.AppendString(string(binfo))
	this.upMmap.WriteInt64(0, info_start+info_lens)

	if doc_id > this.MaxDocId {
		var detail_info DetailInfo
		detail_info.DetailBytes = binfo
		this.MaxDocId++
		this.DetailList = append(this.DetailList, detail_info)

		//新增一个doc_id,写入字典文件中
		this.dicMmap.WriteInt64(0, this.MaxDocId)
		this.dicMmap.AppendInt64(info_start)
		this.dicMmap.AppendInt64(info_lens)
		this.dicMmap.AppendInt64(1)
		//this.detailMmap.AppendString(string(binfo))

	} else {
		this.DetailList[int(doc_id)].DetailBytes = binfo
		//没有新增，需要定位到doc_id的位置上
		start_pos := 16 + (doc_id)*24
		this.dicMmap.WriteInt64(start_pos, info_start)
		start_pos += 8
		this.dicMmap.WriteInt64(start_pos, info_lens)
		start_pos += 8
		this.dicMmap.WriteInt64(start_pos, 1)
	}

	this.DetailList[doc_id].IsInc = true
	return nil
}

func (this *ByteProfile) ReadDetailFromFile() error {

	var err error
	file_name := fmt.Sprintf("./index/%v.dic", this.Name)
	fmt.Printf("Read File : %v \n", file_name)
	this.dicMmap, err = u.NewMmap(file_name, u.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return err
	}

	this.DetailList = make([]DetailInfo, 0)

	this.MaxDocId = this.dicMmap.ReadInt64(0)
	this.Offset = this.dicMmap.ReadInt64(8)
	var start int64 = 16
	var i int64 = 0
	for i = 0; i <= this.MaxDocId; i++ {
		start_pos := this.dicMmap.ReadInt64(start) //int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		byte_len := this.dicMmap.ReadInt64(start) //int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		in_inc := this.dicMmap.ReadInt64(start) //int64(binary.LittleEndian.Uint64(MmapBytes[start : start+8]))
		start += 8
		this.DetailList = append(this.DetailList, DetailInfo{nil, start_pos, byte_len, in_inc, false})
	}
	this.dicMmap.SetFileEnd(start)

	//mmap详细文件
	file_name = fmt.Sprintf("./index/%v.dat", this.Name)
	fmt.Printf("Read File : %v \n", file_name)
	this.detailMmap, err = u.NewMmap(file_name, u.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return err
	}

	//mmap增量文件
	file_name = fmt.Sprintf("./index/%v.up", this.Name)
	fmt.Printf("Read File : %v \n", file_name)
	this.upMmap, err = u.NewMmap(file_name, u.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return err
	}
	up_file_end := this.upMmap.ReadInt64(0)
	if up_file_end == 0 {
		this.upMmap.SetFileEnd(8)
	} else {
		this.upMmap.SetFileEnd(up_file_end)
	}

	return nil

}

func (this *ByteProfile) WriteDetailToFile() error {

	buf := new(bytes.Buffer)

	file_name := fmt.Sprintf("./index/%v.dat", this.Name)
	fmt.Printf("Write File : %v \n", file_name)
	fout, err := os.Create(file_name)
	if err != nil {
		fmt.Printf("Create Error %v\n", err)
		return err
	}
	defer fout.Close()

	file_name = fmt.Sprintf("./index/%v.dic", this.Name)
	fmt.Printf("Write File : %v \n", file_name)
	fdetail_dic_out, err := os.Create(file_name)
	defer fdetail_dic_out.Close()
	if err != nil {
		return err
	}

	buf_detail_dic := new(bytes.Buffer)
	err = binary.Write(buf_detail_dic, binary.LittleEndian, this.MaxDocId)
	if err != nil {
		fmt.Printf("MaxDocId ERROR :%v \n", err)
	}
	err = binary.Write(buf_detail_dic, binary.LittleEndian, this.Offset)
	if err != nil {
		fmt.Printf("Offset ERROR :%v \n", err)
	}

	var isInc int64 = 0
	for index, _ := range this.DetailList {

		err := binary.Write(buf, binary.LittleEndian, this.DetailList[index].DetailBytes)
		if err != nil {
			fmt.Printf("DetailBytes Error ..%v\n", err)
		}
		this.DetailList[index].DetailBytes = nil

		err = binary.Write(buf_detail_dic, binary.LittleEndian, this.DetailList[index].ByteStart)
		if err != nil {
			fmt.Printf("ByteStart Error ..%v\n", err)
		}

		err = binary.Write(buf_detail_dic, binary.LittleEndian, this.DetailList[index].ByteLen)
		if err != nil {
			fmt.Printf("ByteLen Error ..%v\n", err)
		}

		err = binary.Write(buf_detail_dic, binary.LittleEndian, isInc)
		if err != nil {
			fmt.Printf("ByteLen Error ..%v\n", err)
		}
	}

	fout.Write(buf.Bytes())
	fdetail_dic_out.Write(buf_detail_dic.Bytes())
	//utils.WriteToJson(this, "./index/detail.idx.json")

	this.WriteUpDetailFile()

	return nil

}

func (this *ByteProfile) SetCustomInterface(inter u.CustomInterface) error {

	this.customeInter = inter
	return nil
}
