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
	//"bytes"
	//"encoding/binary"
	"errors"
	"fmt"
	//"os"
	//"strconv"
	//"strings"
	//"syscall"
	u "utils"
)


//type ByteProfile Detail



func NewByteProfile(name string) *Detail {
	profile_name:=fmt.Sprintf("%v.pfl",name)
	this := NewDetailWithName(profile_name)
	return this
}


func (this *Detail) PutProfile(doc_id int64, value []byte) error {
	
	if this.IsSearch == true {
		
		return this.SetNewValueByte(doc_id,value)
		
	}else{
		
		return this.PutDocInfoByte(doc_id,value)
	}

}



func (this *Detail) Put(doc_id int64, value interface{}) error{
	value_byte, ok := value.([]byte)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_byte)
}


func (this *Detail) Find(doc_id int64) (interface{}, error){
	

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
		copy(this.DetailList[doc_id].DetailBytes, this.detailMmap.Read(int64(StartPos),int64(EndPos)))
	}else{
		copy(this.DetailList[doc_id].DetailBytes, this.upMmap.Read(int64(StartPos),int64(EndPos)))
	}
	
	return this.DetailList[doc_id].DetailBytes,nil
}


func (this *Detail) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error){
	
	return doc_ids,nil
}


func (this *Detail) Display(){
	
	
}


func (this *Detail) GetType() int64{
	
	return PflByte
}


func (this *Detail) GetMaxDocId() int64{
	
	return this.MaxDocId
}


func (this *Detail) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error){
	
	return doc_ids,nil
}


func (this *Detail) WriteToFile() error{
	
	return this.WriteDetailToFile()
}


func (this *Detail) ReadFromFile() error{
	
	this.IsSearch = true 
	return this.ReadDetailFromFile()
}


func (this *Detail) PutDocInfoByte(doc_id int64, info_byte []byte) error {
	
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

func (this *Detail) SetNewValueByte(doc_id int64, binfo []byte) error {
	//只要是新增的，都需要写入up文件中
	info_start:=this.upMmap.GetPointer()
	info_lens:=int64(len(binfo))
	this.upMmap.AppendString(string(binfo))
	this.upMmap.WriteInt64(0,info_start+info_lens)

	if doc_id > this.MaxDocId {
		var detail_info DetailInfo
		detail_info.DetailBytes = binfo
		this.MaxDocId++
		this.DetailList = append(this.DetailList, detail_info)
		
		
		
		//新增一个doc_id,写入字典文件中
		this.dicMmap.WriteInt64(0,this.MaxDocId)
		this.dicMmap.AppendInt64(info_start)
		this.dicMmap.AppendInt64(info_lens)
		this.dicMmap.AppendInt64(1)
		//this.detailMmap.AppendString(string(binfo))
		

	} else {
		this.DetailList[int(doc_id)].DetailBytes = binfo
		//没有新增，需要定位到doc_id的位置上
		start_pos:=16+(doc_id)*24
		this.dicMmap.WriteInt64(start_pos,info_start)
		start_pos+=8
		this.dicMmap.WriteInt64(start_pos,info_lens)
		start_pos+=8
		this.dicMmap.WriteInt64(start_pos,1)
	}

	this.DetailList[doc_id].IsInc = true
	return nil
}


/*
type ByteNode struct {
	Data     []byte
	ByteStart   int64
	ByteLen     int64
	InInc		int64
	IsInc       bool
}

type ByteProfile struct {
	*Profile
	Offset		  int64
	ProfileList   []ByteNode
	posMmap		  *u.Mmap
	profileMmap	  *u.Mmap
	profileUpMmap *u.Mmap
}

func NewByteProfile(name string) *ByteProfile {
	profile := &Profile{Name:name, Type:PflByte, Len:1, IsMmap:false,IsSearch:false}
	this := &ByteProfile{Profile:profile, ProfileList:make([]ByteNode, 1),posMmap:nil,profileMmap:nil,profileUpMmap:nil,Offset:0}
	return this
}

func (this *ByteProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, string(v.Data))
	}
	fmt.Printf(" ================================================= \n")
}

func (this *ByteProfile) PutProfile(doc_id int64, value []byte) error {


	var byte_node ByteNode
	byte_node.Data = value
	byte_node.ByteStart = 0
	byte_node.ByteLen = int64(len(value))
	byte_node.IsInc = true

	
	
	
	
	if doc_id > this.Len || doc_id < 1 {
		fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ] [ DOC_ID : %v ]============\n", this.Name, this.Len,doc_id)
		return errors.New("docid is wrong")
	}

	var byte_node ByteNode
	byte_node.Data = value
	byte_node.ByteLen = int64(len(value))
	


	if doc_id == this.Len {
		this.ProfileList = append(this.ProfileList, byte_node)
		this.Len++
		
		if this.IsSearch == true {
			
			this.posMmap.WriteInt64(0,this.Len)
			this.profileUpMmap.AppendBytes(value)
			
			
		}else{
			
			byte_node.ByteStart = this.Offset
			byte_node.IsInc = false
			byte_node.InInc = 0
			this.Offset += int64(len(value))
			
			
		}
		return nil
	}

	this.ProfileList[doc_id] = byte_node
	return nil




	return nil

}

func (this *ByteProfile) FindValue(doc_id int64) ([]byte, error) {
	if doc_id >= this.Len || doc_id < 1 {

		return nil, errors.New("docid is wrong")
	}

	if this.ProfileList[doc_id].IsInc == true {
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

	StartPos := this.ProfileList[doc_id].ByteStart
	EndPos := this.ProfileList[doc_id].ByteLen + StartPos
	this.ProfileList[doc_id].Data = make([]byte, this.ProfileList[doc_id].ByteLen)
	copy(this.ProfileList[doc_id].Data, MmapBytes[StartPos:EndPos])
	//fmt.Printf("Cost Time : %v \n",functime("MmapBytes"))

	this.ProfileList[doc_id].IsInc = true
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


	fout, err := os.Create(fmt.Sprintf("./index/%v.pfl.dat",this.Name))
	if err != nil {
		fmt.Printf("Create Error %v\n", err)
		return err
	}
	defer fout.Close()
	
	file_name := fmt.Sprintf("./index/%v.pfl.pos",this.Name)
	profile_pos_out, err := os.Create(file_name)
	defer profile_pos_out.Close()
	if err != nil {
		return err
	}
	
	buf_profile_pos := new(bytes.Buffer)
	err = binary.Write(buf_profile_pos, binary.LittleEndian, this.Len)
	if err != nil {
		fmt.Printf("Len ERROR :%v \n", err)
	}
	err = binary.Write(buf_profile_pos, binary.LittleEndian, this.Type)
	if err != nil {
		fmt.Printf("Type ERROR :%v \n", err)
	}
	
	var isInc int64 = 0
	for index, _ := range this.ProfileList {

		err := binary.Write(buf, binary.LittleEndian, this.ProfileList[index].Data)
		if err != nil {
			fmt.Printf("Data Error ..%v\n", err)
		}
		this.ProfileList[index].Data = nil
		
		err = binary.Write(buf_profile_pos, binary.LittleEndian, this.ProfileList[index].ByteStart)
		if err != nil {
			fmt.Printf("ByteStart Error ..%v\n", err)
		}
		
		err = binary.Write(buf_profile_pos, binary.LittleEndian, this.ProfileList[index].ByteLen)
		if err != nil {
			fmt.Printf("ByteLen Error ..%v\n", err)
		}
		
		err = binary.Write(buf_profile_pos, binary.LittleEndian, isInc)
		if err != nil {
			fmt.Printf("ByteLen Error ..%v\n", err)
		}
	}

	fout.Write(buf.Bytes())
	profile_pos_out.Write(buf_profile_pos.Bytes())
	//utils.WriteToJson(this, "./index/detail.idx.json")
	
	this.WriteUpProfileFile()
	
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


func (this *ByteProfile) WriteUpProfileFile() error {
	
	file_name := fmt.Sprintf("./index/%v.pfl.up",this.Name)
	fout, err := os.Create(file_name)
	defer fout.Close()
	if err != nil {
		//fmt.Printf("Create %v\n",file_name)
		return err
	}
	err=syscall.Ftruncate(int(fout.Fd()),u.APPEND_DATA)
	if err != nil {
		fmt.Printf("ftruncate error : %v\n",err)
		return err
	}
	
	return nil
	
}


*/