package index

import (
	"encoding/binary"
	"fmt"
	"github.com/FalconEngine/tools"
	"strings"
	"github.com/FalconEngine/mlog"
)

type MemoryFalconDocList struct {
	docList []*tools.DocId
	length  uint32
}

func NewMemoryFalconDocList() FalconDocList {
	return &MemoryFalconDocList{docList: make([]*tools.DocId, 0), length: 0}
}

func (mfd *MemoryFalconDocList) GetLength() int {

	return int(mfd.length)

}

func (mfd *MemoryFalconDocList) GetDoc(idx int) (*tools.DocId, error) {
	if idx >= len(mfd.docList) || idx < 0 {
		return nil, fmt.Errorf("Outof ...")
	}

	return mfd.docList[idx], nil

}

func (mfd *MemoryFalconDocList) Push(docid *tools.DocId) error {

	if mfd.length>0 && docid.DocID <= mfd.docList[mfd.length-1].DocID {
		mlog.Error("Doc Id [ %d ] is wrong,max id is : [ %d ]", docid.DocID, mfd.docList[mfd.length-1].DocID)
		return fmt.Errorf("Doc Id [ %d ] is wrong,max id is : [ %d ]", docid.DocID, mfd.docList[mfd.length-1].DocID)
	}

	mfd.docList = append(mfd.docList, docid)
	mfd.length++
	return nil

}

func (mfd *MemoryFalconDocList) FalconEncoding() ([]byte, error) {

	lens := mfd.length*8 + 8
	bytes := make([]byte, lens)
	binary.LittleEndian.PutUint64(bytes[:8], uint64(len(bytes)))
	pos := 8
	for _, docid := range mfd.docList {
		binary.LittleEndian.PutUint32(bytes[pos:pos+4], docid.DocID)
		binary.LittleEndian.PutUint32(bytes[pos+4:pos+8], docid.Weight)
		pos += 8
	}
	return bytes, nil
}

func (mfd *MemoryFalconDocList) FalconDecoding(bytes []byte) error {

	mfd.docList = make([]*tools.DocId, 0)
	for pos := 8; pos < len(bytes); pos += 8 {
		docid := &tools.DocId{DocID: binary.LittleEndian.Uint32(bytes[pos : pos+4]), Weight: binary.LittleEndian.Uint32(bytes[pos+4 : pos+8])}
		mfd.docList = append(mfd.docList, docid)
	}
	mfd.length = uint32(len(mfd.docList))

	return nil

}

func (mfd *MemoryFalconDocList) ToString() string {

	result :=  fmt.Sprintf(" Doc List [ %d ]: [ ",mfd.length)
	max := 10
	if mfd.length < 10 {
		max = int(mfd.length)
	}


	docStrings := make([]string,0)
	for i:=0;i<max;i++{
		docStrings =append(docStrings, mfd.docList[i].ToString())
	}
	result += strings.Join(docStrings,",")
	result += " ]"
	return  result
}
