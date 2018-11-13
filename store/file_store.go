package store

import (
	"github.com/FalconEngine/message"
	"github.com/FalconEngine/mlog"
	"os"
	"encoding/binary"
	"bufio"
	"fmt"
	"io"
)

type FalconSearchFileStore struct {
	name   string
	storer *os.File
	reader *bufio.Reader
	end    int64
}

func NewFalconFileStoreWriteService(name string) FalconSearchStoreWriteService {
	sw := &FalconSearchFileStore{name: name, end: 0}
	var err error
	sw.storer, err = os.Create(name)
	if err != nil {
		mlog.Error("create %s error : %v", name, err)
		return nil
	}
	return sw
}

func NewFalconFileStoreReadService(name string) FalconSearchStoreReadService {
	sw := &FalconSearchFileStore{name: name}
	var err error
	sw.storer, err = os.Open(name)
	if err != nil {
		mlog.Error("open %s error : %v", name, err)
		return nil
	}
	sw.reader = bufio.NewReader(sw.storer)
	return sw
}

//func (sw *FalconSearchFileStore) ReadMessage(offset int64,decoder tools.FalconSearchDecoder) (int64,error) {
//	startValueByte := make([]byte,8)
//	if err:=sw.ReadFullBytesAt(offset,startValueByte);err!=nil{
//		return -1,err
//	}
//	readLens := int64(binary.LittleEndian.Uint64(startValueByte))
//	// readLens,err:=decoder.FalconPrepare(int64(startValue))
//	// if err!=nil{
//	// 	return -1,err
//	// }
//	readValue := make([]byte,readLens)
//	if err:=sw.ReadFullBytesAt(offset,readValue);err!=nil{
//
//		return -1,err
//	}
//	if err:=decoder.FalconDecoding(readValue);err!=nil{
//		return -1,err
//	}
//
//	return offset+8+readLens,nil
//
//}

func (sw *FalconSearchFileStore) ReadFullBytesAt(offset int64, details []byte) error {

	sw.storer.Seek(offset,0)
	//res,err := sw.reader.Peek(len(details))
	//mlog.Info("err %v",err)
	//copy(details,res)
	//return err
	//count:=0
	//for {
	//	rcount,err:= sw.reader.Read(details[count:])  //sw.storer.ReadAt(details,offset)
	//	if err != nil && err != io.EOF{panic(err)}
	//	mlog.Info("count : %d %d %d",count,rcount,len(details))
	//	if rcount==0 {return nil}
	//	count+=rcount
	//
	//	mlog.Info("count : %d  %d",count,len(details))
	//}

	count, err := sw.storer.ReadAt(details, offset) //sw.storer.ReadAt(details,offset)

	if err == nil && count == len(details) {
		return nil
	}
	if err == nil && count != len(details) {
		mlog.Error("Read  ... %v,%d,%d,%q", err, count, len(details), details)
		return fmt.Errorf("Read Bytes Length is wrong: %d ,need %d", count, len(details))
	}

	if err == io.EOF {
		mlog.Info("EOF")
		return io.EOF
	}

	if err != nil {
		mlog.Error("Read Error ... %v,%d,%d", err, count, len(details))
		return err
	}
	return nil
}

func (sw *FalconSearchFileStore) AppendBytes(details []byte) (int64, error) {

	count, err := sw.storer.Write(details)
	if err != nil || count != len(details) {
		mlog.Error("Write Error ... %v", err)
		return -1, err
	}
	ret := sw.end
	sw.end += int64(count)
	return ret, nil
}

//func (sw *FalconSearchFileStore) AppendMessage(encoder tools.FalconSearchEncoder) (int64, error) {
//
//	bytes,err:=encoder.FalconEncoding()
//	if err!=nil{
//		return -1,err
//	}
//	return sw.AppendBytes(bytes)
//
//}

func (sw *FalconSearchFileStore) GetStoreInfo() (*message.FalconSearchStoreInfo, error) {

	fi, err := sw.storer.Stat()
	if err != nil {
		return nil, err
	}

	return &message.FalconSearchStoreInfo{StoreLength: fi.Size()}, nil

}

func (sw *FalconSearchFileStore) Close() error {
	sw.storer.Sync()
	return sw.storer.Close()
}

func (sw *FalconSearchFileStore) Sync() error {
	return sw.storer.Sync()
}

func (sw *FalconSearchFileStore) Destroy() error {
	sw.Close()
	return os.RemoveAll(sw.name)
}


func (sw *FalconSearchFileStore) AppendUint64(val uint64) error{
	bytes := make([]byte,binary.MaxVarintLen64)
	n:=binary.PutUvarint(bytes,val)
	sw.AppendBytes(bytes[:n+1])
	return nil
}
func (sw *FalconSearchFileStore) AppendInt64(val int64) error{

	bytes := make([]byte,binary.MaxVarintLen64)
	n:=binary.PutVarint(bytes,val)
	sw.AppendBytes(bytes[:n+1])
	return nil}