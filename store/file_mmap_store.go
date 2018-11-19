package store

import (
	"os"
	"bufio"
	"syscall"
	"github.com/FalconEngine/message"
	"github.com/FalconEngine/mlog"
	"fmt"
)

type FalconSearchFileMMapStore struct {
	name   string
	storer *os.File
	reader *bufio.Reader
	end    int64

	mmapBytes []byte

}



func NewFalconSearchFileMMapStore(name string) FalconSearchStoreReadService {

	var err error
	fms := &FalconSearchFileMMapStore{mmapBytes: make([]byte, 0),name:name}


	fms.storer, err = os.OpenFile(name, os.O_RDONLY, 0664)

	if err != nil {
		mlog.Error("open file [ %s ] error : %v",err)
		return nil
	}

	fi, err := fms.storer.Stat()
	if err != nil {
		mlog.Error(" file [ %s ] stat error : %v",err)
		return nil
	}
	fms.end = fi.Size();
	fms.mmapBytes, err = syscall.Mmap(int(fms.storer.Fd()), 0,
		int(fi.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		mlog.Error("file [ %s ] mmap error : %v",err)
		return nil
	}

	return fms
}


func (fms *FalconSearchFileMMapStore) ReadFullBytes(offset int64,lens int64) ([]byte,error){

	if offset+lens <= fms.end {
		return fms.mmapBytes[offset : offset+lens],nil
	}
	return nil,fmt.Errorf("length is out of range")
}


func (fms *FalconSearchFileMMapStore) ReadFullBytesAt(offset int64, details []byte) error {

	lens := int64(len(details))
	if offset+lens <= fms.end {
		copy(details,fms.mmapBytes[offset : offset+lens])
		return nil
	}
	return fmt.Errorf("length is out of range")

}

func (fms *FalconSearchFileMMapStore) GetStoreInfo() (*message.FalconSearchStoreInfo, error) {

	return &message.FalconSearchStoreInfo{StoreName:fms.name,StoreLength:fms.end},nil

}

func (fms *FalconSearchFileMMapStore) Close() error {

	syscall.Munmap(fms.mmapBytes)
	return fms.storer.Close()

}

func (fms *FalconSearchFileMMapStore) Destroy() error {

	syscall.Munmap(fms.mmapBytes)
	fms.storer.Close()
	return os.RemoveAll(fms.name)

}