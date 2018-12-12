package store

import (
	"github.com/FalconEngine/mlog"
	"os"
	"syscall"
	"github.com/FalconEngine/util"
	"fmt"
)

type FalconSearchFileMMapReader struct {
	name      string
	fullName  string
	fileStore *os.File
	mmapBytes []byte
	maxLength int64


}


func NewFalconSearchFileMMapReader(setting FalconSearchStoreSetting) *FalconSearchFileMMapReader {

	var err error
	fms := &FalconSearchFileMMapReader{mmapBytes: make([]byte, 0),
		name: setting.Name, fullName: setting.Location + "/" + setting.Name}

	fms.fileStore, err = os.OpenFile(fms.fullName, os.O_RDONLY, 0664)

	if err != nil {
		mlog.Error("open file [ %s ] error : %v", err)
		return nil
	}

	fi, err := fms.fileStore.Stat()
	if err != nil {
		mlog.Error(" file [ %s ] stat error : %v", err)
		return nil
	}
	fms.maxLength = fi.Size()
	fms.mmapBytes, err = syscall.Mmap(int(fms.fileStore.Fd()), 0,
		int(fi.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)

	if err != nil {
		mlog.Error("file [ %s ] mmap error : %v", err)
		return nil
	}

	return fms
}


func (fms *FalconSearchFileMMapReader) readByte(offset int64) (byte,error) {
	if offset>=fms.maxLength {
		return 'a',fmt.Errorf("error")
	}

	return fms.mmapBytes[offset],nil

}


func (fms *FalconSearchFileMMapReader) ReadUint64(offset int64) (uint64, error) {

	var x uint64
	var s uint
	for i := 0; ; i++ {
		b, err := fms.readByte(offset)
		if err != nil {
			return x, err
		}
		offset++
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, overflow
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

func (fms *FalconSearchFileMMapReader) ReadInt64(offset int64) (int64, error) {
	panic("implement me")
}

func (fms *FalconSearchFileMMapReader) ReadUVarInt(offset int64) (uint64, error) {
	panic("implement me")
}

func (fms *FalconSearchFileMMapReader) ReadVarInt(offset int64) (int64, error) {
	panic("implement me")
}

func (fms *FalconSearchFileMMapReader) SubReader(offset int64, lens int) (util.FalconReader, error) {
	panic("implement me")
}

func (fms *FalconSearchFileMMapReader) SubRandomReader(offset int64, lens int) (util.FalconRandomReader, error) {
	panic("implement me")
}

func (fms *FalconSearchFileMMapReader) Destroy() error {
	panic("implement me")
}