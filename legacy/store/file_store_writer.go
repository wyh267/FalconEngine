package store

import (
	"os"
	"github.com/FalconEngine/mlog"
	"encoding/binary"
)

type FalconSearchFileStoreWriter struct {

	fullName string
	fileStore *os.File
	name string


}


func NewFalconSearchFileStoreWriter(setting *FalconSearchStoreSetting) *FalconSearchFileStoreWriter {
	var err error
	fsw := &FalconSearchFileStoreWriter{fullName:setting.Location + "/" + setting.Name,name:setting.Name}
	fsw.fileStore, err = os.Create(fsw.fullName)
	if err != nil {
		mlog.Error("create %s error : %v", fsw.fullName, err)
		return nil
	}
	return fsw


}


func (fsw *FalconSearchFileStoreWriter) Write(p []byte) (n int, err error) {

	count, err := fsw.fileStore.Write(p)
	if err != nil || count != len(p) {
		mlog.Error("Write Error ... %v", err)
		return -1, err
	}
	return count,nil


}

func (fsw *FalconSearchFileStoreWriter) Close() error {
	return fsw.fileStore.Close()
}

func (*FalconSearchFileStoreWriter) WriteUint64(val uint64) error {
	return nil
}

func (fsw *FalconSearchFileStoreWriter) WriteInt64(val int64) error {
	return nil

}

func (fsw *FalconSearchFileStoreWriter) WriteUVarInt(val uint64) error {
	bytes := make([]byte,binary.MaxVarintLen64)
	n:=binary.PutUvarint(bytes,val)
	_,err:=fsw.Write(bytes[:n+1])
	return err


}

func (fsw *FalconSearchFileStoreWriter) WriteVarInt(val int64) error {
	bytes := make([]byte,binary.MaxVarintLen64)
	n:=binary.PutVarint(bytes,val)
	_,err:=fsw.Write(bytes[:n+1])
	return err

}

func (fsw *FalconSearchFileStoreWriter) Destroy() error {
	fsw.fileStore.Close()
	return os.RemoveAll(fsw.fullName)
}

func (fsw *FalconSearchFileStoreWriter) Sync() error {
	return fsw.fileStore.Sync()
}

func (fsw *FalconSearchFileStoreWriter) Name() string {
	return fsw.name

}




