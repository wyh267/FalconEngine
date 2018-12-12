//
//
//
//

/*

 */
package store

import (
	"github.com/FalconEngine/util"
)


// 存储setting接口
type FalconSearchStoreSetting struct {
	Location string
	Name string
	Type string
}


// 二进制写入
type FalconSearchStoreWriteService interface {
	util.FalconWriter
	Name() string
}

// 二进制读取
type FalconSearchStoreReadService interface {
	util.FalconRandomReader
}


func CreateFalconSearchStoreWriteService(setting *FalconSearchStoreSetting) FalconSearchStoreWriteService {

	switch setting.Type {
	case util.TFileStore:
		return NewFalconSearchFileStoreWriter(setting)
	default:
		return nil
	}

}


func NewFalconSearchStoreReadService(name string) FalconSearchStoreReadService {
	return NewFalconSearchFileMMapStore(name)
}