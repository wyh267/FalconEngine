//
//
//
//

/*

 */
package store

import (
	"github.com/FalconEngine/message"
)

// 二进制写入
type FalconSearchStoreWriteService interface {

	AppendBytes(details []byte) (int64, error)
	AppendUint64(val uint64) error
	AppendInt64(val int64) error


	GetStoreInfo() (*message.FalconSearchStoreInfo,error)
	Close() error
	Sync() error
	Destroy() error
}

// 二进制读取
type FalconSearchStoreReadService interface {


	ReadFullBytesAt(offset int64,details []byte) error

	ReadFullBytes(offset int64,lens int64) ([]byte,error)

	GetStoreInfo() (*message.FalconSearchStoreInfo,error)
	Close() error
	Destroy() error

}

func NewFalconSearchStoreReadService(name string) FalconSearchStoreReadService {
	return NewFalconSearchFileMMapStore(name)
}