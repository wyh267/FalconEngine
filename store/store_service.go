//
//
//
//

/*

 */
package store

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/message"
)

// 二进制写入
type FalconSearchStoreWriteService interface {
	AppendBytes(details []byte) (int64, error)
	AppendMessage(encoder tools.FalconSearchEncoder) (int64, error)
	GetStoreInfo() (*message.FalconSearchStoreInfo,error)
	Close() error
	Sync() error
	Destroy() error
}

// 二进制读取
type FalconSearchStoreReadService interface {
	ReadMessage(offset int64,decoder tools.FalconSearchDecoder) (int64,error)
	ReadFullBytesAt(offset int64,details []byte) error
	GetStoreInfo() (*message.FalconSearchStoreInfo,error)
	Close() error
	Destroy() error

}