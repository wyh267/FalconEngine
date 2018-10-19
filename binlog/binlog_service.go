package binlog

import (
	"github.com/FalconEngine/store"
	"sync"
)

type FalconBinlogService interface {
	AppendLog(logMessage *message.BinlogMessage) (int64,error)
}


type FalconBinlog struct {
	storeWriteService store.FalconSearchStoreWriteService
	logId int64
	locker *sync.RWMutex
}



func NewFalconBinlog() FalconBinlogService {

	fb := &FalconBinlog{logId:0,locker:new(sync.RWMutex)}
	fb.storeWriteService = store.NewFalconFileStoreWriteService("./bin.log")
	return fb
}


func (fb *FalconBinlog) AppendLog(logMessage *message.BinlogMessage) (int64, error) {
	fb.locker.Lock()
	defer fb.locker.Unlock()
	logMessage.LogId = fb.logId
	fb.logId++
	fb.storeWriteService.AppendMessage(logMessage)
	return fb.logId,nil
}

