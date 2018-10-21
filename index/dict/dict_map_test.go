package dict

import (
	"testing"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/message"
)

func Test_DictMap(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "dictmap.log")

	mapWriter := NewFalconWriteMap()
	mapWriter.Put("hello",&message.DictValue{Val:100,ExtVal:200})
	mapWriter.Put("hello2",&message.DictValue{Val:200,ExtVal:202})
	mapWriter.Put("hello3",&message.DictValue{Val:300,ExtVal:203})
	mlog.Info("%s",mapWriter.ToString())
	mlog.Info("Write to file ...")
	fmapWriter := store.NewFalconFileStoreWriteService("./map.dic")
	mapWriter.WriteDic(fmapWriter)
	fmapWriter.Close()


	fmapReader := store.NewFalconFileStoreReadService("./map.dic")
	mapReader := NewFalconReadMap()
	if _,err:=fmapReader.ReadMessage(0,mapReader);err!=nil{
		mlog.Error("err : %v",err)
	}
	fmapReader.Close()
	// mapReader.FalconDecoding(bytes)
	mlog.Info("%s",mapReader.ToString())

}


func Test_DictServiceMap(t *testing.T) {

}
