package dict

import (
	"testing"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/message"
	"fmt"
)

func Test_DictMap(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "dictmap.log")

	mapWriter := NewFalconWriteMap()
	mapWriter.Put("hello",&message.DictValue{Offset:100,Length:200})
	mapWriter.Put("hello2",&message.DictValue{Offset:200,Length:202})
	mapWriter.Put("hello3",&message.DictValue{Offset:300,Length:203})
	for i:=0;i<1000;i++{
		mapWriter.Put(fmt.Sprintf("hello%d",i),&message.DictValue{Offset:100,Length:200})
	}
	//mlog.Info("%s",mapWriter.ToString())
	mlog.Info("Write to file ...")
	fmapWriter := store.NewFalconFileStoreWriteService("./map.dic")
	mapWriter.Persistence(fmapWriter)
	fmapWriter.Close()


	fmapReader := store.NewFalconFileStoreReadService("./map.dic")
	mapReader := NewFalconReadMap()
	if err:=mapReader.LoadDic(fmapReader,0);err!=nil{
		mlog.Error("load error %v",err)
		return
	}
	fmapReader.Close()
	// mapReader.FalconDecoding(bytes)
	mlog.Info("%s",mapReader.ToString())

}


func Test_DictServiceMap(t *testing.T) {

}
