package invert

import (
	"testing"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/tools"
	"fmt"
	"github.com/FalconEngine/store"
	"github.com/FalconEngine/message"
)

func Test_InvertWriter(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "iw.log")

	invertListStore := store.NewFalconFileStoreWriteService("./abc.ivt")
	dictStore := store.NewFalconFileStoreWriteService("./abc.dic")

	iw := NewStringInvertWriter("abc")

	iw.Put("abc",&message.DocId{DocID:0,Weight:10})
	iw.Put("abc",&message.DocId{DocID:2,Weight:12})
	iw.Put("abc",&message.DocId{DocID:3,Weight:13})
	iw.Put("a",&message.DocId{DocID:2,Weight:14})
	iw.Put("a",&message.DocId{DocID:9,Weight:19})

	iw.Put("b",&message.DocId{DocID:4,Weight:14})
	iw.Put("b",&message.DocId{DocID:9,Weight:19})

	iw.Store(invertListStore,dictStore)
	invertListStore.Close()
	dictStore.Close()
	mlog.Info(" %s ",iw.ToString())

}

func Test_InvertReader(t *testing.T) {

	invertListStore := store.NewFalconFileStoreReadService("./abc.ivt")
	dictStore := store.NewFalconFileStoreReadService("./abc.dic")

	ir := NewStringInvertReader("abc",0,dictStore,invertListStore)
	fetch(ir,"a")
	fetch(ir,"abc")
	fetch(ir,"c")
	invertListStore.Close()
	dictStore.Close()

}


func Test_InvertInsert(t *testing.T) {

	invertListStore := store.NewFalconFileStoreWriteService("./ivt_insert.ivt")
	dictStore := store.NewFalconFileStoreWriteService("./ivt_insert.dic")

	iw := NewStringInvertWriter("ivt_insert")

	for i:=uint32(0);i<1000;i++{
		iw.Put(fmt.Sprintf("k%d",i),&message.DocId{DocID:i,Weight:i+10})
	}
	iw.Store(invertListStore,dictStore)
	invertListStore.Close()
	dictStore.Close()
	mlog.Info("%s",iw.ToString())

}

func fetch(ir FalconStringInvertReadService,key string) {
	doclist,found,err:=ir.Fetch(key)
	if err!=nil{
		mlog.Error("Key [ %s ] fatch error : %v",key ,err)
		return
	}
	if found {
		mlog.Info("Key [ %s ] >>> %s",key,doclist.ToString())
		return
	}
	mlog.Warning("Key [ %s ] not found",key)
}


func Test_InvertSetTest(t *testing.T) {

	invertSetService := NewInvertSet("segment",".")
	invertSetService.AddField("testfield1",tools.TFalconString)
	invertSetService.AddField("testfield2",tools.TFalconString)

	for i:=uint32(0);i<1000;i++{
		invertSetService.PutString("testfield1",fmt.Sprintf("k%d",i),&message.DocId{DocID:i,Weight:i+10})
		invertSetService.PutString("testfield2",fmt.Sprintf("key%d",i),&message.DocId{DocID:i,Weight:i+10})

	}

	invertSetService.Persistence()

	doclist,found,err:=invertSetService.FetchString("testfield2","key88")

	if err!=nil{
		mlog.Error("Test_InvertSetTest Key [ k88 ] fatch error : %v" ,err)
		return
	}
	if found {
		mlog.Info("Test_InvertSetTest Key [ k88 ] >>> %s",doclist.ToString())
		return
	}
	mlog.Warning("Test_InvertSetTest Key [ k88 ] not found")


}

func fetchField(field,key string,invertSetService FalconInvertSetService) error {
	doclist,found,err:=invertSetService.FetchString(field,key)

	if err!=nil{
		mlog.Error("Test_InvertSetTest Field [ %s ] Key [ %s ] fatch error : %v" ,field,key,err)
		return err
	}
	if found {
		mlog.Info("Test_InvertSetTest Field [ %s ] Key [ %s ] >>> %s",field,key,doclist.ToString())
		return nil
	}
	mlog.Warning("Test_InvertSetTest Field [ %s ] Key [ %s ] not found",field,key)
	return nil
}

func Test_InvertSetReadTest(t *testing.T) {

	invertSetService := NewInvertSet("segment",".")

	fetchField("testfield1","k876",invertSetService)
	fetchField("testfield2","key876",invertSetService)
	fetchField("testfield1","fdet",invertSetService)
	fetchField("fieldddd1","k876",invertSetService)

	invertSetService.Close()

	mlog.Info("set info : %s ",invertSetService.ToString())

}