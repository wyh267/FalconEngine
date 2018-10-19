package index

import (
	"testing"
	"github.com/FalconEngine/mlog"
	"github.com/FalconEngine/tools"
	"fmt"
)

func Test_InvertWriter(t *testing.T) {

	mlog.Start(mlog.LevelInfo, "iw.log")

	iw := NewStringInvertWriter("abc",".")

	iw.Put("abc",&tools.DocId{DocID:0,Weight:10})
	iw.Put("abc",&tools.DocId{DocID:2,Weight:12})
	iw.Put("abc",&tools.DocId{DocID:3,Weight:13})
	iw.Put("a",&tools.DocId{DocID:2,Weight:14})
	iw.Put("a",&tools.DocId{DocID:9,Weight:19})

	iw.Put("b",&tools.DocId{DocID:4,Weight:14})
	iw.Put("b",&tools.DocId{DocID:9,Weight:19})

	iw.Store()
	mlog.Info(" %s ",iw.ToString())

}

func Test_InvertReader(t *testing.T) {

	ir := NewStringInvertReader("abc",".")
	fetch(ir,"a")
	fetch(ir,"abc")
	fetch(ir,"c")


}


func Test_InvertInsert(t *testing.T) {

	iw := NewStringInvertWriter("ivt_insert",".")

	for i:=uint32(0);i<1000;i++{
		iw.Put(fmt.Sprintf("k%d",i),&tools.DocId{DocID:i,Weight:i+10})
	}
	iw.Store()
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