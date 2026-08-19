package index

import (
	"github.com/FalconEngine/tools"
	"github.com/FalconEngine/index/segment"
	"sync"
	"encoding/json"
	"io/ioutil"
	"os"
	"github.com/FalconEngine/mlog"
	"time"
)

// 索引类
type Index struct {
	Name string
	Path string

	Mappings *tools.FalconIndexMappings


	segmentLocker *sync.RWMutex
	activeSegments []segment.FalconSegmentService
	persistenceSegmentChan chan segment.FalconSegmentService
	writeSegment segment.FalconSegmentService
	WriteSegmentNumber uint32
	SegmentNumbers []uint32


	documentLocker *sync.RWMutex
	persistenceSingle chan bool
}



func newIndex(name,path string) *Index{

	return &Index{Name:name,Path:path+"/"+name,Mappings:tools.NewFalconIndexMappings(),
	activeSegments:make([]segment.FalconSegmentService,0),persistenceSegmentChan:make(chan segment.FalconSegmentService,100),
		WriteSegmentNumber:0,SegmentNumbers:make([]uint32,0),
		segmentLocker:new(sync.RWMutex),
		documentLocker:new(sync.RWMutex),persistenceSingle:make(chan bool,1)}
}


func NewIndex(indexName,path string) FalconIndexService {
	idx := newIndex(indexName,path)

	os.MkdirAll(idx.Path,0777)
	// 载入索引
	idx.loadIndexMetaData()
	//mlog.Info("segments %v",idx.SegmentNumbers)
	for _,num := range idx.SegmentNumbers {
		activeSegment:=segment.LoadFalconSegment(num,indexName,idx.Path,idx.Mappings)
		idx.activeSegments = append(idx.activeSegments,activeSegment)
	}

	if len(idx.Mappings.GetMappings())>0{
		idx.writeSegment = segment.NewFalconSegment(idx.WriteSegmentNumber,indexName,idx.Path,idx.Mappings)
	}


	idx.startIndexProcessGoroutine()

	return idx
}


func (idx *Index) storeIndexMetaData() error {

	bMetaBytes,err:=json.Marshal(idx)
	if err!=nil{
		return err
	}
	return ioutil.WriteFile(idx.Path + "/" + idx.Name + ".mt",bMetaBytes,0777)

}


func (idx *Index) loadIndexMetaData() error {

	bMetaBytes,err:=ioutil.ReadFile(idx.Path + "/" + idx.Name + ".mt")
	if err!=nil{
		return err
	}

	return json.Unmarshal(bMetaBytes,idx)

}


func (idx *Index) CreateMappings(mappings *tools.FalconIndexMappings) error {


	if len(idx.Mappings.GetMappings()) == 0 {

		idx.Mappings = mappings
		idx.writeSegment = segment.NewFalconSegment(idx.WriteSegmentNumber,idx.Name,idx.Path,idx.Mappings)
		for _, fm := range idx.Mappings.GetMappings() {
			idx.writeSegment.AddField(fm)
		}
		return idx.storeIndexMetaData()
	}

	return nil

}

func (idx *Index) UpdateDocument(documentID string, document map[string]interface{}) error {

	idx.documentLocker.Lock()
	defer idx.documentLocker.Unlock()

	if err:=idx.writeSegment.UpdateDocument(document);err!=nil{
		mlog.Error("write document [ %s ] error : %v",documentID,err)
		return err
	}

	//select {
	//case <-idx.persistenceSingle:
	//	mlog.Info("persistence Segment ... ")
	//	persistenceSegment:=idx.writeSegment
	//	idx.persistenceSegmentChan <- persistenceSegment
	//	idx.WriteSegmentNumber++
	//	mlog.Info("make new write segment [ %d ] ...",idx.WriteSegmentNumber)
	//	idx.writeSegment = segment.NewFalconSegment(idx.WriteSegmentNumber,idx.Name,idx.Path,idx.Mappings)
	//	idx.storeIndexMetaData()
	//default:
	//
	//}




	return nil

}

func (idx *Index) DeleteDocument(documentID string) error {
	panic("implement me")
}


func (idx *Index) startIndexProcessGoroutine() error {


	go func() {
		mlog.Info("[ %s ] start merge Goroutine ...",idx.Name)
		for {

			time.Sleep(time.Second*5)
			mlog.Trace(" running index process goroutine ")
			select {
			case idx.persistenceSingle <- true:
				//mlog.Info("persistenceSingle....!!!")
			default:
			}
		}


	}()


	go func() {

		for {

			select {
			case writeSegment := <-idx.persistenceSegmentChan:
				mlog.Info("persistence segment [ %s ]",writeSegment.Name())
				writeSegment.Persistence()
				idx.segmentLocker.Lock()
				idx.activeSegments=append(idx.activeSegments,writeSegment)
				idx.SegmentNumbers = append(idx.SegmentNumbers,writeSegment.Number())
				idx.storeIndexMetaData()
				//mlog.Info("add to segments ...")
				idx.segmentLocker.Unlock()
			default:
				time.Sleep(time.Second)
			}

		}


	}()


	go func() {

		for single := range idx.persistenceSingle{
			idx.documentLocker.Lock()
			if idx.writeSegment.DocumentCount() == 0 {
				idx.documentLocker.Unlock()
				continue
			}
			mlog.Trace("persistence Segment Number [ %d ] ... %v ",idx.writeSegment.Number(),single)
			persistenceSegment:=idx.writeSegment
			idx.persistenceSegmentChan <- persistenceSegment
			idx.WriteSegmentNumber++
			mlog.Trace("make new write segment [ %d ] ...",idx.WriteSegmentNumber)
			idx.writeSegment = segment.NewFalconSegment(idx.WriteSegmentNumber,idx.Name,idx.Path,idx.Mappings)
			idx.storeIndexMetaData()
			idx.documentLocker.Unlock()

		}

	}()

	return nil


}