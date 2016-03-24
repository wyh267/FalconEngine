/*****************************************************************************
 *  file name : index.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 索引的源代码，可以完成一次完备的查询
 *
******************************************************************************/

package FalconIndex

import (
	fis "FalconIndex/segment"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"tree"
	"utils"
)

type Index struct {
	Name          string                           `json:"name"`
	Pathname      string                           `json:"pathname"`
	Fields        map[string]utils.SimpleFieldInfo `json:"fields"`
	PrimaryKey    string                           `json:"primarykey"`
	StartDocId    uint32                           `json:"startdocid"`
	MaxDocId      uint32                           `json:"maxdocid"`
	PrefixSegment uint64                           `json:"prefixsegment"`
	SegmentNames  []string                         `json:"segmentnames"`

	segments      []*fis.Segment
	memorySegment *fis.Segment
	primary       *tree.BTreedb
	bitmap        *utils.Bitmap

	idxSegmentMutex *sync.Mutex   //段锁，当段序列化到磁盘或者段合并时使用或者新建段时使用
	Logger          *utils.Log4FE `json:"-"`
}

func NewEmptyIndex(name, pathname string, logger *utils.Log4FE) *Index {

	this := &Index{Name: name, Logger: logger, StartDocId: 0, MaxDocId: 0, PrefixSegment: 1000,
		SegmentNames: make([]string, 0), PrimaryKey: "", segments: make([]*fis.Segment, 0),
		memorySegment: nil, primary: nil, bitmap: nil, Pathname: pathname,
		Fields: make(map[string]utils.SimpleFieldInfo), idxSegmentMutex: new(sync.Mutex)}

	bitmapname := fmt.Sprintf("%v%v.bitmap", pathname, name)
	utils.MakeBitmapFile(bitmapname)
	this.bitmap = utils.NewBitmap(bitmapname)
	return this
}

func NewIndexWithLocalFile(name, pathname string, logger *utils.Log4FE) *Index {
	this := &Index{Name: name, Logger: logger, StartDocId: 0, MaxDocId: 0, PrefixSegment: 1000,
		SegmentNames: make([]string, 0), PrimaryKey: "", segments: make([]*fis.Segment, 0),
		memorySegment: nil, primary: nil, bitmap: nil, Pathname: pathname,
		Fields: make(map[string]utils.SimpleFieldInfo), idxSegmentMutex: new(sync.Mutex)}

	metaFileName := fmt.Sprintf("%v%v.meta", pathname, name)
	buffer, err := utils.ReadFromJson(metaFileName)
	if err != nil {
		return this
	}

	err = json.Unmarshal(buffer, &this)
	if err != nil {
		return this
	}

	for _, segmentname := range this.SegmentNames {
		segment := fis.NewSegmentWithLocalFile(segmentname, logger)
		this.segments = append(this.segments, segment)

	}

	//新建空的段
	segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
	var fields []utils.SimpleFieldInfo
	for _, f := range this.Fields {
		if f.FieldType != utils.IDX_TYPE_PK {
			fields = append(fields, f)
		}

	}
	this.memorySegment = fis.NewEmptySegmentWithFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
	this.PrefixSegment++

	//读取bitmap
	bitmapname := fmt.Sprintf("%v%v.bitmap", pathname, name)
	this.bitmap = utils.NewBitmap(bitmapname)

	if this.PrimaryKey != "" {
		primaryname := fmt.Sprintf("%v%v_primary.pk", this.Pathname, this.Name)
		this.primary = tree.NewBTDB(primaryname)
	}

	this.Logger.Info("[INFO] Load Index %v success", this.Name)
	return this

}

func (this *Index) AddField(field utils.SimpleFieldInfo) error {

	if _, ok := this.Fields[field.FieldName]; ok {
		this.Logger.Warn("[WARN] field %v Exist ", field.FieldName)
		return nil
	}

	this.Fields[field.FieldName] = field
	if field.FieldType == utils.IDX_TYPE_PK {
		this.PrimaryKey = field.FieldName
		primaryname := fmt.Sprintf("%v%v_primary.pk", this.Pathname, this.Name)
		this.primary = tree.NewBTDB(primaryname)
		this.primary.AddBTree(field.FieldName)
	} else {
		this.idxSegmentMutex.Lock()
		defer this.idxSegmentMutex.Unlock()

		if this.memorySegment == nil {
			segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
			var fields []utils.SimpleFieldInfo
			for _, f := range this.Fields {
				if f.FieldType != utils.IDX_TYPE_PK {
					fields = append(fields, f)
				}

			}
			this.memorySegment = fis.NewEmptySegmentWithFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
			this.PrefixSegment++

		} else if this.memorySegment.IsEmpty() {
			err := this.memorySegment.AddField(field)
			if err != nil {
				this.Logger.Error("[ERROR] Add Field Error  %v", err)
				return err
			}
		} else {
			tmpsegment := this.memorySegment
			if err := tmpsegment.Serialization(); err != nil {
				return err
			}
			this.segments = append(this.segments, tmpsegment)
			this.SegmentNames = make([]string, 0)
			for _, seg := range this.segments {
				this.SegmentNames = append(this.SegmentNames, seg.SegmentName)
			}

			segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
			var fields []utils.SimpleFieldInfo
			for _, f := range this.Fields {
				if f.FieldType != utils.IDX_TYPE_PK {
					fields = append(fields, f)
				}

			}
			this.memorySegment = fis.NewEmptySegmentWithFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
			this.PrefixSegment++

		}

	}
	return this.storeStruct()
}

func (this *Index) DeleteField(fieldname string) error {

	if _, ok := this.Fields[fieldname]; !ok {
		this.Logger.Warn("[WARN] field %v not found ", fieldname)
		return nil
	}

	if fieldname == this.PrimaryKey {
		this.Logger.Warn("[WARN] field %v is primary key can not delete ", fieldname)
		return nil
	}

	this.idxSegmentMutex.Lock()
	defer this.idxSegmentMutex.Unlock()

	if this.memorySegment == nil {
		this.memorySegment.DeleteField(fieldname)
		delete(this.Fields, fieldname)
		return this.storeStruct()
	}

	if this.memorySegment.IsEmpty() {
		this.memorySegment.DeleteField(fieldname)
		delete(this.Fields, fieldname)
		return this.storeStruct()
	}

	delete(this.Fields, fieldname)

	tmpsegment := this.memorySegment
	if err := tmpsegment.Serialization(); err != nil {
		return err
	}
	this.segments = append(this.segments, tmpsegment)
	this.SegmentNames = make([]string, 0)
	for _, seg := range this.segments {
		this.SegmentNames = append(this.SegmentNames, seg.SegmentName)
	}

	segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
	var fields []utils.SimpleFieldInfo
	for _, f := range this.Fields {
		if f.FieldType != utils.IDX_TYPE_PK {
			fields = append(fields, f)
		}

	}
	this.memorySegment = fis.NewEmptySegmentWithFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
	this.PrefixSegment++

	return this.storeStruct()

}

func (this *Index) storeStruct() error {
	metaFileName := fmt.Sprintf("%v%v.meta", this.Pathname, this.Name)
	if err := utils.WriteToJson(this, metaFileName); err != nil {
		return err
	}
	return nil

}

func (this *Index) UpdateDocument(content map[string]string) error {

	if len(this.Fields) == 0 {
		this.Logger.Error("[ERROR] No Field or Segment is nil")
		return errors.New("no field or segment is nil")
	}

	if this.memorySegment == nil {
		this.idxSegmentMutex.Lock()
		segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
		var fields []utils.SimpleFieldInfo
		for _, f := range this.Fields {
			if f.FieldType != utils.IDX_TYPE_PK {
				fields = append(fields, f)
			}

		}
		this.memorySegment = fis.NewEmptySegmentWithFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
		this.PrefixSegment++
		if err := this.storeStruct(); err != nil {
			this.idxSegmentMutex.Unlock()
			return err
		}
		this.idxSegmentMutex.Unlock()
	}

	docid := this.MaxDocId
	this.MaxDocId++

	//无主键的表直接添加

	if this.PrimaryKey == "" {
		return this.memorySegment.AddDocument(docid, content)
	}

	if _, hasPrimary := content[this.PrimaryKey]; !hasPrimary {
		this.Logger.Error("[ERROR] Primary Key Not Found %v", this.PrimaryKey)
		return errors.New("No Primary Key")
	}

	if err := this.updatePrimaryKey(content[this.PrimaryKey], docid); err != nil {
		return err
	}

	return this.memorySegment.AddDocument(docid, content)

}

func (this *Index) updatePrimaryKey(key string, docid uint32) error {

	err := this.primary.Set(this.PrimaryKey, key, uint64(docid))

	if err != nil {
		this.Logger.Error("[ERROR] update Put key error  %v", err)
		return err
	}

	return nil
}

func (this *Index) findPrimaryKey(key string) (utils.DocIdNode, bool) {

	ok, val := this.primary.Search(this.PrimaryKey, key)
	if !ok || val >= uint64(this.memorySegment.StartDocId) {
		return utils.DocIdNode{}, false
	}
	return utils.DocIdNode{Docid: uint32(val)}, true

}

func (this *Index) SyncMemorySegment() error {

	if this.memorySegment == nil {
		return nil
	}
	this.idxSegmentMutex.Lock()
	defer this.idxSegmentMutex.Unlock()

	if err := this.memorySegment.Serialization(); err != nil {
		this.Logger.Error("[ERROR] SyncMemorySegment Error %v", err)
		return err
	}
	segmentname := this.memorySegment.SegmentName
	this.memorySegment.Close()
	this.memorySegment = nil
	newSegment := fis.NewSegmentWithLocalFile(segmentname, this.Logger)
	this.segments = append(this.segments, newSegment)
	this.SegmentNames = append(this.SegmentNames, segmentname)

	return this.storeStruct()

}

func (this *Index) checkMerge() (int, int, bool) {
	var start int = -1
	var end int = -1
	docLens := make([]uint32, 0)
	for _, sg := range this.segments {
		docLens = append(docLens, sg.MaxDocId-sg.StartDocId)
	}

	return start, end, false

}

func (this *Index) MergeSegments() error {

	var startIdx int = -1
	this.idxSegmentMutex.Lock()
	defer this.idxSegmentMutex.Unlock()
	for idx := range this.segments {
		if this.segments[idx].MaxDocId-this.segments[idx].StartDocId < 1000000 {
			startIdx = idx
			break
		}
	}
	if startIdx == -1 {
		return nil
	}

	mergeSegments := this.segments[startIdx:]

	segmentname := fmt.Sprintf("%v%v_%v", this.Pathname, this.Name, this.PrefixSegment)
	var fields []utils.SimpleFieldInfo
	for _, f := range this.Fields {
		if f.FieldType != utils.IDX_TYPE_PK {
			fields = append(fields, f)
		}

	}
	tmpSegment := fis.NewEmptySegmentWithFieldsInfo(segmentname, mergeSegments[0].StartDocId, fields, this.Logger)
	this.PrefixSegment++
	if err := this.storeStruct(); err != nil {
		return err
	}
	tmpSegment.MergeSegments(mergeSegments)
	//tmpname:=tmpSegment.SegmentName
	tmpSegment.Close()
	tmpSegment = nil

	for _, sg := range mergeSegments {
		sg.Destroy()
	}

	tmpSegment = fis.NewSegmentWithLocalFile(segmentname, this.Logger)
	if startIdx > 0 {
		this.segments = this.segments[:startIdx]         //make([]*fis.Segment,0)
		this.SegmentNames = this.SegmentNames[:startIdx] //make([]string,0)
	} else {
		this.segments = make([]*fis.Segment, 0)
		this.SegmentNames = make([]string, 0)
	}

	this.segments = append(this.segments, tmpSegment)
	this.SegmentNames = append(this.SegmentNames, segmentname)
	return this.storeStruct()

}

func (this *Index) GetDocument(docid uint32) (map[string]string, bool) {

	for _, segment := range this.segments {
		if docid >= segment.StartDocId && docid < segment.MaxDocId {
			return segment.GetDocument(docid)
		}
	}
	return nil, false
}

func (this *Index) SearchUnitDocIds(querys []utils.FSSearchQuery, filteds []utils.FSSearchFilted) ([]utils.DocIdNode, bool) {

	docids := make([]utils.DocIdNode, 0)
	for _, segment := range this.segments {
		docids, _ = segment.SearchUnitDocIds(querys, filteds, this.bitmap, docids)
		//this.Logger.Info("[INFO] segment[%v] docids %v", segment.SegmentName, docids)
	}

	if len(docids) > 0 {
		return docids, true
	}

	return nil, false
}

func (this *Index) SimpleSearch(querys []utils.FSSearchQuery, filteds []utils.FSSearchFilted, ps, pg int) ([]map[string]string, bool) {

	docids := make([]utils.DocIdNode, 0)
	for _, segment := range this.segments {
		docids, _ = segment.SearchUnitDocIds(querys, filteds, this.bitmap, docids)
		//this.Logger.Info("[INFO] segment[%v] docids %v", segment.SegmentName, docids)
	}
	lens := len(docids)

	if ps*(pg-1) >= lens {
		return nil, false
	}

	start := ps * (pg - 1)
	end := ps * pg

	if end >= lens {
		end = lens
	}

	res := make([]map[string]string, 0)
	for _, docid := range docids[start:end] {
		val, ok := this.GetDocument(docid.Docid)
		if ok {
			res = append(res, val)
		}
	}

	return res, true

}
