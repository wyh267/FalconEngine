/*****************************************************************************
 *  file name : segment.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 段的源代码，索引的基本单元，可以完成一次完整的查询
 *
******************************************************************************/

package segment

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"tree"
	"utils"
)

// FieldInfo description: 字段的描述信息
type FieldInfo struct {
	FieldName string `json:"fieldname"`
	FieldType uint64 `json:"fieldtype"`
	PflOffset int64  `json:"pfloffset"` //正排索引的偏移量
	PflLen    int    `json:"pfllen"`    //正排索引长度

}

// Segment description:段结构
type Segment struct {
	StartDocId uint32 `json:"startdocid"`
	MaxDocId   uint32 `json:"maxdocid"`
	//SegmentNumber uint32                         `json:"segmentnumber"`
	SegmentName string                           `json:"segmentname"`
	FieldInfos  map[string]utils.SimpleFieldInfo `json:"fields"`
	fields      map[string]*FSField
	isMemory    bool
	idxMmap     *utils.Mmap
	pflMmap     *utils.Mmap
	dtlMmap     *utils.Mmap
	btdb        *tree.BTreedb
	dict        *tree.BTreedb
	Logger      *utils.Log4FE `json:"-"`
}

// NewEmptySegmentWithFieldsInfo function description : 新建一个空的段，可以进行数据添加，包含字段信息
// params :
// return :
func NewEmptySegmentWithFieldsInfo(segmentname string, start uint32, fields []utils.SimpleFieldInfo, dict *tree.BTreedb, logger *utils.Log4FE) *Segment {

	this := &Segment{btdb: nil, StartDocId: start,
		MaxDocId: start, SegmentName: segmentname,
		idxMmap: nil, dtlMmap: nil, pflMmap: nil, fields: make(map[string]*FSField), dict: dict,
		Logger: logger, isMemory: true, FieldInfos: make(map[string]utils.SimpleFieldInfo)}

	for _, sfield := range fields {
		field := utils.SimpleFieldInfo{FieldName: sfield.FieldName, FieldType: sfield.FieldType}
		this.FieldInfos[field.FieldName] = field
		indexer := newEmptyField(sfield.FieldName, start, sfield.FieldType, dict, logger)
		this.fields[field.FieldName] = indexer

	}

	this.Logger.Info("[INFO] Segment --> NewEmptySegmentWithFieldsInfo :: [%v] Success ", segmentname)
	return this

}

// NewSegmentWithLocalFile function description : 从文件重建一个段
// params :
// return :
func NewSegmentWithLocalFile(segmentname string, dict *tree.BTreedb, logger *utils.Log4FE) *Segment {

	this := &Segment{btdb: nil, StartDocId: 0, MaxDocId: 0, SegmentName: segmentname,
		idxMmap: nil, dtlMmap: nil, pflMmap: nil, Logger: logger, fields: make(map[string]*FSField),
		FieldInfos: make(map[string]utils.SimpleFieldInfo), isMemory: false, dict: dict}

	metaFileName := fmt.Sprintf("%v.meta", segmentname)
	buffer, err := utils.ReadFromJson(metaFileName)
	if err != nil {
		return this
	}

	err = json.Unmarshal(buffer, &this)
	if err != nil {
		return this
	}

	btdbname := fmt.Sprintf("%v.bt", segmentname)
	if utils.Exist(btdbname) {
		this.Logger.Info("[INFO] Load B+Tree File : %v", btdbname)
		this.btdb = tree.NewBTDB(btdbname, logger)
	}

	this.idxMmap, err = utils.NewMmap(fmt.Sprintf("%v.idx", segmentname), utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	this.idxMmap.SetFileEnd(0)
	this.Logger.Info("[INFO] Load Invert File : %v.idx ", segmentname)

	this.pflMmap, err = utils.NewMmap(fmt.Sprintf("%v.pfl", segmentname), utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	this.pflMmap.SetFileEnd(0)
	this.Logger.Info("[INFO] Load Profile File : %v.pfl", segmentname)

	this.dtlMmap, err = utils.NewMmap(fmt.Sprintf("%v.dtl", segmentname), utils.MODE_APPEND)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	this.dtlMmap.SetFileEnd(0)
	this.Logger.Info("[INFO] Load Detail File : %v.dtl", segmentname)

	for _, field := range this.FieldInfos {
		if field.PflLen == 0 {
			indexer := newEmptyField(field.FieldName, this.StartDocId, field.FieldType, dict, logger)
			this.fields[field.FieldName] = indexer
			continue
		}
		indexer := newFieldWithLocalFile(field.FieldName, segmentname, this.StartDocId,
			this.MaxDocId, field.FieldType, field.PflOffset, field.PflLen,
			this.idxMmap, this.pflMmap, this.dtlMmap, false, this.btdb, dict, logger)
		this.fields[field.FieldName] = indexer
		//this.Logger.Info("[TRACE] %v", this.FieldInfos[field.FieldName])
	}

	return this

}

// AddField function description : 添加字段
// params :
// return :
func (this *Segment) AddField(sfield utils.SimpleFieldInfo) error {

	if _, ok := this.FieldInfos[sfield.FieldName]; ok {
		this.Logger.Warn("[WARN] Segment --> AddField Already has field [%v]", sfield.FieldName)
		return errors.New("Already has field..")
	}

	if this.isMemory && !this.IsEmpty() {
		this.Logger.Warn("[WARN] Segment --> AddField field [%v] fail..", sfield.FieldName)
		return errors.New("memory segment can not add field..")
	}

	indexer := newEmptyField(sfield.FieldName, this.MaxDocId, sfield.FieldType, this.dict, this.Logger)
	this.FieldInfos[sfield.FieldName] = sfield
	this.fields[sfield.FieldName] = indexer
	//if err := this.storeStruct(); err != nil {
	//	return err
	//}
	this.Logger.Info("[INFO] Segment --> AddField :: Success ")
	return nil
}

// DeleteField function description : 删除字段
// params :
// return :
func (this *Segment) DeleteField(fieldname string) error {

	if _, ok := this.FieldInfos[fieldname]; !ok {
		this.Logger.Warn("[WARN] Segment --> DeleteField not found field [%v]", fieldname)
		return errors.New("not found field")
	}

	if this.isMemory && !this.IsEmpty() {
		this.Logger.Warn("[WARN] Segment --> deleteField field [%v] fail..", fieldname)
		return errors.New("memory segment can not delete field..")
	}

	this.fields[fieldname].destroy()
	delete(this.FieldInfos, fieldname)
	delete(this.fields, fieldname)
	//if err := this.storeStruct(); err != nil {
	//	return err
	//}
	this.Logger.Info("[INFO] Segment --> DeleteField[%v] :: Success ", fieldname)
	// this.Fields[field.FieldName].Indexer=idf
	return nil
}

func (this *Segment) UpdateDocument(docid uint32, content map[string]string) error {
	if docid >= this.MaxDocId || docid < this.StartDocId {
		this.Logger.Error("[ERROR] Segment --> UpdateDocument :: Wrong DocId[%v]  MaxDocId[%v]", docid, this.MaxDocId)
		return errors.New("Segment --> UpdateDocument :: Wrong DocId Number")
	}

	for name, _ := range this.fields {
		if _, ok := content[name]; !ok {
			if err := this.fields[name].updateDocument(docid, ""); err != nil {
				this.Logger.Error("[ERROR] Segment --> UpdateDocument :: %v", err)
			}
			continue
		}

		if err := this.fields[name].updateDocument(docid, content[name]); err != nil {
			this.Logger.Error("[ERROR] Segment --> UpdateDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
		}

	}

	return nil
}

// AddDocument function description : 添加文档
// params :
// return :
func (this *Segment) AddDocument(docid uint32, content map[string]string) error {

	if docid != this.MaxDocId {
		this.Logger.Error("[ERROR] Segment --> AddDocument :: Wrong DocId[%v]  MaxDocId[%v]", docid, this.MaxDocId)
		return errors.New("Segment --> AddDocument :: Wrong DocId Number")
	}

	for name, _ := range this.fields {
		if _, ok := content[name]; !ok {
			if err := this.fields[name].addDocument(docid, ""); err != nil {
				this.Logger.Error("[ERROR] Segment --> AddDocument [%v] :: %v", this.SegmentName, err)
			}
			continue
		}

		if err := this.fields[name].addDocument(docid, content[name]); err != nil {
			this.Logger.Error("[ERROR] Segment --> AddDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
		}

	}

	this.MaxDocId++

	return nil

}

// Serialization function description : 序列化
// params :
// return :
func (this *Segment) Serialization() error {

	btdbname := fmt.Sprintf("%v.bt", this.SegmentName)
	if this.btdb == nil {
		this.btdb = tree.NewBTDB(btdbname, this.Logger)
	}

	for name, field := range this.FieldInfos {
		if err := this.fields[name].serialization(this.SegmentName, this.btdb); err != nil {
			this.Logger.Error("[ERROR] Segment --> Serialization %v", err)
			return err
		}
		field.PflOffset = this.fields[name].pflOffset
		field.PflLen = this.fields[name].pflLen
		this.FieldInfos[field.FieldName] = field
		this.Logger.Trace("[TRACE] %v %v %v", name, field.PflOffset, field.PflLen)
	}

	if err := this.storeStruct(); err != nil {
		return err
	}

	this.isMemory = false

	var err error
	this.idxMmap, err = utils.NewMmap(fmt.Sprintf("%v.idx", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.idxMmap.SetFileEnd(0)
	//this.Logger.Info("[INFO] Read Invert File : %v.idx ", this.SegmentName)

	this.pflMmap, err = utils.NewMmap(fmt.Sprintf("%v.pfl", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.pflMmap.SetFileEnd(0)

	this.dtlMmap, err = utils.NewMmap(fmt.Sprintf("%v.dtl", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.dtlMmap.SetFileEnd(0)
	//this.Logger.Info("[INFO] Read Invert File : %v.pfl", this.SegmentName)

	for name := range this.fields {
		this.fields[name].setMmap(this.idxMmap, this.pflMmap, this.dtlMmap)
	}
	this.Logger.Info("[INFO] Serialization Segment File : %v", this.SegmentName)
	return nil

}

func (this *Segment) storeStruct() error {
	metaFileName := fmt.Sprintf("%v.meta", this.SegmentName)
	if err := utils.WriteToJson(this, metaFileName); err != nil {
		return err
	}
	return nil
}

// Close function description : 销毁段
// params :
// return :
func (this *Segment) Close() error {

	for _, field := range this.fields {
		field.destroy()
	}

	if this.idxMmap != nil {
		this.idxMmap.Unmap()
	}

	if this.pflMmap != nil {
		this.pflMmap.Unmap()
	}

	if this.dtlMmap != nil {
		this.dtlMmap.Unmap()
	}

	if this.btdb != nil {
		this.btdb.Close()
	}

	return nil

}

// Destroy function description : 销毁段
// params :
// return :
func (this *Segment) Destroy() error {

	for _, field := range this.fields {
		field.destroy()
	}

	if this.idxMmap != nil {
		this.idxMmap.Unmap()
	}

	if this.pflMmap != nil {
		this.pflMmap.Unmap()
	}

	if this.dtlMmap != nil {
		this.dtlMmap.Unmap()
	}

	if this.btdb != nil {
		this.btdb.Close()
	}

	metaFileName := fmt.Sprintf("%v.meta", this.SegmentName)
	datFilename := fmt.Sprintf("%v.idx", this.SegmentName)
	posFilename := fmt.Sprintf("%v.pos", this.SegmentName)
	pflFilename := fmt.Sprintf("%v.pfl", this.SegmentName)
	dtlFilename := fmt.Sprintf("%v.dtl", this.SegmentName)
	btFilename := fmt.Sprintf("%v.bt", this.SegmentName)
	os.Remove(metaFileName)
	os.Remove(datFilename)
	os.Remove(pflFilename)
	os.Remove(posFilename)
	os.Remove(dtlFilename)
	os.Remove(btFilename)
	return nil

}

func (this *Segment) findField(key, field string, bitmap *utils.Bitmap) ([]utils.DocIdNode, bool) {
	if _, hasField := this.fields[field]; !hasField {
		this.Logger.Info("[INFO] Field %v not found", field)
		return nil, false
	}
	docids, match := this.fields[field].query(key)
	if !match {
		return nil, false
	}
	return docids, true

}

// Query function description : 查询接口
// params :
// return :
func (this *Segment) Query(fieldname string, key interface{}) ([]utils.DocIdNode, bool) {

	if _, hasField := this.fields[fieldname]; !hasField {
		this.Logger.Warn("[WARN] Field[%v] not found", fieldname)
		return nil, false
	}

	return this.fields[fieldname].query(key)

}

// Filter function description : 过滤
// params :
// return :
func (this *Segment) Filter(fieldname string, filtertype uint64, start, end int64, str string, docids []utils.DocIdNode) []utils.DocIdNode {

	if _, hasField := this.fields[fieldname]; !hasField {
		this.Logger.Warn("[WARN] Field[%v] not found", fieldname)
		return nil
	}

	if docids == nil || len(docids) == 0 {
		return nil
	}

	if !(uint32(docids[0].Docid) < this.MaxDocId &&
		uint32(docids[0].Docid) >= this.StartDocId &&
		uint32(docids[len(docids)-1].Docid) < this.MaxDocId &&
		uint32(docids[len(docids)-1].Docid) >= this.StartDocId) {
		return nil
	}

	var res []utils.DocIdNode

	for _, docid := range docids {

		if this.fields[fieldname].filter(docid.Docid, filtertype, start, end, str) {
			res = append(res, docid)
		}

	}
	return res

}

// getFieldValue function description : 获取详情，单个字段
// params :
// return :
func (this *Segment) GetFieldValue(docid uint32, fieldname string) (string, bool) {

	if docid < this.StartDocId || docid >= this.MaxDocId {
		return "", false
	}

	if _, ok := this.fields[fieldname]; !ok {
		return "", false
	}
	return this.fields[fieldname].getValue(docid)

}

// GetDocument function description : 获取详情，全部字段
// params :
// return :
func (this *Segment) GetDocument(docid uint32) (map[string]string, bool) {

	if docid < this.StartDocId || docid >= this.MaxDocId {
		return nil, false
	}

	res := make(map[string]string)

	for name, field := range this.fields {
		res[name], _ = field.getValue(docid)
	}

	return res, true

}

// GetValueWithFields function description : 获取详情，部分字段
// params :
// return :
func (this *Segment) GetValueWithFields(docid uint32, fields []string) (map[string]string, bool) {

	if fields == nil {
		return this.GetDocument(docid)
	}

	if docid < this.StartDocId || docid >= this.MaxDocId {
		return nil, false
	}
	flag := false

	res := make(map[string]string)
	for _, field := range fields {
		if _, ok := this.fields[field]; ok {
			res[field], _ = this.GetFieldValue(docid, field)
			flag = true
		} else {
			res[field] = ""
		}

	}

	return res, flag

}

func (this *Segment) SearchDocIds(query utils.FSSearchQuery,
	filteds []utils.FSSearchFilted,
	bitmap *utils.Bitmap,
	indocids []utils.DocIdNode) ([]utils.DocIdNode, bool) {

	start := len(indocids)
	//query查询
	if query.Value == "" {
		docids := make([]utils.DocIdNode, 0)
		for i := this.StartDocId; i < this.MaxDocId; i++ {
			docids = append(docids, utils.DocIdNode{Docid: i})
		}
		indocids = append(indocids, docids...)
	} else {
		docids, match := this.fields[query.FieldName].query(query.Value)
		// this.Logger.Info("[INFO] key[%v] len:%v",query.Value,len(docids))
		if !match {
			return indocids, false
		}

		indocids = append(indocids, docids...)
	}

	//bitmap去掉数据
	index := start
	if filteds == nil && bitmap != nil {
		for _, docid := range indocids[start:] {
			//去掉bitmap删除的
			if bitmap.GetBit(uint64(docid.Docid)) == 0 {
				indocids[index] = docid
				index++
			}
		}
		return indocids[:index], true
	}

	//过滤操作
	index = start
	for _, docidinfo := range indocids[start:] {
		match := true
		for _, filter := range filteds {
			if _, hasField := this.fields[filter.FieldName]; hasField {
				if (bitmap != nil && bitmap.GetBit(uint64(docidinfo.Docid)) == 1) ||
					(!this.fields[filter.FieldName].filter(docidinfo.Docid, filter.Type, filter.Start, filter.End, filter.MatchStr)) {
					match = false
					break
				}
				this.Logger.Debug("[DEBUG] SEGMENT[%v] QUERY  %v", this.SegmentName, docidinfo)
			} else {
				this.Logger.Error("[ERROR] SEGMENT[%v] FILTER FIELD[%v] NOT FOUND", this.SegmentName, filter.FieldName)
				return indocids[:start], true
			}
		}
		if match {
			indocids[index] = docidinfo
			index++
		}

	}

	return indocids[:index], true

}

// SearchUnitDocIds function description : 搜索的基本单元
// params :
// return :
func (this *Segment) SearchUnitDocIds(querys []utils.FSSearchQuery, filteds []utils.FSSearchFilted, bitmap *utils.Bitmap, indocids []utils.DocIdNode, maxdocid uint32) ([]utils.DocIdNode, bool) {

	start := len(indocids)
	flag := false
	var ok bool

	if len(querys) == 0 || querys == nil {
		docids := make([]utils.DocIdNode, 0)
		for i := this.StartDocId; i < this.MaxDocId; i++ {
			docids = append(docids, utils.DocIdNode{Docid: i})
		}
		indocids = append(indocids, docids...)
	} else {
		for _, query := range querys {
			if _, hasField := this.fields[query.FieldName]; !hasField {
				this.Logger.Info("[INFO] Field %v not found", query.FieldName)
				return indocids[:start], false
			}
			docids, match := this.fields[query.FieldName].query(query.Value)
			//  this.Logger.Info("[INFO] key[%v] len:%v",query.Value,len(docids))
			if !match {
				return indocids[:start], false
			}

			if !flag {
				flag = true
				/*if this.FieldInfos[query.FieldName].FieldType == utils.IDX_TYPE_STRING_SEG{

				    okdf,df := this.dict.Search(query.FieldName,query.Value)
				    if okdf {
				        indocids = utils.ComputeTfIdf(indocids,docids,int(df),maxdocid)
				    }else{
				        indocids = append(indocids, docids...)
				    }

				}else{*/
				indocids = append(indocids, docids...)
				//}

			} else {
				/*if this.FieldInfos[query.FieldName].FieldType == utils.IDX_TYPE_STRING_SEG{
				    okdf,df := this.dict.Search(query.FieldName,query.Value)
				    if okdf {
				        indocids, ok = utils.InteractionWithStartAndDf(indocids, docids, start,int(df),maxdocid)
				        if !ok {
				            return indocids[:start], false
				        }
				    }else{
				        indocids, ok = utils.InteractionWithStart(indocids, docids, start)
				        if !ok {
				            return indocids[:start], false
				        }
				    }
				}else{*/
				indocids, ok = utils.InteractionWithStart(indocids, docids, start)
				if !ok {
					return indocids[:start], false
				}
				//}

			}
		}

	}
	this.Logger.Info("[INFO] ResLen[%v] ", len(indocids))
	//bitmap去掉数据
	index := start

	if filteds == nil && bitmap != nil {
		for _, docid := range indocids[start:] {
			//去掉bitmap删除的
			if bitmap.GetBit(uint64(docid.Docid)) == 0 {
				indocids[index] = docid
				index++
			}
		}
		if index == start {
			return indocids[:start], false
		}
		return indocids[:index], true
	}

	//过滤操作
	index = start
	for _, docidinfo := range indocids[start:] {
		match := true
		for _, filter := range filteds {
			if _, hasField := this.fields[filter.FieldName]; hasField {
				if (bitmap != nil && bitmap.GetBit(uint64(docidinfo.Docid)) == 1) ||
					(!this.fields[filter.FieldName].filter(docidinfo.Docid, filter.Type, filter.Start, filter.End, filter.MatchStr)) {
					match = false
					break
				}
				this.Logger.Debug("[DEBUG] SEGMENT[%v] QUERY  %v", this.SegmentName, docidinfo)
			} else {
				this.Logger.Error("[ERROR] SEGMENT[%v] FILTER FIELD[%v] NOT FOUND", this.SegmentName, filter.FieldName)
				return indocids[:start], false
			}
		}
		if match {
			indocids[index] = docidinfo
			index++
		}

	}

	if index == start {
		return indocids[:start], false
	}
	return indocids[:index], true

}

func (this *Segment) IsEmpty() bool {
	return this.StartDocId == this.MaxDocId
}

func (this *Segment) MergeSegments(sgs []*Segment) error {

	this.Logger.Info("[INFO] Segment >>>>> MergeSegments [%v]", this.SegmentName)
	btdbname := fmt.Sprintf("%v.bt", this.SegmentName)
	if this.btdb == nil {
		this.btdb = tree.NewBTDB(btdbname, this.Logger)
	}

	for name, field := range this.FieldInfos {
		this.Logger.Info("[INFO] Merge Field[%v]", name)
		fs := make([]*FSField, 0)
		for _, sg := range sgs {
			if _, ok := sg.fields[name]; !ok {
				fakefield := newEmptyFakeField(this.fields[name].fieldName, sg.StartDocId,
					this.fields[name].fieldType,
					uint64(sg.MaxDocId-sg.StartDocId), nil, this.Logger)
				fs = append(fs, fakefield)
				continue
			}
			fs = append(fs, sg.fields[name])
		}
		this.fields[name].mergeField(fs, this.SegmentName, this.btdb)
		field.PflOffset = this.fields[name].pflOffset
		field.PflLen = this.fields[name].pflLen
		this.FieldInfos[name] = field

	}
	this.isMemory = false
	var err error
	this.idxMmap, err = utils.NewMmap(fmt.Sprintf("%v.idx", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.idxMmap.SetFileEnd(0)

	this.pflMmap, err = utils.NewMmap(fmt.Sprintf("%v.pfl", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.pflMmap.SetFileEnd(0)

	this.dtlMmap, err = utils.NewMmap(fmt.Sprintf("%v.dtl", this.SegmentName), utils.MODE_APPEND)
	if err != nil {
		this.Logger.Error("[ERROR] mmap error : %v \n", err)
	}
	this.dtlMmap.SetFileEnd(0)

	for name := range this.fields {
		this.fields[name].setMmap(this.idxMmap, this.pflMmap, this.dtlMmap)
	}
	this.Logger.Info("[INFO] MergeSegments Segment File : %v", this.SegmentName)
	this.MaxDocId = sgs[len(sgs)-1].MaxDocId

	return this.storeStruct()

}
