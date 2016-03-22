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
	"fmt"
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

	Logger *utils.Log4FE `json:"-"`
}

func NewEmptyIndex(name, pathname string, logger *utils.Log4FE) *Index {

	this := &Index{Name: name, Logger: logger, StartDocId: 0, MaxDocId: 0, PrefixSegment: 1000,
		SegmentNames: make([]string, 0), PrimaryKey: "", segments: make([]*fis.Segment, 0),
		memorySegment: nil, primary: nil, bitmap: nil, Pathname: pathname,
		Fields: make(map[string]utils.SimpleFieldInfo)}

	bitmapname := fmt.Sprintf("%v%v.bitmap", pathname, name)
	utils.MakeBitmapFile(bitmapname)
	this.bitmap = utils.NewBitmap(bitmapname)
	return this
}

func NewIndexWithLocalFile(name, pathname string, logger *utils.Log4FE) *Index {
	this := &Index{Name: name, Logger: logger, StartDocId: 0, MaxDocId: 0, PrefixSegment: 1000,
		SegmentNames: make([]string, 0), PrimaryKey: "", segments: make([]*fis.Segment, 0),
		memorySegment: nil, primary: nil, bitmap: nil, Pathname: pathname,
		Fields: make(map[string]utils.SimpleFieldInfo)}

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
    
    this.Logger.Info("[INFO] Load Index %v success",this.Name)
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


