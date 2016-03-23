/*****************************************************************************
 *  file name : Field.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 字段的基本单元，一个field中可以包含一个倒排一个正排
 *
******************************************************************************/

package segment

import (
	"errors"
	"tree"
	"utils"
)

// FSField struct description : 字段的基本单元，这是对外的最基本单元
type FSField struct {
	fieldName  string
	startDocId uint32
	maxDocId   uint32
	fieldType  uint64
	isMomery   bool
	Logger     *utils.Log4FE `json:"-"` //logger
	ivt        *invert       //一个倒排接口
	pfl        *profile
	pflOffset  int64 //正排索引的偏移量
	pflLen     int   //正排索引长度
	btree      *tree.BTreedb
}


func newEmptyFakeField(fieldname string, start uint32, fieldtype uint64, docLen uint64, logger *utils.Log4FE) *FSField {
    this := &FSField{fieldName: fieldname, startDocId: start, maxDocId: start,
		fieldType: fieldtype, Logger: logger,
		isMomery: true, ivt: nil, pfl: nil, pflOffset: -1, pflLen: -1, btree: nil}

	
	this.pfl = newEmptyFakeProfile(fieldtype, 0, fieldname, start,docLen, logger)
	return this


}
// newEmptyField function description : 新建空字段
// params :
// return :
func newEmptyField(fieldname string, start uint32, fieldtype uint64, logger *utils.Log4FE) *FSField {

	this := &FSField{fieldName: fieldname, startDocId: start, maxDocId: start,
		fieldType: fieldtype, Logger: logger,
		isMomery: true, ivt: nil, pfl: nil, pflOffset: -1, pflLen: -1, btree: nil}

	if fieldtype == utils.IDX_TYPE_STRING ||
		fieldtype == utils.IDX_TYPE_STRING_SEG ||
		fieldtype == utils.IDX_TYPE_STRING_LIST ||
		fieldtype == utils.GATHER_TYPE {
		this.ivt = newEmptyInvert(fieldtype, start, fieldname, logger)
	}
	this.pfl = newEmptyProfile(fieldtype, 0, fieldname, start, logger)
	return this
}

// newFieldWithLocalFile function description : 从文件重建字段索引
// params :
// return :
func newFieldWithLocalFile(fieldname, segmentname string, start, max uint32,
	fieldtype uint64, pfloffset int64, pfllen int,
	idxMmap *utils.Mmap, pflMmap, dtlMmap *utils.Mmap, isMomery bool, btree *tree.BTreedb,
	logger *utils.Log4FE) *FSField {

	this := &FSField{fieldName: fieldname, startDocId: start, maxDocId: max,
		fieldType: fieldtype, Logger: logger,
		isMomery: isMomery, pflLen: pfllen, pflOffset: pfloffset,
		ivt: nil, pfl: nil, btree: btree}

	if fieldtype == utils.IDX_TYPE_STRING ||
		fieldtype == utils.IDX_TYPE_STRING_SEG ||
		fieldtype == utils.IDX_TYPE_STRING_LIST ||
		fieldtype == utils.GATHER_TYPE {
		this.ivt = newInvertWithLocalFile(btree, fieldtype, fieldname, segmentname,
			idxMmap, logger)
	}

	this.pfl = newProfileWithLocalFile(fieldtype, 0, segmentname, pflMmap, dtlMmap,
		pfloffset, uint64(pfllen), false, logger)

	return this
}

// addDocument function description : 增加一个doc文档
// params : docid docid的编号
//			contentstr string  文档内容
// return : error 成功返回Nil，否则返回相应的错误信息
func (this *FSField) addDocument(docid uint32, contentstr string) error {

	if docid != this.maxDocId || this.isMomery == false || this.pfl == nil {
		this.Logger.Error("[ERROR] FSField --> AddDocument :: Wrong docid %v this.maxDocId %v this.profile %v", docid, this.maxDocId, this.pfl)
		return errors.New("[ERROR] Wrong docid")
	}

	if err := this.pfl.addDocument(docid, contentstr); err != nil {
		this.Logger.Error("[ERROR] FSField --> AddDocument :: Add Document Error %v", err)
		return err
	}

	if this.fieldType != utils.IDX_TYPE_NUMBER &&
		this.fieldType != utils.IDX_TYPE_DATE &&
		this.ivt != nil {
		if err := this.ivt.addDocument(docid, contentstr); err != nil {
			this.Logger.Error("[ERROR] FSField --> AddDocument :: Add Invert Document Error %v", err)
			// return err
		}
	}

	this.maxDocId++
	return nil
}

func (this *FSField) updateDocument(docid uint32, contentstr string) error {
	if docid < this.startDocId || docid >= this.maxDocId || this.pfl == nil {
		this.Logger.Error("[ERROR] FSField --> UpdateDocument :: Wrong docid %v", docid)
		return errors.New("[ERROR] Wrong docid")
	}
	if this.fieldType == utils.IDX_TYPE_NUMBER {
		if err := this.pfl.updateDocument(docid, contentstr); err != nil {
			this.Logger.Error("[ERROR] FSField --> UpdateDocument :: Add Document Error %v", err)
			return err
		}
	}

	return nil

}

// Serialization function description : 序列化倒排索引（标准操作）
// params :
// return : error 正确返回Nil，否则返回错误类型
func (this *FSField) serialization(segmentname string, btree *tree.BTreedb) error {

	var err error
	if this.pfl != nil {
		this.pflOffset, this.pflLen, err = this.pfl.serialization(segmentname)
		if err != nil {
			this.Logger.Error("[ERROR] FSField --> Serialization :: Serialization Error %v", err)
			return err
		}
	}

	if this.ivt != nil {
		this.btree = btree
		if err := this.btree.AddBTree(this.fieldName); err != nil {
			this.Logger.Error("[ERROR] invert --> Create BTree Error %v", err)
			return err
		}
		err = this.ivt.serialization(segmentname, this.btree)
		if err != nil {
			this.Logger.Error("[ERROR] FSField --> Serialization :: Serialization Error %v", err)
			return err
		}
	}

	this.Logger.Info("[INFO] \tField[%v] --> Serialization OK...", this.fieldName)

	return nil
}

// Query function description : 给定一个查询词query，找出doc的列表（标准操作）
// params : key string 查询的key值
// return : docid结构体列表  bool 是否找到相应结果
func (this *FSField) query(key interface{}) ([]utils.DocIdNode, bool) {

	if this.ivt == nil {
		return nil, false
	}

	return this.ivt.query(key)

}

// GetValue function description : 获取值
// params :
// return :
func (this *FSField) getValue(docid uint32) (string, bool) {

	if docid >= this.startDocId && docid < this.maxDocId && this.pfl != nil {
		return this.pfl.getValue(docid - this.startDocId)
	}

	return "", false

}

// Filter function description : 过滤
// params :
// return :
func (this *FSField) filter(docid uint32, filtertype uint64, start, end int64) bool {

	if docid >= this.startDocId && docid < this.maxDocId && this.pfl != nil {

		return this.pfl.filter(docid-this.startDocId, filtertype, start, end)
	}

	return false
}

// Destroy function description : 销毁字段
// params :
// return :
func (this *FSField) destroy() error {
    

	if this.pfl != nil {
		this.pfl.destroy()
	}

	if this.ivt != nil {
		this.ivt.destroy()
	}

	return nil
}

func (this *FSField) setPflMmap(mmap *utils.Mmap) {

	if this.pfl != nil {
		this.pfl.setPflMmap(mmap)
	}

}

func (this *FSField) setDtlMmap(mmap *utils.Mmap) {
	if this.pfl != nil {
		this.pfl.setDtlMmap(mmap)
	}
}

func (this *FSField) setIdxMmap(mmap *utils.Mmap) {
	if this.ivt != nil {
		this.ivt.setIdxMmap(mmap)
	}
}

func (this *FSField) setBtree(btdb *tree.BTreedb) {
	if this.ivt != nil {
		this.ivt.setBtree(btdb)
	}
}

func (this *FSField) setMmap(pfl, dtl, idx *utils.Mmap) {

	this.setPflMmap(pfl)
	this.setDtlMmap(dtl)
	this.setIdxMmap(idx)

}




func (this *FSField) mergeField(fields []*FSField,segmentname string, btree *tree.BTreedb) (int64,int,error) {
 
    var err error
    if this.pfl != nil {
        pfls := make([]*profile,0)
        
        
        for _,fd:=range fields {
            //if fd == nil {
            //    this.Logger.Info("[INFO] fake profile docLen %v",docLen)
            //    fakepfl:=newEmptyFakeProfile(this.fieldType,0,this.fieldName,0,docLen,this.Logger)
            //     pfls = append(pfls,fakepfl)
            //}else{
                 pfls = append(pfls,fd.pfl)
            //}
           
        }
        this.pflOffset, this.pflLen, err = this.pfl.mergeProfiles(pfls,segmentname)
		if err != nil {
			this.Logger.Error("[ERROR] FSField --> Serialization :: Serialization Error %v", err)
			return 0,0,err
		}
        this.maxDocId+=uint32(this.pflLen)
        
    }
    
    
    if this.ivt != nil {
        this.btree = btree
		if err := this.btree.AddBTree(this.fieldName); err != nil {
			this.Logger.Error("[ERROR] invert --> Create BTree Error %v", err)
			return 0,0,err
		}
        ivts := make([]*invert,0)
        for _,fd:=range fields {
            if fd.ivt!=nil{
                ivts = append(ivts,fd.ivt)
            }else{
                this.Logger.Info("[INFO] invert is nil ")
            }
            
        }
        if err:=this.ivt.mergeInvert(ivts,segmentname,btree);err!=nil{
            return 0,0,err
        }
        
    }
    
    
    return this.pflOffset, this.pflLen,nil
}