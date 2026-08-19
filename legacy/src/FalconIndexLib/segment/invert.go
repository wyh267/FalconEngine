/*****************************************************************************
 *  file name : StringIvtert.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 文本类倒排索引类
 *
******************************************************************************/

package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"tree"
	"utils"
)

/************************************************************************

字符型倒排索引，操作文件

[fullname].dic 该字段的字典文件，格式 | termlen | term | termId(uint32) | DF(uint32) |  ......
[segmentname].pos 该段的位置信息
[segmentname].idx 该段的倒排文件

************************************************************************/
//invert 字符串倒排索引
type invert struct {
	curDocId      uint32
	isMomery      bool
	fieldType     uint64
	fieldName     string
	idxMmap       *utils.Mmap
	tempHashTable map[string][]utils.DocIdNode
	Logger        *utils.Log4FE
	btree         *tree.BTreedb
	dict          *tree.BTreedb
}

// newEmptyInvert function description : 新建空的字符型倒排索引
// params :
// return :
func newEmptyInvert(fieldType uint64, startDocId uint32, fieldname string, dict *tree.BTreedb, logger *utils.Log4FE) *invert {
	this := &invert{dict: dict, btree: nil, curDocId: startDocId, fieldName: fieldname, fieldType: fieldType, tempHashTable: make(map[string][]utils.DocIdNode), Logger: logger, isMomery: true}
	return this
}

// newInvertWithLocalFile function description : 通过段的名称建立字符型倒排索引
// params :
// return :
func newInvertWithLocalFile(btdb *tree.BTreedb, fieldType uint64, fieldname, fullsegmentname string,
	idxMmap *utils.Mmap, dict *tree.BTreedb, logger *utils.Log4FE) *invert {

	this := &invert{dict: dict, btree: btdb, fieldType: fieldType, fieldName: fieldname, Logger: logger, isMomery: false, idxMmap: idxMmap}
	return this

}

// addDocument function description : 增加一个doc文档
// params : docid docid的编号
//			contentstr string  文档内容
// return : error 成功返回Nil，否则返回相应的错误信息
func (this *invert) addDocument(docid uint32, content interface{}) error {

	if docid != this.curDocId {
		return errors.New("invert --> AddDocument :: Wrong DocId Number")
	}
	this.Logger.Trace("[TRACE] invert --> AddDocument :: docid %v content %v", docid, content)
	contentstr := fmt.Sprintf("%v", content)
	//根据type进行分词
	var terms []string
	switch this.fieldType {
	case utils.IDX_TYPE_STRING, utils.GATHER_TYPE: //全词匹配模式
		terms = append(terms, contentstr)
	case utils.IDX_TYPE_STRING_LIST: //分号切割模式
		terms = strings.Split(contentstr, ";")
	case utils.IDX_TYPE_STRING_SINGLE: //单个词模式

		terminfos, _ := utils.GSegmenter.SegmentWithSingle(contentstr)

		for _, terminfo := range terminfos {
			docidNode := utils.DocIdNode{Docid: docid, Weight: uint32(terminfo.Tf)}
			if _, inTmp := this.tempHashTable[terminfo.Term]; !inTmp {
				var docidNodes []utils.DocIdNode
				docidNodes = append(docidNodes, docidNode)
				this.tempHashTable[terminfo.Term] = docidNodes
			} else {
				this.tempHashTable[terminfo.Term] = append(this.tempHashTable[terminfo.Term], docidNode)
			}

		}

		this.curDocId++
		return nil

	case utils.IDX_TYPE_STRING_SEG: //分词模式
		terminfos, termcount := utils.GSegmenter.SegmentWithTf(contentstr, true)
		//this.Logger.Info("[INFO] SegmentWithTf >>>>>>>>>>>>>>>>>>>>>>>> ")
		for _, terminfo := range terminfos {
			//this.Logger.Info("[INFO] terminfo.Term %v",terminfo.Term)
			docidNode := utils.DocIdNode{Docid: docid, Weight: uint32((float64(terminfo.Tf) / float64(termcount)) * 10000)}
			if _, inTmp := this.tempHashTable[terminfo.Term]; !inTmp {
				var docidNodes []utils.DocIdNode
				docidNodes = append(docidNodes, docidNode)
				this.tempHashTable[terminfo.Term] = docidNodes
			} else {
				this.tempHashTable[terminfo.Term] = append(this.tempHashTable[terminfo.Term], docidNode)
			}
			/// delete by wuyinghao,不用使用字典了
			//if err:=this.dict.IncValue(this.fieldName,terminfo.Term);err!=nil{
			//    return err
			//}
		}
		//this.Logger.Info("[INFO] SegmentWithTf <<<<<<<<<<<<<<<<<<<<< ")

		this.curDocId++
		return nil

	}

	for _, term := range terms {
		docidNode := utils.DocIdNode{Docid: docid}
		if _, inTmp := this.tempHashTable[term]; !inTmp {
			var docidNodes []utils.DocIdNode
			docidNodes = append(docidNodes, docidNode)
			this.tempHashTable[term] = docidNodes
		} else {
			this.tempHashTable[term] = append(this.tempHashTable[term], docidNode)
		}
	}

	this.curDocId++
	return nil
}

// serialization function description : 序列化倒排索引（标准操作）
// params :
// return : error 正确返回Nil，否则返回错误类型
func (this *invert) serialization(fullsegmentname string, btdb *tree.BTreedb) error {

	//打开倒排文件
	idxFileName := fmt.Sprintf("%v.idx", fullsegmentname)
	idxFd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()
	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	this.btree = btdb

	btMap := make(map[string]uint64)

	for key, value := range this.tempHashTable {
		lens := len(value)
		//offset := /*len(value)*DOCNODE_SIZE + */ totalOffset
		lenBufer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBufer, uint64(lens))

		idxFd.Write(lenBufer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			this.Logger.Error("[ERROR] invert --> Serialization :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		//this.Logger.Info("[INFO] key :%v totalOffset: %v len:%v value:%v",key,totalOffset,lens,value)

		btMap[key] = uint64(totalOffset)

		this.btree.Set(this.fieldName, key, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*utils.DOCNODE_SIZE

	}

	//this.btree.SetBatch(this.fieldName,btMap)

	this.tempHashTable = nil
	this.isMomery = false
	this.Logger.Trace("[TRACE] invert --> Serialization :: Writing to File : [%v.bt] ", fullsegmentname)
	this.Logger.Trace("[TRACE] invert --> Serialization :: Writing to File : [%v.idx] ", fullsegmentname)
	return nil

}

// Query function description : 给定一个查询词query，找出doc的列表（标准操作）
// params : key string 查询的key值
// return : docid结构体列表  bool 是否找到相应结果
func (this *invert) queryTerm(keystr string) ([]utils.DocIdNode, bool) {

	//this.Logger.Info("[INFO] QueryTerm %v",keystr)
	if this.isMomery == true {
		// this.Logger.Info("[INFO] ismemory is  %v",this.isMomery)
		docids, ok := this.tempHashTable[keystr]
		if ok {
			return docids, true
		}

	} else if this.idxMmap != nil {

		ok, offset := this.btree.Search(this.fieldName, keystr)
		//this.Logger.Info("[INFO] found  %v this.FullName %v offset %v",keystr,this.fieldName,offset)
		if !ok {
			return nil, false
		}
		lens := this.idxMmap.ReadInt64(int64(offset))
		//this.Logger.Info("[INFO] found  %v offset %v lens %v",keystr,offset,int(lens))
		res := this.idxMmap.ReadDocIdsArry(uint64(offset+8), uint64(lens))
		//this.Logger.Info("[INFO] KEY[%v] RES ::::: %v",keystr,res)
		return res, true

	}

	return nil, false

}

func (this *invert) query(key interface{}) ([]utils.DocIdNode, bool) {

	//this.Logger.Info("[DEBUG] invert Query %v", key)
	keystr, ok := key.(string)
	if !ok {
		return nil, false
	}

	//全词匹配模式
	if this.fieldType == utils.IDX_TYPE_STRING || this.fieldType == utils.GATHER_TYPE {
		return this.queryTerm(keystr)
	}

	var queryterms []string
	switch this.fieldType {
	case utils.IDX_TYPE_STRING_LIST: //分号切割模式
		queryterms = strings.Split(keystr, ";")
	case utils.IDX_TYPE_STRING_SINGLE: //单字模式
		queryterms = utils.GSegmenter.SegmentSingle(keystr)
	case utils.IDX_TYPE_STRING_SEG: //分词模式
		queryterms = utils.GSegmenter.Segment(keystr, false)
	default:
		return this.queryTerm(keystr)
	}
	if len(queryterms) == 1 {
		return this.queryTerm(queryterms[0])
	}
	var fDocids []utils.DocIdNode
	// var sDocids []utils.DocIdNode
	var hasRes bool
	var match bool
	fDocids, match = this.queryTerm(queryterms[0])
	//fDocids=append(fDocids,sDocids...)
	if match {
		for _, term := range queryterms[1:] {
			subDocids, ok := this.queryTerm(term)
			if !ok {
				return nil, false
			}
			fDocids, hasRes = utils.Interaction(fDocids, subDocids)
			if !hasRes {
				return nil, false
			}
		}
	}

	if len(fDocids) == 0 {
		return nil, false
	}
	return fDocids, true

}

// Destroy function description : 销毁段
// params :
// return :
func (this *invert) destroy() error {
	this.tempHashTable = nil
	return nil
}

func (this *invert) setIdxMmap(mmap *utils.Mmap) {
	this.idxMmap = mmap
}

func (this *invert) setBtree(btdb *tree.BTreedb) {
	this.btree = btdb
}

func (this *invert) mergeInvert(ivtlist []*invert, fullsegmentname string, btdb *tree.BTreedb) error {

	idxFileName := fmt.Sprintf("%v.idx", fullsegmentname)
	idxFd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()
	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	this.btree = btdb
	type ivtmerge struct {
		ivt    *invert
		key    string
		docids []utils.DocIdNode
		pgnum  uint32
		index  int
	}
	ivts := make([]ivtmerge, 0)

	for _, ivt := range ivtlist {

		if ivt.btree == nil {
			continue
		}

		key, _, pgnum, index, ok := ivt.GetFristKV()
		if !ok {
			//this.Logger.Info("[INFO] No Frist KV %v",key)
			continue
		}

		docids, _ := ivt.queryTerm(key)
		//this.Logger.Info("[INFO] Frist DocIDs %v",docids)

		ivts = append(ivts, ivtmerge{ivt: ivt, key: key, docids: docids, pgnum: pgnum, index: index})

	}

	resflag := 0
	for i := range ivts {
		resflag = resflag | (1 << uint(i))
	}
	flag := 0
	for flag != resflag {
		maxkey := ""
		for idx, v := range ivts {
			if ((flag >> uint(idx)) & 0x1) == 0 {
				maxkey = v.key
			}
		}

		for idx, v := range ivts {
			if ((flag>>uint(idx))&0x1) == 0 && maxkey > v.key {
				maxkey = v.key
			}
		}

		//maxkey = ""
		meridxs := make([]int, 0)
		for idx, ivt := range ivts {

			//if (flag>>uint(idx)&0x1) == 0 && maxkey < ivt.key {
			//	maxkey = ivt.key
			//	meridxs = make([]int, 0)
			//	meridxs = append(meridxs, idx)
			//	continue
			//}

			if (flag>>uint(idx)&0x1) == 0 && maxkey == ivt.key {
				//this.Logger.Info("[INFO] MaxKey [%v]", maxkey)
				meridxs = append(meridxs, idx)
				continue
			}

		}

		value := make([]utils.DocIdNode, 0)

		for _, idx := range meridxs {
			//this.Logger.Info("[INFO] Key:%v Docids:%v",maxkey,ivts[idx].docids)
			value = append(value, ivts[idx].docids...)
			//this.Logger.Info("[INFO] maxkey : %v idx[%v] Key %v \t value:%v", maxkey, idx, ivts[idx].key, ivts[idx].docids)
			key, _, pgnum, index, ok := ivts[idx].ivt.GetNextKV( /*ivts[idx].pgnum,ivts[idx].index*/ ivts[idx].key)
			if !ok {
				flag = flag | (1 << uint(idx))
				//this.Logger.Info("[INFO] FLAG %x RESFLAG %x idx %v meridxs len:%v", flag, resflag, idx, len(meridxs))
				continue
			}
			//this.Logger.Info("[INFO] pgnum : %v index : %v ok:%v Key:%v Docids:%v",pgnum,index,ok,key,ivts[idx].docids)

			ivts[idx].key = key
			ivts[idx].pgnum = pgnum
			ivts[idx].index = index
			ivts[idx].docids, ok = ivts[idx].ivt.queryTerm(key)
			//if !ok {
			//    this.Logger.Info("[INFO] not found %v",key)
			//}

		}

		lens := len(value)
		lenBufer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBufer, uint64(lens))
		idxFd.Write(lenBufer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			this.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		//this.Logger.Info("[INFO] key :%v totalOffset: %v len:%v value:%v", maxkey, totalOffset, lens, value)
		this.btree.Set(this.fieldName, maxkey, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*utils.DOCNODE_SIZE

	}

	this.tempHashTable = nil
	this.isMomery = false

	return nil
}

func (this *invert) GetFristKV() (string, uint32, uint32, int, bool) {

	if this.btree == nil {
		this.Logger.Info("[INFO] btree is null")
		return "", 0, 0, 0, false
	}
	//this.Logger.Info("[INFO] this.fieldName %v",this.fieldName)
	return this.btree.GetFristKV(this.fieldName)

}

func (this *invert) GetNextKV( /*pgnum uint32,idx int*/ key string) (string, uint32, uint32, int, bool) {

	if this.btree == nil {
		return "", 0, 0, 0, false
	}

	return this.btree.GetNextKV(this.fieldName /*pgnum,idx*/, key)

}
