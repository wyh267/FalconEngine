/*****************************************************************************
 *  file name : IndexSet.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 索引的集合对象，用于检索的核心类，实际使用中应是单例模式
 *
******************************************************************************/

package indexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/outmana/log4jzl"
	"strings"
	"utils"
	"strconv"

)

type IndexSet struct {
	Logger    *log4jzl.Log4jzl
	IvtIndex  map[string]IndexInterface
	PflIndex  map[string]ProfileInterface
	Segmenter *utils.Segmenter
	MaxDocId  int64
	PrimaryKey	string
	FieldInfo map[string]*IndexFieldInfo
	IncBuilder *utils.IndexBuilder
	BitMap	   *utils.Bitmap
}


type IndexFieldInfo struct {
	IsPK      bool
	IsIvt     bool
	IsPlf     bool
	FType     string
	Name      string
}

/*****************************************************************************
*  function name : NewIndexSet
*  params :
*  return :
*
*  description : 默认初始化函数
*
******************************************************************************/
func NewIndexSet(bitmap *utils.Bitmap,logger *log4jzl.Log4jzl) *IndexSet {
	segment := utils.NewSegmenter("./data/dictionary.txt")
	builder := &utils.IndexBuilder{segment}
	this := &IndexSet{BitMap:bitmap,IncBuilder:builder,FieldInfo:make(map[string]*IndexFieldInfo),MaxDocId:0,PrimaryKey:"PK",Segmenter: segment, IvtIndex: make(map[string]IndexInterface), Logger: logger, PflIndex: make(map[string]ProfileInterface)}
	return this

}

/*****************************************************************************
*  function name : NewIndexSetWithSegment
*  params :
*  return :
*
*  description : 带外部分词器的初始化函数
*
******************************************************************************/
func NewIndexSetWithSegment(logger *log4jzl.Log4jzl, segment *utils.Segmenter) *IndexSet {
	this := &IndexSet{Segmenter: segment, IvtIndex: make(map[string]IndexInterface), Logger: logger, PflIndex: make(map[string]ProfileInterface)}
	return this
}

/*****************************************************************************
*  function name : PutIndex
*  params :
*  return :
*
*  description : 将倒排索引放到相应的索引map中
*
******************************************************************************/
func (this *IndexSet) PutIndex(name string, index IndexInterface) error {

	this.IvtIndex[name] = index
	return nil
}

/*****************************************************************************
*  function name : PutProfile
*  params :
*  return :
*
*  description : 将正排索引放到相应的正排map中
*
******************************************************************************/
func (this *IndexSet) PutProfile(name string, profile ProfileInterface) error {

	this.PflIndex[name] = profile
	return nil
}

func (this *IndexSet) GetProfile(name string) (ProfileInterface, error) {
	return this.PflIndex[name], nil
}
func (this *IndexSet) GetIndex(name string) (IndexInterface, error) {
	return this.IvtIndex[name], nil
}
func (this *IndexSet) GetAll() (map[string]IndexInterface, error) {
	return this.IvtIndex, nil
}
func (this *IndexSet) Display() {

	for _, v := range this.IvtIndex {
		v.Display()
	}

	for _, v := range this.PflIndex {
		v.Display()
	}

}


func (this *IndexSet) GetPflType(field string) int64 {
	
	plf,ok:=this.PflIndex[field]
	if !ok{
		return -1
	}
	
	return plf.GetType()
}


func (this *IndexSet) GetIdxType(field string) int64 {
	
	ivt,ok:=this.IvtIndex[field]
	if !ok{
		return -1
	}
	
	return ivt.GetType()
}

/*****************************************************************************
*  function name : InitIndexSet
*  params : 需要初始化的字段
*  return :
*
*  description : 根据配置文件初始化建立索引和正排的字段并读入内存中
*
******************************************************************************/
func (this *IndexSet) InitIndexSet(fields map[string]string) error {
	for k, v := range fields {
		l := strings.Split(v, ",")
		if len(l) != 4 {
			this.Logger.Error("%v", errors.New("Wrong config file"))
			return errors.New("Wrong configure for index")
		}
		this.FieldInfo[k]=&IndexFieldInfo{false,false,false,"N",k}
		if l[0] == "1"{
			this.PrimaryKey=k
			this.FieldInfo[k].IsPK=true
		}
		
		this.Logger.Info("========= Loading Index/Dictionary and Profile [ %v ] =========", k)
		if l[1] == "1" {
			this.FieldInfo[k].IsIvt=true
			idx_name := fmt.Sprintf("./index/%v_idx.json", k)
			dic_name := fmt.Sprintf("./index/%v_dic.json", k)
			bidx, _ := utils.ReadFromJson(idx_name)
			bdic, _ := utils.ReadFromJson(dic_name)
			var idx utils.InvertIdx
			err := json.Unmarshal(bidx, &idx)
			if err != nil {
				return err
			}
			this.Logger.Info("Loading Index            [ %v ] ...", idx_name)
			if l[3] == "T" { //text ivt
				this.FieldInfo[k].FType="T"
				var dic utils.StringIdxDic
				this.Logger.Info("Loading Index Dictionary [ %v ] type : Text ...", dic_name)
				err = json.Unmarshal(bdic, &dic)
				if err != nil {
					return err
				}
				index := NewTextIndex(k, &idx, &dic)
				this.PutIndex(k, index)

			} else { //number ivt
				var dic utils.NumberIdxDic
				this.Logger.Info("Loading Index Dictionary [ %v ] type : Number...", dic_name)
				err = json.Unmarshal(bdic, &dic)
				if err != nil {
					return err
				}
				index := NewNumberIndex(k, &idx, &dic)
				this.PutIndex(k, index)
			}

		}

		if l[2] == "1" {
			this.FieldInfo[k].IsPlf=true
			pfl_name := fmt.Sprintf("./index/%v_pfl.json", k)
			bpfl, _ := utils.ReadFromJson(pfl_name)

			if l[3] == "T" {
				this.FieldInfo[k].FType="T"
				var pfl TextProfile
				this.Logger.Info("Loading Index Profile    [ %v ] type : Text ...", pfl_name)
				err := json.Unmarshal(bpfl, &pfl)
				if err != nil {
					return err
				}
				this.PutProfile(k, &pfl)

			} else {
				var pfl NumberProfile
				this.Logger.Info("Loading Index Profile    [ %v ] type : Number ...", pfl_name)
				err := json.Unmarshal(bpfl, &pfl)
				if err != nil {
					return err
				}
				this.PutProfile(k, &pfl)
			}
		}
	}
	
	//保存最大DocId
	this.MaxDocId = this.PflIndex[this.PrimaryKey].GetMaxDocId()
		
	return nil
}

/*****************************************************************************
*  function name : SearchByRules
*  params : map[string]interface{}
*  return : []utils.DocIdInfo,bool
*
*  description : 搜索核心函数，根据输入的参数输出结果
*		输入: rules 的 key 表示字段，如果前缀带有"-"表示正向过滤，带有"_"表示反向过滤，"~"表示范围过滤
*			 key是"query"表全字段检索，否则表示指定关键词检索
*
*
******************************************************************************/
type SearchRule struct{
	Field	string
	Query	interface{}
}

func (this *IndexSet) SearchByRules(rules /*map[string]interface{}*/[]SearchRule) ([]utils.DocIdInfo, bool) {

	var res []utils.DocIdInfo
	for index, rule := range rules {
		var sub_res []utils.DocIdInfo
		var ok bool
		if rule.Field == "query" {
			sub_res, ok = this.Search(rule.Query)
		} else {
			//this.Logger.Info(" Field : %v Query : %v",rule.Field, rule.Query)
			sub_res, ok = this.SearchField(rule.Query, rule.Field)
		}
		if !ok {
			return nil, false
		}
		if index==0 {
			res = sub_res
		} else {
			res, ok = utils.Interaction(res, sub_res)
			if !ok {
				return nil, false
			}
		}
		//this.Logger.Info(" RES :: %v ", res)
	}

	//BitMap过滤失效的doc_id
	this.Logger.Info(" %v ",res)
	r:=make([]utils.DocIdInfo,0)
	for i,_:=range res{
		//this.Logger.Info(" %v ",res[i].DocId)
		if this.BitMap.GetBit(uint64(res[i].DocId)) == 0 {
			r = append(r,res[i])
		}
	}

	//TODO 自定义过滤

	return r, true
}

/*****************************************************************************
*  function name : Search
*  params : query interface{}
*  return :
*
*  description : 搜索函数，根据类型进行不同的搜索
*
******************************************************************************/
func (this *IndexSet) Search(query interface{}) ([]utils.DocIdInfo, bool) {

	query_str, ok := query.(string)
	if ok {
		return this.SearchString(query_str)
	}

	query_num, ok := query.(int64)
	if ok {
		return this.SearchNumber(query_num)
	}

	return nil, false
}

/*****************************************************************************
*  function name : SearchField
*  params : query field
*  return :
*
*  description : 指定字段检索，判断query类型调用不同的实现接口
*
******************************************************************************/

func (this *IndexSet) SearchField(query interface{}, field string) ([]utils.DocIdInfo, bool) {

	query_str, ok := query.(string)
	if ok {
		return this.SearchFieldByString(query_str, field)
	}

	query_num, ok := query.(int64)
	if ok {

		return this.SearchFieldByNumber(query_num, field)
	}

	return nil, false

}

type FilterRule struct {
	Field     string
	IsForward bool
	Value     interface{}
}

/*****************************************************************************
*  function name : FilterByRules
*  params : doc_ids []utils.DocIdInfo,rules []FilterRule
*  return :
*
*  description : 根据传入的规则进行过滤
*
******************************************************************************/
func (this *IndexSet) FilterByRules(doc_ids []utils.DocIdInfo, rules []FilterRule) ([]utils.DocIdInfo, error) {

	var res []utils.DocIdInfo
	res = doc_ids
	for _, rule := range rules {
		_, ok := this.PflIndex[rule.Field]
		if !ok {
			continue
		}

		res, _ = this.PflIndex[rule.Field].Filter(res, rule.Value, rule.IsForward)
	}
	return res, nil

}

/*****************************************************************************
*  function name : FilterByCustom
*  params :
*  return :
*
*  description : 自定义过滤，当自定义函数返回true的时候保留数据
*
******************************************************************************/
func (this *IndexSet) FilterByCustom(doc_ids []utils.DocIdInfo, field string, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]utils.DocIdInfo, error) {

	_, ok := this.PflIndex[field]
	if !ok {
		return doc_ids, errors.New("No field ")
	}
	return this.PflIndex[field].CustomFilter(doc_ids, value, r, cf)

}

/*****************************************************************************
*  function name : SearchString
*  params : query
*  return : doc链
*
*  description : 检索函数，根据关键字进行检索，先在同一个字段中检索，如果没有结果进行跨字段检索
*
******************************************************************************/
func (this *IndexSet) SearchString(query string) ([]utils.DocIdInfo, bool) {

	//按照最大切分进行切词
	//terms := utils.RemoveDuplicatesAndEmpty(this.Segmenter.Segment(query,false))

	//首先按照字段检索
	var res_list []utils.DocIdInfo
	var ok bool
	for key, _ := range this.IvtIndex {
		if this.IvtIndex[key].GetType() != 1 {
			continue
		}
		res_list, ok = this.SearchFieldByString(query, key)
		if ok {
			return res_list, true
		}

	}

	//如果数量不够，跨字段检索 TODO
	var res_merge []utils.DocIdInfo
	if len(res_list) == 0 {
		terms := utils.RemoveDuplicatesAndEmpty(this.Segmenter.Segment(query, false))
		//this.Logger.Info("OUT TERMS :: %v ",terms)
		for index, term := range terms {
			l, ok := this.SearchFieldsByTerm(term)
			if !ok {
				return nil, false
			}

			if index == 0 {
				res_merge = l
			} else {
				res_merge, ok = utils.Interaction(res_merge, l)
				//this.Logger.Info("Interaction Term:%v Docids: %v",term,res_merge)
				if !ok {
					return nil, false
				}
			}
		}
	}

	return res_merge, true

}

/*****************************************************************************
*  function name : SearchFieldsByTerm
*  params : term
*  return :
*
*  description : 将term在全字段进行检索，求并集，用于跨字段检索
*
******************************************************************************/
func (this *IndexSet) SearchFieldsByTerm(term string) ([]utils.DocIdInfo, bool) {

	var res_list []utils.DocIdInfo
	for key, _ := range this.IvtIndex {
		if this.IvtIndex[key].GetType() != 1 {
			continue
		}
		l, _ := this.IvtIndex[key].Find(term)
		this.Logger.Info("Field:%v Term:%v Docids: %v", key, term, l)

		res_list, _ = utils.Merge(res_list, l)
		this.Logger.Info("Merged Field:%v Term:%v Docids: %v", key, term, res_list)
	}

	if len(res_list) == 0 {
		return nil, false
	}

	return res_list, true

}

//数字类型搜索不存在关键字一说，不需要实现
func (this *IndexSet) SearchNumber(query int64) ([]utils.DocIdInfo, bool) {

	return nil, false
}

/*****************************************************************************
*  function name : SearchFieldByString
*  params : query field
*  return :
*
*  description : 在指定字段检索相应的query，用于正常检索
*
******************************************************************************/
func (this *IndexSet) SearchFieldByString(query string, field string) ([]utils.DocIdInfo, bool) {

	//按照最大切分进行切词
	terms := utils.RemoveDuplicatesAndEmpty(this.Segmenter.Segment(query, false))
	//this.Logger.Info("TERMS :: %v ", terms)
	//首先按照字段检索
	//交集结果
	var res_list []utils.DocIdInfo
	_, ok := this.IvtIndex[field]
	if !ok {
		return nil, false
	}
	isFound := true
	for index, term := range terms {
		l, ok := this.IvtIndex[field].Find(term)
		if !ok {
			isFound = false
			break
		}
		//this.Logger.Info("[Term : %v ] [Field: %v ] DocIDs : %v", term, field, l)
		//求交集
		if index == 0 {
			res_list = l
		} else {
			res_list, ok = utils.Interaction(l, res_list)
			if !ok {
				isFound = false
				break
			}
		}

	}
	if len(res_list) > 0 && isFound == true {
		return res_list, true
	}
	return nil, false
}

/*****************************************************************************
*  function name : SearchFieldByNumber
*  params : query field
*  return :
*
*  description : 在指定字段检索数字
*
******************************************************************************/
func (this *IndexSet) SearchFieldByNumber(query int64, field string) ([]utils.DocIdInfo, bool) {

	_, ok := this.IvtIndex[field]
	if !ok {
		return nil, false
	}

	l, ok := this.IvtIndex[field].Find(query)
	if !ok {
		return nil, false
	}
	//this.Logger.Info("[Number : %v ] [Field: %v ] DocIDs : %v", query, field, l)

	return l, true
}



func (this *IndexSet) GetDetails(doc_ids []utils.DocIdInfo) ([]int64){
	
	ids := make([]int64,len(doc_ids))
	for i,doc_id := range doc_ids{
		
		tmp,_:=this.PflIndex["id"].Find(doc_id.DocId)
		ids[i],_=tmp.(int64)
		
	}
	
	return ids 
}


/*****************************************************************************
*
*  数据更新
*
******************************************************************************/

//func (this *IndexSet) AddRecord()

func (this *IndexSet) UpdateRecord(info map[string]string,isProfileUpdate bool) error {
	
	//检查是否有PrimaryKey字段，如果没有的话，不允许更新
	_,hasPK:=info[this.PrimaryKey]
	if !hasPK{
		this.Logger.Error("No Primary Key,Update is now allow ")
		return errors.New("No Primary Key,Update is now allow")
	}
	pk, err := strconv.ParseInt(info[this.PrimaryKey], 0, 0)
	if err != nil {
		this.Logger.Error("No Primary Key,Update is now allow  %v",  err)
		return err
	}
	
	//检查是否有着primary key ,如果没有，需要新增一个doc_id，进行全字段更新
	//_,has_key:=this.SearchField(pk,this.PrimaryKey)
	//if !has_key{
	//}else{//如果存在primary key的话，要看需要更新的字段，如果都是正排文件，不需要新增doc_id，否则也需要新增doc_id进行全字段更新	
	//}
	
	//如果仅更新正排文件，不需要新建doc_id，直接更新
	Doc_id,has_key:=this.SearchField(pk,this.PrimaryKey)
	if isProfileUpdate {
		if !has_key{
			this.Logger.Error("isProfileUpdate  %v",  err)
			return errors.New("Update err...no doc_id to update")
		}
		for k,v := range info{
			this.UpdateProfile(k,v,Doc_id[0].DocId)
		}
		
	}else{//新增doc_id，全字段更新
		//如果doc_id存在，删除之前的doc_id
		if has_key{
			this.BitMap.SetBit(uint64(Doc_id[0].DocId),1)
			this.Logger.Error("has_key,Update is now allow  %v",  err)
		}
		//新增一个doc_id
		doc_id := this.MaxDocId + 1	
		for k,v := range info {
			this.Logger.Info("K : %v  === V : %v === Doc_ID : %v",k,v,doc_id)
			this.UpdateInvert(k,v,doc_id)
			this.UpdateProfile(k,v,doc_id)
		}
		
		this.MaxDocId++	
	}
	
	
	return nil
	
	
}



func (this *IndexSet) UpdateInvert(k,v string,doc_id int64){
	
	field_info,ok:=this.FieldInfo[k]
	if !ok{
		this.Logger.Error("UpdateInvert ")
		return
	}
	
	if field_info.IsIvt {

		if field_info.FType == "T" {
			err := this.IncBuilder.BuildTextIndex(doc_id, v, this.IvtIndex[k].GetIvtIndex(), this.IvtIndex[k].GetStrDic())
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}
		}

		if field_info.FType == "N" {
			v_num, err := strconv.ParseInt(v, 0, 0)
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}

			err = this.IncBuilder.BuildNumberIndex(doc_id, v_num, this.IvtIndex[k].GetIvtIndex(), this.IvtIndex[k].GetNumDic())
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}
		}

	}
}




func (this *IndexSet) UpdateProfile(k,v string,doc_id int64) {
	
	field_info,ok:=this.FieldInfo[k]
	if !ok{
		this.Logger.Error("UpdateProfile  ")
		return 
	}
	
	if field_info.IsPlf {

		if field_info.FType == "T" {

			err := this.PflIndex[k].Put(doc_id, v)
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}
		}

		if field_info.FType == "N" {
			v_num, err := strconv.ParseInt(v, 0, 0)
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}
			err = this.PflIndex[k].Put(doc_id, v_num)
			if err != nil {
				this.Logger.Error("ERROR : %v", err)
			}
		}
	}
	
	
	
}