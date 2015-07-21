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
)

type IndexSet struct {
	Logger    *log4jzl.Log4jzl
	IvtIndex  map[string]IndexInterface
	PflIndex  map[string]ProfileInterface
	Segmenter *utils.Segmenter
}

/*****************************************************************************
*  function name : NewIndexSet
*  params :
*  return :
*
*  description : 默认初始化函数
*
******************************************************************************/
func NewIndexSet(logger *log4jzl.Log4jzl) *IndexSet {
	segment := utils.NewSegmenter("./data/dictionary.txt")
	this := &IndexSet{Segmenter: segment, IvtIndex: make(map[string]IndexInterface), Logger: logger, PflIndex: make(map[string]ProfileInterface)}
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
		this.Logger.Info("========= Loading Index/Dictionary and Profile [ %v ] =========", k)
		if l[1] == "1" {
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
			pfl_name := fmt.Sprintf("./index/%v_pfl.json", k)
			bpfl, _ := utils.ReadFromJson(pfl_name)

			if l[3] == "T" {
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
	return nil
}

/*****************************************************************************
*  function name : SearchByRule
*  params : map[string]interface{}
*  return : []utils.DocIdInfo,bool
*
*  description : 搜索核心函数，根据输入的参数输出结果
*		输入: rules 的 key 表示字段，如果前缀带有"-"表示正向过滤，带有"_"表示反向过滤，"~"表示范围过滤
*			 key是"query"表全字段检索，否则表示指定关键词检索
*
*
******************************************************************************/
func (this *IndexSet) SearchByRule(rules map[string]interface{}) ([]utils.DocIdInfo, bool) {

	var res []utils.DocIdInfo
	isFirst := true
	for field, query := range rules {
		var sub_res []utils.DocIdInfo
		var ok bool
		if field == "query" {
			sub_res, ok = this.Search(query)
		} else {
			sub_res, ok = this.SearchField(query, field)
		}
		if !ok {
			return nil, false
		}
		if isFirst {
			res = sub_res
			isFirst = false
		} else {
			res, ok = utils.Interaction(res, sub_res)
			if !ok {
				return nil, false
			}
		}
		this.Logger.Info(" RES :: %v ", res)
	}

	//TODO 过滤操作

	//TODO 自定义过滤

	return res, false
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
	this.Logger.Info("TERMS :: %v ", terms)
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
		this.Logger.Info("[Term : %v ] [Field: %v ] DocIDs : %v", term, field, l)
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
	this.Logger.Info("[Number : %v ] [Field: %v ] DocIDs : %v", query, field, l)

	return l, true
}
