/*****************************************************************************
 *  file name : Searcher.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 搜索引擎
 *
******************************************************************************/

package main

import (
	"BaseFunctions"
	"encoding/json"
	"errors"
	"fmt"
	//"github.com/Shopify/sarma"
	"indexer"
	"net/url"
	"strconv"
	"utils"
)

type Searcher struct {
	*BaseFunctions.BaseProcessor
	Indexer *indexer.IndexSet
}

func NewSearcher(processor *BaseFunctions.BaseProcessor, indexer *indexer.IndexSet) *Searcher {
	this := &Searcher{processor, indexer}
	return this
}

const PAGE_NUM string = "pg"
const PAGE_SIZE string = "ps"
const SORT_BY string = "sort_by"
const GROUP_BY string = "group_by"
const QUERY string = "query"

func (this *Searcher) SearchCount(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {

	this.Logger.Info("[LOG_ID:%v] Begin to Counting....Time: %v ", log_id, ftime("SearchCount"))
	searchRules, _, err := this.ParseSearchInfo(log_id, params, body)
	if err != nil {
		this.Logger.Error("[LOG_ID:%v] Counting error : %v ..Time: %v ", log_id, err, ftime("search fields"))
		result["DATA"] = "NO DATA"
		result["COUNT"] = 0
		result["HAS_RESULT"] = 0
		result["ERR_MSG"] = err

	}

	this.Logger.Info("[LOG_ID:%v]Running Searcher  %v....Time: %v ", log_id, searchRules, ftime("ParseSearchInfo"))

	total_doc_ids := make([]utils.DocIdInfo, 0)
	for _, search_rule := range searchRules {
		doc_ids, _ := this.Indexer.SearchByRules(search_rule.SR)
		//this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ", log_id, ftime("search fields"))
		doc_ids, _ = this.Indexer.FilterByRules(doc_ids, search_rule.FR)
		//this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ", log_id, ftime("fliter fields"))
		total_doc_ids, _ = utils.Merge(total_doc_ids, doc_ids)
	}
	result["COUNT"] = len(total_doc_ids)
	if len(total_doc_ids) == 0 {
		result["HAS_RESULT"] = 0
	} else {
		result["HAS_RESULT"] = 1
	}



	var tmp_doc_ids []utils.DocIdInfo
	if len(total_doc_ids) > 10 {
		tmp_doc_ids = total_doc_ids[:10]
	} else {
		tmp_doc_ids = total_doc_ids
	}
	/*
	ids, fields := this.Indexer.GetDetails(tmp_doc_ids)
	var infos []map[string]string
	for _, id := range ids {
		info, err := this.RedisCli.GetFields(id, fields)
		if err != nil {
			this.Logger.Error("%v", err)
		}
		infos = append(infos, info)
	}
*/
	result["DATA"] = this.Indexer.GetDetailsByDocId(tmp_doc_ids)





	this.Logger.Info("[LOG_ID:%v] End Counting....Time: %v \n\n", log_id, ftime("SearchCount"))

	return nil

}

func (this *Searcher) ComputeScore(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {

	this.Logger.Info("[LOG_ID:%v] Begin to ComputeScore ....Time: %v ", log_id, ftime("ComputeScore"))
	cid, err := strconv.ParseInt(params["cid"], 0, 0)
	if err != nil {
		this.Logger.Error("No CID.... Error : %v", err)
		return err
	}
	searchRule := make([]indexer.SearchRule, 0)
	searchRule = append(searchRule, indexer.SearchRule{Field: "cid", Query: cid})
	doc_ids, ok := this.Indexer.SearchByRules(searchRule)
	if !ok {
		return errors.New("No Data")
	}

	const COMPUTINGSCORE_SQL string = "UPDATE jzl_user_score SET status=?,last_modify_time=NOW() WHERE cid=?"
	err = this.DbAdaptor.ExecFormat(COMPUTINGSCORE_SQL, 1, cid)
	if err != nil {
		this.Logger.Error("[LOG_ID:%v] ComputeScore error :  %v", log_id, err)
		return err
	}

	for _, doc_id := range doc_ids {
		id, fields := this.Indexer.GetId(doc_id)
		info, err := this.RedisCli.GetFields(id, fields)
		if err != nil {
			this.Logger.Error("%v", err)
			continue
		}
		score, err := utils.ComputScore(body, info)
		if err != nil {
			this.Logger.Error("%v", err)
			continue
		}
		info["score"] = fmt.Sprintf("%v", score)
		//写入正排文件中
		/*
			upinfo := builder.UpdateInfo{info,true,make(chan error)}
			Data_chan <- upinfo
			errinfo:= <-upinfo.ErrChan
			if errinfo != nil {
				this.Logger.Info("Update Fail.... %v ", errinfo)
			}else{
				this.Logger.Info("Update Success.... ")
			}

			//写入redis中
			//this.RedisCli.SetFields(0, info)
		*/
		//写入数据库中
		const UPDATESCORE_SQL string = "UPDATE jzl_dmp SET score=?,last_modify_time=NOW() WHERE cid=? AND contact_id=?"
		err = this.DbAdaptor.ExecFormat(UPDATESCORE_SQL, info["score"], cid, info["contact_id"])
		if err != nil {
			this.Logger.Error("[LOG_ID:%v]  %v", log_id, err)
			return err
		}
		

	}


	//读取redis数据
	group_infos,err := this.RemoteRedisCli.GetGroupInfos(cid)
	if err != nil {
		this.Logger.Error("[LOG_ID:%v] ComputeScore GroupContact Error : %v", log_id, err)
		return err
	}
	
	for _,group_info := range group_infos {
		err := this.GroupContact(log_id,[]byte(group_info),params,result,ftime)
		if err != nil {
			this.Logger.Error("[LOG_ID:%v] ComputeScore GroupContact Error : %v", log_id, err)
		}
	}

	err = this.DbAdaptor.ExecFormat(COMPUTINGSCORE_SQL, 0, cid)
	if err != nil {
		this.Logger.Error("[LOG_ID:%v]  %v", log_id, err)
		return err
	}
	this.Logger.Info("[LOG_ID:%v] End ComputeScore ....Time: %v \n\n", log_id, ftime("ComputeScore"))
	return nil
}

func (this *Searcher) GroupContact(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {
	this.Logger.Info("[LOG_ID:%v] Begin to Grouping ....Time: %v ", log_id, ftime("GroupContact"))
	searchRules, si, err := this.ParseSearchInfo(log_id, params, body)
	if err != nil || si.Id == 0 {
		this.Logger.Error("[LOG_ID:%v] GroupContact error : %v ..Time: %v ", log_id, err, ftime("search fields"))
		result["DATA"] = "NO DATA"
		result["COUNT"] = 0
		result["HAS_RESULT"] = 0
		result["ERR_MSG"] = err

	}

	//this.Logger.Info("[LOG_ID:%v]Running GroupContact  %v....Time: %v ", log_id, searchRules, ftime("ParseSearchInfo"))

	total_doc_ids := make([]utils.DocIdInfo, 0)
	for _, search_rule := range searchRules {
		doc_ids, _ := this.Indexer.SearchByRules(search_rule.SR)
		//this.Logger.Info("[LOG_ID:%v]Running GroupContact ....Time: %v ", log_id, ftime("search fields"))
		doc_ids, _ = this.Indexer.FilterByRules(doc_ids, search_rule.FR)
		//this.Logger.Info("[LOG_ID:%v]Running GroupContact ....Time: %v ", log_id, ftime("fliter fields"))
		total_doc_ids, _ = utils.Merge(total_doc_ids, doc_ids)
	}

	//fields := make([]string, 0)
	//fields = append(fields, "contact_id")
	//fields = append(fields, "cid")
	//this.Logger.Info("doc_ids : %v ",total_doc_ids)
	
	infos := this.Indexer.GetDetailsByDocId(total_doc_ids)
	for _,info := range infos {
		v,ok:=info.(map[string]string)
		if ok {
			const ADDCONTACTSTOGROUP_SQL string = "REPLACE INTO jzl_groups_contacts (cid,creator_id,last_editor_id,group_id,contact_id,create_time,last_modify_time,is_delete) VALUES (?,?,?,?,?,NOW(),NOW(),0)"
			err = this.DbAdaptor.ExecFormat(ADDCONTACTSTOGROUP_SQL, v["cid"], si.Editor_id, si.Editor_id, si.Id, v["contact_id"])
			if err != nil {
				this.Logger.Error("[LOG_ID:%v]  %v", log_id, err)
				return err
			}
		}
		
	}
	
	
	/*
	for _, doc_id := range total_doc_ids {

		id, _ := this.Indexer.GetId(doc_id)
		//this.Logger.Info("Fields : %v",fields)
		info, err := this.RedisCli.GetFields(id, fields)
		if err != nil {
			this.Logger.Error("%v", err)
		}
		//this.Logger.Info("DOC INFO ::  %v ",info)
		const ADDCONTACTSTOGROUP_SQL string = "REPLACE INTO jzl_groups_contacts (cid,creator_id,last_editor_id,group_id,contact_id,create_time,last_modify_time,is_delete) VALUES (?,?,?,?,?,NOW(),NOW(),0)"
		err = this.DbAdaptor.ExecFormat(ADDCONTACTSTOGROUP_SQL, info["cid"], si.Editor_id, si.Editor_id, si.Id, info["contact_id"])
		if err != nil {
			this.Logger.Error("[LOG_ID:%v]  %v", log_id, err)
			return err
		}
	}
	*/
	this.Logger.Info("[LOG_ID:%v] End Grouping ....Time: %v \n\n", log_id, ftime("GroupContact"))
	return nil

}

func (this *Searcher) SimpleSearch(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {
	srules, frules, _, _ := this.ParseParams(log_id, params)

	total_doc_ids, ok := this.Indexer.SearchByRules(srules)
	if !ok {
		result["DATA"] = "NO DATA"
		return nil
	}
	//this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ", log_id, ftime("search fields"))
	total_doc_ids, _ = this.Indexer.FilterByRules(total_doc_ids, frules)
	this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ", log_id, ftime("search fields"))

	var tmp_doc_ids []utils.DocIdInfo
	if len(total_doc_ids) > 10 {
		tmp_doc_ids = total_doc_ids[:10]
	} else {
		tmp_doc_ids = total_doc_ids
	}
	/*
	this.Indexer.GetDetailsByDocId(tmp_doc_ids)
	ids, fields := this.Indexer.GetDetails(tmp_doc_ids)
	var infos []map[string]string
	for _, id := range ids {
		info, err := this.RedisCli.GetFields(id, fields)
		if err != nil {
			this.Logger.Error("%v", err)
		}
		infos = append(infos, info)
	}
	*/
	this.Logger.Info("[LOG_ID:%v]Running Simple Searcher ....Time: %v \n\n", log_id, ftime("Display Detail"))
	result["DATA"] = this.Indexer.GetDetailsByDocId(tmp_doc_ids)
	return nil
	//
	//result["PAGES"] = len(doc_ids)/int(ps) + 1

	//result["DATA"]=doc_ids

}

func (this *Searcher) Process(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {

	_, has_ctl := params["_contrl"]
	if has_ctl {
		this.Indexer.GetIndexInfo(result)
		return nil
	}

	_, has_count := params["_count"]
	if has_count {
		return this.SearchCount(log_id, body, params, result, ftime)
	}

	_, has_group := params["_group"]
	if has_group {

		go this.GroupContact(log_id, body, params, result, ftime)
		result["DATA"] = "OK"
		return nil
	}

	_, has_compute := params["_compute"]
	if has_compute {
		go this.ComputeScore(log_id, body, params, result, ftime)
		result["DATA"] = "OK"
		return nil
	}

	//_,has_search := params["~search"]
	//if has_search {
	return this.SimpleSearch(log_id, body, params, result, ftime)
	//}
	/*

	*/

}

type Condition struct {
	Key   string `json:"key"`
	Op    string `json:"operate"`
	Value string `json:"value"`
	Desc  string `json:"desc"`
}

type CommonStruct struct {
	Childs []Condition `json:"childs"`
	Score  int64       `json:"score"`
}

type ConditionData struct {
	Data []CommonStruct `json:"data"`
}

type SearchInfo struct {
	Customer_id      int64         `json:"customer_id"`
	Contact_id       int64         `json:"contact_id"`
	Id               int64         `json:"_id"`
	Creator_id       int64         `json:"creator_id"`
	Last_editor_id   int64         `json:"last_editor_id"`
	Create_time      string        `json:"create_time"`
	Last_modify_time string        `json:"last_modify_time"`
	Editor_id        int64         `json:"editor_id"`
	Group_type       int64         `json:"group_type"`
	Name             string        `json:"name"`
	Conditions       ConditionData `json:"conditions"`
}

type SearchRules struct {
	SR []indexer.SearchRule
	FR []indexer.FilterRule
}

func (this *Searcher) ParseSearchInfo(log_id string, params map[string]string, body []byte) ([]SearchRules, SearchInfo, error) { // ([]indexer.SearchRule,[]indexer.FilterRule,int64,int64){

	var searchInfo SearchInfo

	err := json.Unmarshal(body, &searchInfo)
	if err != nil {
		this.Logger.Error("[LOG_ID:%v]  %v", log_id, err)
		return nil, searchInfo, err
	}

	searchrules := make([]SearchRules, 0)

	this.Logger.Info("SearchInfo : %v \n", searchInfo)
	for i, data := range searchInfo.Conditions.Data {
		v := data.Childs
		this.Logger.Info("Conditions[%v] : %v \n", i, v)
		var SRs SearchRules
		SRs.SR = make([]indexer.SearchRule, 0)
		SRs.FR = make([]indexer.FilterRule, 0)
		SRs.SR = append(SRs.SR, indexer.SearchRule{Field: "cid", Query: searchInfo.Customer_id})
		if searchInfo.Contact_id != 0 {
			SRs.FR = append(SRs.FR, indexer.FilterRule{Field: "contact_id", Value: searchInfo.Contact_id, FiltType: indexer.FILT_TYPE_EQUAL, IsForward: true})
		}
		for ii, vv := range v {
			this.Logger.Info("\t\t Condition[%v] : %v \n", ii, vv)
			if vv.Key == "user_attrib" {
				//如果是包含，表示倒排检索
				if vv.Op == "include" {
					if vv.Desc == "zip" || vv.Desc == "email" || vv.Desc == "mobile_phone"{
						var FR indexer.FilterRule
						FR.Field = vv.Desc
						FR.Value = vv.Value
						FR.IsForward = true
						FR.FiltType = indexer.FILT_TYPE_INCLUDE
						SRs.FR = append(SRs.FR, FR)
					}else{
						var SR indexer.SearchRule
						SR.Field = vv.Desc
						SR.Query = vv.Value
						SRs.SR = append(SRs.SR, SR)
					}
					
				} else { //正排检索
					var FR indexer.FilterRule
					FR.Field = vv.Desc
					FR.Value = vv.Value
					FR.IsForward = true
					switch vv.Op {
					case "less":
						FR.FiltType = indexer.FILT_TYPE_LESS
					case "more":
						FR.FiltType = indexer.FILT_TYPE_ABOVE
					case "equal":
						FR.FiltType = indexer.FILT_TYPE_EQUAL
					case "unequal":
						FR.FiltType = indexer.FILT_TYPE_UNEQUAL
					default:
						FR.FiltType = indexer.FILT_TYPE_LESS
					}
					SRs.FR = append(SRs.FR, FR)
				}
			} else {

				if vv.Key == "mail" { //如果是邮件，需要拼接字符串，特殊处理
					var SR indexer.SearchRule
					switch vv.Op {
					case "look": //查看
						SR.Query = vv.Value + "_" + "1"
						SR.Field = "email_view"
					case "click":
						SR.Query = vv.Value + "_" + "1"
						SR.Field = "email_click"
					case "send":
						SR.Query = vv.Value + "_" + "1"
						SR.Field = "email_sended"
					case "unlook":
						SR.Query = vv.Value + "_" + "0"
						SR.Field = "email_view"
					case "unclick":
						SR.Query = vv.Value + "_" + "0"
						SR.Field = "email_click"
					case "unsend":
						SR.Query = vv.Value + "_" + "0"
						SR.Field = "email_sended"
					default:

					}
					SRs.SR = append(SRs.SR, SR)
					continue

				}

				if vv.Key == "sms" { //如果是短信，需要拼接字符串,特殊处理
					var SR indexer.SearchRule
					switch vv.Op {
					case "click":
						SR.Query = vv.Value + "_" + "1"
						SR.Field = "sms_click"
					case "send":
						SR.Query = vv.Value + "_" + "1"
						SR.Field = "sms_sended"
					case "unclick":
						SR.Query = vv.Value + "_" + "0"
						SR.Field = "sms_click"
					case "unsend":
						SR.Query = vv.Value + "_" + "0"
						SR.Field = "sms_sended"
					default:

					}
					SRs.SR = append(SRs.SR, SR)
					continue

				}

				if vv.Key == "area" { //如果是地区，需要特殊处理 TODO
					from_num, err := strconv.ParseInt(vv.Value, 0, 0)
					if err != nil {
						continue
					}
					//from_source
					var FR indexer.FilterRule
					FR.Field = "from_source"
					FR.Value = from_num
					FR.IsForward = true

					if from_num < 1000 {
						//FR.Value = from_num*10000
						//FR.FiltType = indexer.FILT_TYPE_EQUAL
						//SRs.FR = append(SRs.FR,FR)

						FR.FiltType = indexer.FILT_TYPE_ABOVE
						SRs.FR = append(SRs.FR, FR)

						FR.Value = from_num*10000 + 10000
						FR.FiltType = indexer.FILT_TYPE_LESS
						SRs.FR = append(SRs.FR, FR)
						continue
					}

					if from_num < 100000 {
						FR.FiltType = indexer.FILT_TYPE_ABOVE
						SRs.FR = append(SRs.FR, FR)
						FR.FiltType = indexer.FILT_TYPE_LESS
						FR.Value = from_num + 100
						SRs.FR = append(SRs.FR, FR)
						continue
					}

					continue
				}

				if vv.Key == "source" {
					var FR indexer.FilterRule
					FR.Field = vv.Key
					FR.IsForward = true
					var addbyadmin int64 = 1
					var export int64 = 2
					var unknown int64 = 3
					switch vv.Value {
					case "addbyadmin":
						FR.Value = addbyadmin
					case "export":
						FR.Value = export
					case "unknown":
						FR.Value = unknown
					default:
						FR.Value = unknown
					}
					if vv.Op == "equal" {
						FR.FiltType = indexer.FILT_TYPE_EQUAL
					} else {
						FR.FiltType = indexer.FILT_TYPE_UNEQUAL
					}
					SRs.FR = append(SRs.FR, FR)
					continue
				}

				if vv.Key == "email_client" { //TODO

				}
				
				
				if vv.Key == "buys" {
					var FR indexer.FilterRule
					FR.Field = vv.Key
					FR.IsForward = true
					if vv.Op == "less" {
						FR.FiltType = indexer.FILT_TYPE_LESS_DATERANGE
					} else {
						FR.FiltType = indexer.FILT_TYPE_ABOVE_DATERANGE
					}
					FR.Value = vv.Value
					SRs.FR = append(SRs.FR, FR)
					continue
					
				}

				if vv.Op == "include" { //其他检索，倒排索引

					var SR indexer.SearchRule
					SR.Field = vv.Desc
					SR.Query = vv.Value
					SRs.SR = append(SRs.SR, SR)

				} else { //其他检索，正排索引

					var FR indexer.FilterRule
					FR.Field = vv.Key
					FR.Value = vv.Value
					FR.IsForward = true
					switch vv.Op {
					case "less":
						FR.FiltType = indexer.FILT_TYPE_LESS
					case "more":
						FR.FiltType = indexer.FILT_TYPE_ABOVE
					case "equal":
						FR.FiltType = indexer.FILT_TYPE_EQUAL
					case "unequal":
						FR.FiltType = indexer.FILT_TYPE_UNEQUAL
					default:
						FR.FiltType = indexer.FILT_TYPE_LESS
					}
					SRs.FR = append(SRs.FR, FR)
				}

			}

		} // end for ii,vv := range v
		searchrules = append(searchrules, SRs)
	}

	return searchrules, searchInfo, nil

}

func (this *Searcher) ParseParams(log_id string, params map[string]string) ([]indexer.SearchRule, []indexer.FilterRule, int64, int64) {

	srules := make([]indexer.SearchRule, 0)
	frules := make([]indexer.FilterRule, 0)

	var ps int64
	var pg int64
	var err error

	ps = 10
	pg = 1
	for k, v := range params {
		v, _ = url.QueryUnescape(v)
		if k == PAGE_NUM {
			pg, err = strconv.ParseInt(params[PAGE_NUM], 0, 0)
			if err != nil {
				ps = 10
			}
			continue
		}

		if k == PAGE_SIZE {
			ps, err = strconv.ParseInt(params[PAGE_SIZE], 0, 0)
			if err != nil {
				pg = 1
			}
			continue
		}

		if k == QUERY {
			//this.Logger.Info(" query K : %v ,V : %v",k,v)
			srules = append(srules, indexer.SearchRule{k, v})
			continue
		}

		if k[0] != '-' && k[0] != '_' {
			this.Logger.Info(" string field K : %v ,V : %v", k, v)
			stype := this.Indexer.GetIdxType(k)
			if stype == -1 {
				continue
			}
			if stype == 1 {
				srules = append(srules, indexer.SearchRule{k, v})
			} else {
				v_n, err := strconv.ParseInt(v, 0, 0)
				if err != nil {
					this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, err)
					continue
				}
				srules = append(srules, indexer.SearchRule{k, v_n})
			}

			continue
		}

		if k[0] == '-' {
			this.Logger.Info(" filter1 field K : %v ,V : %v", k, v)
			stype := this.Indexer.GetPflType(k[1:])
			if stype == -1 {
				this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, k[1:])
				continue
			}
			if stype == 1 || stype ==2 {
				frules = append(frules, indexer.FilterRule{k[1:], true, 3, v})
			} else {
				v_n, err := strconv.ParseInt(v, 0, 0)
				if err != nil {
					this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, err)
					continue
				}
				frules = append(frules, indexer.FilterRule{k[1:], true, 3, v_n})
			}

			continue
		}

		if k[0] == '_' {
			this.Logger.Info(" filter2 field K : %v ,V : %v", k, v)
			stype := this.Indexer.GetPflType(k[1:])
			if stype == -1 {
				this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, k[1:])
				continue
			}
			if stype == 1 {
				frules = append(frules, indexer.FilterRule{k[1:], false, 4, v})
			} else {
				v_n, err := strconv.ParseInt(v, 0, 0)
				if err != nil {
					this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v_n, err)
					continue
				}
				frules = append(frules, indexer.FilterRule{k[1:], false, 4, v_n})
			}
			continue
		}

	}

	return srules, frules, ps, pg
}

type GroupContact struct {
	GroupId   int64
	ContactId int64
	Cid       int64
}

func (this *Searcher) InsertToGroup(GC chan GroupContact) {

	for {
		select {
		case gc := <-GC:
			this.Logger.Info("Insert ... CID : %v , CONTACTID : %v , GROUPID : %v \n", gc.Cid, gc.ContactId, gc.GroupId)
		}
	}

}
