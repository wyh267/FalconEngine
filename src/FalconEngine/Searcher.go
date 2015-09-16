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
	"builder"
	"indexer"
	"net/url"
	"strconv"
	"utils"
)

type Searcher struct {
	*BaseFunctions.BaseProcessor
	Indexer   *indexer.IndexSet
	Data_chan chan builder.UpdateInfo
}

func NewSearcher(processor *BaseFunctions.BaseProcessor, indexer *indexer.IndexSet, data_chan chan builder.UpdateInfo) *Searcher {
	this := &Searcher{processor, indexer, data_chan}
	return this
}

const PAGE_NUM string = "pg"
const PAGE_SIZE string = "ps"
const SORT_BY string = "sort_by"
const GROUP_BY string = "group_by"
const QUERY string = "query"



func (this *Searcher) SimpleSearch(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {
	srules, frules, _, _ := this.ParseParams(log_id, params)

	total_doc_ids, ok := this.Indexer.SearchByRules(srules)
	if !ok {
		result["DATA"] = "NO DATA"
		return nil
	}
	total_doc_ids, _ = this.Indexer.FilterByRules(total_doc_ids, frules)
	//this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ", log_id, ftime("search fields"))
	result["COUNT"] = len(total_doc_ids)
	var tmp_doc_ids []utils.DocIdInfo
	if len(total_doc_ids) > 10 {
		tmp_doc_ids = total_doc_ids[:10]
	} else {
		tmp_doc_ids = total_doc_ids
	}

	//this.Logger.Info("[LOG_ID:%v]Running Simple Searcher ....Time: %v \n\n", log_id, ftime("Display Detail"))
	result["DATA"] = this.Indexer.GetDetailsByDocId(tmp_doc_ids)
	return nil

}

func (this *Searcher) Process(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {

	_, has_ctl := params["_contrl"]
	if has_ctl {
		this.Indexer.GetIndexInfo(result)
		return nil
	}

	return this.SimpleSearch(log_id, body, params, result, ftime)

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
			//this.Logger.Info(" string field K : %v ,V : %v", k, v)
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
			//this.Logger.Info(" filter1 field K : %v ,V : %v", k, v)
			stype := this.Indexer.GetPflType(k[1:])
			if stype == -1 {
				this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, k[1:])
				continue
			}
			if stype == 1 || stype == 2 {
				frules = append(frules, indexer.FilterRule{k[1:], true, 3, v})
			} else {
				v_n, err := strconv.ParseInt(v, 0, 0)
				if err != nil {
					this.Logger.Error("[LOG_ID:%v] %v %v", log_id, v, err)
					continue
				}
				if stype == 0 {
					frules = append(frules, indexer.FilterRule{k[1:], true, 3, v_n})
				} else {
					frules = append(frules, indexer.FilterRule{k[1:], false, 3, v_n})
				}

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
