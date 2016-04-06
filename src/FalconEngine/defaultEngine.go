/*****************************************************************************
 *  file name : defaultEngine.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据层之上的引擎层,都需要实现引擎的接口
 *
******************************************************************************/

package FalconEngine

import (
	fi "FalconIndex"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
	"utils"
)

const (
	eProcessorOnlySupportPost    string = "提交方式错误,请查看提交方式是否正确"
	eProcessoParms               string = "参数错误"
	eProcessoJsonParse           string = "JSON格式解析错误"
	eProcessoUpdateProcessorBusy string = "处理进程繁忙，请稍候提交"
	eProcessoQueryError          string = "查询条件有问题，请检查查询条件"
	eDefaultEngineNotFound       string = `{"status":"NotFound"}`
    eDefaultEngineDeleteOk       string = `eDefaultEngineDeleteOk`
	eDefaultEngineLoadOk         string = `{"status":"OK"}`
	eDefaultEngineLoadFail       string = `{"status":"Fail"}`
)

type DefaultResult struct {
	TotalCount int64                     `json:"totalCount"`
	From       int64                     `json:"from"`
	To         int64                     `json:"to"`
	Status     string                    `json:"status"`
	CostTime   string                    `json:"costTime"`
	Gater      map[string]map[string]int `json:"Gaters"`
	Result     []map[string]string       `json:"dataDetail"`
}

type DefaultEngine struct {
	idxManager *IndexMgt
	Logger     *utils.Log4FE `json:"-"`
}

func NewDefaultEngine(logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{Logger: logger, idxManager: newIndexMgt(logger)}
	return this
}

func (this *DefaultEngine) Search(method string, parms map[string]string, body []byte) (string, error) {

	//this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")
	var shows []string
    crossSort:=false
	startTime := time.Now()
	indexname, hasindex := parms["index"]
	query, hasquery := parms["q"]
	ps, hasps := parms["ps"]
	pg, haspg := parms["pg"]
	show, hasshow := parms["show"]
	gater, hasgater := parms["gater"]
	sortfield, hassort := parms["sort"]

	if !hasindex || !haspg || !hasps {
		return "", errors.New(eProcessoParms)
	}

	//获取索引
	indexer := this.idxManager.GetIndex(indexname)
	if indexer == nil {
		return "", errors.New(eDefaultEngineNotFound)
	}
    
	// 建立过滤条件
	searchfilters := this.parseFilted(parms, indexer)

	//首先在主字段进行检索
	mainsearchquerys := make([]utils.FSSearchQuery, 0)
    
    //this.Logger.Info("[INFO] Terms %v",terms)
	if hasquery {
		terms := utils.GSegmenter.Segment(query, false)
		for _, term := range terms {
			var queryst utils.FSSearchQuery
			queryst.FieldName = "title"
			queryst.Value = term
			mainsearchquerys = append(mainsearchquerys, queryst)
		}
	}

	//进行主字段搜索过滤
	docids, mainFound := indexer.SearchDocIds(mainsearchquerys, searchfilters)
	searchquerys := make([]utils.FSSearchCrossFieldsQuery, 0)
	if len(docids) < 10 && len(mainsearchquerys)>0{
       
		//TODO : 跨字段搜索
		if hasquery {
			terms := utils.GSegmenter.Segment(query, false)
			for _, term := range terms {
				var queryst utils.FSSearchCrossFieldsQuery
				queryst.FieldNames = []string{"title", "nickname", "content"}
				queryst.Value = term
				searchquerys = append(searchquerys, utils.FSSearchCrossFieldsQuery{FieldNames: []string{"title", "nickname", "content"}, Value: term})
			}
		}
         
		//进行搜索过滤
		crossDocids, crossFound := indexer.SearchDocIdsCrossFields(searchquerys, searchfilters)
       // this.Logger.Info("[INFO] SearchDocIdsCrossFields %v found:%v docidslen:%v",searchquerys,crossFound,crossDocids)
		if crossFound {
            
            if !(hassort && sortfield == "false") && len(searchquerys) > 0 {
		        sort.Sort(utils.DocWeightSort(docids))
                sort.Sort(utils.DocWeightSort(crossDocids))
                crossSort=true
	        }
			docids = append(docids, crossDocids...)
			utils.GiveDocIDsChan <- crossDocids
		} else if !crossFound && !mainFound {
            utils.GiveDocIDsChan <- crossDocids
            utils.GiveDocIDsChan <- docids
			return eDefaultEngineNotFound, nil

		}

	}

	lens := int64(len(docids))

	//进行排序
	if !(hassort && sortfield == "false") && len(mainsearchquerys) > 0 && !crossSort{
		sort.Sort(utils.DocWeightSort(docids))
	}

	var defaultResult DefaultResult
	// 进行汇总
	if hasgater {
		gaters := strings.Split(gater, ",")
		defaultResult.Gater = indexer.GatherFields(docids, gaters)
	}

	// 进行展示
	if !hasshow {
		shows = indexer.GetFields()
	} else {
		shows = strings.Split(show, ",")
	}
	start, end, pageerr := this.calcStartEnd(ps, pg, lens)
	if pageerr != nil {
		return eDefaultEngineNotFound, nil
	}
	defaultResult.Result = make([]map[string]string, 0)
	for _, docid := range docids[start:end] {
		val, ok := indexer.GetDocumentWithFields(docid.Docid, shows)
		if ok {
			//val["_id"] = fmt.Sprintf("%d", docid.Docid)
			//val["_weight"] = fmt.Sprintf("%d", docid.Weight)
			defaultResult.Result = append(defaultResult.Result, val)
		}
	}

	// 释放docids
	utils.GiveDocIDsChan <- docids

	//填写元信息
	defaultResult.From = start + 1
	defaultResult.To = end
	defaultResult.Status = "OK"
	defaultResult.TotalCount = lens
	endTime := time.Now()
	defaultResult.CostTime = fmt.Sprintf("%v", endTime.Sub(startTime))

	//生成json结构
	r, err := json.Marshal(defaultResult)
	if err != nil {
		return eDefaultEngineNotFound, err
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&r))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh)), nil

}

func (this *DefaultEngine) CreateIndex(method string, parms map[string]string, body []byte) error {

	indexname, hasindex := parms["index"]

	if !hasindex {
		return errors.New(eProcessoParms)
	}

	var indexstruct utils.IndexStrct
	err := json.Unmarshal(body, &indexstruct)
	if err != nil {
		this.Logger.Error("[ERROR]  %v : %v ", eProcessoJsonParse, err)
		return fmt.Errorf("[ERROR]  %v : %v ", eProcessoJsonParse, err)
	}

	return this.idxManager.CreateIndex(indexname, indexstruct.IndexMapping)

}

func (this *DefaultEngine) CreateEmptyIndex(indexname string) error {

	return this.idxManager.CreateEmptyIndex(indexname)

}

func (this *DefaultEngine) AddField(indexname string, field utils.SimpleFieldInfo) error {

	return this.idxManager.AddField(indexname, field)

}

func (this *DefaultEngine) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {
	indexname, hasindex := parms["index"]

	if !hasindex {
		return "", errors.New(eProcessoParms)
	}
    indexer:=this.idxManager.GetIndex(indexname)
    switch method {
    case "POST":
        document := make(map[string]string)
        err := json.Unmarshal(body, &document)
        if err != nil {
            this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
            return "", errors.New(eProcessoJsonParse)
        }

	    return this.idxManager.updateDocument(indexname, document)
    case "DELETE":
        if pk,haspk:=parms["_pk"];haspk{
            err:=indexer.DeleteDocument(pk)
            if err!=nil{
                return "",err
            }
            return eDefaultEngineLoadOk,nil
        }
        
        if docidstr,hasdocid:=parms["_docid"];hasdocid{
            docid,converr := strconv.ParseInt(docidstr,0,0)
            if converr!= nil {
                return "",converr
            }
            err:=indexer.DeleteDocumentByDocId(uint32(docid))
            if err!=nil{
                return "",err
            }
            return eDefaultEngineLoadOk,nil
        }
        
    default:
        return "", errors.New(eProcessoParms)
    }

	return "", errors.New(eProcessoParms)
}


func (this *DefaultEngine) DeleteDocument(method string, parms map[string]string, body []byte) (string, error) {
    
    
    return eDefaultEngineDeleteOk,nil
}

func (this *DefaultEngine) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	indexname, hasindex := parms["index"]

	if !hasindex || method != "POST" {
		return eDefaultEngineLoadFail, errors.New(eProcessoParms)
	}

	var loadstruct utils.FSLoadStruct
	err := json.Unmarshal(body, &loadstruct)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return eDefaultEngineLoadFail, errors.New(eProcessoJsonParse)
	}

	this.Logger.Info("[INFO] loadstruct %v %v", loadstruct, string(body))

	datafile, err := os.Open(loadstruct.Filename)
	if err != nil {
		this.Logger.Error("[ERROR] Open File[%v] Error %v", loadstruct.Filename, err)

		return eDefaultEngineLoadFail, errors.New("[ERROR] Open File Error")
	}
	defer datafile.Close()

	scanner := bufio.NewScanner(datafile)
	i := 0
	var isJson bool
	if loadstruct.Split == "json" {
		isJson = true
	}

	if loadstruct.SyncCount <= 0 {
		loadstruct.SyncCount = 10000
	}

	for scanner.Scan() {
		content := make(map[string]string)
		if isJson {

			if err := json.Unmarshal([]byte(scanner.Text()), &content); err != nil {
				this.Logger.Error("[ERROR]  %v", err)
				continue
			}

		} else {
			sptext := strings.Split(scanner.Text(), loadstruct.Split)
			if len(sptext) != len(loadstruct.Fields) {
				continue
			}
			for idx, fname := range loadstruct.Fields {
				content[fname] = sptext[idx]
			}
		}

		this.idxManager.updateDocument(indexname, content)

		i++
		if i%loadstruct.SyncCount == 0 {
			this.idxManager.sync(indexname)
		}
		//fmt.Println(sptext)
	}
	this.idxManager.sync(indexname)
	if loadstruct.IsMerge {
		return eDefaultEngineLoadOk, this.idxManager.mergeIndex(indexname)
	}

	return eDefaultEngineLoadOk, nil

}

func (this *DefaultEngine) parseFilted(parms map[string]string, indexer *fi.Index) []utils.FSSearchFilted {

	searchfilters := make([]utils.FSSearchFilted, 0)
	for k, v := range parms {
		switch k[0] {
		case '-':
			ftype, ok := indexer.GetFieldType(k[1:])
			if ok {
				if ftype == utils.IDX_TYPE_NUMBER {
					start, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_EQ,
						Start: start})
				} else if ftype == utils.IDX_TYPE_DATE {
					timestmp, err := utils.IsDateTime(v)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_EQ,
						Start: timestmp})
				}
			}
		case '>':
			ftype, ok := indexer.GetFieldType(k[1:])
			if ok {
				if ftype == utils.IDX_TYPE_NUMBER {
					start, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_OVER,
						Start: start})
				} else if ftype == utils.IDX_TYPE_DATE {
					timestmp, err := utils.IsDateTime(v)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_OVER,
						Start: timestmp})
				}
			}
		case '<':
			ftype, ok := indexer.GetFieldType(k[1:])
			if ok {
				if ftype == utils.IDX_TYPE_NUMBER {
					start, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_LESS,
						Start: start})
				} else if ftype == utils.IDX_TYPE_DATE {
					timestmp, err := utils.IsDateTime(v)
					if err != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_LESS,
						Start: timestmp})
				}
			}
		case '~':
			ftype, ok := indexer.GetFieldType(k[1:])
			if ok {
				if ftype == utils.IDX_TYPE_NUMBER {
					vsplit := strings.Split(v, ",")
					if len(vsplit) != 2 {
						continue
					}
					start, err1 := strconv.ParseInt(vsplit[0], 0, 0)
					if err1 != nil {
						continue
					}
					end, err2 := strconv.ParseInt(vsplit[1], 0, 0)
					if err2 != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_RANGE,
						Start: start,
						End:   end})
				} else if ftype == utils.IDX_TYPE_DATE {
					vsplit := strings.Split(v, ",")
					if len(vsplit) != 2 {
						continue
					}
					starttimestmp, err1 := utils.IsDateTime(vsplit[0])
					if err1 != nil {
						continue
					}
					endtimestmp, err2 := utils.IsDateTime(vsplit[2])
					if err2 != nil {
						continue
					}
					searchfilters = append(searchfilters, utils.FSSearchFilted{FieldName: k[1:],
						Type:  utils.FILT_RANGE,
						Start: starttimestmp,
						End:   endtimestmp})
				}
			}
		}

	}

	return searchfilters

}

func (this *DefaultEngine) calcStartEnd(ps, pg string, doclen int64) (int64, int64, error) {

	nps, ok1 := strconv.ParseInt(ps, 0, 0)
	npg, ok2 := strconv.ParseInt(pg, 0, 0)
	if ok1 != nil || ok2 != nil {
		nps = 10
		npg = 1
	}

	if nps <= 0 {
		nps = 10
	}

	if npg <= 0 {
		npg = 1
	}

	lens := doclen

	start := nps * (npg - 1)
	end := nps * npg

	if start >= lens {
		return 0, 0, fmt.Errorf("out page")
	}

	if end >= lens {
		end = lens
	}

	return start, end, nil
}
