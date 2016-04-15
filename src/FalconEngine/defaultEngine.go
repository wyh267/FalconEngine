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

type Dbfield struct {
	DbField   string `json:"field_db"`
	IdxField  string `json:"field_index"`
	FieldType uint64 `json:"field_type"`
	//FieldPriority uint64 `json:"field_priority"`
}

type DBLoadInfo struct {
	Sql            string    `json:"sql"`
	User           string    `json:"user"`
	Pass           string    `json:"password"`
	Host           string    `json:"host"`
	Port           string    `json:"port"`
	CharSet        string    `json:"charset"`
	Dbname         string    `json:"dbname"`
	IndexName      string    `json:"indexname"`
	TableName      string    `json:"tablename"`
	SyncCount      int       `json:"synccount"`
	IsMerge        bool      `json:"ismerge"`
	Mapping        []Dbfield `json:"mapping"`
	StartTime      string    `json:"starttime"`
	UpdateSql      string    `json:"updatesql"`
    UpdateField    string    `json:"updatefield"`
	UpdateInterval int       `json:"interval"`
	SyncInterval   int       `json:"syncinterval"`
	MergeInterval  int       `json:"mergeinterval"`
	FieldPriority  []string  `json:"field_priority"`
}

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
	idxManager        *IndexMgt
	mysql             *utils.MysqlDBAdaptor
	idxFieldProiority map[string][]string
	Logger            *utils.Log4FE `json:"-"`
}

func NewDefaultEngine(logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{Logger: logger, idxManager: newIndexMgt(logger), mysql: nil, idxFieldProiority: nil}
	return this
}

func (this *DefaultEngine) Search(method string, parms map[string]string, body []byte) (string, error) {

	//this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")
	var shows []string
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
	docids := make([]utils.DocIdNode, 0)
	//如果没有query，直接进行过滤操作
	if !hasquery {
		var fliterFound bool
		docids, fliterFound = indexer.SearchDocIds(nil, searchfilters)
		if !fliterFound {
			return eDefaultEngineNotFound, nil
		}
	} else {
		var searchFields []string
		if _, haspro := this.idxFieldProiority[indexname]; haspro {
			searchFields = this.idxFieldProiority[indexname]
		} else {
			idxfields := indexer.GetFields()
			for _, field := range idxfields {
				if fty, ok := indexer.GetFieldType(field); ok {
					if fty == utils.IDX_TYPE_STRING_SEG {
						searchFields = append(searchFields, field)
					}
				}
			}

		}
		//首先按照字段优先级进行字段内搜索
		terms := utils.GSegmenter.Segment(query, false)
		//this.Logger.Info("[INFO] terms :  %v",terms)
		innFieldsFlag := false
		for _, fieldname := range searchFields { //[]string{"title"/*, "content"*/} {
			mainsearchquerys := make([]utils.FSSearchQuery, 0)
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = fieldname
				queryst.Value = term
				mainsearchquerys = append(mainsearchquerys, queryst)
			}
			innFieldsdocids, _ := indexer.SearchDocIds(mainsearchquerys, searchfilters)
			//this.Logger.Info("[INFO] innFieldsdocids %v",mainsearchquerys)
			if !(hassort && sortfield == "false") && len(innFieldsdocids) > 0 {
				sort.Sort(utils.DocWeightSort(innFieldsdocids))
			}
			docids = append(docids, innFieldsdocids...)
			if len(docids) > 10 {
				innFieldsFlag = true
				break
			}
		}
		//结果集不够，进行跨字段搜索
		if !innFieldsFlag {
			terms := utils.GSegmenter.Segment(query, false)
			searchquerys := make([]utils.FSSearchCrossFieldsQuery, 0)
			for _, term := range terms {
				var queryst utils.FSSearchCrossFieldsQuery
				queryst.FieldNames = searchFields //[]string{"title"/*, "content"*/}//{"name", "content"}
				queryst.Value = term
				searchquerys = append(searchquerys, utils.FSSearchCrossFieldsQuery{FieldNames: []string{"title" /*, "content"*/} /*{"name", "content"}*/, Value: term})
			}
			//进行搜索过滤
			crossDocids, crossFound := indexer.SearchDocIdsCrossFields(searchquerys, searchfilters)
			if crossFound {
				if !(hassort && sortfield == "false") && len(searchquerys) > 0 {
					sort.Sort(utils.DocWeightSort(crossDocids))
				}
				docids = append(docids, crossDocids...)
			}
		}

	}

	lens := int64(len(docids))
	if lens == 0 {
		return eDefaultEngineNotFound, nil
	}

	//计算起始和终止位置
	start, end, pageerr := this.calcStartEnd(ps, pg, lens)
	if pageerr != nil {
		return eDefaultEngineNotFound, nil
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
	// Delete free utils.GiveDocIDsChan <- docids

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
	indexer := this.idxManager.GetIndex(indexname)
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
		if pk, haspk := parms["_pk"]; haspk {
			err := indexer.DeleteDocument(pk)
			if err != nil {
				return "", err
			}
			return eDefaultEngineLoadOk, nil
		}

		if docidstr, hasdocid := parms["_docid"]; hasdocid {
			docid, converr := strconv.ParseInt(docidstr, 0, 0)
			if converr != nil {
				return "", converr
			}
			err := indexer.DeleteDocumentByDocId(uint32(docid))
			if err != nil {
				return "", err
			}
			return eDefaultEngineLoadOk, nil
		}

	default:
		return "", errors.New(eProcessoParms)
	}

	return "", errors.New(eProcessoParms)
}

func (this *DefaultEngine) DeleteDocument(method string, parms map[string]string, body []byte) (string, error) {

	return eDefaultEngineDeleteOk, nil
}

func (this *DefaultEngine) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	indexname, hasindex := parms["index"]
	_, hasfromdb := parms["fromdb"]

	if !hasindex || method != "POST" {
		return eDefaultEngineLoadFail, errors.New(eProcessoParms)
	}

	if hasfromdb {
		var dbinfo DBLoadInfo
		dbinfo.CharSet = "utf8"
		dbinfo.SyncInterval = 10
		dbinfo.MergeInterval = 60
		dbinfo.SyncCount = 10000
		dbinfo.IsMerge = true
		err := json.Unmarshal(body, &dbinfo)
		if err != nil {
			this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
			return eDefaultEngineLoadFail, errors.New(eProcessoJsonParse)
		}
		indexer := this.idxManager.GetIndex(indexname)
        curr_time := time.Now().Add(-10*time.Minute).Format("2006-01-02 15:04:05")
		if indexer == nil {
			var fieldinfos []utils.SimpleFieldInfo
			db2idx := make(map[string]string)
			for _, finfo := range dbinfo.Mapping {
				fieldinfos = append(fieldinfos, utils.SimpleFieldInfo{FieldName: finfo.IdxField, FieldType: finfo.FieldType})
				db2idx[finfo.DbField] = finfo.IdxField
			}
			this.Logger.Info("[INFO] fieldinfos %v", fieldinfos)
			if err := this.idxManager.CreateIndex(indexname, fieldinfos); err != nil {
				return "", err
			}

			this.mysql, err = utils.NewMysqlDBAdaptor(dbinfo.User, dbinfo.Pass, dbinfo.Host, dbinfo.Port,
				dbinfo.Dbname, dbinfo.CharSet, this.Logger)
			if err != nil {
				return "", err
			}

			rows, err := this.mysql.QueryFormat(dbinfo.Sql)
			if err != nil {
				this.Logger.Error("[ERROR] DB err %v", err.Error())
				return "", err
			}
			defer rows.Close()

			//读取全量数据
			cols, _ := rows.Columns()
			rawResult := make([][]byte, len(cols))

			dest := make([]interface{}, len(cols))
			for i, _ := range rawResult {
				dest[i] = &rawResult[i]
			}
			count := 1
			for rows.Next() {
				if err := rows.Scan(dest...); err != nil {
					return "", err
				}
				document := make(map[string]string)

				for idx, raw := range rawResult {
					if raw == nil {
						continue
					} else {
						if _, ok := db2idx[cols[idx]]; ok {
							document[cols[idx]] = string(raw)
						}
					}
				}

				//this.Logger.Info("[INFO] document %v",document)
				if _, err := this.idxManager.updateDocument(indexname, document); err != nil {
					return "", err
				}

				if count%dbinfo.SyncCount == 0 {
					this.idxManager.sync(indexname)
				}
				count++
			}

			this.idxManager.sync(indexname)

			if dbinfo.IsMerge {
				this.idxManager.mergeIndex(indexname)
			}

			//启动增量数据 TODO
            go this.incUpdate(indexname,curr_time,dbinfo)
			return eDefaultEngineLoadOk, nil
		}

		return eDefaultEngineLoadOk, nil

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

func (this *DefaultEngine) incUpdate(indexname string,starttime string, dbinfo DBLoadInfo) {
    
    db2idx := make(map[string]string)
    var fieldinfos []utils.SimpleFieldInfo
    for _, finfo := range dbinfo.Mapping {
        fieldinfos = append(fieldinfos, utils.SimpleFieldInfo{FieldName: finfo.IdxField, FieldType: finfo.FieldType})
        db2idx[finfo.DbField] = finfo.IdxField
    }
    indexer := this.idxManager.GetIndex(indexname)
    if indexer == nil {
			return
    }
    synccount := 0
    curr_time := starttime
    updateSql := fmt.Sprintf(dbinfo.UpdateSql,curr_time)
    
	for {
        this.Logger.Info("[INFO] IncUpdate Running ... %v", updateSql)
        
		rows, err := this.mysql.QueryFormat(updateSql)
		if err != nil {
			this.Logger.Error("[ERROR] DB err %v", err.Error())
			return
		}
		defer rows.Close()

		//读取全量数据
		cols, _ := rows.Columns()
		rawResult := make([][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i, _ := range rawResult {
			dest[i] = &rawResult[i]
		}
        //curr_time = time.Now().Add(-10*time.Minute).Format("2006-01-02 15:04:05")
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				return
			}
			document := make(map[string]string)

			for idx, raw := range rawResult {
				if raw == nil {
					continue
				} else {
					if _, ok := db2idx[cols[idx]]; ok {
						document[cols[idx]] = string(raw)
					}
                    if cols[idx] == dbinfo.UpdateField {
                         curr_time = string(raw)
                    }
				}
			}

			//this.Logger.Info("[INFO] document %v",document)
			if _, err := this.idxManager.updateDocument(indexname, document); err != nil {
				return 
			}

		}

		synccount++
        time.Sleep(time.Second)
        if synccount == dbinfo.SyncInterval {
            synccount=0
            indexer.SyncMemorySegment()
            
            if dbinfo.IsMerge {
			    indexer.MergeSegments()
		    }
        }
        
        //curr_time = time.Now().Add(-10*time.Second).Format("2006-01-02 15:04:05") //new_values[incField]
        updateSql = fmt.Sprintf(dbinfo.UpdateSql,curr_time)

		

	}

}
