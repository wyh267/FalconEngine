/*****************************************************************************
 *  file name : defaultEngine.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据层之上的引擎层
 *
******************************************************************************/

package FalconEngine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
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
	eDefaultEngineLoadOk         string = `{"status":"OK"}`
	eDefaultEngineLoadFail       string = `{"status":"Fail"}`
)

type DefaultResult struct {
	CostTime   string              `json:"costTime"`
	PageSize   int64               `json:"pageSize"`
	PageNum    int64               `json:"pageNumber"`
	TotalCount int64               `json:"totalCount"`
	Status     string              `json:"status"`
	Result     []map[string]string `json:"dataDetail"`
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
	startTime := time.Now()
	indexname, hasindex := parms["index"]
	query, hasquery := parms["q"]
	ps, hasps := parms["ps"]
	pg, haspg := parms["pg"]

	if !hasindex || !hasquery || !haspg || !hasps {
		return "", errors.New(eProcessoParms)
	}

	terms := utils.GSegmenter.Segment(query, false)
	if len(terms) == 0 {
		return eDefaultEngineNotFound, nil
	}
	//this.Logger.Info("[INFO] SegmentTerms >>>  %v ",terms)

	searchquerys := make([]utils.FSSearchQuery, 0)
	for _, term := range terms {
		var queryst utils.FSSearchQuery
		queryst.FieldName = "title"
		queryst.Value = term
		searchquerys = append(searchquerys, queryst)
	}

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

	indexer := this.idxManager.GetIndex(indexname)

	//this.queryAnalyse(searchunit)
	docids, found := indexer.SearchDocIds(searchquerys, nil)

	if !found {
		return eDefaultEngineNotFound, nil
	}

	lens := int64(len(docids))

	start := nps * (npg - 1)
	end := nps * npg

	if start >= lens {
		return eDefaultEngineNotFound, nil
	}

	if end >= lens {
		end = lens
	}

	var defaultResult DefaultResult

	defaultResult.Result = make([]map[string]string, 0)
	for _, docid := range docids[start:end] {
		val, ok := indexer.GetDocument(docid.Docid)
		if ok {
			for _, term := range terms {
				val["title"]=strings.Replace(val["title"],term,"[["+term+"]]",-1)
			}
			defaultResult.Result = append(defaultResult.Result, val)
		}
	}
	utils.GiveDocIDsChan <- docids

	endTime := time.Now()
	defaultResult.CostTime = fmt.Sprintf("%v", endTime.Sub(startTime))
	defaultResult.PageNum = npg
	defaultResult.PageSize = nps
	defaultResult.Status = "Found"
	defaultResult.TotalCount = lens

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

func (this *DefaultEngine) queryAnalyse(query utils.FSSearchFrontend) utils.FSSearchUnit {

	var unit utils.FSSearchUnit

	return unit

}

func (this *DefaultEngine) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {
	indexname, hasindex := parms["index"]

	if !hasindex || method != "POST" {
		return "", errors.New(eProcessoParms)
	}

	document := make(map[string]string)
	err := json.Unmarshal(body, &document)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return "", errors.New(eProcessoJsonParse)
	}

	return this.idxManager.updateDocument(indexname, document)
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
		loadstruct.SyncCount = 1000
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
